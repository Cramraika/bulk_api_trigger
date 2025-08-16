# === webhook_trigger.py (fixed / extended) ===
# Order matters: imports and logger first to keep Pylance happy.

import csv
import json
import requests
import time
import argparse
import sys
import os
import glob
import logging
import yaml
import sqlite3
import hashlib
import shutil
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore, Event, Thread
from tqdm import tqdm
from typing import List, Dict, Optional, Tuple, Set
import schedule
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import queue
import re
import traceback
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

# ---------- Logging ----------
def setup_logging():
    """Setup enhanced logging with rotation"""
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
    )
    os.makedirs('/app/data/logs', exist_ok=True)
    file_handler = RotatingFileHandler(
        '/app/data/logs/webhook_trigger.log',
        maxBytes=100*1024*1024,  # 100MB
        backupCount=5
    )
    max_mb = int(os.getenv('MAX_LOG_SIZE_MB', '100'))
    file_handler = RotatingFileHandler(
    '/app/data/logs/webhook_trigger.log',
    maxBytes=max_mb * 1024 * 1024,
    backupCount=5
    )
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    return logging.getLogger(__name__)

logger = setup_logging()

# Keep stdout line-buffered in Docker-ish envs
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ---------- Small utils ----------
def _to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

def _detect_encoding(csv_path: str) -> str:
    """Detect CSV file encoding"""
    for enc in ('utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252'):
        try:
            with open(csv_path, 'r', encoding=enc) as f:
                f.read(256)
            return enc
        except UnicodeDecodeError:
            continue
    return 'utf-8'

def _detect_delimiter(file_obj) -> str:
    """Detect CSV delimiter from file object"""
    pos = file_obj.tell()
    sample = file_obj.read(4096)
    file_obj.seek(pos)
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[',',';','\t','|'])
        return dialect.delimiter
    except Exception:
        # heuristic fallback
        if sample.count(';') > sample.count(',') and sample.count(';') > sample.count('\t'):
            return ';'
        if sample.count('\t') > sample.count(','):
            return '\t'
        return ','

def _normalize_headers(raw_headers):
    """Normalize CSV headers by stripping BOM, trimming, lowercase"""
    return [ (h or '').lstrip('\ufeff').strip().lower() for h in (raw_headers or []) ]


# =========================
# Database
# =========================
class DatabaseManager:
    def __init__(self, db_path='/app/data/webhook_results.db'):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self._connection_lock = Lock()
        self.init_database()

    @contextmanager
    def get_connection(self):
        """Thread-safe database connection context manager"""
        with self._connection_lock:
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                yield conn
            finally:
                conn.close()

    def init_database(self):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhook_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    url TEXT NOT NULL,
                    method TEXT,
                    status TEXT NOT NULL,
                    status_code INTEGER,
                    response_time REAL,
                    timestamp TEXT NOT NULL,
                    attempt INTEGER,
                    error_message TEXT,
                    response_preview TEXT,
                    request_size INTEGER,
                    response_size INTEGER,
                    headers_sent TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS job_history (
                    job_id TEXT PRIMARY KEY,
                    job_name TEXT,
                    csv_file TEXT,
                    total_requests INTEGER,
                    successful_requests INTEGER,
                    failed_requests INTEGER,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    duration_seconds REAL,
                    config TEXT,
                    status TEXT DEFAULT 'running',
                    triggered_by TEXT DEFAULT 'manual',
                    average_response_time REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT NOT NULL,
                    cron_expression TEXT,
                    csv_file TEXT,
                    config TEXT,
                    last_run TEXT,
                    next_run TEXT,
                    enabled BOOLEAN DEFAULT 1,
                    created_at TEXT NOT NULL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_tracking (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT UNIQUE NOT NULL,
                    file_hash TEXT NOT NULL,
                    last_processed DATETIME,
                    status TEXT DEFAULT 'pending',
                    job_id TEXT,
                    file_size INTEGER,
                    rows_count INTEGER,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS system_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    metric_name TEXT NOT NULL,
                    metric_value REAL NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_job_id ON webhook_results(job_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_timestamp ON webhook_results(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_status ON job_history(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_status ON file_tracking(status)')
            conn.commit()

    def save_webhook_result(self, job_id: str, result: Dict):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO webhook_results 
                (job_id, url, method, status, status_code, response_time, timestamp, attempt, 
                 error_message, response_preview, request_size, response_size, headers_sent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job_id,
                result.get('url'),
                result.get('method'),
                result.get('status'),
                result.get('status_code'),
                result.get('response_time'),
                result.get('timestamp'),
                result.get('attempt'),
                result.get('error_message'),
                result.get('response_preview'),
                result.get('request_size', 0),
                result.get('response_size', 0),
                json.dumps(result.get('headers_sent', {}))
            ))
            conn.commit()

    def start_job(self, job_id: str, job_name: str, csv_file: str, total_requests: int, config: Dict, triggered_by: str = 'manual'):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO job_history 
                (job_id, job_name, csv_file, total_requests, successful_requests, failed_requests, 
                 start_time, config, status, triggered_by)
                VALUES (?, ?, ?, ?, 0, 0, ?, ?, 'running', ?)
            ''', (job_id, job_name, csv_file, total_requests, datetime.now().isoformat(), 
                  json.dumps(config), triggered_by))
            conn.commit()

    def finish_job(self, job_id: str, successful: int, failed: int):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT AVG(response_time) 
                FROM webhook_results 
                WHERE job_id = ? AND response_time IS NOT NULL
            ''', (job_id,))
            avg_response_time = cursor.fetchone()[0] or 0
            cursor.execute('SELECT start_time FROM job_history WHERE job_id = ?', (job_id,))
            result = cursor.fetchone()
            if result:
                start_time = datetime.fromisoformat(result[0])
                duration = (datetime.now() - start_time).total_seconds()
            else:
                duration = 0
            cursor.execute('''
                UPDATE job_history 
                SET successful_requests = ?, failed_requests = ?, end_time = ?, 
                    duration_seconds = ?, status = 'completed', average_response_time = ?
                WHERE job_id = ?
            ''', (successful, failed, datetime.now().isoformat(), duration, avg_response_time, job_id))
            conn.commit()

    def track_file(self, file_path: str, file_hash: str, file_size: int, rows_count: int = 0):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO file_tracking 
                (file_path, file_hash, file_size, rows_count, status)
                VALUES (?, ?, ?, ?, 'pending')
            ''', (file_path, file_hash, file_size, rows_count))
            conn.commit()

    def is_file_processed(self, file_path: str, file_hash: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT COUNT(*) FROM file_tracking 
                WHERE file_path = ? AND file_hash = ? AND status = 'completed'
            ''', (file_path, file_hash))
            return cursor.fetchone()[0] > 0

    def update_file_status(self, file_path: str, status: str, job_id: str = None):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE file_tracking 
                SET status = ?, job_id = ?, last_processed = CURRENT_TIMESTAMP
                WHERE file_path = ?
            ''', (status, job_id, file_path))
            conn.commit()

    def get_file_status(self, file_path: str) -> Optional[str]:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT status FROM file_tracking WHERE file_path = ?', (file_path,))
            row = cursor.fetchone()
            return row[0] if row else None

    def get_job_stats(self, job_id: str) -> Dict:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            job = None
            status_stats = {}
            cursor.execute('SELECT * FROM job_history WHERE job_id = ?', (job_id,))
            job = cursor.fetchone()
            cursor.execute('''
                SELECT status, COUNT(*), AVG(response_time), MIN(response_time), MAX(response_time)
                FROM webhook_results 
                WHERE job_id = ? 
                GROUP BY status
            ''', (job_id,))
            for row in cursor.fetchall():
                status_stats[row[0]] = {
                    'count': row[1] or 0,
                    'avg_response_time': _to_float(row[2], 0.0),
                    'min_response_time': _to_float(row[3], 0.0),
                    'max_response_time': _to_float(row[4], 0.0)
                }
            if not job:
                return {
                    'job_id': job_id,
                    'job_name': '',
                    'csv_file': '',
                    'total_requests': 0,
                    'successful_requests': 0,
                    'failed_requests': 0,
                    'start_time': '',
                    'end_time': '',
                    'duration_seconds': 0.0,
                    'triggered_by': 'manual',
                    'average_response_time': 0.0,
                    'status_breakdown': status_stats,
                }
            duration = _to_float(job[8], 0.0)
            avg_response_time = _to_float(job[12], 0.0) if len(job) > 12 else 0.0
            return {
                'job_id': job[0],
                'job_name': job[1],
                'csv_file': job[2],
                'total_requests': int(_to_float(job[3], 0.0)),
                'successful_requests': int(_to_float(job[4], 0.0)),
                'failed_requests': int(_to_float(job[5], 0.0)),
                'start_time': job[6],
                'end_time': job[7],
                'duration_seconds': duration,
                'triggered_by': job[11] if len(job) > 11 else 'manual',
                'average_response_time': avg_response_time,
                'status_breakdown': status_stats
            }

    def save_metric(self, metric_name: str, metric_value: float):
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO system_metrics (metric_name, metric_value) VALUES (?, ?)',
                           (metric_name, metric_value))
            conn.commit()


# =========================
# Notifications (Slack & Email)
# =========================
class NotificationManager:
    def __init__(self, config: Dict):
        self.config = config
        self.email_config = config.get('email', {})
        self.slack_config = config.get('slack', {})

    def send_email_notification(self, subject: str, body: str, to_emails: List[str]):
        if not self.email_config.get('enabled', False):
            return
        max_retries = 3
        for attempt in range(max_retries):
            try:
                msg = MIMEMultipart()
                msg['From'] = self.email_config.get('from_email', '')
                msg['To'] = ', '.join(to_emails)
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'html'))
                smtp_port = int(self.email_config.get('smtp_port', 587))
                smtp_server = self.email_config.get('smtp_server', 'smtp.gmail.com')
                use_ssl = (smtp_port == 465)
                if use_ssl:
                    server = smtplib.SMTP_SSL(smtp_server, smtp_port)
                else:
                    server = smtplib.SMTP(smtp_server, smtp_port)
                    server.ehlo()
                    if smtp_port == 587:
                        server.starttls()
                        server.ehlo()
                server.login(self.email_config.get('username', ''), self.email_config.get('password', ''))
                server.send_message(msg)
                server.quit()
                logger.info(f"📧 Email notification sent to {to_emails}")
                break
            except Exception as e:
                logger.error(f"Failed to send email notification (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

    def send_slack_notification(self, message: str, urgency: str = 'normal'):
        if not self.slack_config.get('enabled', False):
            return
        try:
            emoji_map = {'low': '🔵','normal': '🟢','high': '🟡','critical': '🔴'}
            payload = {
                'text': f"{emoji_map.get(urgency, '🟢')} {message}",
                'username': 'Bulk API Trigger Bot',
                'icon_emoji': ':robot_face:',
                'attachments': [{
                    'color': 'good' if urgency in ['low', 'normal'] else ('warning' if urgency == 'high' else 'danger'),
                    'ts': int(time.time())
                }]
            }
            resp = requests.post(self.slack_config.get('webhook_url', ''), json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("📱 Slack notification sent successfully")
            else:
                logger.error(f"Failed to send Slack notification: {resp.status_code}")
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")

    def send_job_completion_notification(self, job_stats: Dict):
        total = job_stats.get('total_requests') or 0
        succ  = job_stats.get('successful_requests') or 0
        success_rate = (succ / total * 100.0) if total else 0.0
        urgency = 'normal' if success_rate >= 95 else ('high' if success_rate >= 80 else 'critical')
        if self.slack_config.get('enabled') and self.slack_config.get('notify_on_completion', True):
            slack_message = (
                f"*🚀 Bulk API Job Completed*\n\n"
                f"*Job:* {job_stats.get('job_name','')}\n"
                f"*File:* {os.path.basename(job_stats.get('csv_file','Multiple files'))}\n"
                f"*Triggered:* {job_stats.get('triggered_by','manual')}\n"
                f"*Success Rate:* {success_rate:.2f}%\n"
                f"*Requests:* {succ}/{total} successful\n"
                f"*Duration:* {_to_float(job_stats.get('duration_seconds')):.2f}s\n"
                f"*Avg Response:* {_to_float(job_stats.get('average_response_time')):.3f}s"
            )
            self.send_slack_notification(slack_message, urgency)
        if self.email_config.get('enabled') and self.email_config.get('notify_on_completion', True):
            subject = f"Bulk API Job {'Completed' if success_rate > 0 else 'Failed'}: {job_stats.get('job_name','')}"
            html_body = f"""
            <h2>🚀 Job Completion Report</h2>
            <table border="1" cellpadding="10" style="border-collapse: collapse;">
                <tr><td><strong>Job Name</strong></td><td>{job_stats.get('job_name','')}</td></tr>
                <tr><td><strong>Job ID</strong></td><td>{job_stats.get('job_id','')}</td></tr>
                <tr><td><strong>CSV File</strong></td><td>{job_stats.get('csv_file','Multiple files')}</td></tr>
                <tr><td><strong>Triggered By</strong></td><td>{job_stats.get('triggered_by','manual')}</td></tr>
                <tr><td><strong>Total Requests</strong></td><td>{total}</td></tr>
                <tr><td><strong>✅ Successful</strong></td><td>{succ}</td></tr>
                <tr><td><strong>❌ Failed</strong></td><td>{job_stats.get('failed_requests') or 0}</td></tr>
                <tr><td><strong>📊 Success Rate</strong></td><td>{success_rate:.2f}%</td></tr>
                <tr><td><strong>⏱️ Duration</strong></td><td>{_to_float(job_stats.get('duration_seconds')):.2f} seconds</td></tr>
                <tr><td><strong>📈 Avg Response Time</strong></td><td>{_to_float(job_stats.get('average_response_time')):.3f}s</td></tr>
                <tr><td><strong>🕐 Start Time</strong></td><td>{job_stats.get('start_time','')}</td></tr>
                <tr><td><strong>🏁 End Time</strong></td><td>{job_stats.get('end_time','')}</td></tr>
            </table>
            """
            self.send_email_notification(subject, html_body, self.email_config.get('recipients', []))

    def send_file_detected_notification(self, file_info: Dict):
        if not self.slack_config.get('enabled') or not self.slack_config.get('notify_on_file_detected', True):
            return
        message = (
            f"📁 *New CSV File Detected*\n\n"
            f"*File:* {os.path.basename(file_info['csv_file'])}\n"
            f"*Size:* {file_info['file_size']} bytes\n"
            f"*Rows:* {file_info['row_count']} webhooks\n"
            f"*Event:* {file_info['event_type']}\n"
            f"*Time:* {file_info['detected_at']}\n\n"
            f"Processing will start shortly..."
        )
        self.send_slack_notification(message, 'low')


# =========================
# Rate Limiter & Results Tracker
# =========================
class RateLimiter:
    """
    Simple rate controller that tracks an adjustable 'rate' (requests/sec).
    Not a hard token bucket — executor's max_workers handles concurrency.
    """
    def __init__(self, starting_rate: float = 3.0, max_workers: int = 3):
        self._rate_lock = Lock()
        self._rate = max(0.1, float(starting_rate))  # requests/sec
        self._max_workers = int(max_workers)
        self._last_tick = time.time()
        self._completed = 0
        self._started_at = time.time()

    def adjust_rate(self, new_rate: float):
        with self._rate_lock:
            self._rate = max(0.1, float(new_rate))

    def get_rate(self) -> float:
        with self._rate_lock:
            return self._rate

    def get_throughput(self) -> float:
        elapsed = max(0.001, time.time() - self._started_at)
        return self._completed / elapsed

    def tick(self):
        # Called per request completion to update throughput basis
        self._completed += 1

    def sleep_if_needed(self):
        """Crude pacing to avoid too-aggressive bursts."""
        target_interval = 1.0 / max(0.1, self.get_rate())  # seconds between starts
        now = time.time()
        since = now - self._last_tick
        if since < target_interval:
            time.sleep(target_interval - since)
        self._last_tick = time.time()


class EnhancedResultsTracker:
    """Accumulates metrics and writes each result to DB via DatabaseManager."""
    def __init__(self, job_id: str, db: DatabaseManager):
        self.job_id = job_id
        self.db = db
        self._metrics = {
            'total_request_size': 0,
            'total_response_size': 0,
            'throughput': 0.0
        }
        self._lock = Lock()
        self._started_at = time.time()
        self._completed = 0

    def record(self, result: Dict):
        # persist row
        try:
            self.db.save_webhook_result(self.job_id, result)
        except Exception as e:
            logger.error(f"DB save failed: {e}")
        # metrics
        with self._lock:
            self._metrics['total_request_size'] += int(result.get('request_size', 0) or 0)
            self._metrics['total_response_size'] += int(result.get('response_size', 0) or 0)
            self._completed += 1
            elapsed = max(0.001, time.time() - self._started_at)
            self._metrics['throughput'] = self._completed / elapsed

    def save_results(self):
        # no-op: results saved per record; keep method for API parity
        pass

    def get_metrics(self) -> Dict:
        return dict(self._metrics)


def _slack_urgency_for_progress(successful_count: int, processed_count: int) -> str:
    if processed_count <= 0:
        return 'normal'
    fail_rate = 1.0 - (successful_count / processed_count)
    if fail_rate >= 0.4:
        return 'critical'
    if fail_rate >= 0.2:
        return 'high'
    return 'normal'


# =========================
# CSV reading & request execution
# =========================
def read_csv_with_validation(csv_path: str, chunk_size: int = 1000, skip_rows: int = 0) -> Tuple[List[Dict], Dict]:
    """Read a single CSV, validate 'webhook_url', build webhooks list + stats."""
    webhooks: List[Dict] = []
    stats = {
        'file': csv_path,
        'file_size': os.path.getsize(csv_path) if os.path.exists(csv_path) else 0,
        'valid_rows': 0,
        'invalid_rows': 0,
        'skipped_rows': max(0, int(skip_rows)),
    }
    enc = _detect_encoding(csv_path)
    with open(csv_path, 'r', encoding=enc, newline='') as f:
        delim = _detect_delimiter(f)
        reader = csv.DictReader(f, delimiter=delim)
        raw_headers = reader.fieldnames or []
        headers = _normalize_headers(raw_headers)
        req = [h.strip().lower() for h in os.getenv('CSV_REQUIRED_COLUMNS', 'webhook_url').split(',') if h.strip()]
        missing = [h for h in req if h not in headers]
        if missing:
            raise ValueError(f"{csv_path}: missing required column(s): {missing} (headers={headers})")
        # map original->normalized
        name_map = {orig: norm for orig, norm in zip(raw_headers, headers)}
        row_index = -1
        for row in reader:
            row_index += 1
            if row_index < skip_rows:
                continue
            nrow = {name_map.get(k, (k or '')).lower(): (v or '').strip() for k, v in row.items()}
            url = nrow.get('webhook_url', '')
            if not url:
                stats['invalid_rows'] += 1
                continue
            method = (nrow.get('method') or 'GET').upper()
            headers_sent = {}
            if nrow.get('header'):
                try:
                    headers_sent = json.loads(nrow['header'])
                except Exception:
                    # allow semi-colon separated "Key:Value" if json not provided
                    try:
                        for part in nrow['header'].split(';'):
                            if ':' in part:
                                k, v = part.split(':', 1)
                                headers_sent[k.strip()] = v.strip()
                    except Exception:
                        pass
            payload = None
            if nrow.get('payload'):
                ptxt = nrow['payload']
                try:
                    payload = json.loads(ptxt)
                except Exception:
                    payload = ptxt  # send as-is (text)
            webhooks.append({
                'url': url,
                'method': method,
                'payload': payload,
                'header': headers_sent,
                'name': nrow.get('name') or url,
                'group': nrow.get('group') or ''
            })
            stats['valid_rows'] += 1

    return webhooks, stats


def read_multiple_csv_files(file_patterns: List[str] = None, chunk_size: int = 1000, skip_rows: int = 0):
    """Enhanced multi-file CSV reader with pattern matching"""
    if not file_patterns:
        env_patterns = os.getenv('CSV_FILE_PATTERNS', '')
        if env_patterns.strip():
            file_patterns = [p.strip() for p in env_patterns.split(',') if p.strip()]
        else:
            file_patterns = [
                '/app/data/csv/*.csv',
                '/app/data/*.csv'
            ]

    found_files = []
    for pattern in file_patterns:
        found_files.extend(glob.glob(pattern))

    found_files = list(set(found_files))
    found_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

    if not found_files:
        logger.error(f"No CSV files found matching patterns: {file_patterns}")
        return [], {}

    logger.info(f"📁 Found {len(found_files)} CSV file(s): {[os.path.basename(f) for f in found_files]}")

    all_webhooks = []
    combined_stats = {
        'files_processed': 0,
        'total_valid_rows': 0,
        'total_invalid_rows': 0,
        'total_file_size': 0,
        'files_stats': []
    }

    for csv_file in found_files:
        try:
            logger.info(f"📖 Processing {os.path.basename(csv_file)}...")
            # Only skip rows for first file
            sr = skip_rows if combined_stats['files_processed'] == 0 else 0
            webhooks, stats = read_csv_with_validation(csv_file, chunk_size, sr)
            all_webhooks.extend(webhooks)
            combined_stats['files_processed'] += 1
            combined_stats['total_valid_rows'] += stats['valid_rows']
            combined_stats['total_invalid_rows'] += stats['invalid_rows']
            combined_stats['total_file_size'] += stats['file_size']
            combined_stats['files_stats'].append({'file': csv_file, 'stats': stats})
        except Exception as e:
            logger.error(f"❌ Error processing {csv_file}: {e}")
            continue

    return all_webhooks, combined_stats

def _parse_retry_after_seconds(header_value: str, default: float) -> float:
    """
    Parses Retry-After header per RFC 7231.
    - If integer, treat as seconds.
    - If HTTP-date, compute delta from now.
    """
    try:
        # integer seconds case
        secs = int(header_value.strip())
        return float(secs)
    except Exception:
        pass

    # Try HTTP-date format
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(header_value)
        if dt:
            delay = (dt - datetime.utcnow().replace(tzinfo=dt.tzinfo)).total_seconds()
            return max(default, delay)
    except Exception:
        pass

    return default

def make_request(
    url: str,
    method: str,
    payload,
    headers: Dict[str, str],
    max_retries: int,
    rate_limiter: RateLimiter,
    pbar: tqdm,
    results_tracker: EnhancedResultsTracker,
    timeout: int,
    global_request_sema: Optional[Semaphore] = None,
    retry_delay: float = 1.0,
) -> int:
    """
    Executes one HTTP request with basic retry/backoff.
    Returns 0 on success, 1 on error (for moving average error window).
    """
    attempt = 0
    # Base backoff (from env), minimum 0.1s to avoid hot loops if user sets 0
    base_backoff = max(0.1, float(retry_delay or 0.0))
    backoff = base_backoff
    MAX_BACKOFF = 30.0  # keep your existing cap
    error_code = 1  # assume failure until success

    while attempt <= max_retries:
        attempt += 1
        if global_request_sema:
            global_request_sema.acquire()
        try:
            rate_limiter.sleep_if_needed()
            started = time.time()
            method_u = (method or 'GET').upper()
            req_size = 0
            resp_size = 0

            if method_u in ('POST', 'PUT', 'PATCH'):
                if isinstance(payload, (dict, list)):
                    data = json.dumps(payload)
                    req_size = len(data.encode('utf-8'))
                    resp = requests.request(
                        method_u, url, data=data,
                        headers={'Content-Type': 'application/json', **(headers or {})},
                        timeout=timeout
                    )
                elif payload is not None:
                    req_size = len(str(payload).encode('utf-8'))
                    resp = requests.request(
                        method_u, url, data=str(payload),
                        headers=headers or {}, timeout=timeout
                    )
                else:
                    resp = requests.request(method_u, url, headers=headers or {}, timeout=timeout)
            else:
                if isinstance(payload, dict) and method_u == 'GET':
                    resp = requests.get(url, params=payload, headers=headers or {}, timeout=timeout)
                else:
                    resp = requests.request(method_u, url, headers=headers or {}, timeout=timeout)

            elapsed = time.time() - started
            try:
                resp_size = len(resp.content or b'')
            except Exception:
                resp_size = 0

            ok = 200 <= resp.status_code < 300
            result_row = {
                'url': url,
                'method': method_u,
                'status': 'success' if ok else 'error',
                'status_code': resp.status_code,
                'response_time': elapsed,
                'timestamp': datetime.now().isoformat(),
                'attempt': attempt,
                'error_message': '' if ok else f'HTTP {resp.status_code}',
                'response_preview': (resp.text[:1000] if resp.text else '') if not ok else '',
                'request_size': req_size,
                'response_size': resp_size,
                'headers_sent': headers or {},
            }
            results_tracker.record(result_row)
            rate_limiter.tick()
            pbar.update(1)

            if ok:
                error_code = 0
                return 0

            # --- Retry policy ---
            # 429 or >=500 are usually retryable; 4xx (except 429) are not.
            if resp.status_code == 429 or resp.status_code >= 500:
                # Respect Retry-After if provided (seconds or HTTP-date)
                retry_after_hdr = (resp.headers or {}).get('Retry-After')
                if retry_after_hdr:
                    sleep_s = _parse_retry_after_seconds(retry_after_hdr, default=backoff)
                else:
                    sleep_s = backoff
                time.sleep(min(MAX_BACKOFF, max(0.1, sleep_s)))
                backoff = min(MAX_BACKOFF, backoff * 2)
                continue
            else:
                # Non-retryable client error
                return 1

        except Exception as e:
            elapsed = 0.0
            result_row = {
                'url': url,
                'method': method or 'GET',
                'status': 'exception',
                'status_code': None,
                'response_time': elapsed,
                'timestamp': datetime.now().isoformat(),
                'attempt': attempt,
                'error_message': str(e),
                'response_preview': '',
                'request_size': 0,
                'response_size': 0,
                'headers_sent': headers or {},
            }
            results_tracker.record(result_row)
            pbar.update(1)
            time.sleep(min(MAX_BACKOFF, backoff))
            backoff = min(MAX_BACKOFF, backoff * 2)
        finally:
            if global_request_sema:
                try:
                    global_request_sema.release()
                except Exception:
                    pass

    return error_code


# =========================
# File move helper
# =========================
def move_file_reasoned(file_path: str, target_dir: str, reason_prefix: str = "") -> bool:
    """Move file to target directory with timestamp and reason prefix"""
    try:
        if not target_dir:
            return False
        os.makedirs(target_dir, exist_ok=True)
        filename = os.path.basename(file_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{ts}_{reason_prefix}{filename}" if reason_prefix else f"{ts}_{filename}"
        shutil.move(file_path, os.path.join(target_dir, new_name))
        logger.info(f"📦 Moved file: {filename} -> {new_name} [{reason_prefix.rstrip('_')}]")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to move {file_path} to {target_dir}: {e}")
        return False


# =========================
# Watchdog handler
# =========================
class FileWatchdog(FileSystemEventHandler):
    """Enhanced file system watchdog for CSV monitoring"""
    def __init__(self, job_queue, db_manager, config,
                 inflight_files=None, inflight_lock=None):
        super().__init__()
        self.job_queue = job_queue
        self.db_manager = db_manager
        self.config = config
        self.inflight_files = inflight_files if inflight_files is not None else set()
        self.inflight_lock = inflight_lock if inflight_lock is not None else Lock()
        self.processed_files: Set[str] = set()
        self.file_lock = Lock()
        self._load_processed_files()

    def _load_processed_files(self):
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT file_path FROM file_tracking WHERE status = "completed"')
                self.processed_files = {row[0] for row in cursor.fetchall()}
                logger.info(f"Loaded {len(self.processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {e}")

    def _calculate_file_hash(self, file_path: str) -> str:
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {e}")
            return ""

    def _is_valid_csv_file(self, file_path: str) -> bool:
        try:
            if not file_path.lower().endswith('.csv'):
                return False
            initial_size = os.path.getsize(file_path)
            time.sleep(1.0)
            final_size = os.path.getsize(file_path)
            if initial_size != final_size:
                logger.info(f"File {file_path} is still being written, skipping for now")
                return False
            enc = _detect_encoding(file_path)
            with open(file_path, 'r', encoding=enc, newline='') as f:
                delim = _detect_delimiter(f)
                reader = csv.DictReader(f, delimiter=delim)
                raw_headers = reader.fieldnames or []
                headers = _normalize_headers(raw_headers)
                if 'webhook_url' not in headers:
                    logger.warning(
                        f"File {file_path} missing required 'webhook_url' column "
                        f"(detected headers={headers}, enc={enc}, delim='{delim}')"
                    )
                    try:
                        if self.config.get('csv', {}).get('archive_on_validation_failure', True):
                            move_file_reasoned(
                                file_path,
                                self.config.get('csv', {}).get('rejected_path', ''),
                                reason_prefix="invalid_missing-webhook_url_"
                            )
                    except Exception as _e:
                        logger.debug(f"Archive on validation failure skipped: {_e}")
                    return False
                has_data = False
                name_map = {orig: norm for orig, norm in zip(raw_headers, headers)}
                for row in reader:
                    nrow = {name_map.get(k, (k or '')).lower(): (v or '').strip() for k, v in row.items()}
                    if nrow.get('webhook_url'):
                        has_data = True
                        break
                if not has_data:
                    logger.warning(f"File {file_path} has no non-empty webhook_url values")
                    try:
                        if self.config.get('csv', {}).get('archive_on_validation_failure', True):
                            move_file_reasoned(
                                file_path,
                                self.config.get('csv', {}).get('rejected_path', ''),
                                reason_prefix="rejected_empty-webhook-url_"
                            )
                    except Exception as _e:
                        logger.debug(f"Archive on validation failure skipped: {_e}")
                    return False
            return True
        except Exception as e:
            logger.error(f"Error validating CSV file {file_path}: {e}")
            return False

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_file_event(event.src_path, 'created')

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle_file_event(event.src_path, 'modified')

    def on_moved(self, event):
        if event.is_directory:
            return
        self._handle_file_event(event.dest_path, 'moved')

    def _handle_file_event(self, file_path: str, event_type: str):
        try:
            with self.file_lock:
                file_path = os.path.abspath(file_path)
                if not self._is_valid_csv_file(file_path):
                    return
                file_hash = self._calculate_file_hash(file_path)
                if not file_hash:
                    return
                if self.db_manager.is_file_processed(file_path, file_hash):
                    logger.info(f"⏭️  Duplicate content detected for {os.path.basename(file_path)} (hash match).")
                    dup_dir = self.config.get('csv', {}).get('duplicates_path', '')
                    moved = move_file_reasoned(file_path, dup_dir, reason_prefix="dup_")
                    if moved:
                        try:
                            self.db_manager.update_file_status(file_path, 'skipped_duplicate')
                        except Exception:
                            pass
                    return
                enc = _detect_encoding(file_path)
                with open(file_path, 'r', encoding=enc, newline='') as f:
                    delim = _detect_delimiter(f)
                    reader = csv.DictReader(f, delimiter=delim)
                    raw_headers = reader.fieldnames or []
                    headers = _normalize_headers(raw_headers)
                if 'webhook_url' not in headers:
                    logger.warning(f"File {file_path} missing required 'webhook_url' column")
                    rej_dir = self.config.get('csv', {}).get('rejected_path', '')
                    move_file_reasoned(file_path, rej_dir, reason_prefix="invalid_")
                    return
                status = self.db_manager.get_file_status(file_path)
                if status in ('queued', 'processing'):
                    logger.debug(f"Already {status} (DB): {file_path} — event {event_type} ignored")
                    return
                with self.inflight_lock:
                    if file_path in self.inflight_files:
                        logger.debug(f"Already pending (in-memory): {file_path} — event {event_type} ignored")
                        return
                file_stats = os.stat(file_path)
                file_size = file_stats.st_size
                try:
                    with open(file_path, 'r', encoding=enc) as f:
                        row_count = max(sum(1 for _ in csv.reader(f, delimiter=delim)) - 1, 0)
                except Exception:
                    row_count = 0
                self.db_manager.track_file(file_path, file_hash, file_size, row_count)
                self.db_manager.update_file_status(file_path, 'queued')
                job_info = {
                    'csv_file': file_path,
                    'file_hash': file_hash,
                    'file_size': file_size,
                    'row_count': row_count,
                    'event_type': event_type,
                    'detected_at': datetime.now().isoformat()
                }
                with self.inflight_lock:
                    self.inflight_files.add(file_path)
                self.job_queue.put(job_info)
                logger.info(f"🆕 New CSV file detected: {os.path.basename(file_path)} "
                            f"({row_count} rows, {file_size} bytes) via {event_type}")
        except Exception as e:
            logger.error(f"Error handling file event for {file_path}: {e}")
            logger.error(traceback.format_exc())


# =========================
# Archiving helpers
# =========================
def generate_job_id(prefix: str = "job") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hash_part = hashlib.md5(f"{timestamp}{time.time()}".encode()).hexdigest()[:8]
    return f"{prefix}_{timestamp}_{hash_part}"

def archive_processed_file(file_path: str, config: Dict) -> bool:
    try:
        csv_config = config.get('csv', {})
        if not csv_config.get('archive_processed', False):
            return False
        archive_path = csv_config.get('archive_path', '/app/data/csv/processed')
        os.makedirs(archive_path, exist_ok=True)
        filename = os.path.basename(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_filename = f"{timestamp}_{filename}"
        archived_path = os.path.join(archive_path, archived_filename)
        shutil.move(file_path, archived_path)
        logger.info(f"📦 Archived processed file: {filename} -> {archived_filename}")
        return True
    except Exception as e:
        logger.error(f"❌ Error archiving file {file_path}: {e}")
        return False


# =========================
# BulkAPITrigger
# =========================
class BulkAPITrigger:
    def __init__(self, config: Dict):
        self.config = config
        self.inflight_lock = Lock()
        self.inflight_files: Set[str] = set()
        gmax = int(os.getenv('GLOBAL_MAX_REQUESTS', '0') or '0')
        self.global_request_sema = Semaphore(gmax) if gmax > 0 else None
        self.db_manager = DatabaseManager(config.get('database', {}).get('path', '/app/data/webhook_results.db'))
        self.notification_manager = NotificationManager(config.get('notifications', {}))
        self.job_queue = queue.Queue(maxsize=config.get('watchdog', {}).get('max_queue_size', 100))
        self.watchdog_enabled = config.get('watchdog', {}).get('enabled', False)
        self.observer = None
        self.processing_threads: List[Thread] = []
        self.shutdown_event = Event()

    def trigger_webhooks(self, 
                        csv_files: List[str] = None, 
                        job_name: str = None,
                        skip_rows: int = 0,
                        triggered_by: str = 'manual') -> str:
        """Main method to trigger bulk webhooks with enhanced features"""
        job_id = generate_job_id()
        if not job_name:
            job_name = f"Bulk API Job {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        logger.info(f"🚀 Starting job: {job_name} (ID: {job_id})")
        try:
            # Load webhooks
            if csv_files and csv_files[0] != "AUTO":
                all_webhooks, stats = read_csv_with_validation(csv_files[0], skip_rows=skip_rows)
                csv_file_path = csv_files[0]
            else:
                all_webhooks, stats = read_multiple_csv_files(skip_rows=skip_rows)
                csv_file_path = f"Multiple files ({stats.get('files_processed', 0)} files)"
            if not all_webhooks:
                logger.warning("⚠️  No valid webhook URLs found.")
                return job_id
            total_requests = len(all_webhooks)
            logger.info(f"📊 Loaded {total_requests} webhook requests")
            # Initialize job in database
            self.db_manager.start_job(job_id, job_name, csv_file_path, total_requests, self.config, triggered_by)
            # Setup rate limiting and tracking
            rate_config = self.config.get('rate_limiting', {})
            rate_limiter = RateLimiter(
                rate_config.get('starting_rate_limit', 3.0),
                rate_config.get('max_workers', 3)
            )
            results_tracker = EnhancedResultsTracker(job_id, self.db_manager)
            error_window: List[int] = []
            successful_count = 0
            failed_count = 0
            processed_count = 0
            logger.info(f"⚡ Processing {total_requests} requests with {rate_config.get('max_workers', 3)} workers")
            with ThreadPoolExecutor(max_workers=rate_config.get('max_workers', 3)) as executor:
                with tqdm(
                        total=total_requests,
                        desc="🌐 API Requests",
                        dynamic_ncols=False,
                        ascii=True,
                        file=sys.stdout,
                        mininterval=1.0,
                        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                        disable=not (sys.stdout.isatty() or os.getenv("FORCE_TQDM","0") == "1")
                    ) as pbar:

                    futures = {
                        executor.submit(
                            make_request,
                            webhook['url'],
                            webhook['method'],
                            webhook['payload'],
                            webhook['header'],
                            self.config.get('retry', {}).get('max_retries', 3),
                            rate_limiter,
                            pbar,
                            results_tracker,
                            self.config.get('retry', {}).get('timeout', 30),
                            self.global_request_sema,
                            self.config.get('retry', {}).get('retry_delay', 1.0)
                        ): webhook
                        for webhook in all_webhooks
                    }

                    for future in as_completed(futures):
                        try:
                            error = future.result()
                            error_window.append(error)
                            if len(error_window) > rate_config.get('window_size', 20):
                                error_window.pop(0)
                            processed_count += 1
                            if error == 0:
                                successful_count += 1
                                try:
                                    if (
                                        self.notification_manager.slack_config.get('enabled')
                                        and os.getenv("SLACK_NOTIFY_PROGRESS", "false").lower() == "true"
                                        and processed_count % int(os.getenv("SLACK_PROGRESS_EVERY_N", "25")) == 0
                                    ):
                                        urgency = os.getenv("SLACK_PROGRESS_URGENCY", "auto")
                                        if urgency == "auto":
                                            urgency = _slack_urgency_for_progress(successful_count, processed_count)
                                        self.notification_manager.send_slack_notification(
                                            (
                                                f"⏳ *{job_name}* in progress\n"
                                                f"{processed_count}/{total_requests} processed\n"
                                                f"✅ {successful_count} success | ❌ {failed_count} failed\n"
                                                f"~{(processed_count/total_requests*100):.1f}% done"
                                            ),
                                            urgency
                                        )
                                except Exception:
                                    pass
                            else:
                                failed_count += 1

                            PROGRESS_EVERY_N = int(os.getenv("PROGRESS_EVERY_N", "50"))
                            if processed_count % PROGRESS_EVERY_N == 0:
                                success_rate = (successful_count / processed_count) * 100
                                throughput = rate_limiter.get_throughput()
                                logger.info(
                                    f"📈 Progress: {processed_count}/{total_requests} "
                                    f"({success_rate:.1f}% success, {throughput:.2f} req/s)"
                                )

                        except Exception as e:
                            webhook = futures[future]
                            logger.error(f"💥 Failed processing {webhook['url']}: {e}")
                            failed_count += 1
                            processed_count += 1

                        # Dynamic rate adjustment (fix: reduce on errors, increase on low errors)
                        if processed_count % rate_config.get('window_size', 20) == 0 and error_window:
                            error_rate = sum(error_window) / len(error_window)
                            current_rate = rate_limiter.get_rate()
                            if error_rate > rate_config.get('error_threshold', 0.3):
                                # too many errors -> slow down
                                new_rate = max(self.config.get('rate_limiting', {}).get('base_rate_limit', 3.0),
                                               current_rate / 1.2)
                                rate_limiter.adjust_rate(new_rate)
                            elif error_rate < rate_config.get('error_threshold', 0.3) / 2:
                                # healthy -> speed up (but cap)
                                new_rate = min(self.config.get('rate_limiting', {}).get('max_rate_limit', 5.0),
                                               current_rate * 1.2)
                                rate_limiter.adjust_rate(new_rate)

            # Finalize job
            self.db_manager.finish_job(job_id, successful_count, failed_count)
            results_tracker.save_results()

            # Archive processed (single-file mode)
            if csv_files and csv_files[0] != "AUTO":
                archive_processed_file(csv_files[0], self.config)

            # Summary
            job_stats = self.db_manager.get_job_stats(job_id)
            success_rate = (successful_count / total_requests) * 100 if total_requests > 0 else 0
            metrics = results_tracker.get_metrics()

            BOX_WIDTH = 80
            logger.info(f"""
╔{'═' * (BOX_WIDTH - 2)}╗
║{' ' * ((BOX_WIDTH - 2 - len('🎯 JOB COMPLETED')) // 2)}🎯 JOB COMPLETED{' ' * ((BOX_WIDTH - 2 - len('🎯 JOB COMPLETED') + 1) // 2)}║
╠{'═' * (BOX_WIDTH - 2)}╣
║ Job Name: {job_name:<{BOX_WIDTH - 15}}║
║ Job ID: {job_id:<{BOX_WIDTH - 13}}║
║ CSV File: {os.path.basename(csv_file_path) if len(csv_file_path) < 50 else '...' + csv_file_path[-47:]:<{BOX_WIDTH - 15}}║
║ Triggered By: {triggered_by:<{BOX_WIDTH - 17}}║
║ Total Requests: {total_requests:<{BOX_WIDTH - 20}}║
║ ✅ Successful: {successful_count:<{BOX_WIDTH - 18}}║
║ ❌ Failed: {failed_count:<{BOX_WIDTH - 14}}║
║ 📊 Success Rate: {success_rate:.2f}%{' ' * (BOX_WIDTH - 23)}║
║ ⏱️  Duration: {job_stats.get('duration_seconds', 0):.2f} seconds{' ' * (BOX_WIDTH - 19 - len(f'{job_stats.get("duration_seconds", 0):.2f}'))}║
║ 📈 Throughput: {metrics['throughput']:.2f} req/s{' ' * (BOX_WIDTH - 24 - len(f'{metrics["throughput"]:.2f}'))}║
║ 📁 Data Processed: {metrics['total_request_size'] + metrics['total_response_size']} bytes{' ' * (BOX_WIDTH - 26 - len(str(metrics['total_request_size'] + metrics['total_response_size'])))}║
╚{'═' * (BOX_WIDTH - 2)}╝
            """)

            # Notify + metrics
            self.notification_manager.send_job_completion_notification(job_stats)
            if self.config.get('deployment', {}).get('metrics_enabled', True):
                self.db_manager.save_metric('job_success_rate', success_rate)
                self.db_manager.save_metric('job_duration', job_stats.get('duration_seconds', 0))
                self.db_manager.save_metric('job_throughput', metrics['throughput'])

            return job_id

        except Exception as e:
            logger.error(f"Job {job_name} failed to start or run: {e}")
            logger.error(traceback.format_exc())
            return job_id  # return id even on failure (so callers can look up partials)

    def start_watchdog(self):
        if not self.watchdog_enabled:
            logger.info("📁 Watchdog disabled in configuration")
            return

        watch_paths = self.config.get('watchdog', {}).get('watch_paths', ['/app/data/csv'])
        for path in watch_paths:
            os.makedirs(path, exist_ok=True)

        logger.info(f"👀 Starting watchdog for paths: {watch_paths}")

        self.observer = Observer()
        event_handler = FileWatchdog(self.job_queue, self.db_manager, self.config, 
                                   self.inflight_files, self.inflight_lock)
        for path in watch_paths:
            self.observer.schedule(event_handler, path, recursive=False)
        self.observer.start()

        worker_count = int(os.getenv('PROCESSING_WORKERS', '1'))
        self.processing_threads = []
        for i in range(worker_count):
            t = Thread(target=self._process_queue_worker, name=f"file-worker-{i+1}", daemon=True)
            t.start()
            self.processing_threads.append(t)

        logger.info(f"🚀 Watchdog system started successfully with {worker_count} file worker(s)")

    def _process_queue_worker(self):
        """Background worker to process queued jobs"""
        debounce_delay = self.config.get('watchdog', {}).get('debounce_delay', 3.0)
        while not self.shutdown_event.is_set():
            try:
                job_info = self.job_queue.get(timeout=1.0)
                try:
                    logger.info(f"⏳ Debouncing file: {os.path.basename(job_info['csv_file'])} ({debounce_delay}s)")
                    time.sleep(debounce_delay)
                    if not os.path.exists(job_info['csv_file']):
                        logger.warning(f"File no longer exists: {job_info['csv_file']}")
                        self.db_manager.update_file_status(job_info['csv_file'], 'failed')
                        continue
                    current_hash = self._calculate_file_hash(job_info['csv_file'])
                    if current_hash and current_hash != job_info['file_hash']:
                        logger.info(f"File changed during debounce, re-queuing: {job_info['csv_file']}")
                        job_info['file_hash'] = current_hash
                        self.job_queue.put(job_info)
                        continue
                    if self.db_manager.is_file_processed(job_info['csv_file'], job_info['file_hash']):
                        logger.info(f"File already processed: {os.path.basename(job_info['csv_file'])}")
                        continue
                    self.notification_manager.send_file_detected_notification(job_info)
                    job_name = f"{os.path.basename(job_info['csv_file'])}"
                    logger.info(f"🎬 Auto-processing file: {os.path.basename(job_info['csv_file'])}")
                    self.db_manager.update_file_status(job_info['csv_file'], 'processing')
                    try:
                        job_id = self.trigger_webhooks(
                            csv_files=[job_info['csv_file']],
                            job_name=job_name,
                            triggered_by='watchdog'
                        )
                        self.db_manager.update_file_status(job_info['csv_file'], 'completed', job_id)
                        logger.info(f"✅ Auto-processing completed for: {os.path.basename(job_info['csv_file'])}")
                    except Exception as e:
                        logger.error(f"❌ Auto-processing failed for {job_info['csv_file']}: {e}")
                        self.db_manager.update_file_status(job_info['csv_file'], 'failed')
                finally:
                    with self.inflight_lock:
                        self.inflight_files.discard(job_info['csv_file'])
                    self.job_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in queue processing worker: {e}")
                logger.error(traceback.format_exc())

    def _calculate_file_hash(self, file_path: str) -> str:
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating hash for {file_path}: {e}")
            return ""

    def stop_watchdog(self):
        """Stop the watchdog system gracefully"""
        logger.info("🛑 Stopping watchdog system...")
        self.shutdown_event.set()
        if getattr(self, 'processing_threads', None):
            for t in self.processing_threads:
                t.join(timeout=10)
        if self.observer:
            self.observer.stop()
            self.observer.join()
        logger.info("🛑 Watchdog system stopped")

    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        try:
            queue_size = self.job_queue.qsize() if hasattr(self.job_queue, 'qsize') else 0
        except Exception:
            queue_size = 0
        processing_threads_running = bool(self.processing_threads) and any(t.is_alive() for t in self.processing_threads)
        return {
            'watchdog_enabled': self.watchdog_enabled,
            'watchdog_running': self.observer is not None and self.observer.is_alive() if self.observer else False,
            'processing_thread_running': processing_threads_running,
            'queue_size': queue_size,
            'database_accessible': self._test_database_connection(),
            'timestamp': datetime.now().isoformat()
        }

    def _test_database_connection(self) -> bool:
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1')
                return True
        except Exception:
            return False


# =========================
# Health server / keep-alive / backups
# =========================
def create_health_check_server():
    """Create a simple health check HTTP server"""
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json

        class HealthCheckHandler(BaseHTTPRequestHandler):
            def __init__(self, trigger_instance, *args, **kwargs):
                self.trigger = trigger_instance
                super().__init__(*args, **kwargs)

            def do_GET(self):
                if self.path == '/health' or self.path == '/':
                    status = self.trigger.get_system_status()
                    if status['database_accessible'] and (not status['watchdog_enabled'] or status['watchdog_running']):
                        self.send_response(200)
                        response_data = {
                            'status': 'healthy',
                            'system': status,
                            'message': 'Bulk API Trigger is running normally'
                        }
                    else:
                        self.send_response(503)
                        response_data = {
                            'status': 'unhealthy',
                            'system': status,
                            'message': 'System components not functioning properly'
                        }
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(response_data, indent=2).encode())

                elif self.path == '/metrics':
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    try:
                        with self.trigger.db_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            cursor.execute('''
                                SELECT COUNT(*) as total_jobs,
                                       AVG(duration_seconds) as avg_duration,
                                       AVG(CAST(successful_requests AS FLOAT) / total_requests * 100) as avg_success_rate
                                FROM job_history 
                                WHERE start_time >= datetime('now', '-24 hours')
                            ''')
                            row = cursor.fetchone()
                            metrics = {
                                'jobs_last_24h': row[0] if row[0] else 0,
                                'avg_job_duration': round(row[1], 2) if row[1] else 0,
                                'avg_success_rate': round(row[2], 2) if row[2] else 0,
                                'timestamp': datetime.now().isoformat()
                            }
                    except Exception as e:
                        metrics = {'error': str(e)}
                    self.wfile.write(json.dumps(metrics, indent=2).encode())

                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, format, *args):
                pass  # Suppress default logging

        def start_server(trigger_instance, port=8000):
            handler = lambda *args, **kwargs: HealthCheckHandler(trigger_instance, *args, **kwargs)
            server = HTTPServer(('0.0.0.0', port), handler)
            server.serve_forever()

        return start_server
    except ImportError:
        logger.warning("HTTP server not available, health checks disabled")
        return None


def keep_alive_with_watchdog(trigger: BulkAPITrigger):
    """Enhanced keep-alive with watchdog functionality"""
    logger.info("🔄 Webhook processing completed. Starting keep-alive with watchdog...")
    health_server_starter = create_health_check_server()
    if health_server_starter and trigger.config.get('deployment', {}).get('health_check_enabled', True):
        health_thread = Thread(target=health_server_starter, args=(trigger, 8000), daemon=True)
        health_thread.start()
        logger.info("🏥 Health check server started on port 8000")
    trigger.start_watchdog()
    logger.info("📊 Database contains job history and results for analysis")
    logger.info("👀 Watchdog is monitoring for new CSV files")
    logger.info("🏥 Container health: OK - Running with watchdog")
    try:
        heartbeat_count = 0
        while True:
            time.sleep(300)  # 5 minute intervals
            heartbeat_count += 1
            status = trigger.get_system_status()
            logger.info(f"💓 Heartbeat #{heartbeat_count} - System Status: "
                        f"Queue: {status['queue_size']}, "
                        f"Watchdog: {'✅' if status['watchdog_running'] else '❌'}, "
                        f"DB: {'✅' if status['database_accessible'] else '❌'}")
            backup_hours = trigger.config.get('database', {}).get('backup_interval_hours', 24)
            ticks_per_backup = max(1, int((backup_hours * 60) / 5))
            if trigger.config.get('database', {}).get('backup_enabled', True) and (heartbeat_count % ticks_per_backup == 0):
                logger.info("🗄️  Scheduled maintenance: database backup")
                try:
                    backup_database(trigger.db_manager)
                except Exception as e:
                    logger.error(f"Database backup failed: {e}")
            if status['queue_size'] > 0:
                logger.info(f"📋 Processing queue has {status['queue_size']} pending files")
    except KeyboardInterrupt:
        logger.info("🛑 Received interrupt signal. Shutting down gracefully...")
        trigger.stop_watchdog()


def backup_database(db_manager: DatabaseManager):
    """Create database backup"""
    try:
        backup_dir = '/app/data/backups'
        os.makedirs(backup_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f'webhook_results_backup_{timestamp}.db')
        shutil.copy2(db_manager.db_path, backup_path)
        backup_files = sorted(glob.glob(os.path.join(backup_dir, 'webhook_results_backup_*.db')))
        if len(backup_files) > 7:
            for old_backup in backup_files[:-7]:
                os.remove(old_backup)
                logger.debug(f"Removed old backup: {os.path.basename(old_backup)}")
        logger.info(f"💾 Database backup created: {os.path.basename(backup_path)}")
    except Exception as e:
        logger.error(f"Database backup failed: {e}")


def scan_existing_files(trigger: BulkAPITrigger):
    """Scan for existing CSV files on startup"""
    watch_paths = trigger.config.get('watchdog', {}).get('watch_paths', ['/app/data/csv'])
    for watch_path in watch_paths:
        if not os.path.exists(watch_path):
            continue
        logger.info(f"🔍 Scanning existing files in {watch_path}")
        csv_files = glob.glob(os.path.join(watch_path, '*.csv'))
        for csv_file in csv_files:
            try:
                file_hash = trigger._calculate_file_hash(csv_file)
                if not file_hash:
                    continue
                if trigger.db_manager.is_file_processed(csv_file, file_hash):
                    logger.debug(f"File already processed: {os.path.basename(csv_file)}")
                    continue
                if not os.path.getsize(csv_file) > 0:
                    continue
                file_stats = os.stat(csv_file)
                job_info = {
                    'csv_file': csv_file,
                    'file_hash': file_hash,
                    'file_size': file_stats.st_size,
                    'row_count': 0,
                    'event_type': 'startup_scan',
                    'detected_at': datetime.now().isoformat()
                }
                trigger.db_manager.track_file(csv_file, file_hash, file_stats.st_size, rows_count=0)
                trigger.db_manager.update_file_status(csv_file, 'queued')
                with trigger.inflight_lock:
                    trigger.inflight_files.add(csv_file)
                trigger.job_queue.put(job_info)
                logger.info(f"📄 Queued existing file: {os.path.basename(csv_file)}")
            except Exception as e:
                logger.error(f"Startup scan failed for {csv_file}: {e}")


# =========================
# Config
# =========================
class ConfigManager:
    @staticmethod
    def create_sample_config(path: str = 'config.yaml'):
        sample = {
            'watchdog': {'enabled': True, 'watch_paths': ['/app/data/csv'], 'debounce_delay': 3.0},
            'rate_limiting': {'base_rate_limit': 3.0, 'starting_rate_limit': 3.0, 'max_rate_limit': 5.0, 'window_size': 20, 'error_threshold': 0.3, 'max_workers': 3},
            'retry': {'max_retries': 3, 'timeout': 30},
            'deployment': {'keep_alive': True, 'skip_rows': 0, 'job_name': None, 'metrics_enabled': True, 'health_check_enabled': True},
            'notifications': {'email': {'enabled': False}, 'slack': {'enabled': False}},
            'database': {'enabled': True, 'path': '/app/data/webhook_results.db', 'backup_enabled': True},
            'csv': {'archive_processed': True, 'archive_path': '/app/data/csv/processed', 'duplicates_path': '/app/data/csv/duplicates', 'rejected_path': '/app/data/csv/rejected', 'chunk_size': 1000, 'archive_on_validation_failure': True}
        }
        with open(path, 'w') as f:
            yaml.safe_dump(sample, f)
        logger.info(f"Sample config written to {path}")

    @staticmethod
    def load_config_file(path: str) -> Dict:
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        with open(path, 'r') as f:
            text = f.read()
        try:
            if path.endswith(('.yaml', '.yml')):
                return yaml.safe_load(text) or {}
            else:
                return json.loads(text)
        except Exception as e:
            raise ValueError(f"Failed to parse config file {path}: {e}")


def load_environment_config():
    """Load configuration from environment variables with watchdog support"""
    return {
        'watchdog': {
            'enabled': os.getenv('WATCHDOG_ENABLED', 'true').lower() == 'true',
            'watch_paths': os.getenv('WATCH_PATHS', '/app/data/csv').split(','),
            'auto_process': os.getenv('AUTO_PROCESS', 'true').lower() == 'true',
            'debounce_delay': float(os.getenv('DEBOUNCE_DELAY', '3.0')),
            'max_queue_size': int(os.getenv('MAX_QUEUE_SIZE', '100'))
        },
        'rate_limiting': {
            'base_rate_limit': float(os.getenv('BASE_RATE_LIMIT', '3.0')),
            'starting_rate_limit': float(os.getenv('STARTING_RATE_LIMIT', '3.0')),
            'max_rate_limit': float(os.getenv('MAX_RATE_LIMIT', '5.0')),
            'window_size': int(os.getenv('WINDOW_SIZE', '20')),
            'error_threshold': float(os.getenv('ERROR_THRESHOLD', '0.3')),
            'max_workers': int(os.getenv('MAX_WORKERS', '3'))
        },
        'retry': {
            'max_retries': int(os.getenv('MAX_RETRIES', '3')),
            'timeout': int(os.getenv('REQUEST_TIMEOUT', '30')),
            'retry_delay': float(os.getenv('RETRY_DELAY', '1.0')),
        },
        'deployment': {
            'keep_alive': os.getenv('KEEP_ALIVE', 'true').lower() == 'true',
            'skip_rows': int(os.getenv('SKIP_ROWS', '0')),
            'job_name': os.getenv('JOB_NAME', None),
            'metrics_enabled': os.getenv('METRICS_ENABLED', 'true').lower() == 'true',
            'health_check_enabled': os.getenv('HEALTH_CHECK_ENABLED', 'true').lower() == 'true'
        },
        'notifications': {
            'email': {
                'enabled': os.getenv('EMAIL_NOTIFICATIONS', 'false').lower() == 'true',
                'smtp_server': os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com'),
                'smtp_port': int(os.getenv('EMAIL_SMTP_PORT', '587')),
                'username': os.getenv('EMAIL_USERNAME', ''),
                'password': os.getenv('EMAIL_PASSWORD', ''),
                'from_email': os.getenv('EMAIL_FROM', ''),
                'recipients': os.getenv('EMAIL_RECIPIENTS', '').split(',') if os.getenv('EMAIL_RECIPIENTS') else [],
                'notify_on_completion': os.getenv('EMAIL_NOTIFY_COMPLETION', 'true').lower() == 'true',
                'notify_on_file_detected': os.getenv('EMAIL_NOTIFY_FILE_DETECTED', 'false').lower() == 'true'
            },
            'slack': {
                'enabled': os.getenv('SLACK_NOTIFICATIONS', 'false').lower() == 'true',
                'webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
                'notify_on_completion': os.getenv('SLACK_NOTIFY_COMPLETION', 'true').lower() == 'true',
                'notify_on_file_detected': os.getenv('SLACK_NOTIFY_FILE_DETECTED', 'true').lower() == 'true'
            }
        },
        'database': {
            'enabled': os.getenv('DATABASE_ENABLED', 'true').lower() == 'true',
            # accept either DATABASE_BACKUP or DATABASE_BACKUP_ENABLED
            'backup_enabled': (os.getenv('DATABASE_BACKUP', os.getenv('DATABASE_BACKUP_ENABLED', 'true'))).lower() == 'true',
            'path': os.getenv('DATABASE_PATH', '/app/data/webhook_results.db'),
            # new: interval hours
            'backup_interval_hours': int(os.getenv('DATABASE_BACKUP_INTERVAL_HOURS', '24')),
        },
        'csv': {
            'archive_processed': os.getenv('ARCHIVE_PROCESSED', 'true').lower() == 'true',
            'archive_path': os.getenv('ARCHIVE_PATH', '/app/data/csv/processed'),
            'duplicates_path': os.getenv('DUPLICATES_PATH', '/app/data/csv/duplicates'),
            'rejected_path': os.getenv('REJECTED_PATH', '/app/data/csv/rejected'),
            'chunk_size': int(os.getenv('CSV_CHUNK_SIZE', '1000')),
            'archive_on_validation_failure': os.getenv('ARCHIVE_ON_VALIDATION_FAILURE', 'true').lower() == 'true'
        }
    }


# =========================
# Main
# =========================
def main():
    """Enhanced main function with watchdog and comprehensive error handling"""
    import signal
    trigger_instance = None
    def signal_handler(signum, frame):
        logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
        if trigger_instance:
            trigger_instance.stop_watchdog()
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("""
🚀 Enhanced Bulk API Trigger Platform with Watchdog
==================================================
(…help text trimmed for brevity…)
        """)
        return

    if len(sys.argv) > 1 and sys.argv[1] == '--create-config':
        ConfigManager.create_sample_config()
        return

    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        print("🎯 Interactive Enhanced Bulk API Trigger")
        print("=" * 45)
        csv_file = input("CSV file path (or 'auto' for watchdog mode): ").strip()
        job_name = input("Job name (optional): ").strip()
        skip_rows = input("Rows to skip (default: 0): ").strip()
        keep_running = input("Keep container alive with watchdog? (Y/n): ").strip().lower()
        enable_watchdog = input("Enable file monitoring? (Y/n): ").strip().lower()
        config = load_environment_config()
        if job_name:
            config.setdefault('deployment', {})['job_name'] = job_name
        if skip_rows.isdigit():
            config.setdefault('deployment', {})['skip_rows'] = int(skip_rows)
        config.setdefault('deployment', {})['keep_alive'] = keep_running not in ['n', 'no', 'false']
        config.setdefault('watchdog', {})['enabled'] = enable_watchdog not in ['n', 'no', 'false']
        trigger = BulkAPITrigger(config)
        trigger_instance = trigger
        if csv_file.lower() not in ['auto', 'watchdog']:
            files = [csv_file]
            job_id = trigger.trigger_webhooks(
                csv_files=files,
                job_name=job_name or None,
                skip_rows=int(skip_rows) if skip_rows.isdigit() else 0
            )
            logger.info(f"🏆 Job completed with ID: {job_id}")
        if config['deployment']['keep_alive']:
            keep_alive_with_watchdog(trigger)
        return

    if os.getenv('DEPLOYMENT_MODE') or os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('DOCKER_CONTAINER'):
        logger.info("🐳 Enhanced deployment mode detected")
        config = load_environment_config()
        # Optional: merge file config
        for config_file in ['config.yaml', 'config.yml', 'config.json']:
            if os.path.exists(config_file):
                file_config = ConfigManager.load_config_file(config_file)
                def merge_configs(base, override):
                    for key, value in override.items():
                        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                            merge_configs(base[key], value)
                        else:
                            base[key] = value
                merge_configs(config, file_config)
                logger.info(f"📋 Loaded additional config from {config_file}")
                break
        logger.info(f"⚙️  Configuration loaded successfully")
        logger.debug(f"Config details: {json.dumps(config, indent=2, default=str)}")
        trigger = BulkAPITrigger(config)
        trigger_instance = trigger
        csv_file = os.getenv('CSV_FILE', 'AUTO')
        if csv_file != 'AUTO':
            logger.info(f"📄 Processing specific file: {csv_file}")
            job_id = trigger.trigger_webhooks(
                csv_files=[csv_file],
                job_name=config['deployment'].get('job_name'),
                skip_rows=config['deployment'].get('skip_rows', 0)
            )
            logger.info(f"🏆 Initial job completed with ID: {job_id}")
        else:
            scan_existing_files(trigger)
        if config['deployment']['keep_alive']:
            keep_alive_with_watchdog(trigger)
        else:
            logger.info("🏁 Job completed. Container will exit.")
        return

    # CLI mode
    parser = argparse.ArgumentParser(description="🚀 Enhanced Bulk API Trigger Platform")
    parser.add_argument("csv_file", nargs='?', help="Path to CSV file, 'auto' for auto-discovery, or 'watchdog' for monitoring mode")
    parser.add_argument("--config", "-c", help="Configuration file path (YAML/JSON)")
    parser.add_argument("--job-name", "-n", help="Custom job name")
    parser.add_argument("--skip-rows", "-s", type=int, default=0, help="Number of rows to skip")
    parser.add_argument("--keep-alive", "-k", action="store_true", help="Keep process running with watchdog")
    parser.add_argument("--workers", "-w", type=int, help="Number of parallel workers")
    parser.add_argument("--rate-limit", "-r", type=float, help="Base rate limit (requests/sec)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Validate CSV without sending requests")
    parser.add_argument("--watchdog", action="store_true", help="Enable file monitoring")
    parser.add_argument("--no-watchdog", action="store_true", help="Disable file monitoring")
    parser.add_argument("--health-port", type=int, default=8000, help="Health check server port")
    args = parser.parse_args()

    if not args.csv_file:
        logger.error("❌ CSV file path required. Use --help for usage information.")
        return

    config = load_environment_config()
    if args.config:
        file_config = ConfigManager.load_config_file(args.config)
        for key, value in file_config.items():
            if isinstance(value, dict) and key in config:
                config[key].update(value)
            else:
                config[key] = value
    if args.workers:
        config['rate_limiting']['max_workers'] = args.workers
    if args.rate_limit:
        config['rate_limiting']['base_rate_limit'] = args.rate_limit
        config['rate_limiting']['starting_rate_limit'] = args.rate_limit
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.watchdog:
        config['watchdog']['enabled'] = True
    if args.no_watchdog:
        config['watchdog']['enabled'] = False

    logger.info(f"🎯 Starting CLI mode with config: {args.config or 'environment/defaults'}")

    if args.dry_run:
        logger.info("🧪 DRY RUN MODE - No requests will be sent")
        try:
            if args.csv_file.lower() == 'auto':
                webhooks, stats = read_multiple_csv_files(skip_rows=args.skip_rows)
            else:
                webhooks, stats = read_csv_with_validation(args.csv_file, skip_rows=args.skip_rows)
            logger.info(f"✅ Validation complete: {len(webhooks)} valid webhooks found")
            logger.info(f"📊 Statistics: {json.dumps(stats, indent=2, default=str)}")
            if webhooks:
                logger.info("📋 Sample webhooks:")
                for i, webhook in enumerate(webhooks[:5]):
                    logger.info(f"  {i+1}. {webhook['name']}: {webhook['url']} [{webhook.get('method', 'GET')}]")
                if len(webhooks) > 5:
                    logger.info(f"  ... and {len(webhooks) - 5} more")
        except Exception as e:
            logger.error(f"❌ Validation failed: {e}")
        return

    trigger = BulkAPITrigger(config)
    trigger_instance = trigger

    if args.csv_file.lower() == 'watchdog':
        logger.info("👀 Starting in watchdog monitoring mode")
        scan_existing_files(trigger)
        keep_alive_with_watchdog(trigger)
        return

    files = [args.csv_file] if args.csv_file.lower() != 'auto' else None
    job_id = trigger.trigger_webhooks(
        csv_files=files,
        job_name=args.job_name,
        skip_rows=args.skip_rows
    )
    logger.info(f"🏆 Job completed with ID: {job_id}")
    if args.keep_alive:
        keep_alive_with_watchdog(trigger)


if __name__ == "__main__":
    try:
        try:
            import watchdog  # noqa: F401 (ensure available)
        except ImportError:
            logger.warning("📦 Installing watchdog dependency...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "watchdog"])
            import watchdog  # noqa: F401
            logger.info("✅ Watchdog dependency installed successfully")
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)