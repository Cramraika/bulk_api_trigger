# === webhook_trigger.py (fixed / extended) ===
# Order matters: imports and logger first to keep Pylance happy.

import csv
import json
import requests
import time
import argparse
import sys
import os
import random
import glob
import logging
import yaml
import sqlite3
import hashlib
import shutil
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
import heapq
from threading import Lock, Semaphore, Event, Thread
from tqdm import tqdm
from typing import List, Dict, Optional, Tuple, Set
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import email.utils
import smtplib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import queue
import signal
import traceback
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler

# Webhook rate limiting
from collections import defaultdict
import time

class WebhookRateLimiter:
    def __init__(self, max_requests_per_minute=60):
        self.max_requests = max_requests_per_minute
        self.requests = defaultdict(list)
        self.lock = Lock()
    
    def is_allowed(self, client_ip: str) -> bool:
        with self.lock:
            now = time.time()
            minute_ago = now - 60
            
            # Clean old requests
            self.requests[client_ip] = [t for t in self.requests[client_ip] if t > minute_ago]
            
            # Check rate limit
            if len(self.requests[client_ip]) >= self.max_requests:
                return False
            
            # Record this request
            self.requests[client_ip].append(now)
            return True
    
    def get_reset_time(self, client_ip: str) -> int:
        with self.lock:
            if not self.requests[client_ip]:
                return 0
            oldest = min(self.requests[client_ip])
            return int(oldest + 60)

# Global rate limiter instance
webhook_rate_limiter = WebhookRateLimiter(
    max_requests_per_minute=int(os.getenv('WEBHOOK_RATE_LIMIT', '60'))
)

# ---------- Logging ----------
def setup_logging():
    """Setup enhanced logging with rotation (robust, UTF-8, single-init, flush on exit)."""
    import atexit
    from logging.handlers import RotatingFileHandler
    
    # Global module-level guard to prevent any possibility of duplicate setup
    global _logging_initialized
    if '_logging_initialized' in globals() and _logging_initialized:
        return logging.getLogger(__name__)
    
    globals()['_logging_initialized'] = True

    # Avoid duplicate handlers if setup_logging() is ever called twice.
    root_logger = logging.getLogger()
    if getattr(root_logger, "_already_configured", False):
        return logging.getLogger(__name__)
    
    # Also check for existing handlers of same type
    for handler in root_logger.handlers:
        if isinstance(handler, (RotatingFileHandler, logging.StreamHandler)):
            return logging.getLogger(__name__)

    # Read level early
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    # Ensure directories exist and are writable
    log_dir = '/app/data/logs'
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'webhook_trigger.log')

    # Formatter: include thread and time
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(threadName)s] - %(name)s - %(message)s'
    )

    # File handler with rotation - UTF-8, explicit size from env if present
    max_mb = int(os.getenv('MAX_LOG_SIZE_MB', '100'))
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_mb * 1024 * 1024,
        backupCount=5,
        encoding='utf-8',   # important for emoji
        delay=False
    )
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(level)

    # Console handler to stdout
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(level)

    # Reset root handlers and apply exactly ours
    root_logger.handlers.clear()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    # Mark configured to avoid re-adding on imports
    root_logger._already_configured = True

    # Flush everything on exit (and on SIGTERM/INT via your signal handler)
    def _flush_handlers():
        for h in list(root_logger.handlers):
            try:
                h.flush()
            except Exception:
                pass
    atexit.register(_flush_handlers)

    lg = logging.getLogger(__name__)
    lg.debug("Logging initialized (level=%s, path=%s, max_mb=%d)", level_name, log_path, max_mb)
    return lg

logger = setup_logging()

# Webhook-specific logger
webhook_logger = logging.getLogger('webhook')
webhook_logger.setLevel(logging.INFO)

def log_webhook_trigger(endpoint: str, data: Dict, response_code: int, client_addr: str = None):
    """Log webhook trigger events"""
    try:
        log_entry = {
            'timestamp': datetime.now().isoformat(),
            'endpoint': endpoint,
            'client': client_addr,
            'request': data,
            'response_code': response_code
        }
        webhook_logger.info(f"Webhook trigger: {json.dumps(log_entry)}")
    except Exception as e:
        logger.error(f"Failed to log webhook event: {e}")

# Constants
DEFAULT_DEBOUNCE_DELAY = 3.0
MAX_BACKOFF_SECONDS = 30.0
MAX_HEADER_SIZE = 10000
MAX_HEADER_COUNT = 50
HEARTBEAT_INTERVAL = 300  # 5 minutes

# Global shutdown coordination
SHUTDOWN_EVENT = Event()

def _handle_sigterm(signum, frame):
    try:
        logger.info("Received signal %s, shutting down gracefully...", signum)
        SHUTDOWN_EVENT.set()
    except Exception as e:
        print(f"Error in signal handler: {e}")

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)
# Keep stdout line-buffered in Docker-ish envs
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

# ---------- Small utils ----------
def _to_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default

SENSITIVE_HEADER_KEYS = {"authorization", "x-api-key", "api-key", "proxy-authorization", "authentication"}

def _deep_merge(dst: dict, src: dict) -> None:
    """Deep merge src dict into dst dict"""
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v

# File hash cache to avoid recalculation
_hash_cache = {}
_hash_cache_lock = Lock()

def _calculate_file_hash(file_path: str) -> str:
    """Calculate file hash with caching to avoid redundant calculations"""
    if not file_path or not os.path.exists(file_path):
        return ""
    
    try:
        # Use file path and modification time as cache key
        stat = os.stat(file_path)
        cache_key = f"{file_path}:{stat.st_mtime}:{stat.st_size}"
        
        with _hash_cache_lock:
            if cache_key in _hash_cache:
                return _hash_cache[cache_key]
        
        file_size = stat.st_size
        if file_size == 0:
            hash_result = hashlib.md5().hexdigest()
        else:
            hash_md5 = hashlib.md5()
            # Use adaptive chunk size with reasonable bounds
            chunk_size = min(65536, max(4096, file_size // 100))
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
            hash_result = hash_md5.hexdigest()
        
        with _hash_cache_lock:
            # Keep cache size reasonable
            if len(_hash_cache) > 1000:
                # Remove oldest entries (simple cleanup)
                keys_to_remove = list(_hash_cache.keys())[:500]
                for k in keys_to_remove:
                    _hash_cache.pop(k, None)
            _hash_cache[cache_key] = hash_result
        
        return hash_result
        
    except (IOError, OSError) as e:
        logger.error(f"Error calculating hash for {file_path}: {e}")
        return ""
    except Exception as e:
        logger.error(f"Unexpected error calculating hash for {file_path}: {e}")
        return ""

def _redact_headers(h: Dict[str, str]) -> Dict[str, str]:
    redacted = {}
    for k, v in (h or {}).items():
        if k.lower() in SENSITIVE_HEADER_KEYS:
            redacted[k] = "***REDACTED***"
        else:
            redacted[k] = v
    return redacted

def _resume_marker_path(file_path: str, file_hash: str, config: Dict = None) -> str:
    # Priority: resume config > deployment config > env > default
    base_dir = (
        (config or {}).get('resume', {}).get('marker_path')
        or (config or {}).get('deployment', {}).get('report_path')
        or os.getenv('RESUME_MARKER_PATH')
        or os.getenv('REPORT_PATH')
        or '/app/data/reports'
    )
    os.makedirs(base_dir, exist_ok=True)
    base = os.path.basename(file_path).replace('/', '_').replace('\\', '_')
    safe_hash = file_hash[:16] if file_hash else 'unknown'  # Use first 16 chars of hash
    return os.path.join(base_dir, f".resume_{safe_hash}_{base}.json")

def _load_resume_rows(file_path: str, file_hash: str, config: Dict = None) -> int:
    try:
        p = _resume_marker_path(file_path, file_hash, config)
        if not os.path.exists(p):
            return 0
        
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Validate the resume data
        stored_hash = data.get("file_hash", "")
        if stored_hash and stored_hash != file_hash:
            logger.warning(f"Resume marker hash mismatch for {file_path}, clearing marker")
            _clear_resume_marker(file_path, file_hash, config)
            return 0
        
        skip_rows = int(data.get("skip_rows", 0))
        last_checkpoint = data.get("last_checkpoint")
        
        # Check if checkpoint is too old (configurable, default 7 days)
        if last_checkpoint:
            try:
                checkpoint_time = datetime.fromisoformat(last_checkpoint)
                max_age_days = int(os.getenv('RESUME_MAX_AGE_DAYS', '7'))
                if (datetime.now() - checkpoint_time).days > max_age_days:
                    logger.warning(f"Resume marker too old ({last_checkpoint}), ignoring")
                    return 0
            except Exception:
                pass
        
        logger.info(f"Loaded resume position {skip_rows} for {file_path} (checkpoint: {last_checkpoint})")
        return max(0, skip_rows)
    except Exception as e:
        logger.error(f"Failed to load resume marker: {e}")
        return 0

def _save_resume_rows(file_path: str, file_hash: str, skip_rows: int, config: Dict = None) -> None:
    try:
        p = _resume_marker_path(file_path, file_hash, config)
        resume_data = {
            "skip_rows": int(max(0, skip_rows)),
            "file_path": file_path,
            "file_hash": file_hash,
            "last_checkpoint": datetime.now().isoformat(),
            "total_rows": _count_csv_rows(file_path) if os.path.exists(file_path) else 0
        }
        with open(p, "w", encoding="utf-8") as f:
            json.dump(resume_data, f, indent=2)
    except Exception as e:
        logger.error(f"Resume marker write failed for {file_path}: {e}")

def _clear_resume_marker(file_path: str, file_hash: str, config: Dict = None) -> None:
    try:
        p = _resume_marker_path(file_path, file_hash, config)
        if os.path.exists(p):
            os.remove(p)
    except Exception:
        pass

def prune_old_reports(report_dir: str, keep: int = 200):
    try:
        if not report_dir or not os.path.exists(report_dir):
            logger.debug(f"Report directory does not exist: {report_dir}")
            return
        keep = max(1, keep)  # Keep at least 1 report
        files = sorted(
            [os.path.join(report_dir, f) for f in os.listdir(report_dir) 
             if f.startswith('job_') and f.endswith('.json')],
            key=lambda x: os.path.getmtime(x) if os.path.exists(x) else 0,
            reverse=True
        )
        removed_count = 0
        for f in files[keep:]:
            try:
                os.remove(f)
                removed_count += 1
            except Exception as e:
                logger.warning(f"Could not remove old report {f}: {e}")
        if removed_count > 0:
            logger.debug(f"Pruned {removed_count} old reports from {report_dir}")
    except Exception as e:
        logger.debug(f"Report pruning skipped: {e}")

# CSV row counting cache to avoid multiple file reads
_row_count_cache = {}
_row_count_cache_lock = Lock()

def _count_csv_rows(file_path: str) -> int:
    """Count non-header rows with caching to avoid redundant file reads"""
    if not file_path or not os.path.exists(file_path):
        return 0
    
    try:
        # Use file path and modification time as cache key
        stat = os.stat(file_path)
        cache_key = f"{file_path}:{stat.st_mtime}:{stat.st_size}"
        
        with _row_count_cache_lock:
            if cache_key in _row_count_cache:
                return _row_count_cache[cache_key]
        
        enc = _detect_encoding(file_path)
        with open(file_path, 'r', encoding=enc, newline='') as f:
            delim = _detect_delimiter(f)
            reader = csv.DictReader(f, delimiter=delim)
            raw_headers = reader.fieldnames or []
            headers = _normalize_headers(raw_headers)
            if 'webhook_url' not in headers:
                result = 0
            else:
                name_map = {orig: norm for orig, norm in zip(raw_headers, headers)}
                count = 0
                for row in reader:
                    nrow = {name_map.get(k, k or ''): (v or '').strip() for k, v in row.items()}
                    u = nrow.get('webhook_url')
                    if u and _is_valid_url(u):
                        count += 1
                result = count
        
        with _row_count_cache_lock:
            # Keep cache size reasonable
            if len(_row_count_cache) > 100:
                # Remove oldest entries
                keys_to_remove = list(_row_count_cache.keys())[:50]
                for k in keys_to_remove:
                    _row_count_cache.pop(k, None)
            _row_count_cache[cache_key] = result
        
        return result
        
    except Exception:
        return 0

def _slack_urgency_for_progress(successful_count: int, processed_count: int) -> str:
    """Pick a Slack urgency based on live success rate."""
    if processed_count <= 0:
        return "normal"
    rate = max(0.0, min(1.0, successful_count / max(1, processed_count)))  # Prevent division by zero and bound rate
    if rate >= 0.98:
        return "low"
    if rate >= 0.90:
        return "normal"
    if rate >= 0.80:
        return "high"
    return "critical"

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
    try:
        pos = file_obj.tell()
    except Exception:
        pos = 0
    try:
        sample = file_obj.read(4096)
        file_obj.seek(pos)
    except Exception:
        try:
            file_obj.seek(pos)
        except Exception:
            pass
        return ','  # Default to comma on any error
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

from urllib.parse import urlparse

def _is_valid_url(u: str) -> bool:
    try:
        if not u:
            return False
        p = urlparse(u.strip())
        return p.scheme in ('http', 'https') and bool(p.netloc)
    except Exception:
        return False

from requests.adapters import HTTPAdapter

# ---- HTTP result classification & execution helpers ----
from requests.exceptions import RequestException

NON_RETRIABLE_STATUS = {400, 401, 403, 404, 405, 410, 413, 415, 422}

def _create_http_session() -> requests.Session:
    s = requests.Session()
    pool = max(1, int(os.getenv('HTTP_POOL_MAXSIZE', '16') or '16'))
    adapter = HTTPAdapter(pool_connections=pool, pool_maxsize=pool, max_retries=0)
    s.mount('http://', adapter)
    s.mount('https://', adapter)
    # Set a sane default User-Agent; allow override via env
    s.headers.update({'User-Agent': os.getenv('HTTP_USER_AGENT', 'BulkAPITrigger/1.0')})
    return s

import threading
_HTTP_LOCAL = threading.local()

def get_http_session():
    """Get thread-local HTTP session with proper cleanup"""
    if not hasattr(_HTTP_LOCAL, 'session') or _HTTP_LOCAL.session is None:
        try:
            _HTTP_LOCAL.session = _create_http_session()
        except Exception as e:
            logger.error(f"Failed to create HTTP session: {e}")
            # Create a new session without pooling as fallback
            _HTTP_LOCAL.session = requests.Session()
    return _HTTP_LOCAL.session

def cleanup_http_session():
    """Cleanup thread-local HTTP session"""
    if hasattr(_HTTP_LOCAL, 'session') and _HTTP_LOCAL.session:
        try:
            _HTTP_LOCAL.session.close()
        except Exception:
            pass
        _HTTP_LOCAL.session = None

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
        """Thread-safe database connection context manager with proper cleanup"""
        with self._connection_lock:
            conn = None
            try:
                conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                # Test connection
                conn.execute("SELECT 1")
                yield conn
            except Exception as e:
                logger.error(f"Database connection error: {e}")
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error closing database connection: {e}")

    def is_file_processed(self, file_path: str, file_hash: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM file_tracking
                WHERE file_path = ? AND file_hash = ? AND status = 'completed'
                LIMIT 1
            ''', (file_path, file_hash))
            return cursor.fetchone() is not None

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
                    error_message TEXT,
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
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS webhook_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    endpoint TEXT NOT NULL,
                    client_ip TEXT,
                    request_data TEXT,
                    response_code INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_timestamp ON webhook_metrics(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_endpoint ON webhook_metrics(endpoint)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_job_id ON webhook_results(job_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_timestamp ON webhook_results(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_status ON job_history(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_status ON file_tracking(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_path_hash_status ON file_tracking(file_path, file_hash, status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_hash_status ON file_tracking(file_hash, status)')
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
            try:
                # Calculate average response time
                cursor.execute('''
                    SELECT AVG(response_time) 
                    FROM webhook_results 
                    WHERE job_id = ? AND response_time IS NOT NULL AND response_time > 0
                ''', (job_id,))
                result = cursor.fetchone()
                avg_response_time = float(result[0]) if result and result[0] is not None else 0.0
                
                # Calculate duration from start time
                cursor.execute('SELECT start_time FROM job_history WHERE job_id = ?', (job_id,))
                result = cursor.fetchone()
                duration = 0
                if result and result[0]:
                    try:
                        start_time = datetime.fromisoformat(result[0])
                        duration = max(0, (datetime.now() - start_time).total_seconds())
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Invalid start_time format for job {job_id}: {e}, using 0 duration")
                        duration = 0
                    
                # Update job record
                cursor.execute('''
                    UPDATE job_history 
                    SET successful_requests = ?, failed_requests = ?, end_time = ?, 
                        duration_seconds = ?, status = 'completed', average_response_time = ?
                    WHERE job_id = ?
                ''', (successful, failed, datetime.now().isoformat(), duration, avg_response_time, job_id))
                conn.commit()
            except sqlite3.Error as e:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                logger.error(f"Database error finishing job {job_id}: {e}")
                raise
            except (ValueError, TypeError) as e:
                logger.error(f"Data error finishing job {job_id}: {e}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error finishing job {job_id}: {e}")
                raise

    def mark_job_failed(self, job_id: str, error_message: str):
        """Mark a job as failed with error message"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE job_history 
                SET status = 'failed', 
                    end_time = ?,
                    error_message = ?
                WHERE job_id = ?
            ''', (datetime.now().isoformat(), error_message, job_id))
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

    def is_hash_processed(self, file_hash: str) -> bool:
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM file_tracking 
                WHERE file_hash = ? AND status = 'completed'
                LIMIT 1
            ''', (file_hash,))
            row = cursor.fetchone()
            return bool(row)

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
            status_stats = {}
            try:
                cursor.execute('SELECT * FROM job_history WHERE job_id = ?', (job_id,))
                job = cursor.fetchone()
            except Exception as e:
                logger.error(f"Error fetching job {job_id}: {e}")
                job = None
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
            # Safely access tuple elements with bounds checking
            def safe_get(tup, idx, default=None):
                return tup[idx] if tup and len(tup) > idx else default
            
            duration = _to_float(safe_get(job, 8, 0), 0.0)
            avg_response_time = _to_float(safe_get(job, 12, 0), 0.0)
            return {
                'job_id': safe_get(job, 0, job_id),
                'job_name': safe_get(job, 1, ''),
                'csv_file': safe_get(job, 2, ''),
                'total_requests': int(_to_float(safe_get(job, 3, 0), 0.0)),
                'successful_requests': int(_to_float(safe_get(job, 4, 0), 0.0)),
                'failed_requests': int(_to_float(safe_get(job, 5, 0), 0.0)),
                'start_time': safe_get(job, 6, ''),
                'end_time': safe_get(job, 7, ''),
                'duration_seconds': duration,
                'triggered_by': safe_get(job, 11, 'manual'),
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
        self.config = config or {}
        self.email_config = config.get('email', {}) or {}
        self.slack_config = config.get('slack', {}) or {}

    @staticmethod
    def send_startup_test_notifications(trigger: "BulkAPITrigger"):
        try:
            if os.getenv("SEND_TEST_NOTIFICATIONS_ON_STARTUP", "false").lower() != "true":
                return
            nm = trigger.notification_manager
            if nm.slack_config.get('enabled'):
                nm.send_slack_notification("✅ Test Slack from Bulk API Trigger (startup)", "low")
            if nm.email_config.get('enabled') and nm.email_config.get('recipients'):
                nm.send_email_notification(
                    subject="Test Email from Bulk API Trigger (startup)",
                    body="<p>This is a startup test notification.</p>",
                    to_emails=nm.email_config.get('recipients', [])
                )
            logger.info("Startup test notifications attempted")
        except Exception as e:
            logger.error(f"Startup test notifications failed: {e}")

    def send_email_notification(self, subject: str, body: str, to_emails: List[str]):
        if not self.email_config.get('enabled', False):
            logger.debug("Email notifications disabled in config; skipping send")
            return
        
        # Validate required configuration
        required_fields = ['smtp_server', 'username', 'password', 'from_email']
        missing_fields = [field for field in required_fields if not self.email_config.get(field)]
        if missing_fields:
            logger.error(f"Email configuration missing required fields: {missing_fields}")
            return
        
        if not to_emails:
            logger.warning("No email recipients specified")
            return
        max_retries = 3
        for attempt in range(max_retries):
            try:
                msg = MIMEMultipart()
                msg['From'] = self.email_config.get('from_email', '')
                msg['To'] = ', '.join(to_emails)
                msg['Subject'] = subject
                msg.attach(MIMEText(body, 'html'))
                smtp_port = int(self.email_config.get('smtp_port', 587) or 587)
                smtp_server = self.email_config.get('smtp_server', 'smtp.gmail.com') or 'smtp.gmail.com'
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
                logger.info(f"Email notification sent to {to_emails}")
                break
            except (smtplib.SMTPException, OSError) as e:
                logger.error(f"Failed to send email notification (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)

    def send_slack_notification(self, message: str, urgency: str = 'normal'):
        if not self.slack_config.get('enabled', False):
            logger.debug("Slack notifications disabled in config; skipping send")
            return
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
        url = self.slack_config.get('webhook_url', '')
        if not url:
            logger.error("Slack webhook URL not configured")
            return
            
        max_attempts = 3
        delay = 1.0
        for attempt in range(1, max_attempts + 1):
            try:
                http_session = get_http_session()
                resp = http_session.post(url, json=payload, timeout=10)
                if resp.status_code == 200:
                    logger.info("Slack notification sent successfully")
                    return
                logger.error(f"Failed to send Slack notification: HTTP {resp.status_code} (attempt {attempt}/{max_attempts})")
            except requests.RequestException as e:
                logger.error(f"Slack send exception (attempt {attempt}/{max_attempts}): {e}")

            if attempt < max_attempts:
                time.sleep(delay)
                delay = min(8.0, delay * 2)

    def send_job_completion_notification(self, job_stats: Dict):
        total = job_stats.get('total_requests') or 0
        succ  = job_stats.get('successful_requests') or 0
        success_rate = (succ / total * 100.0) if total else 0.0
        urgency = 'normal' if success_rate >= 95 else ('high' if success_rate >= 80 else 'critical')
        if self.slack_config.get('enabled') and self.slack_config.get('notify_on_completion', True):
            completion_emoji = "🎉" if success_rate >= 95 else ("⚠️" if success_rate >= 80 else "🚨")
            slack_message = (
                f"{completion_emoji} *Bulk API Job Completed*\n\n"
                f"🏷️ *Job:* {job_stats.get('job_name','')}\n"
                f"📁 *File:* {os.path.basename(job_stats.get('csv_file','Multiple files'))}\n"
                f"🎯 *Triggered:* {job_stats.get('triggered_by','manual')}\n"
                f"📊 *Success Rate:* {success_rate:.2f}%\n"
                f"📈 *Requests:* {succ}/{total} successful\n"
                f"⏱️ *Duration:* {_to_float(job_stats.get('duration_seconds')):.2f}s\n"
                f"⚡ *Avg Response:* {_to_float(job_stats.get('average_response_time')):.3f}s"
            )
            self.send_slack_notification(slack_message, urgency)
        if self.email_config.get('enabled') and self.email_config.get('notify_on_completion', True):
            status_word = ("Completed" if succ == total else ("Completed (with failures)" if succ > 0 else "Failed"))
            subject = f"Bulk API Job {status_word}: {job_stats.get('job_name','')}"
            html_body = f"""
            <h2>Job Completion Report</h2>
            <table border="1" cellpadding="10" style="border-collapse: collapse;">
                <tr><td><strong>Job Name</strong></td><td>{job_stats.get('job_name','')}</td></tr>
                <tr><td><strong>Job ID</strong></td><td>{job_stats.get('job_id','')}</td></tr>
                <tr><td><strong>CSV File</strong></td><td>{job_stats.get('csv_file','Multiple files')}</td></tr>
                <tr><td><strong>Triggered By</strong></td><td>{job_stats.get('triggered_by','manual')}</td></tr>
                <tr><td><strong>Total Requests</strong></td><td>{total}</td></tr>
                <tr><td><strong>Successful</strong></td><td>{succ}</td></tr>
                <tr><td><strong>Failed</strong></td><td>{job_stats.get('failed_requests') or 0}</td></tr>
                <tr><td><strong>Success Rate</strong></td><td>{success_rate:.2f}%</td></tr>
                <tr><td><strong>Duration</strong></td><td>{_to_float(job_stats.get('duration_seconds')):.2f} seconds</td></tr>
                <tr><td><strong>Avg Response Time</strong></td><td>{_to_float(job_stats.get('average_response_time')):.3f}s</td></tr>
                <tr><td><strong>Start Time</strong></td><td>{job_stats.get('start_time','')}</td></tr>
                <tr><td><strong>End Time</strong></td><td>{job_stats.get('end_time','')}</td></tr>
            </table>
            """
            self.send_email_notification(subject, html_body, self.email_config.get('recipients', []))

    def send_file_detected_notification(self, file_info: Dict):
        if not self.slack_config.get('enabled') or not self.slack_config.get('notify_on_file_detected', True):
            return
        
        # Check if this is a resume case
        resume_position = file_info.get('resume_position', 0)
        total_rows = file_info.get('row_count', 0)
        
        if resume_position > 0:
            remaining_rows = max(0, total_rows - resume_position)
            message = (
                f"🔄 *CSV File Resume Detected*\n\n"
                f"📁 *File:* {os.path.basename(file_info['csv_file'])}\n"
                f"📊 *Size:* {file_info['file_size']} bytes\n"
                f"📋 *Total Rows:* {total_rows} webhooks\n"
                f"⏭️ *Resume Position:* {resume_position}\n"
                f"🎯 *Remaining:* {remaining_rows} webhooks\n"
                f"🔍 *Event:* {file_info['event_type']}\n"
                f"🕐 *Time:* {file_info['detected_at']}\n\n"
                f"🚀 Processing will resume shortly..."
            )
        else:
            message = (
                f"📄 *New CSV File Detected*\n\n"
                f"📁 *File:* {os.path.basename(file_info['csv_file'])}\n"
                f"📊 *Size:* {file_info['file_size']} bytes\n"
                f"📋 *Rows:* {total_rows} webhooks\n"
                f"🔍 *Event:* {file_info['event_type']}\n"
                f"🕐 *Time:* {file_info['detected_at']}\n\n"
                f"🚀 Processing will start shortly..."
            )
        self.send_slack_notification(message, 'low')


# =========================
# Rate Limiter & Results Tracker
# =========================
class RateLimiter:
    """
    Simple rate controller that tracks an adjustable 'rate' (requests/sec).
    Not a hard token bucket - executor's max_workers handles concurrency.
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
        with self._rate_lock:
            self._completed += 1

    def sleep_if_needed(self):
        """Crude pacing to avoid too-aggressive bursts."""
        current_rate = self.get_rate()
        if current_rate <= 0:
            time.sleep(0.1)  # Small delay if rate is invalid
            return
        target_interval = 1.0 / max(0.1, current_rate)  # seconds between starts
        now = time.time()
        since = now - self._last_tick
        if since < target_interval and since >= 0:
            sleep_time = target_interval - since
            if sleep_time > 0:
                time.sleep(min(sleep_time, 60.0))  # Cap sleep to prevent infinite waits
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
            if isinstance(result.get('headers_sent'), dict):
                result = dict(result)  # Create a copy to avoid modifying original
                result['headers_sent'] = _redact_headers(result.get('headers_sent', {}))
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

    def get_metrics(self) -> Dict:
        with self._lock:
            return dict(self._metrics)

# =========================
# CSV reading & request execution
# =========================
def read_csv_with_validation(csv_path: str, chunk_size: int = 1000, skip_rows: int = 0):
    """Read a single CSV, validate rows, and return (webhooks, stats)"""
    # Allow env to override only if caller passed the default
    if chunk_size == 1000:
        try:
            chunk_size = int(os.getenv("CSV_CHUNK_SIZE", "1000"))
        except ValueError:
            chunk_size = 1000
    
    webhooks: List[Dict] = []
    stats = {
        'file': csv_path,
        'file_size': os.path.getsize(csv_path) if os.path.exists(csv_path) else 0,
        'valid_rows': 0,
        'invalid_rows': 0,
        'skipped_rows': max(0, int(skip_rows)),
    }
    
    if not os.path.exists(csv_path):
        raise ValueError(f"CSV file not found: {csv_path}")
    
    enc = _detect_encoding(csv_path)

    try:
        with open(csv_path, 'r', encoding=enc, newline='') as f:
            delim = _detect_delimiter(f)
            try:
                reader = csv.DictReader(f, delimiter=delim)
                raw_headers = reader.fieldnames or []
            except Exception as e:
                raise ValueError(f"Failed to parse CSV headers in {csv_path}: {e}")
            headers = _normalize_headers(raw_headers)

            # validate required columns
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

                nrow = {name_map.get(k, k if k else ''): (v if v else '').strip() for k, v in row.items()}
                url = nrow.get('webhook_url', '').strip()
                if not url:
                    stats['invalid_rows'] += 1
                    continue
                if not _is_valid_url(url):
                    # Try to normalize by adding scheme only if missing
                    if not url.startswith(('http://', 'https://')):
                        candidate = 'https://' + url
                        if _is_valid_url(candidate):
                            url = candidate
                        else:
                            stats['invalid_rows'] += 1
                            continue
                    else:
                        stats['invalid_rows'] += 1
                        continue
                nrow['webhook_url'] = url

                method = (nrow.get('method') or 'GET').upper()

                # Normalize header columns from CSV: accept 'header', 'headers', or 'headers_sent'
                headers_raw = nrow.get('header') or nrow.get('headers') or nrow.get('headers_sent')
                headers_dict = {}
                if headers_raw is not None:
                    if isinstance(headers_raw, dict):
                        # Validate and sanitize key/value lengths & characters
                        for k, v in list(headers_raw.items())[:MAX_HEADER_COUNT]:
                            if isinstance(k, str) and isinstance(v, (str, int, float)):
                                k_clean = ''.join(c for c in str(k).strip() if c.isprintable() and c not in '\r\n')
                                v_clean = ''.join(c for c in str(v).strip() if c.isprintable() and c not in '\r\n')
                                if k_clean and len(k_clean) <= 100 and len(v_clean) <= 1000:
                                    headers_dict[k_clean] = v_clean
                    else:
                        header_text = str(headers_raw).strip()
                        if len(header_text) > MAX_HEADER_SIZE:
                            logger.warning(f"Header too large for URL {url}, truncating")
                            header_text = header_text[:MAX_HEADER_SIZE]
                        try:
                            parsed = json.loads(header_text)
                            if isinstance(parsed, dict):
                                for k, v in list(parsed.items())[:MAX_HEADER_COUNT]:
                                    if isinstance(k, str) and isinstance(v, (str, int, float)):
                                        k_clean = ''.join(c for c in str(k).strip() if c.isprintable() and c not in '\r\n')
                                        v_clean = ''.join(c for c in str(v).strip() if c.isprintable() and c not in '\r\n')
                                        if k_clean and len(k_clean) <= 100 and len(v_clean) <= 1000:
                                            headers_dict[k_clean] = v_clean
                        except Exception:
                            # Fallback: "Key: Val; Key2: Val2"
                            try:
                                parts = header_text.split(';')[:MAX_HEADER_COUNT]
                                for part in parts:
                                    if ':' in part:
                                        k, v = part.split(':', 1)
                                        k_clean = ''.join(c for c in k.strip() if c.isprintable() and c not in '\r\n')
                                        v_clean = ''.join(c for c in v.strip() if c.isprintable() and c not in '\r\n')
                                        if k_clean and len(k_clean) <= 100 and len(v_clean) <= 1000:
                                            headers_dict[k_clean] = v_clean
                            except Exception:
                                pass

                payload = None
                if nrow.get('payload'):
                    ptxt = nrow['payload'].strip()
                    if ptxt:
                        MAX_PAYLOAD_SIZE = int(os.getenv('MAX_PAYLOAD_SIZE_BYTES', '1048576'))  # 1MB default
                        if len(ptxt) > MAX_PAYLOAD_SIZE:
                            logger.warning(f"Payload too large for {url}, truncating from {len(ptxt)} to {MAX_PAYLOAD_SIZE} bytes")
                            ptxt = ptxt[:MAX_PAYLOAD_SIZE]
                        try:
                            payload = json.loads(ptxt)
                        except json.JSONDecodeError:
                            # fallback: send as raw text for endpoints expecting non-JSON
                            payload = ptxt

                webhooks.append({
                    'url': url,
                    'method': method,
                    'payload': payload,
                    'headers': headers_dict,        # normalized
                    'header': headers_dict,         # backward-compat
                    'headers_sent': headers_dict,   # backward-compat
                    'name': nrow.get('name') or url,
                    'group': nrow.get('group') or ''
                })
                stats['valid_rows'] += 1

    except IOError as e:
        raise ValueError(f"Cannot read CSV file {csv_path}: {e}")

    return webhooks, stats

def read_multiple_csv_files(file_patterns: List[str] = None, chunk_size: int = 1000, skip_rows: int = 0):
    """Enhanced multi-file CSV reader with pattern matching"""
    # Allow environment overrides (comma-separated)
    if not file_patterns:
        raw_patterns = os.getenv("CSV_FILE_PATTERNS", "/app/data/csv/*.csv,/app/data/*.csv")
        file_patterns = [p.strip() for p in raw_patterns.split(",") if p.strip()]

    # Allow env overrides for chunk/skip (preserve explicit args if passed)
    if chunk_size == 1000:
        try:
            chunk_size = int(os.getenv("CSV_CHUNK_SIZE", chunk_size))
        except ValueError:
            pass
    if skip_rows == 0:
        try:
            skip_rows = int(os.getenv("SKIP_ROWS", skip_rows))
        except ValueError:
            pass

    found_files = []
    for pattern in file_patterns:
        found_files.extend(glob.glob(pattern))

    # De-dup, drop vanished files, and sort newest first
    found_files = sorted({f for f in found_files if os.path.exists(f)}, key=os.path.getmtime, reverse=True)

    if not found_files:
        logger.error(f"No CSV files found matching patterns: {file_patterns}")
        return [], {}

    logger.info(f"Found {len(found_files)} CSV file(s): {[os.path.basename(f) for f in found_files]}")

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
            logger.info(f"Processing {os.path.basename(csv_file)}...")
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
            logger.error(f"Error processing {csv_file}: {e}")
            continue

    return all_webhooks, combined_stats

def _parse_retry_after_seconds(header_value: str, default: float) -> float:
    """
    Parses Retry-After header per RFC 7231.
    - If integer, treat as seconds.
    - If HTTP-date, compute delta from now.
    """
    if not header_value:
        return default
    
    try:
        # integer seconds case
        secs = int(header_value.strip())
        if secs < 0:
            return default
        return float(min(MAX_BACKOFF_SECONDS, secs))
    except (ValueError, AttributeError):
        pass

    # Try HTTP-date format
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(header_value)
        if dt:
            now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.utcnow()
            delay = (dt - now).total_seconds()
            # Clamp to sane minimum/maximum before returning
            return min(MAX_BACKOFF_SECONDS, max(default, max(0.1, delay)))
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
    attempt_start: int = 0,
    backoff_override: Optional[float] = None,
) -> int:
    """
    Executes one HTTP request with basic retry/backoff.
    Returns 0 on success, 1 on error (for moving average error window).
    """
    # Normalize headers param to a clean dict[str, str]
    if not isinstance(headers, dict):
        headers = {}
    else:
        try:
            headers = {str(k): (v if isinstance(v, str) else str(v)) for k, v in headers.items()}
        except Exception:
            headers = dict(headers)

    attempt = int(attempt_start or 0)
    # Base backoff (from env), minimum 0.1s to avoid hot loops if user sets 0
    base_backoff = max(0.1, float(retry_delay if retry_delay is not None else 1.0))
    backoff = backoff_override if backoff_override is not None else base_backoff
    error_code = 1  # assume failure until success

    while attempt <= max_retries:
        attempt += 1
        acquired = False
        if global_request_sema:
            try:
                # Avoid indefinite blocking if the semaphore can't be acquired
                acquire_timeout = max(1.0, float(os.getenv('SEMAPHORE_ACQUIRE_TIMEOUT', '30')))
            except (ValueError, TypeError):
                acquire_timeout = 30.0
            try:
                if not global_request_sema.acquire(timeout=acquire_timeout):
                    # Back off briefly and re-attempt this loop iteration
                    time.sleep(min(MAX_BACKOFF_SECONDS, base_backoff))
                    continue
                acquired = True
            except Exception:
                # If semaphore is misconfigured, proceed without it rather than deadlock
                acquired = False

        try:
            rate_limiter.sleep_if_needed()
            timeout = max(1, int(timeout or 0))
            # Optional connect/read split via env (keeps old behavior if unset)
            try:
                ct_env = int(os.getenv('REQUEST_CONNECT_TIMEOUT', '0') or '0')
            except Exception:
                ct_env = 0
            req_timeout = (min(ct_env, timeout), timeout) if ct_env > 0 else timeout
            started = time.time()
            method_u = (method or 'GET').upper()
            req_size = 0
            resp_size = 0

            if method_u in ('POST', 'PUT', 'PATCH'):
                if isinstance(payload, (dict, list)):
                    try:
                        data = json.dumps(payload)
                        req_size = len(data.encode('utf-8'))
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Failed to serialize payload for {url}: {e}")
                        data = str(payload)
                        req_size = len(data.encode('utf-8'))
                    http_session = get_http_session()
                    resp = http_session.request(
                        method_u, url, data=data,
                        headers={'Content-Type': 'application/json', **(headers or {})},
                        timeout=req_timeout
                    )
                elif payload is not None:
                    data = str(payload)
                    req_size = len(data.encode('utf-8'))
                    http_session = get_http_session()
                    resp = http_session.request(
                        method_u, url, data=data,
                        headers={'Content-Type': 'text/plain', **(headers or {})},
                        timeout=req_timeout
                    )
                else:
                    http_session = get_http_session()
                    resp = http_session.request(method_u, url, headers=headers or {}, timeout=req_timeout)
            else:
                if isinstance(payload, dict) and method_u == 'GET':
                    http_session = get_http_session()
                    resp = http_session.get(url, params=payload, headers=headers or {}, timeout=req_timeout)
                else:
                    http_session = get_http_session()
                    resp = http_session.request(method_u, url, headers=headers or {}, timeout=req_timeout)

            elapsed = time.time() - started
            try:
                resp_size = len(resp.content or b'')
            except Exception as e:
                logger.debug(f"Error getting response size: {e}")
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
                'response_preview': (resp.text[:1000] if (not ok and getattr(resp, "text", None)) else ''),
                'request_size': req_size,
                'response_size': resp_size,
                'headers_sent': headers or {},
            }
            results_tracker.record(result_row)

            if ok:
                error_code = 0
                return 0

            # Retry policy - 429 or >=500 are usually retryable; 4xx (except 429) are not.
            if resp.status_code == 429 or resp.status_code >= 500:
                # Respect Retry-After if provided (seconds or HTTP-date)
                retry_after_hdr = (resp.headers or {}).get('Retry-After')
                if retry_after_hdr:
                    sleep_s = _parse_retry_after_seconds(retry_after_hdr, default=backoff)
                    sleep_s = min(MAX_BACKOFF_SECONDS, max(0.1, sleep_s))
                else:
                    sleep_s = backoff
                # Light jitter (±50%) to avoid thundering herd; caps preserved
                j = 0.5 + random.random()  # 0.5..1.5
                sleep_j = min(MAX_BACKOFF_SECONDS, max(0.1, sleep_s * j))
                next_backoff = min(MAX_BACKOFF_SECONDS, max(0.1, backoff * 2))
                return {
                    '_retry_in': sleep_j,
                    '_next_backoff': next_backoff,
                    'attempt': attempt
                }
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
            # Do not update progress here; caller will retry and count final outcome
            j = 0.5 + random.random()
            next_backoff = min(MAX_BACKOFF_SECONDS, max(0.1, backoff * 2))
            return {
                '_retry_in': min(MAX_BACKOFF_SECONDS, max(0.1, backoff * j)),
                '_next_backoff': next_backoff,
                'attempt': attempt
            }
        finally:
            if global_request_sema and acquired:
                try:
                    global_request_sema.release()
                except Exception as e:
                    logger.warning(f"Semaphore release error: {e}")

    # If we exit the retry loop without returning, it means max retries exceeded
    return error_code if isinstance(error_code, int) else 1


# =========================
# File move helper
# =========================
def move_file_reasoned(file_path: str, target_dir: str, reason_prefix: str = "") -> bool:
    """Move file to target directory with timestamp and reason prefix"""

    def is_within_prefix(path, prefix):
        try:
            return os.path.commonpath([path, prefix]) == prefix
        except ValueError:
            return False  # Handles mismatched drives on Windows

    try:
        file_path = os.path.abspath(os.path.realpath(file_path))
        target_dir = os.path.abspath(os.path.realpath(target_dir))

        if not target_dir:
            logger.error(f"Invalid target directory: {target_dir}")
            return False
        if not os.path.exists(file_path):
            logger.debug(f"File no longer exists: {file_path}")
            return False
        if not os.path.isfile(file_path):
            logger.error(f"Path is not a file: {file_path}")
            return False

        allowed_prefixes = [os.path.abspath(p) for p in ['/app/data/', '/tmp/', os.getcwd()]]
        if not any(is_within_prefix(file_path, p) for p in allowed_prefixes) or \
           not any(is_within_prefix(target_dir, p) for p in allowed_prefixes):
            logger.error(f"Path outside allowed: {file_path} -> {target_dir}")
            return False

        if os.path.dirname(file_path) == target_dir:
            logger.debug(f"Already in target: {file_path}")
            return True

        os.makedirs(target_dir, exist_ok=True)
        filename = os.path.basename(file_path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        reason_prefix_clean = "".join(c for c in reason_prefix if c.isalnum() or c in ('-', '_')).rstrip('_')
        base_name = f"{ts}_{reason_prefix_clean}_{filename}" if reason_prefix_clean else f"{ts}_{filename}"
        target_path = os.path.join(target_dir, base_name)

        counter = 1
        while os.path.exists(target_path):
            base, ext = os.path.splitext(base_name)
            target_path = os.path.join(target_dir, f"{base}_{counter}{ext}")
            counter += 1

        shutil.move(file_path, target_path)

        logger.info(f"Moved file: {filename} -> {os.path.basename(target_path)} [{reason_prefix_clean}]")
        return True

    except Exception as e:
        logger.error(f"Failed to move {file_path} to {target_dir}: {e}")
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
                rows = cursor.fetchall()
                self.processed_files = {row[0] for row in rows if row and row[0]}
                logger.info(f"Loaded {len(self.processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {e}")
            self.processed_files = set()  # Initialize to empty set on error

    def _is_valid_csv_file(self, file_path: str) -> bool:
        """Consolidated file validation with proper error handling"""
        try:
            if not file_path.lower().endswith('.csv'):
                logger.warning(f"Ignoring non-CSV file {file_path}")
                self._handle_invalid_file(file_path, "invalid_not-csv_", 'rejected_not_csv')
                return False

            if not os.path.exists(file_path):
                logger.warning(f"File disappeared during validation: {file_path}")
                return False

            initial_size = os.path.getsize(file_path)
            if initial_size == 0:
                # Give the writer time to actually write data
                debounce = float(self.config.get('watchdog', {}).get('debounce_delay', 3.0))
                debounce = max(0.5, min(10.0, debounce))  # clamp 0.5..10s
                time.sleep(debounce)
                
                # Re-check after wait
                if not os.path.exists(file_path):
                    return False
                    
                size_after_wait = os.path.getsize(file_path)
                if size_after_wait == 0:
                    logger.warning(f"File {file_path} is empty after debounce")
                    self._handle_invalid_file(file_path, "invalid_empty-file_", 'rejected_empty_file')
                    return False

            # Adaptive delay based on file size, with reasonable bounds
            delay = min(3.0, max(0.5, initial_size / (1024 * 1024)))  # 0.5s to 3s based on MB
            time.sleep(delay)
            
            if not os.path.exists(file_path):
                return False
                
            final_size = os.path.getsize(file_path)
            if initial_size != final_size:
                logger.info(f"File {file_path} is still being written, skipping for now")
                return False

            # Validate CSV structure and content
            return self._validate_csv_content(file_path)

        except Exception as e:
            logger.error(f"Error validating CSV file {file_path}: {e}")
            return False

    def _validate_csv_content(self, file_path: str) -> bool:
        """Validate CSV structure and webhook_url column"""
        try:
            enc = _detect_encoding(file_path)
            with open(file_path, 'r', encoding=enc, newline='') as f:
                delim = _detect_delimiter(f)
                reader = csv.DictReader(f, delimiter=delim)
                raw_headers = reader.fieldnames or []
                headers = _normalize_headers(raw_headers)
                
                if 'webhook_url' not in headers:
                    logger.warning(f"File {file_path} missing required 'webhook_url' column (detected headers={headers})")
                    self._handle_invalid_file(file_path, "invalid_missing-webhook_url_", 'rejected_missing_webhook_url')
                    return False

                # Check for at least one valid webhook URL
                name_map = {orig: norm for orig, norm in zip(raw_headers, headers)}
                has_valid_data = False
                for row in reader:
                    nrow = {name_map.get(k, k or ''): (v or '').strip() for k, v in row.items()}
                    url = nrow.get('webhook_url', '').strip()
                    if url:
                        # Normalize URL if needed
                        if not url.startswith(('http://', 'https://')):
                            url = 'https://' + url
                        if _is_valid_url(url):
                            has_valid_data = True
                            break

                if not has_valid_data:
                    logger.warning(f"File {file_path} has no valid webhook_url values")
                    self._handle_invalid_file(file_path, "rejected_empty-webhook-url_", 'rejected_empty_webhook_url')
                    return False

                return True

        except Exception as e:
            logger.error(f"CSV content validation failed for {file_path}: {e}")
            return False

    def _handle_invalid_file(self, file_path: str, reason_prefix: str, db_status: str):
        """Consolidated handling for invalid files"""
        try:
            # Calculate breadcrumbs before move
            file_hash = _calculate_file_hash(file_path)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            self.db_manager.track_file(file_path, file_hash or '', file_size, rows_count=0)

            # Move to rejected directory if configured
            if self.config.get('csv', {}).get('archive_on_validation_failure', True):
                rejected_path = self.config.get('csv', {}).get('rejected_path', '')
                if rejected_path:
                    move_file_reasoned(file_path, rejected_path, reason_prefix=reason_prefix)

            # Update database status
            self.db_manager.update_file_status(file_path, db_status)

        except Exception as e:
            logger.debug(f"Error handling invalid file {file_path}: {e}")

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
        """Enhanced file event handling with proper race condition prevention"""
        try:
            with self.file_lock:
                file_path = os.path.abspath(file_path)
                
                # Check inflight status first to prevent race conditions
                with self.inflight_lock:
                    if file_path in self.inflight_files:
                        logger.debug(f"Already pending (in-memory): {file_path} - event {event_type} ignored")
                        return
                
                if not self._is_valid_csv_file(file_path):
                    return
                    
                if not os.path.exists(file_path):
                    logger.warning(f"File disappeared during processing: {file_path}")
                    return
                    
                file_hash = _calculate_file_hash(file_path)
                if not file_hash:
                    logger.warning(f"Could not calculate hash for {file_path}, skipping")
                    return

                # Check for duplicates
                if self.db_manager.is_file_processed(file_path, file_hash):
                    logger.info(f"Duplicate content detected for {os.path.basename(file_path)} (hash match).")
                    self._handle_duplicate_file(file_path, file_hash, "dup_")
                    return

                if self.db_manager.is_hash_processed(file_hash):
                    logger.info(f"Duplicate content detected (hash match with previously completed file).")
                    self._handle_duplicate_file(file_path, file_hash, "dup_hash_")
                    return

                # Check current status to prevent race conditions
                status = self.db_manager.get_file_status(file_path)
                if status in ('queued', 'processing'):
                    logger.debug(f"Already {status} (DB): {file_path} - event {event_type} ignored")
                    return

                # Atomically mark as queued and add to inflight
                with self.inflight_lock:
                    # Double-check inflight status
                    if file_path in self.inflight_files:
                        logger.debug(f"Already pending (in-memory): {file_path} - event {event_type} ignored")
                        return

                    # Double-check DB status
                    current_status = self.db_manager.get_file_status(file_path)
                    if current_status in ('queued', 'processing'):
                        logger.debug(f"Already {current_status} (DB): {file_path} - event {event_type} ignored")
                        return

                    # Mark as queued and add to inflight atomically
                    self.db_manager.update_file_status(file_path, 'queued')
                    self.inflight_files.add(file_path)

                # Prepare job info
                try:
                    file_stats = os.stat(file_path)
                    file_size = file_stats.st_size
                    row_count = _count_csv_rows(file_path)
                except Exception as e:
                    logger.warning(f"Error getting file stats for {file_path}: {e}")
                    file_size = 0
                    row_count = 0

                self.db_manager.track_file(file_path, file_hash, file_size, row_count)

                job_info = {
                    'csv_file': file_path,
                    'file_hash': file_hash,
                    'file_size': file_size,
                    'row_count': row_count,
                    'event_type': event_type,
                    'detected_at': datetime.now().isoformat()
                }

                # Add to processing queue
                try:
                    max_queue_size = self.config.get('watchdog', {}).get('max_queue_size', 100)
                    if self.job_queue.qsize() >= max_queue_size:
                        logger.warning(f"Queue full ({max_queue_size}), skipping file: {os.path.basename(file_path)}")
                        self.db_manager.update_file_status(file_path, 'queue_full')
                        with self.inflight_lock:
                            self.inflight_files.discard(file_path)
                        return

                    self.job_queue.put_nowait(job_info)
                    logger.info(f"New CSV file detected: {os.path.basename(file_path)} ({row_count} rows, {file_size} bytes) via {event_type}")

                except queue.Full:
                    logger.error(f"Queue full, cannot process: {os.path.basename(file_path)}")
                    with self.inflight_lock:
                        self.inflight_files.discard(file_path)
                    self.db_manager.update_file_status(file_path, 'queue_full')

        except Exception as e:
            logger.error(f"Error handling file event for {file_path}: {e}")
            logger.error(traceback.format_exc())

    def _handle_duplicate_file(self, file_path: str, file_hash: str, reason_prefix: str):
        """Handle duplicate file detection and archival"""
        try:
            dup_dir = self.config.get('csv', {}).get('duplicates_path', '')
            if dup_dir:
                move_file_reasoned(file_path, dup_dir, reason_prefix=reason_prefix)
            else:
                logger.warning("No duplicates_path configured, cannot move duplicate file")

            # Track in database
            try:
                file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                self.db_manager.track_file(file_path, file_hash, file_size, rows_count=0)
                self.db_manager.update_file_status(file_path, 'skipped_duplicate')
            except Exception as e:
                logger.debug(f"Error tracking duplicate file: {e}")

        except Exception as e:
            logger.error(f"Error handling duplicate file {file_path}: {e}")


# =========================
# Archiving helpers
# =========================
def generate_job_id(prefix: str = "job") -> str:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hash_part = hashlib.md5(f"{timestamp}{time.time()}".encode()).hexdigest()[:8]
    return f"{prefix}_{timestamp}_{hash_part}"

def archive_processed_file(file_path: str, config: Dict) -> bool:
    try:
        if not file_path or not os.path.exists(file_path):
            logger.warning(f"Cannot archive non-existent file: {file_path}")
            return False
        csv_config = config.get('csv', {}) or {}
        if not csv_config.get('archive_processed', False):
            return False
        archive_path = csv_config.get('archive_path', '/app/data/csv/processed')
        os.makedirs(archive_path, exist_ok=True)
        filename = os.path.basename(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        archived_filename = f"{timestamp}_{filename}"
        archived_path = os.path.join(archive_path, archived_filename)
        shutil.move(file_path, archived_path)
        logger.info(f"Archived processed file: {filename} -> {archived_filename}")
        return True
    except Exception as e:
        logger.error(f"Error archiving file {file_path}: {e}")
        return False
# =========================
# BulkAPITrigger
# =========================
class BulkAPITrigger:
    def __init__(self, config: Dict):
        self.config = config or {}
        self.inflight_lock = Lock()
        self.inflight_files: Set[str] = set()
        try:
            gmax = int(os.getenv('GLOBAL_MAX_REQUESTS', '0') or '0')
        except (ValueError, TypeError):
            gmax = 0
        self.global_request_sema = Semaphore(gmax) if gmax > 0 else None
        db_path = (config.get('database', {}) or {}).get('path', '/app/data/webhook_results.db')
        self.db_manager = DatabaseManager(db_path)
        self.notification_manager = NotificationManager(config.get('notifications', {}))
        max_queue = (config.get('watchdog', {}) or {}).get('max_queue_size', 100)
        self.job_queue = queue.Queue(maxsize=max_queue)
        self.watchdog_enabled = (config.get('watchdog', {}) or {}).get('enabled', False)
        self.observer = None
        self.processing_threads: List[Thread] = []
        self.shutdown_event = Event()
    
    def _clear_resume_marker(self, file_path: str, file_hash: str) -> None:
        """Clear resume marker for completed file processing"""
        _clear_resume_marker(file_path, file_hash, self.config)

    def _load_resume_rows(self, file_path: str, file_hash: str) -> int:
        """Load resume position for file processing"""
        return _load_resume_rows(file_path, file_hash, self.config)
    
    def trigger_webhooks(self, 
                        csv_files: List[str] = None, 
                        job_name: str = None,
                        skip_rows: int = 0,
                        triggered_by: str = 'manual') -> str:
        """Main method to trigger bulk webhooks with enhanced features"""
        if skip_rows == 0:
                try:
                    skip_rows = int(os.getenv("SKIP_ROWS", "0"))
                except ValueError:
                    pass
        job_id = generate_job_id()
        if not job_name:
            job_name = f"Bulk API Job {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        job_logger = logging.LoggerAdapter(logger, {"job_id": job_id, "job_name": job_name})
        job_logger.info("🚀 Starting job")
        try:
            # Load webhooks - explicit arg > .env CSV_FILE > AUTO patterns
            env_csv = os.getenv("CSV_FILE", "AUTO").strip()
            env_csv_is_auto = env_csv.upper() == "AUTO"

            # normalize caller input (can be list/tuple/str)
            target_from_arg = None
            if csv_files:
                if isinstance(csv_files, (list, tuple)) and len(csv_files) > 0:
                    first = str(csv_files[0]).strip()
                    if first and first.upper() != "AUTO":
                        target_from_arg = first
                elif isinstance(csv_files, str):
                    s = csv_files.strip()
                    if s and s.upper() != "AUTO":
                        target_from_arg = s

            try:
                chunk_size_env = int(os.getenv("CSV_CHUNK_SIZE", "1000"))
            except ValueError:
                chunk_size_env = 1000

            if target_from_arg:
                all_webhooks, stats = read_csv_with_validation(target_from_arg, chunk_size=chunk_size_env, skip_rows=skip_rows)
                csv_file_path = target_from_arg
            elif not env_csv_is_auto:
                all_webhooks, stats = read_csv_with_validation(env_csv, chunk_size=chunk_size_env, skip_rows=skip_rows)
                csv_file_path = env_csv
            else:
                all_webhooks, stats = read_multiple_csv_files(skip_rows=skip_rows)
                files_count = (stats or {}).get('files_processed', 0) if isinstance(stats, dict) else 0
                csv_file_path = f"Multiple files ({files_count} files)"
            
            resume_file_hash = ""
            resume_enabled = self.config.get('resume', {}).get('enabled', True)
            resume_checkpoint_interval = self.config.get('resume', {}).get('checkpoint_interval', 100)
            
            try:
                # Only meaningful if we are processing a single concrete file path
                if csv_file_path and isinstance(csv_file_path, str) and os.path.isfile(csv_file_path):
                    resume_file_hash = _calculate_file_hash(csv_file_path) or ""
                    
                    # Load existing resume position if available and not explicitly skipping
                    if resume_file_hash and skip_rows == 0 and resume_enabled:
                        existing_skip = _load_resume_rows(csv_file_path, resume_file_hash, self.config)
                        if existing_skip > 0:
                            # Validate that resume position is valid
                            total_rows_in_file = _count_csv_rows(csv_file_path)
                            if existing_skip < total_rows_in_file:
                                logger.info(f"Resuming from row {existing_skip}/{total_rows_in_file} based on previous progress")
                                skip_rows = existing_skip
                                
                                # Adjust webhooks list if already loaded
                                if all_webhooks and existing_skip > 0:
                                    all_webhooks = all_webhooks[existing_skip:]
                                    logger.info(f"Adjusted webhook list to start from position {existing_skip}")
                            else:
                                logger.warning(f"Resume position {existing_skip} >= total rows {total_rows_in_file}, starting from beginning")
                                _clear_resume_marker(csv_file_path, resume_file_hash, self.config)
                                skip_rows = 0
            except Exception as e:
                logger.error(f"Resume initialization failed: {e}")
                # Continue without resume on error
                resume_enabled = False
            
            if not all_webhooks:
                # Be explicit to avoid downstream division by zero or progress at 1%
                message = f"No valid rows found in {csv_file_path}; skipping job {job_id}."
                try:
                    job_logger.warning(message)
                except NameError:
                    logger.warning(message)
                # Soft notifications if available; never fail the job for this
                try:
                    if self.notification_manager.slack_config.get('enabled'):
                        self.notification_manager.send_slack_notification(message, 'low')
                except Exception:
                    pass
                return job_id
            
            total_requests = len(all_webhooks)
            logger.info(f"Loaded {total_requests} webhook requests")
            
            # Initialize job in database with correct totals for resume cases
            effective_total = len(all_webhooks)  # This reflects actual work to be done
            self.db_manager.start_job(job_id, job_name, csv_file_path, effective_total, self.config, triggered_by)
            # Update local total_requests to match what we're actually processing
            total_requests = effective_total

            # Setup rate limiting and tracking
            rate_config = self.config.get('rate_limiting', {})
            rate_limiter = RateLimiter(
                rate_config.get('starting_rate_limit', 3.0),
                rate_config.get('max_workers', 3)
            )
            results_tracker = EnhancedResultsTracker(job_id, self.db_manager)
            
            error_window: List[int] = []
            MAX_ERROR_WINDOW = min(100, rate_config.get('window_size', 20) * 2)  # Cap based on config
            successful_count = 0
            failed_count = 0
            processed_count = 0
            
            logger.info(f"Processing {total_requests} requests with {rate_config.get('max_workers', 3)} workers")
            
            progress_enabled = (
                self.notification_manager.slack_config.get('enabled')
                and os.getenv("SLACK_NOTIFY_PROGRESS", "false").lower() == "true"
            )

            min_pct   = float(os.getenv("SLACK_PROGRESS_MIN_PCT", "5"))        # gate on any notification at all
            first_pct = float(os.getenv("SLACK_PROGRESS_FIRST_AT_N", "10"))    # first ping at >= this %
            step_pct  = float(os.getenv("SLACK_PROGRESS_EVERY_N", "5"))        # subsequent pings every this many %

            _last_notified_bucket = -1.0  # local state to prevent duplicates
            
            with ThreadPoolExecutor(max_workers=rate_config.get('max_workers', 3)) as executor:
                pbar = None
                try:
                    resume_desc = f"API Requests (Resumed from {skip_rows})" if skip_rows > 0 else "API Requests"
                    pbar = tqdm(
                        total=total_requests,
                        desc=resume_desc,
                        dynamic_ncols=False,
                        ascii=True,
                        file=sys.stdout,
                        mininterval=1.0,
                        bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                        disable=not (sys.stdout.isatty() or os.getenv("FORCE_TQDM","0") == "1")
                    )

                    futures: Dict = {}

                    # retry heap entries: (run_at_epoch, seq, webhook_dict, attempt_int, next_backoff_float)
                    retry_heap: List[Tuple[float, int, Dict, int, float]] = []
                    _seq = 0  # tie-breaker for heap

                    def _submit_now(webhook: Dict, attempt: int = 0, next_backoff: Optional[float] = None):
                        if SHUTDOWN_EVENT.is_set():
                            return None
                        fut = executor.submit(
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
                            self.config.get('retry', {}).get('retry_delay', 1.0),
                            attempt,
                            next_backoff
                        )
                        futures[fut] = webhook
                        return fut

                    # seed initial submissions
                    for webhook in all_webhooks:
                        _submit_now(webhook, attempt=0, next_backoff=None)

                    # main loop: process completions and submit due retries
                    while (futures or retry_heap) and not SHUTDOWN_EVENT.is_set():
                        # submit any due retries
                        now = time.time()
                        while retry_heap and retry_heap[0][0] <= now and not SHUTDOWN_EVENT.is_set():
                            _, _, wh, att, nb = heapq.heappop(retry_heap)
                            _submit_now(wh, attempt=att, next_backoff=nb)

                        if not futures:
                            # nothing in flight; sleep until next retry due or exit
                            sleep_for = max(0.05, (retry_heap[0][0] - time.time()) if retry_heap else 0.1)
                            time.sleep(min(0.5, sleep_for))
                            continue

                        done, _ = wait(list(futures.keys()), timeout=0.5, return_when=FIRST_COMPLETED)
                        if not done:
                            continue

                        for future in done:
                            webhook = futures.pop(future)
                            try:
                                result = future.result()
                                # Retry contract: dict with '_retry_in' schedules another attempt
                                if isinstance(result, dict) and (result.get('_retry_in') is not None):
                                    # Parse defensively
                                    retry_hint = result.get('_retry_in')
                                    try:
                                        delay = float(retry_hint)
                                    except (TypeError, ValueError):
                                        delay = 0.0
                                    try:
                                        attempt_val = int(result.get('attempt', 0))
                                    except (TypeError, ValueError):
                                        attempt_val = 0
                                    try:
                                        next_backoff = float(result.get('_next_backoff', self.config.get('retry', {}).get('retry_delay', 1.0)))
                                    except (TypeError, ValueError):
                                        next_backoff = float(self.config.get('retry', {}).get('retry_delay', 1.0))

                                    # Bound delay to reasonable range
                                    delay = min(MAX_BACKOFF_SECONDS, max(0.1, delay))
                                    max_retries_cfg = int(self.config.get('retry', {}).get('max_retries', 3))

                                    if attempt_val < max_retries_cfg and not SHUTDOWN_EVENT.is_set():
                                        # Limit retry heap size to prevent memory issues
                                        if len(retry_heap) < 10000:
                                            _seq += 1
                                            heapq.heappush(retry_heap, (time.time() + delay, _seq, webhook, attempt_val, next_backoff))
                                        else:
                                            logger.warning(f"Retry heap full, dropping retry for {webhook['url']}")
                                            failed_count += 1
                                            processed_count += 1
                                            if pbar:
                                                pbar.update(1)
                                    else:
                                        # exhausted retries - finalize as failure
                                        error_window.append(1)
                                        window_size = min(rate_config.get('window_size', 20), MAX_ERROR_WINDOW)
                                        if len(error_window) > window_size:
                                            error_window[:] = error_window[-window_size:]
                                        processed_count += 1
                                        failed_count += 1
                                        if pbar:
                                            pbar.update(1)
                                    continue  # handle next completed future

                                # normal path: result is an int error flag
                                error = int(result)
                                error_window.append(error)
                                window_size = min(rate_config.get('window_size', 20), MAX_ERROR_WINDOW)
                                if len(error_window) > window_size:
                                    error_window[:] = error_window[-window_size:]

                                processed_count += 1
                                if error == 0:
                                    successful_count += 1
                                else:
                                    failed_count += 1

                                if pbar:
                                    pbar.update(1)
                                rate_limiter.tick()
                                
                                # Save resume checkpoint at configured interval
                                if resume_file_hash and resume_enabled and (processed_count % resume_checkpoint_interval == 0):
                                    try:
                                        # 'skip_rows' is the caller-provided initial skip; add what we've finished now
                                        checkpoint_position = skip_rows + processed_count
                                        _save_resume_rows(csv_file_path, resume_file_hash, checkpoint_position, self.config)
                                        logger.debug(f"Resume checkpoint saved at position {checkpoint_position}")
                                    except Exception as e:
                                        logger.error(f"Resume checkpoint save failed: {e}")

                                # periodic progress logging
                                PROGRESS_EVERY_N = int(os.getenv("PROGRESS_EVERY_N", "50"))
                                if processed_count % PROGRESS_EVERY_N == 0:
                                    success_rate = (successful_count / max(1, processed_count)) * 100
                                    throughput = rate_limiter.get_throughput()
                                    logger.info(
                                        f"Progress: {processed_count}/{total_requests} "
                                        f"({success_rate:.1f}% success, {throughput:.2f} req/s)"
                                    )
                                
                                # percent-only throttled progress notification
                                if progress_enabled and total_requests > 0 and processed_count > 0:
                                    pct_done = (processed_count / max(1, total_requests)) * 100.0

                                    if pct_done >= min_pct:
                                        # decide the "bucket" to notify at (first at >= first_pct, then every step_pct)
                                        if pct_done < first_pct:
                                            bucket = -1.0
                                        else:
                                            safe_step_pct = max(0.1, step_pct)  # Prevent division by zero
                                            step_index = max(0, int((pct_done - first_pct) // safe_step_pct))
                                            bucket = first_pct + step_index * safe_step_pct

                                        # send once per bucket
                                        if bucket >= 0 and bucket > _last_notified_bucket:
                                            urgency = os.getenv("SLACK_PROGRESS_URGENCY", "auto")
                                            if urgency == "auto":
                                                urgency = _slack_urgency_for_progress(successful_count, processed_count)

                                            # Show context for resume vs fresh start
                                            if skip_rows > 0:
                                                actual_file_position = skip_rows + processed_count
                                                msg = (
                                                    f"🔄 *{job_name}* in progress (Resume)\n"
                                                    f"📍 File Position: {actual_file_position} (Skip: {skip_rows} + Processed: {processed_count})\n"
                                                    f"⚡ Current Batch: {processed_count}/{total_requests}\n"
                                                    f"✅ {successful_count} success | ❌ {failed_count} failed\n"
                                                    f"📊 ~{pct_done:.1f}% of remaining done"
                                                )
                                            else:
                                                msg = (
                                                    f"⏳ *{job_name}* in progress\n"
                                                    f"⚡ {processed_count}/{total_requests} processed\n"
                                                    f"✅ {successful_count} success | ❌ {failed_count} failed\n"
                                                    f"📊 ~{pct_done:.1f}% done"
                                                )
                                            try:
                                                self.notification_manager.send_slack_notification(msg, urgency)
                                                _last_notified_bucket = bucket
                                            except Exception as e:
                                                logger.debug(f"Progress Slack suppressed: {type(e).__name__}: {e}")

                            except Exception as e:
                                logger.error(f"Failed processing {webhook['url']}: {e}")
                                failed_count += 1
                                processed_count += 1
                                if pbar:
                                    pbar.update(1)

                            # dynamic rate adjustment on a sliding window
                            if processed_count % rate_config.get('window_size', 20) == 0 and error_window:
                                error_rate = sum(error_window) / len(error_window)
                                current_rate = rate_limiter.get_rate()
                                if error_rate > rate_config.get('error_threshold', 0.3):
                                    new_rate = max(self.config.get('rate_limiting', {}).get('base_rate_limit', 3.0),
                                                current_rate / 1.2)
                                    rate_limiter.adjust_rate(new_rate)
                                elif error_rate < rate_config.get('error_threshold', 0.3) / 2:
                                    new_rate = min(self.config.get('rate_limiting', {}).get('max_rate_limit', 5.0),
                                                current_rate * 1.2)
                                    rate_limiter.adjust_rate(new_rate)
                finally:
                    if pbar:
                        try:
                            pbar.close()
                        except Exception as e:
                            logger.debug(f"Error closing progress bar: {e}")
                    # Ensure all handlers are properly closed
                    try:
                        for handler in logging.getLogger().handlers:
                            handler.flush()
                    except Exception:
                        pass
                    # Cleanup HTTP sessions
                    try:
                        cleanup_http_session()
                    except Exception:
                        pass

            # Finalize job with verification
            try:
                self.db_manager.finish_job(job_id, successful_count, failed_count)
                
                # Verify job was properly marked as completed
                final_stats = self.db_manager.get_job_stats(job_id)
                if final_stats.get('status') != 'completed':
                    logger.warning(f"Job {job_id} status verification failed, current status: {final_stats.get('status')}")
                    # Force status update
                    with self.db_manager.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('UPDATE job_history SET status = ? WHERE job_id = ?', ('completed', job_id))
                        conn.commit()
                        logger.info(f"Forced job {job_id} status to completed")
            except Exception as e:
                logger.error(f"Error finalizing job {job_id}: {e}")
                # Ensure job doesn't remain in running state
                try:
                    self.db_manager.mark_job_failed(job_id, f"Finalization error: {e}")
                except Exception:
                    pass

            # Archive processed (single-file mode, including CSV_FILE from env)
            try:
                if csv_file_path and os.path.isfile(csv_file_path):
                    archive_processed_file(csv_file_path, self.config)
            except Exception as e:
                logger.debug(f"Archive check skipped: {type(e).__name__}: {e}")

            # Summary
            job_stats = self.db_manager.get_job_stats(job_id)
            success_rate = (successful_count / total_requests) * 100 if total_requests > 0 else 0
            metrics = results_tracker.get_metrics()

            BOX_WIDTH = 80
            logger.info(f"""
{'=' * (BOX_WIDTH - 2)}
{' ' * ((BOX_WIDTH - 2 - len('JOB COMPLETED')) // 2)}JOB COMPLETED{' ' * ((BOX_WIDTH - 2 - len('JOB COMPLETED') + 1) // 2)}
{'=' * (BOX_WIDTH - 2)}
Job Name: {job_name:<{BOX_WIDTH - 15}}
Job ID: {job_id:<{BOX_WIDTH - 13}}
CSV File: {os.path.basename(csv_file_path) if len(csv_file_path) < 50 else '...' + csv_file_path[-47:]:<{BOX_WIDTH - 15}}
Triggered By: {triggered_by:<{BOX_WIDTH - 17}}
Total Requests: {total_requests:<{BOX_WIDTH - 20}}
Successful: {successful_count:<{BOX_WIDTH - 18}}
Failed: {failed_count:<{BOX_WIDTH - 14}}
Success Rate: {success_rate:.2f}%{' ' * (BOX_WIDTH - 23)}
Duration: {job_stats.get('duration_seconds', 0):.2f} seconds{' ' * (BOX_WIDTH - 19 - len(f'{job_stats.get("duration_seconds", 0):.2f}'))}
Throughput: {metrics['throughput']:.2f} req/s{' ' * (BOX_WIDTH - 24 - len(f'{metrics["throughput"]:.2f}'))}
Data Processed: {metrics['total_request_size'] + metrics['total_response_size']} bytes{' ' * (BOX_WIDTH - 26 - len(str(metrics['total_request_size'] + metrics['total_response_size'])))}
{'=' * (BOX_WIDTH - 2)}
            """)

            # Notify + metrics
            self.notification_manager.send_job_completion_notification(job_stats)
            if self.config.get('deployment', {}).get('metrics_enabled', True):
                self.db_manager.save_metric('job_success_rate', success_rate)
                self.db_manager.save_metric('job_duration', job_stats.get('duration_seconds', 0))
                self.db_manager.save_metric('job_throughput', metrics['throughput'])
            
            # Write JSON report
            try:
                report_dir = (
                    self.config.get('deployment', {}).get('report_path')
                    or os.getenv('REPORT_PATH')
                    or os.getenv('report_path')
                    or '/app/data/reports'
                )

                report_path = write_job_json_report(
                    db_manager=self.db_manager,
                    job_id=job_id,
                    job_name=job_name,
                    csv_file_path=csv_file_path,
                    total_requests=total_requests,
                    successful_count=successful_count,
                    failed_count=failed_count,
                    metrics=metrics,
                    extra_stats=stats,
                    out_dir=report_dir,
                )
                logger.info(f"Job report written: {report_path}")
                prune_old_reports(os.path.dirname(report_path), keep=int(os.getenv("REPORT_KEEP", "200")))

                if (
                    self.notification_manager.slack_config.get('enabled')
                    and os.getenv("SLACK_NOTIFY_COMPLETION", "true").lower() == "true"
                ):
                    try:
                        self.notification_manager.send_slack_notification(
                            f"Job *{job_name}* report saved:\n{report_path}",
                            'low'
                        )
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Failed to write job JSON report: {e}")

            try:
                if resume_file_hash:
                    _clear_resume_marker(csv_file_path, resume_file_hash)
            except Exception:
                pass

            return job_id

        except Exception as e:
            logger.error(f"Job {job_name} failed to start or run: {e}")
            logger.error(traceback.format_exc())
            return job_id  # return id even on failure (so callers can look up partials)

    def start_watchdog(self):
        if not self.watchdog_enabled:
            logger.info("Watchdog disabled in configuration")
            return

        watch_paths = self.config.get('watchdog', {}).get('watch_paths', ['/app/data/csv'])
        for path in watch_paths:
            os.makedirs(path, exist_ok=True)

        logger.info(f"Starting watchdog for paths: {watch_paths}")

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

        logger.info(f"Watchdog system started successfully with {worker_count} file worker(s)")

    def _process_queue_worker(self):
        """Background worker to process queued jobs"""
        debounce_delay = self.config.get('watchdog', {}).get('debounce_delay', 3.0)
        while not self.shutdown_event.is_set():
            try:
                job_info = self.job_queue.get(timeout=1.0)
                try:
                    logger.info(f"Debouncing file: {os.path.basename(job_info['csv_file'])} ({debounce_delay}s)")
                    time.sleep(debounce_delay)
                    if not os.path.exists(job_info['csv_file']):
                        logger.warning(f"File no longer exists: {job_info['csv_file']}")
                        self.db_manager.update_file_status(job_info['csv_file'], 'failed')
                        continue
                    current_hash = _calculate_file_hash(job_info['csv_file'])
                    if current_hash and job_info.get('file_hash') and current_hash != job_info['file_hash']:
                        logger.info(f"File changed during debounce, re-queuing: {job_info['csv_file']}")
                        job_info['file_hash'] = current_hash
                        try:
                            self.job_queue.put_nowait(job_info)
                        except queue.Full:
                            logger.warning(f"Queue full, cannot re-queue changed file: {job_info['csv_file']}")
                            self.db_manager.update_file_status(job_info['csv_file'], 'queue_full')
                        continue
                    if self.db_manager.is_file_processed(job_info['csv_file'], job_info['file_hash']):
                        logger.info(f"File already processed: {os.path.basename(job_info['csv_file'])}")
                        continue
                    if not job_info.get('row_count'):
                        try:
                            job_info['row_count'] = _count_csv_rows(job_info['csv_file'])
                        except Exception:
                            pass

                    # Add resume context for notifications
                    fh_for_resume = job_info.get('file_hash') or current_hash or _calculate_file_hash(job_info['csv_file']) or ""
                    if fh_for_resume:
                        try:
                            resume_skip = int(_load_resume_rows(job_info['csv_file'], fh_for_resume) or 0)
                            job_info['resume_position'] = resume_skip
                        except Exception:
                            job_info['resume_position'] = 0
                    else:
                        job_info['resume_position'] = 0

                    self.notification_manager.send_file_detected_notification(job_info)
                    job_name = f"{os.path.basename(job_info['csv_file'])}"
                    logger.info(f"Auto-processing file: {os.path.basename(job_info['csv_file'])}")
                    self.db_manager.update_file_status(job_info['csv_file'], 'processing')
                    try:
                        resume_skip = 0
                        try:
                            # prefer the hash we already have; else use the one we computed above; else compute now
                            fh_for_resume = job_info.get('file_hash') or current_hash or _calculate_file_hash(job_info['csv_file']) or ""
                            if fh_for_resume:
                                resume_skip = int(_load_resume_rows(job_info['csv_file'], fh_for_resume) or 0)
                                if resume_skip > 0:
                                    logger.info(f"Resuming {os.path.basename(job_info['csv_file'])} from row {resume_skip}")
                                    # Validate resume point doesn't exceed file size
                                    try:
                                        total_rows = _count_csv_rows(job_info['csv_file'])
                                        if total_rows > 0 and resume_skip >= total_rows:
                                            logger.warning(f"Resume point {resume_skip} exceeds file size {total_rows}, starting from beginning")
                                            resume_skip = 0
                                            _clear_resume_marker(job_info['csv_file'], fh_for_resume)
                                    except Exception as row_count_e:
                                        logger.warning(f"Could not validate resume point: {row_count_e}, proceeding with resume_skip={resume_skip}")
                        except Exception as e:
                            logger.warning(f"Resume check failed: {e}, starting from beginning")
                            resume_skip = 0

                        job_id = self.trigger_webhooks(
                            csv_files=[job_info['csv_file']],
                            job_name=job_name,
                            skip_rows=resume_skip,
                            triggered_by='watchdog'
                        )

                        self.db_manager.update_file_status(job_info['csv_file'], 'completed', job_id)
                        logger.info(f"Auto-processing completed for: {os.path.basename(job_info['csv_file'])}")
                    except Exception as e:
                        logger.error(f"Auto-processing failed for {job_info['csv_file']}: {e}")
                        self.db_manager.update_file_status(job_info['csv_file'], 'failed')
                finally:
                    try:
                        with self.inflight_lock:
                            self.inflight_files.discard(job_info['csv_file'])
                    except Exception as e:
                        logger.error(f"Error cleaning up inflight files: {e}")
                    try:
                        self.job_queue.task_done()
                    except Exception as e:
                        logger.error(f"Error marking queue task as done: {e}")
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in queue processing worker: {e}")
                logger.error(traceback.format_exc())

    def stop_watchdog(self):
        """Stop the watchdog system gracefully"""
        logger.info("Stopping watchdog system...")
        if hasattr(self, 'shutdown_event'):
            self.shutdown_event.set()
        
        # Stop observer first
        if hasattr(self, 'observer') and self.observer:
            try:
                self.observer.stop()
                self.observer.join(timeout=5)
            except Exception as e:
                logger.error(f"Error stopping observer: {e}")
        
        # Wait for processing threads
        if getattr(self, 'processing_threads', None):
            for i, t in enumerate(self.processing_threads):
                try:
                    t.join(timeout=10)
                    if t.is_alive():
                        logger.warning(f"Processing thread {i} did not shutdown gracefully")
                except Exception as e:
                    logger.error(f"Error joining thread {i}: {e}")
        
        logger.info("Watchdog system stopped")

    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        queue_size = -1  # Default to unknown
        try:
            if self.job_queue and hasattr(self.job_queue, 'qsize'):
                queue_size = self.job_queue.qsize()
            elif self.job_queue:
                queue_size = 0  # Queue exists but no qsize method
        except (AttributeError, OSError) as e:
            logger.debug(f"Cannot get queue size: {e}")
        except Exception as e:
            logger.error(f"Unexpected error getting queue size: {e}")
        
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
                result = cursor.fetchone()
                return result is not None and result[0] == 1
        except Exception as e:
            logger.debug(f"Database health check failed: {e}")
            return False


# =========================
# Health server / keep-alive / backups
# =========================

def create_health_check_server():
    """Create a simple health check HTTP server with webhook endpoints"""
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import json
        import queue
        from threading import Thread
        from datetime import datetime
        import os
        import time

        class HealthCheckHandler(BaseHTTPRequestHandler):
            trigger = None
            
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)

            def _check_auth(self):
                """Check authentication if enabled"""
                webhook_auth = os.getenv('WEBHOOK_AUTH_TOKEN')
                if not webhook_auth:
                    return True  # No auth configured
                
                auth_header = self.headers.get('Authorization', '')
                if auth_header.startswith('Bearer '):
                    token = auth_header[7:]
                    return token == webhook_auth
                return False

            def do_GET(self):
                if self.path == '/':
                    # Return API documentation
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    docs = {
                        'service': 'Bulk API Trigger Platform',
                        'version': '2.0',
                        'endpoints': {
                            'GET /health': 'Health check and system status',
                            'GET /jobs': 'List recent jobs',
                            'GET /jobs/{job_id}': 'Get job details',
                            'GET /jobs/{job_id}/errors': 'Get job errors',
                            'GET /jobs/{job_id}/report': 'Get job report',
                            'GET /metrics': 'System metrics',
                            'GET /config': 'Current configuration',
                            'GET /status': 'Status of all active and recent jobs',
                            'GET /resume/stats': 'Resume statistics across all files',
                            'POST /trigger': 'Trigger a new job',
                            'POST /trigger/batch': 'Trigger multiple CSV files',
                            'POST /resume/status': 'Get resume status for a file',
                            'POST /resume/clear': 'Clear resume marker for a file'
                        },
                        'webhook_endpoint': {
                            'url': 'POST /trigger',
                            'payload': {
                                'csv_file': 'Path to CSV file (required)',
                                'job_name': 'Custom job name (optional)',
                                'skip_rows': 'Number of rows to skip (optional, default: 0)',
                                'resume': 'Enable resume from last checkpoint (optional, default: true)',
                                'force_restart': 'Force restart from beginning (optional, default: false)'
                            },
                            'example': {
                                'csv_file': '/app/data/csv/webhooks.csv',
                                'job_name': 'My Custom Job',
                                'resume': True
                            }
                        }
                    }
                    self.wfile.write(json.dumps(docs, indent=2).encode())
                    
                elif self.path == '/health':
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

                elif self.path.startswith('/jobs'):
                    parts = [p for p in self.path.split('/') if p]
                    if len(parts) == 1:  # /jobs
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        try:
                            with self.trigger.db_manager.get_connection() as conn:
                                cur = conn.cursor()
                                cur.execute("""
                                    SELECT job_id, job_name, total_requests, successful_requests, failed_requests,
                                        start_time, end_time, duration_seconds, status
                                    FROM job_history
                                    WHERE status IN ('completed', 'failed', 'running')
                                    ORDER BY start_time DESC
                                    LIMIT 50
                                """)
                                rows = cur.fetchall()
                                jobs = [{
                                    "job_id": r[0], "job_name": r[1], "total": r[2],
                                    "successful": r[3], "failed": r[4],
                                    "start_time": r[5], "end_time": r[6],
                                    "duration_seconds": r[7], "status": r[8],
                                } for r in rows]
                        except Exception as e:
                            jobs = {"error": str(e)}
                        self.wfile.write(json.dumps(jobs, indent=2, default=str).encode())

                    elif len(parts) >= 2:
                        job_id = parts[1]
                        if len(parts) == 2:  # /jobs/<job_id>
                            self.send_response(200)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            data = self.trigger.db_manager.get_job_stats(job_id)
                            self.wfile.write(json.dumps(data, indent=2, default=str).encode())

                        elif len(parts) == 3 and parts[2] == 'errors':  # /jobs/<job_id>/errors
                            self.send_response(200)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            try:
                                with self.trigger.db_manager.get_connection() as conn:
                                    cur = conn.cursor()
                                    cur.execute("""
                                        SELECT url, method, status, status_code, error_message, timestamp
                                        FROM webhook_results
                                        WHERE job_id = ?
                                          AND (status = 'error' OR status = 'exception')
                                        ORDER BY timestamp DESC
                                        LIMIT 50
                                    """, (job_id,))
                                    errs = [{
                                        "url": r[0], "method": r[1], "status": r[2],
                                        "status_code": r[3], "error_message": r[4], "timestamp": r[5]
                                    } for r in cur.fetchall()]
                            except Exception as e:
                                errs = {"error": str(e)}
                            self.wfile.write(json.dumps(errs, indent=2, default=str).encode())

                        elif len(parts) == 3 and parts[2] == 'report':  # /jobs/<job_id>/report
                            self.send_response(200)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            rpt_dir = self.trigger.config.get('deployment', {}).get('report_path', '/app/data/reports')
                            path = os.path.join(rpt_dir, f"job_{job_id}.json")
                            try:
                                with open(path, "r", encoding="utf-8") as f:
                                    content = json.load(f)
                            except Exception as e:
                                content = {"error": str(e), "path": path}
                            self.wfile.write(json.dumps(content, indent=2, default=str).encode())

                        else:
                            self.send_response(404)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(b'{"error": "Invalid job endpoint"}')

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

                elif self.path == '/status':
                    # Get status of all active and recent jobs
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    try:
                        with self.trigger.db_manager.get_connection() as conn:
                            cur = conn.cursor()
                            # Get running jobs
                            cur.execute("""
                                SELECT job_id, job_name, csv_file, total_requests, 
                                       successful_requests, failed_requests, status, 
                                       start_time, triggered_by
                                FROM job_history
                                WHERE status = 'running'
                                ORDER BY start_time DESC
                            """)
                            running = []
                            for row in cur.fetchall():
                                running.append({
                                    'job_id': row[0],
                                    'job_name': row[1],
                                    'csv_file': row[2],
                                    'total': row[3],
                                    'processed': (row[4] or 0) + (row[5] or 0),
                                    'successful': row[4] or 0,
                                    'failed': row[5] or 0,
                                    'status': row[6],
                                    'start_time': row[7],
                                    'triggered_by': row[8]
                                })
                            
                            # Get inflight files
                            with self.trigger.inflight_lock:
                                inflight = list(self.trigger.inflight_files)
                            
                            # Get queue size
                            try:
                                queue_size = self.trigger.job_queue.qsize()
                            except:
                                queue_size = 0
                            
                            status = {
                                'running_jobs': running,
                                'inflight_files': inflight,
                                'queue_size': queue_size,
                                'watchdog_enabled': self.trigger.watchdog_enabled,
                                'timestamp': datetime.now().isoformat()
                            }
                    except Exception as e:
                        status = {'error': str(e)}
                    self.wfile.write(json.dumps(status, indent=2, default=str).encode())

                elif self.path == '/resume/stats':
                    # Get resume statistics across all files
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    try:
                        resume_dir = (
                            self.trigger.config.get('resume', {}).get('marker_path')
                            or self.trigger.config.get('deployment', {}).get('report_path')
                            or '/app/data/reports'
                        )
                        
                        resume_files = []
                        if os.path.exists(resume_dir):
                            for f in os.listdir(resume_dir):
                                if f.startswith('.resume_') and f.endswith('.json'):
                                    try:
                                        path = os.path.join(resume_dir, f)
                                        with open(path, 'r') as rf:
                                            data = json.load(rf)
                                            resume_files.append({
                                                'file': data.get('file_path', 'unknown'),
                                                'position': data.get('skip_rows', 0),
                                                'total': data.get('total_rows', 0),
                                                'checkpoint': data.get('last_checkpoint', ''),
                                                'progress': (data.get('skip_rows', 0) / data.get('total_rows', 1) * 100) if data.get('total_rows', 0) > 0 else 0
                                            })
                                    except Exception:
                                        pass
                        
                        stats = {
                            'resume_markers': len(resume_files),
                            'files': sorted(resume_files, key=lambda x: x.get('checkpoint', ''), reverse=True)[:20],
                            'resume_dir': resume_dir
                        }
                        self.wfile.write(json.dumps(stats, indent=2, default=str).encode())
                    except Exception as e:
                        self.wfile.write(json.dumps({'error': str(e)}).encode())

                elif self.path == '/config':
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    try:
                        cfg = dict(self.trigger.config)  # shallow copy
                        # redact secrets
                        try:
                            if 'notifications' in cfg and 'email' in cfg['notifications']:
                                cfg['notifications']['email'] = dict(cfg['notifications']['email'])
                                if 'password' in cfg['notifications']['email']:
                                    cfg['notifications']['email']['password'] = '***REDACTED***'
                            if 'notifications' in cfg and 'slack' in cfg['notifications']:
                                cfg['notifications']['slack'] = dict(cfg['notifications']['slack'])
                                if 'webhook_url' in cfg['notifications']['slack']:
                                    cfg['notifications']['slack']['webhook_url'] = '***REDACTED***'
                        except Exception:
                            pass
                        self.wfile.write(json.dumps(cfg, indent=2, default=str).encode())
                    except Exception as e:
                        self.wfile.write(json.dumps({'error': str(e)}).encode())

                else:
                    self.send_response(404)
                    self.end_headers()

            def do_POST(self):
                """Handle POST requests for webhook endpoints"""
                # Check rate limiting for webhook endpoints
                if self.path in ['/trigger', '/trigger/batch']:
                    client_ip = self.client_address[0] if self.client_address else 'unknown'
                    if not webhook_rate_limiter.is_allowed(client_ip):
                        reset_time = webhook_rate_limiter.get_reset_time(client_ip)
                        self.send_response(429)  # Too Many Requests
                        self.send_header('Content-type', 'application/json')
                        self.send_header('X-RateLimit-Reset', str(reset_time))
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            'error': 'Rate limit exceeded',
                            'retry_after': reset_time - int(time.time())
                        }).encode())
                        return

                if self.path == '/trigger':
                    try:
                        content_length = int(self.headers.get('Content-Length', 0))
                        if content_length > 1048576:  # 1MB limit
                            self.send_response(413)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'Request too large'}).encode())
                            return
                        
                        post_data = self.rfile.read(content_length)
                        data = json.loads(post_data.decode('utf-8'))
                        
                        # Validate required fields
                        csv_file = data.get('csv_file')
                        if not csv_file:
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'csv_file is required'}).encode())
                            return
                        
                        # Check if file exists
                        if not os.path.exists(csv_file):
                            self.send_response(404)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': f'CSV file not found: {csv_file}'}).encode())
                            return
                        
                        # Validate file is actually a CSV
                        if not csv_file.lower().endswith('.csv'):
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': f'File must be a CSV: {csv_file}'}).encode())
                            return
                        
                        # Validate file has valid content
                        try:
                            row_count = _count_csv_rows(csv_file)
                            if row_count == 0:
                                self.send_response(400)
                                self.send_header('Content-type', 'application/json')
                                self.end_headers()
                                self.wfile.write(json.dumps({
                                    'error': 'CSV file has no valid webhook URLs',
                                    'file': csv_file
                                }).encode())
                                return
                        except Exception as e:
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({
                                'error': f'Failed to validate CSV: {str(e)}',
                                'file': csv_file
                            }).encode())
                            return
                        
                        # Extract optional parameters
                        job_name = data.get('job_name', f'API Triggered - {os.path.basename(csv_file)}')
                        skip_rows = int(data.get('skip_rows', 0))
                        resume = data.get('resume', True)  # Default to resume enabled
                        force_restart = data.get('force_restart', False)  # Force restart from beginning
                        
                        # Clear resume marker if force_restart
                        if force_restart and resume:
                            try:
                                file_hash = _calculate_file_hash(csv_file)
                                if file_hash:
                                    _clear_resume_marker(csv_file, file_hash, self.trigger.config)
                                    skip_rows = 0
                            except Exception as e:
                                logger.warning(f"Failed to clear resume marker: {e}")
                        
                        # Check if job is already running for this file
                        with self.trigger.inflight_lock:
                            if csv_file in self.trigger.inflight_files:
                                self.send_response(409)
                                self.send_header('Content-type', 'application/json')
                                self.end_headers()
                                self.wfile.write(json.dumps({
                                    'error': 'Job already running for this file',
                                    'file': csv_file
                                }).encode())
                                return
                            self.trigger.inflight_files.add(csv_file)
                        
                        # Trigger the job asynchronously
                        def run_job():
                            try:
                                job_id = self.trigger.trigger_webhooks(
                                    csv_files=[csv_file],
                                    job_name=job_name,
                                    skip_rows=skip_rows,
                                    triggered_by='webhook'
                                )
                                logger.info(f"Webhook triggered job {job_id} for {csv_file}")
                            finally:
                                with self.trigger.inflight_lock:
                                    self.trigger.inflight_files.discard(csv_file)
                        
                        job_thread = Thread(target=run_job, daemon=True)
                        job_thread.start()
                        
                        # Log the webhook trigger
                        log_webhook_trigger(
                            '/trigger',
                            {'csv_file': csv_file, 'job_name': job_name},
                            202,
                            self.client_address[0] if self.client_address else None
                        )
                        
                        # Return immediate response
                        self.send_response(202)  # Accepted
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        response = {
                            'status': 'accepted',
                            'message': 'Job queued for processing',
                            'csv_file': csv_file,
                            'job_name': job_name,
                            'resume_enabled': resume and not force_restart
                        }
                        self.wfile.write(json.dumps(response).encode())
                    
                    except json.JSONDecodeError:
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': 'Invalid JSON'}).encode())
                    except Exception as e:
                        logger.error(f"Webhook endpoint error: {e}")
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': str(e)}).encode())
                
                elif self.path == '/trigger/batch':
                    # Trigger multiple CSV files
                    try:
                        content_length = int(self.headers.get('Content-Length', 0))
                        if content_length > 1048576:  # 1MB limit
                            self.send_response(413)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'Request too large'}).encode())
                            return
                        
                        post_data = self.rfile.read(content_length)
                        data = json.loads(post_data.decode('utf-8'))
                        
                        # Validate required fields
                        csv_files = data.get('csv_files', [])
                        if not csv_files or not isinstance(csv_files, list):
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'csv_files array is required'}).encode())
                            return
                        
                        # Validate all files exist
                        missing_files = [f for f in csv_files if not os.path.exists(f)]
                        if missing_files:
                            self.send_response(404)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({
                                'error': 'Some CSV files not found',
                                'missing_files': missing_files
                            }).encode())
                            return
                        
                        # Queue all files
                        queued_files = []
                        skipped_files = []
                        
                        for csv_file in csv_files[:100]:  # Limit to 100 files
                            with self.trigger.inflight_lock:
                                if csv_file in self.trigger.inflight_files:
                                    skipped_files.append({'file': csv_file, 'reason': 'already running'})
                                    continue
                                self.trigger.inflight_files.add(csv_file)
                            
                            # Queue the file
                            file_hash = _calculate_file_hash(csv_file)
                            job_info = {
                                'csv_file': csv_file,
                                'file_hash': file_hash,
                                'file_size': os.path.getsize(csv_file),
                                'row_count': _count_csv_rows(csv_file),
                                'event_type': 'webhook_batch',
                                'detected_at': datetime.now().isoformat()
                            }
                            
                            try:
                                self.trigger.job_queue.put_nowait(job_info)
                                queued_files.append(csv_file)
                            except queue.Full:
                                with self.trigger.inflight_lock:
                                    self.trigger.inflight_files.discard(csv_file)
                                skipped_files.append({'file': csv_file, 'reason': 'queue full'})
                        
                        self.send_response(202)  # Accepted
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        response = {
                            'status': 'accepted',
                            'queued_files': queued_files,
                            'skipped_files': skipped_files,
                            'total_queued': len(queued_files),
                            'total_skipped': len(skipped_files)
                        }
                        self.wfile.write(json.dumps(response).encode())
                    
                    except Exception as e:
                        logger.error(f"Batch trigger error: {e}")
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': str(e)}).encode())

                elif self.path == '/resume/status':
                    # Get resume status for a file
                    try:
                        content_length = int(self.headers.get('Content-Length', 0))
                        post_data = self.rfile.read(content_length)
                        data = json.loads(post_data.decode('utf-8'))
                        
                        csv_file = data.get('csv_file')
                        if not csv_file:
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'csv_file is required'}).encode())
                            return
                        
                        file_hash = _calculate_file_hash(csv_file)
                        resume_rows = _load_resume_rows(csv_file, file_hash, self.trigger.config) if file_hash else 0
                        total_rows = _count_csv_rows(csv_file)
                        
                        response = {
                            'csv_file': csv_file,
                            'file_hash': file_hash,
                            'resume_position': resume_rows,
                            'total_rows': total_rows,
                            'progress_percentage': (resume_rows / total_rows * 100) if total_rows > 0 else 0,
                            'is_complete': resume_rows >= total_rows if total_rows > 0 else False
                        }
                        
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps(response).encode())
                    
                    except Exception as e:
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': str(e)}).encode())
                
                elif self.path == '/resume/clear':
                    # Clear resume marker for a file
                    try:
                        content_length = int(self.headers.get('Content-Length', 0))
                        post_data = self.rfile.read(content_length)
                        data = json.loads(post_data.decode('utf-8'))
                        
                        csv_file = data.get('csv_file')
                        if not csv_file:
                            self.send_response(400)
                            self.send_header('Content-type', 'application/json')
                            self.end_headers()
                            self.wfile.write(json.dumps({'error': 'csv_file is required'}).encode())
                            return
                        
                        file_hash = _calculate_file_hash(csv_file)
                        if file_hash:
                            _clear_resume_marker(csv_file, file_hash, self.trigger.config)
                        
                        self.send_response(200)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            'status': 'success',
                            'message': 'Resume marker cleared',
                            'csv_file': csv_file
                        }).encode())
                    
                    except Exception as e:
                        self.send_response(500)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({'error': str(e)}).encode())
                
                else:
                    self.send_response(404)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({'error': 'Endpoint not found'}).encode())

            def do_OPTIONS(self):
                # Handle CORS preflight requests
                self.send_response(200)
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
                self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
                self.send_header('Access-Control-Max-Age', '3600')
                self.end_headers()
            
            def end_headers(self):
                # Add CORS headers to all responses
                self.send_header('Access-Control-Allow-Origin', '*')
                super().end_headers()

            def log_message(self, format, *args):
                pass  # Suppress default logging

        def start_server(trigger_instance, port=8000):
            HealthCheckHandler.trigger = trigger_instance
            server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
            server.serve_forever()

        def stop_server(server):
            try:
                server.shutdown()
                server.server_close()
            except Exception as e:
                logger.error(f"Error stopping health server: {e}")
        
        return start_server, stop_server

    except ImportError:
        logger.warning("HTTP server not available, health checks disabled")
        return None, None

def keep_alive_with_watchdog(trigger: BulkAPITrigger, port: int = 8000):
    """Enhanced keep-alive with watchdog functionality"""
    logger.info("Webhook processing completed. Starting keep-alive with watchdog...")
    health_server_funcs = create_health_check_server()
    if health_server_funcs and trigger.config.get('deployment', {}).get('health_check_enabled', True):
        health_server_starter = health_server_funcs[0] if isinstance(health_server_funcs, tuple) else health_server_funcs
        health_thread = Thread(target=health_server_starter, args=(trigger, port), daemon=True)
        health_thread.start()
        logger.info(f"Health check server started on port {port}")
    trigger.start_watchdog()
    logger.info("Database contains job history and results for analysis")
    logger.info("Watchdog is monitoring for new CSV files")
    logger.info("Container health: OK - Running with watchdog")
    try:
        heartbeat_count = 0
        while True:
            time.sleep(HEARTBEAT_INTERVAL)  # honor global heartbeat interval
            heartbeat_count += 1
            try:
                status = trigger.get_system_status()
                logger.info(f"Heartbeat #{heartbeat_count} - System Status: "
                            f"Queue: {status['queue_size']}, "
                            f"Watchdog: {'OK' if status['watchdog_running'] else 'ERROR'}, "
                            f"DB: {'OK' if status['database_accessible'] else 'ERROR'}")
            except Exception as e:
                logger.error(f"Heartbeat #{heartbeat_count} - Status check failed: {e}")
                status = {'queue_size': 0, 'watchdog_running': False, 'database_accessible': False}

            backup_hours = trigger.config.get('database', {}).get('backup_interval_hours', 24)
            ticks_per_backup = max(1, int((backup_hours * 60) / 5))
            if trigger.config.get('database', {}).get('backup_enabled', True) and (heartbeat_count % ticks_per_backup == 0):
                logger.info("Scheduled maintenance: database backup")
                try:
                    backup_database(trigger.db_manager)
                except Exception as e:
                    logger.error(f"Database backup failed: {e}")

            if status.get('queue_size', 0) > 0:
                logger.info(f"Processing queue has {status['queue_size']} pending files")
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down gracefully...")
        trigger.stop_watchdog()


def backup_database(db_manager: DatabaseManager):
    """Create database backup"""
    try:
        if not db_manager or not hasattr(db_manager, 'db_path'):
            logger.error("Invalid DatabaseManager for backup")
            return
        if not os.path.exists(db_manager.db_path):
            logger.error(f"Database file not found: {db_manager.db_path}")
            return
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
        logger.info(f"Database backup created: {os.path.basename(backup_path)}")
    except Exception as e:
        logger.error(f"Database backup failed: {e}")

def write_job_json_report(
    db_manager: "DatabaseManager",
    job_id: str,
    job_name: str,
    csv_file_path: str,
    total_requests: int,
    successful_count: int,
    failed_count: int,
    metrics: Dict,
    extra_stats: Dict = None,
    out_dir: str = "/app/data/reports",
) -> str:
    """
    Generate a JSON report for the completed job and write it to disk.
    Returns the absolute path to the written JSON file.
    """
    try:
        os.makedirs(out_dir, exist_ok=True)
    except Exception as e:
        try:
            # fallback: put it under /app/data if reports fails
            out_dir = "/app/data"
            os.makedirs(out_dir, exist_ok=True)
        except Exception:
            # last resort: current dir
            out_dir = "."
    
    # Pull fresh stats from DB
    try:
        job_stats = db_manager.get_job_stats(job_id) or {}
    except Exception:
        job_stats = {}
    
    # Build status breakdown straight from DB (again) for completeness
    status_breakdown = job_stats.get("status_breakdown", {})
    
    # Collect a small sample of errors to help debugging without bloating the file
    error_samples = []
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT url, method, status, status_code, error_message, timestamp
                FROM webhook_results
                WHERE job_id = ?
                  AND (status = 'error' OR status = 'exception')
                ORDER BY timestamp DESC
                LIMIT 50
                """,
                (job_id,),
            )
            rows = cur.fetchall()
            for r in rows:
                error_samples.append({
                    "url": r[0],
                    "method": r[1],
                    "status": r[2],
                    "status_code": r[3],
                    "error_message": r[4],
                    "timestamp": r[5],
                })
    except Exception:
        pass
    
    # Compose report
    report = {
        "job": {
            "id": job_id,
            "name": job_name,
            "csv_file": csv_file_path,
            "start_time": job_stats.get("start_time"),
            "end_time": job_stats.get("end_time"),
            "duration_seconds": job_stats.get("duration_seconds", 0.0),
            "triggered_by": job_stats.get("triggered_by", "manual"),
        },
        "totals": {
            "total_requests": total_requests,
            "successful": successful_count,
            "failed": failed_count,
            "success_rate": (successful_count / total_requests * 100.0) if total_requests else 0.0,
        },
        "performance": {
            "average_response_time": job_stats.get("average_response_time", 0.0),
            "status_breakdown": status_breakdown,
            "metrics": metrics or {},  # throughput, bytes if your tracker provides them
        },
        "data": {
            "extra_stats": extra_stats or {},   # e.g., combined CSV stats from multi-file
        },
        "errors_sample": error_samples,
        "generated_at": datetime.now().isoformat(),
        "version": 1,
    }
    
    # Stable filename based on job id
    safe_job_id = job_id.replace("/", "_").replace("\\", "_")
    out_path = os.path.join(out_dir, f"job_{safe_job_id}.json")
    
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning(f"Failed to write report to {out_path}: {e}")
        # fallback: write to /app/data with a timestamped name
        try:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_path = os.path.join("/app/data", f"job_{safe_job_id}_{ts}.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, ensure_ascii=False, default=str)
        except Exception as e2:
            logger.error(f"Failed to write report even to fallback path: {e2}")
            raise
    
    return out_path


def scan_existing_files(trigger: BulkAPITrigger):
    """Scan for existing files on startup (CSV -> queue, non-CSV -> reject)."""
    watch_paths = trigger.config.get('watchdog', {}).get('watch_paths', ['/app/data/csv'])
    for watch_path in watch_paths:
        if not os.path.exists(watch_path):
            continue

        logger.info(f"Scanning existing files in {watch_path}")

        # 1) Handle NON-CSV first: breadcrumb -> move -> status
        try:
            try:
                paths = glob.glob(os.path.join(watch_path, '*'))
            except Exception as e:
                logger.error(f"Failed to scan directory {watch_path}: {e}")
                continue
            for path in paths:
                if not os.path.isfile(path):
                    continue
                if path.lower().endswith('.csv'):
                    continue  # handled next
                try:
                    # breadcrumb (hash/size) before any move
                    file_hash = _calculate_file_hash(path)
                    file_size = os.path.getsize(path) if os.path.exists(path) else 0
                    trigger.db_manager.track_file(path, file_hash or '', file_size, rows_count=0)
                except Exception as e:
                    logger.debug(f"Breadcrumb capture failed for non-CSV {os.path.basename(path)}: {e}")

                # move to rejected (if configured)
                try:
                    if trigger.config.get('csv', {}).get('archive_on_validation_failure', True):
                        move_file_reasoned(
                            path,
                            trigger.config.get('csv', {}).get('rejected_path', ''),
                            reason_prefix="invalid_not-csv_"
                        )
                except Exception as e:
                    logger.debug(f"Archive on validation failure skipped for {os.path.basename(path)}: {e}")

                # update DB status
                try:
                    trigger.db_manager.update_file_status(path, 'rejected_not_csv')
                except Exception as e:
                    logger.debug(f"DB status update failed for non-CSV {os.path.basename(path)}: {e}")
        except Exception as e:
            logger.error(f"Startup non-CSV sweep failed in {watch_path}: {e}")

        # 2) Existing CSV files (unchanged logic)
        try:
            csv_files = glob.glob(os.path.join(watch_path, '*.csv'))
            for csv_file in csv_files:
                try:
                    file_hash = _calculate_file_hash(csv_file)
                    if not file_hash:
                        continue
                    if trigger.db_manager.is_hash_processed(file_hash):
                        logger.info("Duplicate content detected on startup scan (hash match).")
                        dup_dir = trigger.config.get('csv', {}).get('duplicates_path', '')
                        if dup_dir:
                            move_file_reasoned(csv_file, dup_dir, reason_prefix="dup_hash_")
                        try:
                            trigger.db_manager.track_file(csv_file, file_hash, os.path.getsize(csv_file), rows_count=0)
                            trigger.db_manager.update_file_status(csv_file, 'skipped_duplicate')
                        except Exception:
                            pass
                        continue

                    if os.path.getsize(csv_file) <= 0:
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
                    logger.info(f"Queued existing file: {os.path.basename(csv_file)}")
                except Exception as e:
                    logger.error(f"Startup scan failed for {csv_file}: {e}")
        except Exception as e:
            logger.error(f"Startup CSV scan failed in {watch_path}: {e}")


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
        if not path:
            raise ValueError("Config path cannot be empty")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config file not found: {path}")
        with open(path, 'r', encoding='utf-8') as f:
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
        'resume': {
            'enabled': os.getenv('RESUME_ENABLED', 'true').lower() == 'true',
            'checkpoint_interval': int(os.getenv('RESUME_CHECKPOINT_INTERVAL', '100')),
            'marker_path': os.getenv('RESUME_MARKER_PATH', os.getenv('REPORT_PATH', '/app/data/reports'))
        },
        'retry': {
            'max_retries': max(0, min(int(os.getenv('MAX_RETRIES', '3') or '3'), 10)),
            'timeout': max(1, min(int(os.getenv('REQUEST_TIMEOUT', '30') or '30'), 300)),
            'retry_delay': max(0.1, min(float(os.getenv('RETRY_DELAY', '1.0') or '1.0'), 60.0)),
        },
        'deployment': {
            'keep_alive': os.getenv('KEEP_ALIVE', 'true').lower() == 'true',
            'skip_rows': int(os.getenv('SKIP_ROWS', '0')),
            'job_name': os.getenv('JOB_NAME', None),
            'metrics_enabled': os.getenv('METRICS_ENABLED', 'true').lower() == 'true',
            'health_check_enabled': os.getenv('HEALTH_CHECK_ENABLED', 'true').lower() == 'true',
            'report_path': os.getenv('REPORT_PATH', os.getenv('report_path', '/app/data/reports')),
        },
        'notifications': {
            'email': {
                'enabled': os.getenv('EMAIL_NOTIFICATIONS', 'false').lower() == 'true',
                'smtp_server': os.getenv('EMAIL_SMTP_SERVER', 'smtp.gmail.com'),
                'smtp_port': int(os.getenv('EMAIL_SMTP_PORT', '587')),
                'username': os.getenv('EMAIL_USERNAME', ''),
                'password': os.getenv('EMAIL_PASSWORD', ''),
                'from_email': os.getenv('EMAIL_FROM', ''),
                'recipients': [e.strip() for e in os.getenv('EMAIL_RECIPIENTS', '').split(',') if e.strip()] if os.getenv('EMAIL_RECIPIENTS') else [],
                'notify_on_completion': os.getenv('EMAIL_NOTIFY_COMPLETION', 'true').lower() == 'true',
                'notify_on_file_detected': os.getenv('EMAIL_NOTIFY_FILE_DETECTED', 'false').lower() == 'true'
            },
            'slack': {
                'enabled': os.getenv('SLACK_NOTIFICATIONS', 'false').lower() == 'true',
                'webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
                'notify_on_completion': os.getenv('SLACK_NOTIFY_COMPLETION', 'true').lower() == 'true',
                'notify_on_file_detected': os.getenv('SLACK_NOTIFY_FILE_DETECTED', 'true').lower() == 'true',
                'notify_on_progress': os.getenv('SLACK_NOTIFY_PROGRESS', 'false').lower() == 'true'
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

def validate_config(config: Dict) -> Dict:
    """Validate and sanitize configuration"""
    validated = dict(config)
    
    # Ensure numeric values are within reasonable bounds
    rate_limit = validated.get('rate_limiting', {}) or {}
    rate_limit['max_workers'] = max(1, min(int(rate_limit.get('max_workers', 3) or 3), 50))
    rate_limit['base_rate_limit'] = max(0.1, min(float(rate_limit.get('base_rate_limit', 3.0)), 100.0))
    rate_limit['max_rate_limit'] = max(rate_limit.get('base_rate_limit', 3.0), 
                                      min(float(rate_limit.get('max_rate_limit', 5.0)), 100.0))
    rate_limit['window_size'] = max(10, min(int(rate_limit.get('window_size', 20)), 1000))
    rate_limit['error_threshold'] = max(0.01, min(float(rate_limit.get('error_threshold', 0.3)), 1.0))
    
    # Ensure retry values are reasonable
    retry = validated.get('retry', {})
    retry['max_retries'] = max(0, min(int(retry.get('max_retries', 3) or 3), 10))
    retry['timeout'] = max(1, min(int(retry.get('timeout', 30) or 30), 300))
    retry['retry_delay'] = max(0.1, min(float(retry.get('retry_delay', 1.0) or 1.0), 60.0))
    
    # Ensure paths exist for CSV operations
    csv_config = validated.get('csv', {})
    for path_key in ['archive_path', 'duplicates_path', 'rejected_path']:
        path = csv_config.get(path_key)
        if path:
            try:
                os.makedirs(path, exist_ok=True)
            except Exception as e:
                logger.warning(f"Cannot create {path_key} directory {path}: {e}")
    
    try:
        slack_cfg = validated.get('notifications', {}).get('slack', {}) or {}
        if slack_cfg.get('enabled') and not str(slack_cfg.get('webhook_url', '')).startswith('https://hooks.slack.com/'):
            logger.warning("Slack enabled but webhook URL does not look like a Slack webhook URL.")
    except Exception:
        pass
    try:
        email_cfg = validated.get('notifications', {}).get('email', {}) or {}
        if email_cfg.get('enabled'):
            missing = [k for k in ['smtp_server', 'smtp_port', 'username', 'password', 'from_email']
                       if not email_cfg.get(k)]
            if missing:
                logger.warning(f"Email enabled but missing required fields: {missing}")
    except Exception:
        pass

    return validated

# =========================
# Main
# =========================
def main():
    """Enhanced main function with watchdog and comprehensive error handling"""
    import signal
    global trigger_instance
    trigger_instance = None
    def signal_handler(signum, frame):
        global trigger_instance
        try:
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            if trigger_instance:
                trigger_instance.stop_watchdog()
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("""
Enhanced Bulk API Trigger Platform with Watchdog
===============================================
Usage: python webhook_trigger.py [options] [csv_file]

Options:
  --config, -c       Configuration file (YAML/JSON)
  --job-name, -n     Custom job name
  --skip-rows, -s    Number of rows to skip
  --keep-alive, -k   Keep process running with watchdog
  --workers, -w      Number of parallel workers
  --rate-limit, -r   Base rate limit (requests/sec)
  --verbose, -v      Verbose logging
  --dry-run, -d      Validate CSV without sending requests
  --watchdog         Enable file monitoring
  --no-watchdog      Disable file monitoring
  --health-port      Health check server port
  --interactive      Interactive mode
  --create-config    Create sample configuration file

Environment Variables:
  CSV_FILE           Path to CSV file or 'AUTO' for discovery
  WATCHDOG_ENABLED   Enable/disable file monitoring
  SLACK_WEBHOOK_URL  Slack webhook for notifications
  EMAIL_*            Email notification settings
  See documentation for complete list
        """)
        return

    if len(sys.argv) > 1 and sys.argv[1] == '--create-config':
        ConfigManager.create_sample_config()
        return

    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        print("Interactive Enhanced Bulk API Trigger")
        print("=" * 45)
        csv_file = input("CSV file path (or 'auto' for watchdog mode): ").strip()
        job_name = input("Job name (optional): ").strip()
        skip_rows = input("Rows to skip (default: 0): ").strip()
        keep_running = input("Keep container alive with watchdog? (Y/n): ").strip().lower()
        enable_watchdog = input("Enable file monitoring? (Y/n): ").strip().lower()
        config = validate_config(load_environment_config())
        if job_name:
            config.setdefault('deployment', {})['job_name'] = job_name
        if skip_rows.isdigit():
            config.setdefault('deployment', {})['skip_rows'] = int(skip_rows)
        config.setdefault('deployment', {})['keep_alive'] = keep_running not in ['n', 'no', 'false']
        config.setdefault('watchdog', {})['enabled'] = enable_watchdog not in ['n', 'no', 'false']
        trigger = BulkAPITrigger(config)
        NotificationManager.send_startup_test_notifications(trigger)
        
        trigger_instance = trigger
        if csv_file.lower() not in ['auto', 'watchdog']:
            files = [csv_file]
            job_id = trigger.trigger_webhooks(
                csv_files=files,
                job_name=job_name or None,
                skip_rows=int(skip_rows) if skip_rows.isdigit() else 0
            )
            logger.info(f"Job completed with ID: {job_id}")
        if config['deployment']['keep_alive']:
            keep_alive_with_watchdog(trigger, port=int(os.getenv('HEALTH_PORT', '8000')))
        return

    if os.getenv('DEPLOYMENT_MODE') or os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('DOCKER_CONTAINER'):
        logger.info("Enhanced deployment mode detected")
        config = validate_config(load_environment_config())

        # Optional: merge file config
        for config_file in ['config.yaml', 'config.yml', 'config.json']:
            if os.path.exists(config_file):
                file_config = ConfigManager.load_config_file(config_file)

                # Start from file as defaults, then apply ENV on top so ENV wins
                merged = dict(file_config or {})
                _deep_merge(merged, config)   # ENV-based dict from load_environment_config()
                config = merged

                logger.info(f"Loaded additional config from {config_file} (ENV overrides file)")
                break

        logger.info("Configuration loaded successfully")

        def _redact_cfg(cfg: dict) -> dict:
            try:
                cleaned = json.loads(json.dumps(cfg))  # deep copy
            except Exception:
                cleaned = dict(cfg)
            try:
                notif = cleaned.get('notifications', {})
                if 'email' in notif:
                    notif['email'] = dict(notif['email'])
                    if 'password' in notif['email']:
                        notif['email']['password'] = '***REDACTED***'
                if 'slack' in notif:
                    notif['slack'] = dict(notif['slack'])
                    if 'webhook_url' in notif['slack']:
                        notif['slack']['webhook_url'] = '***REDACTED***'
            except Exception:
                pass
            return cleaned

        logger.debug(f"Config details: {json.dumps(_redact_cfg(config), indent=2, default=str)}")

        # Ensure keys exist
        config.setdefault("deployment", {})
        config.setdefault("notifications", {})
        config["notifications"].setdefault("slack", {})
        config["notifications"].setdefault("email", {})

        # Ensure report path exists
        report_path = config["deployment"].get("report_path", "/app/data/logs")
        os.makedirs(report_path, exist_ok=True)
        config["deployment"]["report_path"] = report_path

        # Normalize booleans (sometimes YAML "yes" or ENV "1" cause surprises)
        for k in ["notify_on_completion", "notify_on_file_detected"]:
            if k in config["notifications"].get("slack", {}):
                v = str(config["notifications"]["slack"].get(k)).lower()
                config["notifications"]["slack"][k] = v in ["1", "true", "yes"]
            if k in config["notifications"].get("email", {}):
                v = str(config["notifications"]["email"].get(k)).lower()
                config["notifications"]["email"][k] = v in ["1", "true", "yes"]

        logger.debug("Finalized config after defaults & normalization")

        # log effective notification switches at startup
        slack_cfg = config.get('notifications', {}).get('slack', {})
        email_cfg = config.get('notifications', {}).get('email', {})
        logger.info(
            "Notifications - Slack: %s (progress=%s, on_completion=%s, on_new_file=%s) | "
            "Email: %s (on_completion=%s, on_new_file=%s)",
            slack_cfg.get('enabled'),
            os.getenv('SLACK_NOTIFY_PROGRESS', 'false'),
            slack_cfg.get('notify_on_completion'),
            slack_cfg.get('notify_on_file_detected'),
            email_cfg.get('enabled'),
            email_cfg.get('notify_on_completion'),
            email_cfg.get('notify_on_file_detected'),
        )

        trigger = BulkAPITrigger(config)
        NotificationManager.send_startup_test_notifications(trigger)
        trigger_instance = trigger

        csv_file = os.getenv('CSV_FILE', 'AUTO')
        if csv_file != 'AUTO':
            logger.info(f"Processing specific file: {csv_file}")
            job_id = trigger.trigger_webhooks(
                csv_files=[csv_file],
                job_name=config['deployment'].get('job_name'),
                skip_rows=config['deployment'].get('skip_rows', 0)
            )
            logger.info(f"Initial job completed with ID: {job_id}")
        else:
            scan_existing_files(trigger)

        if config['deployment']['keep_alive']:
            keep_alive_with_watchdog(trigger, port=int(os.getenv('HEALTH_PORT', '8000')))
        else:
            logger.info("Job completed. Container will exit.")
        return


    # CLI mode
    parser = argparse.ArgumentParser(description="Enhanced Bulk API Trigger Platform")
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
        logger.error("CSV file path required. Use --help for usage information.")
        return

    config = validate_config(load_environment_config())
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

    logger.info(f"Starting CLI mode with config: {args.config or 'environment/defaults'}")

    if args.dry_run:
        logger.info("DRY RUN MODE - No requests will be sent")
        try:
            if args.csv_file.lower() == 'auto':
                webhooks, stats = read_multiple_csv_files(skip_rows=args.skip_rows)
            else:
                webhooks, stats = read_csv_with_validation(args.csv_file, skip_rows=args.skip_rows)
            logger.info(f"Validation complete: {len(webhooks)} valid webhooks found")
            logger.info(f"Statistics: {json.dumps(stats, indent=2, default=str)}")
            if webhooks:
                logger.info("Sample webhooks:")
                for i, webhook in enumerate(webhooks[:5]):
                    logger.info(f"  {i+1}. {webhook['name']}: {webhook['url']} [{webhook.get('method', 'GET')}]")
                if len(webhooks) > 5:
                    logger.info(f"  ... and {len(webhooks) - 5} more")
        except Exception as e:
            logger.error(f"Validation failed: {e}")
        return

    trigger = BulkAPITrigger(config)
    NotificationManager.send_startup_test_notifications(trigger)
    trigger_instance = trigger

    if args.csv_file.lower() == 'watchdog':
        logger.info("Starting in watchdog monitoring mode")
        scan_existing_files(trigger)
        keep_alive_with_watchdog(trigger)
        return

    files = [args.csv_file] if args.csv_file.lower() != 'auto' else None
    job_id = trigger.trigger_webhooks(
        csv_files=files,
        job_name=args.job_name,
        skip_rows=args.skip_rows
    )
    logger.info(f"Job completed with ID: {job_id}")
    if args.keep_alive:
        keep_alive_with_watchdog(trigger, port=args.health_port)


if __name__ == "__main__":
    try:
        try:
            import watchdog  # noqa: F401 (ensure available)
        except ImportError:
            logger.warning("Installing watchdog dependency...")
            import subprocess
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", "watchdog"])
                import watchdog  # noqa: F401
                logger.info("Watchdog dependency installed successfully")
            except Exception as e:
                logger.error(f"Watchdog install failed: {e}. Continuing without watchdog.")
                os.environ["WATCHDOG_ENABLED"] = "false"
        main()
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)