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
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
from tqdm import tqdm
from typing import List, Dict, Optional, Tuple
import schedule
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('webhook_trigger.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='webhook_results.db'):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database for storing results and job history"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create tables
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
                response_preview TEXT
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_history (
                job_id TEXT PRIMARY KEY,
                job_name TEXT,
                total_requests INTEGER,
                successful_requests INTEGER,
                failed_requests INTEGER,
                start_time TEXT NOT NULL,
                end_time TEXT,
                duration_seconds REAL,
                config TEXT,
                status TEXT DEFAULT 'running'
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
        
        conn.commit()
        conn.close()
    
    def save_webhook_result(self, job_id: str, result: Dict):
        """Save individual webhook result"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO webhook_results 
            (job_id, url, method, status, status_code, response_time, timestamp, attempt, error_message, response_preview)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            result.get('response_preview')
        ))
        
        conn.commit()
        conn.close()
    
    def start_job(self, job_id: str, job_name: str, total_requests: int, config: Dict):
        """Record job start"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO job_history 
            (job_id, job_name, total_requests, successful_requests, failed_requests, start_time, config, status)
            VALUES (?, ?, ?, 0, 0, ?, ?, 'running')
        ''', (job_id, job_name, total_requests, datetime.now().isoformat(), json.dumps(config)))
        
        conn.commit()
        conn.close()
    
    def finish_job(self, job_id: str, successful: int, failed: int):
        """Record job completion"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get start time to calculate duration
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
                duration_seconds = ?, status = 'completed'
            WHERE job_id = ?
        ''', (successful, failed, datetime.now().isoformat(), duration, job_id))
        
        conn.commit()
        conn.close()
    
    def get_job_stats(self, job_id: str) -> Dict:
        """Get job statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM job_history WHERE job_id = ?', (job_id,))
        job = cursor.fetchone()
        
        cursor.execute('''
            SELECT status, COUNT(*) 
            FROM webhook_results 
            WHERE job_id = ? 
            GROUP BY status
        ''', (job_id,))
        
        status_counts = dict(cursor.fetchall())
        conn.close()
        
        if job:
            return {
                'job_id': job[0],
                'job_name': job[1],
                'total_requests': job[2],
                'successful_requests': job[3],
                'failed_requests': job[4],
                'start_time': job[5],
                'end_time': job[6],
                'duration_seconds': job[7],
                'status_breakdown': status_counts
            }
        return {}

class NotificationManager:
    def __init__(self, config: Dict):
        self.config = config
        self.email_config = config.get('email', {})
        self.webhook_config = config.get('webhook', {})
        self.slack_config = config.get('slack', {})
    
    def send_email_notification(self, subject: str, body: str, to_emails: List[str]):
        """Send email notification"""
        if not self.email_config.get('enabled', False):
            return
            
        try:
            msg = MIMEMultipart()
            msg['From'] = self.email_config['from_email']
            msg['To'] = ', '.join(to_emails)
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'html'))
            
            server = smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port'])
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email notification sent to {to_emails}")
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def send_slack_notification(self, message: str):
        """Send Slack notification"""
        if not self.slack_config.get('enabled', False):
            return
            
        try:
            payload = {
                'text': message,
                'username': 'Bulk API Trigger Bot',
                'icon_emoji': ':robot_face:'
            }
            
            response = requests.post(self.slack_config['webhook_url'], json=payload)
            if response.status_code == 200:
                logger.info("Slack notification sent successfully")
            else:
                logger.error(f"Failed to send Slack notification: {response.status_code}")
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {e}")
    
    def send_job_completion_notification(self, job_stats: Dict):
        """Send notification when job completes"""
        success_rate = (job_stats['successful_requests'] / job_stats['total_requests'] * 100) if job_stats['total_requests'] > 0 else 0
        
        subject = f"Bulk API Job Completed: {job_stats['job_name']}"
        
        html_body = f"""
        <h2>Job Completion Report</h2>
        <table border="1" cellpadding="10">
            <tr><td><strong>Job Name</strong></td><td>{job_stats['job_name']}</td></tr>
            <tr><td><strong>Job ID</strong></td><td>{job_stats['job_id']}</td></tr>
            <tr><td><strong>Total Requests</strong></td><td>{job_stats['total_requests']}</td></tr>
            <tr><td><strong>Successful</strong></td><td>{job_stats['successful_requests']}</td></tr>
            <tr><td><strong>Failed</strong></td><td>{job_stats['failed_requests']}</td></tr>
            <tr><td><strong>Success Rate</strong></td><td>{success_rate:.2f}%</td></tr>
            <tr><td><strong>Duration</strong></td><td>{job_stats.get('duration_seconds', 0):.2f} seconds</td></tr>
            <tr><td><strong>Start Time</strong></td><td>{job_stats['start_time']}</td></tr>
            <tr><td><strong>End Time</strong></td><td>{job_stats['end_time']}</td></tr>
        </table>
        """
        
        slack_message = f"""
🚀 *Bulk API Job Completed*

*Job:* {job_stats['job_name']}
*Success Rate:* {success_rate:.2f}%
*Requests:* {job_stats['successful_requests']}/{job_stats['total_requests']} successful
*Duration:* {job_stats.get('duration_seconds', 0):.2f}s
        """
        
        # Send notifications
        if self.email_config.get('enabled') and self.email_config.get('notify_on_completion'):
            self.send_email_notification(subject, html_body, self.email_config.get('recipients', []))
        
        if self.slack_config.get('enabled') and self.slack_config.get('notify_on_completion'):
            self.send_slack_notification(slack_message)

class ConfigManager:
    @staticmethod
    def load_config_file(config_path: str) -> Dict:
        """Load configuration from YAML or JSON file"""
        if not os.path.exists(config_path):
            logger.warning(f"Config file {config_path} not found. Using defaults.")
            return {}
        
        try:
            with open(config_path, 'r') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    return yaml.safe_load(f)
                else:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Error loading config file: {e}")
            return {}
    
    @staticmethod
    def create_sample_config():
        """Create a sample configuration file"""
        sample_config = {
            'rate_limiting': {
                'base_rate_limit': 3.0,
                'starting_rate_limit': 3.0,
                'max_rate_limit': 5.0,
                'window_size': 20,
                'error_threshold': 0.3,
                'max_workers': 3
            },
            'retry': {
                'max_retries': 3,
                'retry_delay': 1.0,
                'timeout': 30
            },
            'notifications': {
                'email': {
                    'enabled': False,
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'username': 'your_email@gmail.com',
                    'password': 'your_app_password',
                    'from_email': 'your_email@gmail.com',
                    'recipients': ['admin@example.com'],
                    'notify_on_completion': True,
                    'notify_on_errors': True
                },
                'slack': {
                    'enabled': False,
                    'webhook_url': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK',
                    'notify_on_completion': True,
                    'notify_on_errors': True
                }
            },
            'database': {
                'enabled': True,
                'path': 'webhook_results.db'
            },
            'csv': {
                'required_columns': ['webhook_url'],
                'optional_columns': ['method', 'payload', 'header', 'name', 'group'],
                'chunk_size': 1000
            },
            'deployment': {
                'keep_alive': True,
                'log_level': 'INFO',
                'max_log_size_mb': 100
            }
        }
        
        with open('config.yaml', 'w') as f:
            yaml.dump(sample_config, f, default_flow_style=False, indent=2)
        
        logger.info("Sample configuration file 'config.yaml' created!")

class RateLimiter:
    def __init__(self, rate_limit, max_concurrent):
        self.rate_limit = rate_limit
        self.lock = Lock()
        self.last_request_time = 0
        self.semaphore = Semaphore(max_concurrent)
        
    def wait_if_needed(self):
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_request_time
            if time_since_last < self.rate_limit:
                time.sleep(self.rate_limit - time_since_last)
            self.last_request_time = time.time()
    
    def adjust_rate(self, new_rate_limit):
        with self.lock:
            self.rate_limit = new_rate_limit
            logger.info(f"Rate limit adjusted to {new_rate_limit:.2f} seconds")

    def get_rate(self):
        with self.lock:
            return self.rate_limit

class EnhancedResultsTracker:
    def __init__(self, job_id: str, db_manager: DatabaseManager):
        self.job_id = job_id
        self.db_manager = db_manager
        self.results = []
        self.lock = Lock()
    
    def add_result(self, result: Dict):
        with self.lock:
            result['job_id'] = self.job_id
            self.results.append(result)
            # Save to database
            self.db_manager.save_webhook_result(self.job_id, result)
    
    def save_results(self, filename: Optional[str] = None):
        """Save results to JSON file (legacy support)"""
        if not filename:
            filename = f'/logs/webhook_results_{self.job_id}.json'
        
        with self.lock:
            try:
                with open(filename, 'w') as f:
                    json.dump(self.results, f, indent=2)
                logger.info(f"Results saved to {filename}")
            except Exception as e:
                logger.error(f"Error saving results: {e}")

def make_request(url, method, payload, header, retries, rate_limiter, pbar, results_tracker, request_timeout=30):
    """Enhanced request function with better error handling and response tracking"""
    with rate_limiter.semaphore:
        rate_limiter.wait_if_needed()
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(retries):
            try:
                # Determine HTTP method
                if not method:
                    method_to_use = "POST" if payload else "GET"
                else:
                    method_to_use = method.upper()
                
                # Prepare headers
                headers = {'User-Agent': 'Bulk-API-Trigger/1.0'}
                if header:
                    try:
                        custom_headers = json.loads(header)
                        if isinstance(custom_headers, dict):
                            headers.update(custom_headers)
                    except Exception as e:
                        logger.error(f"Error parsing header JSON for {url}: {e}")

                # Prepare request parameters
                request_params = {
                    'timeout': request_timeout,
                    'headers': headers,
                    'allow_redirects': True
                }
                
                # Add payload if present
                if payload and method_to_use in ['POST', 'PUT', 'PATCH']:
                    try:
                        request_params['json'] = json.loads(payload)
                    except json.JSONDecodeError:
                        request_params['data'] = payload
                        headers['Content-Type'] = 'text/plain'

                # Execute the request
                response = requests.request(method_to_use, url, **request_params)
                
                # Handle response
                response_time = time.time() - start_time
                response_preview = response.text[:200] + "..." if len(response.text) > 200 else response.text
                
                if response.status_code in [200, 201, 202, 204]:
                    logger.info(f"✅ Success: {url} [{response.status_code}] ({response_time:.2f}s)")
                    
                    results_tracker.add_result({
                        'url': url,
                        'method': method_to_use,
                        'status': 'success',
                        'status_code': response.status_code,
                        'timestamp': datetime.now().isoformat(),
                        'response_time': response_time,
                        'attempt': attempt + 1,
                        'response_preview': response_preview
                    })
                    
                    pbar.update(1)
                    return 0
                else:
                    last_error = f"HTTP {response.status_code}: {response.text[:100]}"
                    logger.warning(f"❌ Error: {url} [Status: {response.status_code}] (attempt {attempt + 1}/{retries})")
                    
            except requests.exceptions.Timeout:
                last_error = f"Timeout after {request_timeout}s"
                logger.error(f"⏰ Timeout: {url} (attempt {attempt + 1}/{retries})")
            except requests.exceptions.ConnectionError as e:
                last_error = f"Connection error: {str(e)}"
                logger.error(f"🔌 Connection error: {url} (attempt {attempt + 1}/{retries})")
            except Exception as e:
                last_error = f"Unexpected error: {str(e)}"
                logger.error(f"💥 Error: {url}, Exception: {e} (attempt {attempt + 1}/{retries})")
            
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retrying

        # All retries failed
        results_tracker.add_result({
            'url': url,
            'method': method_to_use if 'method_to_use' in locals() else 'UNKNOWN',
            'status': 'failed',
            'status_code': response.status_code if 'response' in locals() else None,
            'timestamp': datetime.now().isoformat(),
            'response_time': time.time() - start_time,
            'attempt': retries,
            'error_message': last_error
        })
        
        pbar.update(1)
        return 1

def read_csv_with_validation(csv_file: str, chunk_size: int = 1000, skip_rows: int = 0) -> Tuple[List, Dict]:
    """Enhanced CSV reader with validation and statistics"""
    stats = {
        'total_rows': 0,
        'valid_rows': 0,
        'skipped_rows': skip_rows,
        'invalid_rows': 0,
        'columns_found': [],
        'missing_columns': []
    }
    
    all_webhooks = []
    
    try:
        with open(csv_file, 'r', encoding='utf-8', errors='replace') as file:
            reader = csv.DictReader(file)
            stats['columns_found'] = reader.fieldnames or []
            
            # Check for required columns
            required_cols = ['webhook_url']
            missing_cols = [col for col in required_cols if col not in stats['columns_found']]
            if missing_cols:
                stats['missing_columns'] = missing_cols
                raise ValueError(f"Missing required columns: {missing_cols}")
            
            for row_num, row in enumerate(reader, 1):
                stats['total_rows'] += 1
                
                # Skip rows if needed
                if row_num <= skip_rows:
                    continue
                    
                # Validate row
                if 'webhook_url' in row and row['webhook_url'].strip():
                    webhook_data = {
                        'url': row['webhook_url'].strip(),
                        'method': row.get('method', '').strip() or None,
                        'payload': row.get('payload', '').strip() or None,
                        'header': row.get('header', '').strip() or None,
                        'name': row.get('name', '').strip() or f"Request-{row_num}",
                        'group': row.get('group', '').strip() or 'default'
                    }
                    all_webhooks.append(webhook_data)
                    stats['valid_rows'] += 1
                else:
                    stats['invalid_rows'] += 1
                    logger.warning(f"Row {row_num}: Missing or empty webhook_url")
                    
    except FileNotFoundError:
        logger.error(f"CSV file '{csv_file}' not found.")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        raise
    
    logger.info(f"CSV Statistics: {stats}")
    return all_webhooks, stats

def read_multiple_csv_files(file_patterns: List[str] = None, chunk_size: int = 1000, skip_rows: int = 0):
    """Enhanced multi-file CSV reader"""
    if not file_patterns:
        file_patterns = [
            '/csv/*.csv'
        ]
    
    found_files = []
    for pattern in file_patterns:
        found_files.extend(glob.glob(pattern))
    
    # Remove duplicates and sort
    found_files = sorted(list(set(found_files)))
    
    if not found_files:
        logger.error(f"No CSV files found matching patterns: {file_patterns}")
        return [], {}
    
    logger.info(f"Found {len(found_files)} CSV file(s): {found_files}")
    
    all_webhooks = []
    combined_stats = {'files_processed': 0, 'total_valid_rows': 0, 'total_invalid_rows': 0}
    
    for csv_file in found_files:
        try:
            logger.info(f"Processing {csv_file}...")
            webhooks, stats = read_csv_with_validation(csv_file, chunk_size, skip_rows if combined_stats['files_processed'] == 0 else 0)
            all_webhooks.extend(webhooks)
            
            combined_stats['files_processed'] += 1
            combined_stats['total_valid_rows'] += stats['valid_rows']
            combined_stats['total_invalid_rows'] += stats['invalid_rows']
            
            # Only skip rows in first file
            skip_rows = 0
            
        except Exception as e:
            logger.error(f"Error processing {csv_file}: {e}")
            continue
    
    return all_webhooks, combined_stats

def generate_job_id(prefix: str = "job") -> str:
    """Generate unique job ID"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hash_part = hashlib.md5(f"{timestamp}{time.time()}".encode()).hexdigest()[:8]
    return f"{prefix}_{timestamp}_{hash_part}"

class BulkAPITrigger:
    def __init__(self, config: Dict):
        self.config = config
        self.db_manager = DatabaseManager(config.get('database', {}).get('path', 'webhook_results.db'))
        self.notification_manager = NotificationManager(config.get('notifications', {}))
    
    def trigger_webhooks(self, 
                        csv_files: List[str] = None, 
                        job_name: str = None,
                        skip_rows: int = 0) -> str:
        """Main method to trigger bulk webhooks"""
        
        job_id = generate_job_id()
        if not job_name:
            job_name = f"Bulk API Job {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        logger.info(f"🚀 Starting job: {job_name} (ID: {job_id})")
        
        try:
            # Load webhooks
            if csv_files and csv_files[0] != "AUTO":
                # Single file mode
                all_webhooks, stats = read_csv_with_validation(csv_files[0], skip_rows=skip_rows)
            else:
                # Multi-file mode
                all_webhooks, stats = read_multiple_csv_files(skip_rows=skip_rows)
            
            if not all_webhooks:
                logger.warning("No valid webhook URLs found.")
                return job_id
            
            total_requests = len(all_webhooks)
            logger.info(f"📊 Loaded {total_requests} webhook requests")
            
            # Initialize job in database
            self.db_manager.start_job(job_id, job_name, total_requests, self.config)
            
            # Setup rate limiting and tracking
            rate_config = self.config.get('rate_limiting', {})
            rate_limiter = RateLimiter(
                rate_config.get('starting_rate_limit', 3.0),
                rate_config.get('max_workers', 3)
            )
            
            results_tracker = EnhancedResultsTracker(job_id, self.db_manager)
            error_window = []
            
            # Progress tracking
            successful_count = 0
            failed_count = 0
            processed_count = 0
            
            logger.info(f"⚡ Processing {total_requests} requests with {rate_config.get('max_workers', 3)} workers")
            
            # Execute requests
            with ThreadPoolExecutor(max_workers=rate_config.get('max_workers', 3)) as executor:
                with tqdm(total=total_requests, desc="🌐 API Requests", dynamic_ncols=True, 
                         bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                    
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
                            self.config.get('retry', {}).get('timeout', 30)
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
                            else:
                                failed_count += 1
                            
                            # Progress updates
                            if processed_count % 10 == 0:
                                success_rate = (successful_count / processed_count) * 100
                                logger.info(f"📈 Progress: {processed_count}/{total_requests} ({success_rate:.1f}% success)")
                            
                        except Exception as e:
                            webhook = futures[future]
                            logger.error(f"💥 Failed processing {webhook['url']}: {e}")
                            failed_count += 1
                            processed_count += 1
                        
                        # Dynamic rate adjustment
                        if pbar.n % rate_config.get('window_size', 20) == 0 and pbar.n > 0 and error_window:
                            error_rate = sum(error_window) / len(error_window)
                            current_rate = rate_limiter.get_rate()
                            
                            if error_rate > rate_config.get('error_threshold', 0.3):
                                new_rate = min(rate_config.get('max_rate_limit', 5.0), current_rate * 1.2)
                                rate_limiter.adjust_rate(new_rate)
                            elif error_rate < rate_config.get('error_threshold', 0.3) / 2:
                                new_rate = max(rate_config.get('base_rate_limit', 3.0), current_rate / 1.2)
                                rate_limiter.adjust_rate(new_rate)
            
            # Finalize job
            self.db_manager.finish_job(job_id, successful_count, failed_count)
            results_tracker.save_results()
            
            # Generate summary
            job_stats = self.db_manager.get_job_stats(job_id)
            success_rate = (successful_count / total_requests) * 100 if total_requests > 0 else 0

            BOX_WIDTH = 70  # Total width of the box (including borders)
            # Calculate padding for the success rate line
            success_rate_str = f"{success_rate:.2f}%"
            label = "📊 Success Rate: "
            pad_length = BOX_WIDTH - 4 - len(label) - len(success_rate_str)  # 4 for borders and spaces
            logger.info(f"""
╔{'═' * (BOX_WIDTH - 2)}╗
║{' ' * ((BOX_WIDTH - 2 - len('🎯 JOB COMPLETED')) // 2)}🎯 JOB COMPLETED{' ' * ((BOX_WIDTH - 2 - len('🎯 JOB COMPLETED') + 1) // 2)}║
╠{'═' * (BOX_WIDTH - 2)}╣
║ Job Name: {job_name:<{BOX_WIDTH - 15}}║
║ Job ID: {job_id:<{BOX_WIDTH - 13}}║
║ Total Requests: {total_requests:<{BOX_WIDTH - 20}}║
║ ✅ Successful: {successful_count:<{BOX_WIDTH - 18}}║
║ ❌ Failed: {failed_count:<{BOX_WIDTH - 14}}║
║ {label}{success_rate_str}{' ' * pad_length}║
║ ⏱️  Duration: {job_stats.get('duration_seconds', 0):.2f} seconds{' ' * (BOX_WIDTH - 19 - len(f'{job_stats.get("duration_seconds", 0):.2f} seconds'))}║
╚{'═' * (BOX_WIDTH - 2)}╝
            """)
            
            # Send notifications
            self.notification_manager.send_job_completion_notification(job_stats)
            
            return job_id
            
        except Exception as e:
            logger.error(f"💥 Job failed: {e}")
            self.db_manager.finish_job(job_id, 0, 0)
            raise

def load_environment_config():
    """Load configuration from environment variables with enhanced options"""
    return {
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
            'timeout': int(os.getenv('REQUEST_TIMEOUT', '30'))
        },
        'deployment': {
            'keep_alive': os.getenv('KEEP_ALIVE', 'true').lower() == 'true',
            'skip_rows': int(os.getenv('SKIP_ROWS', '0')),
            'job_name': os.getenv('JOB_NAME', None)
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
                'notify_on_completion': os.getenv('EMAIL_NOTIFY_COMPLETION', 'true').lower() == 'true'
            },
            'slack': {
                'enabled': os.getenv('SLACK_NOTIFICATIONS', 'false').lower() == 'true',
                'webhook_url': os.getenv('SLACK_WEBHOOK_URL', ''),
                'notify_on_completion': os.getenv('SLACK_NOTIFY_COMPLETION', 'true').lower() == 'true'
            }
        },
        'database': {
            'enabled': os.getenv('DATABASE_ENABLED', 'true').lower() == 'true',
            'path': os.getenv('DATABASE_PATH', 'webhook_results.db')
        }
    }

def keep_alive():
    """Enhanced keep-alive with health check endpoint simulation"""
    logger.info("🔄 Webhook processing completed. Keeping container alive...")
    logger.info("📊 Database contains job history and results for analysis")
    logger.info("🏥 Container health: OK - Running indefinitely")
    
    try:
        heartbeat_count = 0
        while True:
            time.sleep(3600)  # 1 hour intervals
            heartbeat_count += 1
            logger.info(f"💓 Heartbeat #{heartbeat_count} - Container running for {heartbeat_count} hours")
            
            # Optional: Clean up old logs or perform maintenance
            if heartbeat_count % 24 == 0:  # Every 24 hours
                logger.info("🧹 Daily maintenance check...")
                
    except KeyboardInterrupt:
        logger.info("🛑 Received interrupt signal. Shutting down gracefully...")

def main():
    """Enhanced main function with multiple execution modes"""
    
    # Check for different execution modes
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("""
🚀 Bulk API Trigger Platform - Enhanced Version
===============================================

USAGE MODES:
1. Deployment Mode (Auto-detected):
   - Set DEPLOYMENT_MODE=true or RAILWAY_ENVIRONMENT
   - Processes all CSV files automatically
   
2. CLI Mode:
   python webhook_trigger.py <csv_file> [options]
   
3. Config Generation:
   python webhook_trigger.py --create-config
   
4. Interactive Mode:
   python webhook_trigger.py --interactive

ENVIRONMENT VARIABLES:
- DEPLOYMENT_MODE=true          : Enable deployment mode
- KEEP_ALIVE=true              : Keep container running
- JOB_NAME="My API Job"        : Custom job name
- CSV_FILE_PATTERN="*.csv"     : CSV file search pattern
- MAX_WORKERS=5                : Parallel request limit
- BASE_RATE_LIMIT=2.0          : Base delay between requests
- EMAIL_NOTIFICATIONS=true     : Enable email alerts
- SLACK_NOTIFICATIONS=true     : Enable Slack alerts
- DATABASE_ENABLED=true        : Enable result storage

CSV FORMAT:
Required columns: webhook_url
Optional columns: method, payload, header, name, group

EXAMPLES:
- Basic: webhook_url
- Advanced: webhook_url,method,payload,header,name,group
- With auth: webhook_url,header (header contains {"Authorization": "Bearer token"})
        """)
        return
    
    # Create sample config
    if len(sys.argv) > 1 and sys.argv[1] == '--create-config':
        ConfigManager.create_sample_config()
        return
    
    # Interactive mode
    if len(sys.argv) > 1 and sys.argv[1] == '--interactive':
        print("🎯 Interactive Bulk API Trigger")
        print("=" * 40)
        
        csv_file = input("CSV file path (or 'auto' for all CSV files): ").strip()
        job_name = input("Job name (optional): ").strip()
        skip_rows = input("Rows to skip (default: 0): ").strip()
        keep_running = input("Keep container alive after completion? (y/N): ").strip().lower()
        
        config = load_environment_config()
        if job_name:
            config['deployment']['job_name'] = job_name
        if skip_rows.isdigit():
            config['deployment']['skip_rows'] = int(skip_rows)
        config['deployment']['keep_alive'] = keep_running in ['y', 'yes', 'true']
        
        trigger = BulkAPITrigger(config)
        files = [csv_file] if csv_file.lower() != 'auto' else None
        
        job_id = trigger.trigger_webhooks(
            csv_files=files,
            job_name=job_name or None,
            skip_rows=int(skip_rows) if skip_rows.isdigit() else 0
        )
        
        if config['deployment']['keep_alive']:
            keep_alive()
        return
    
    # Deployment mode (Coolify, Railway, Docker, etc.)
    if os.getenv('DEPLOYMENT_MODE') or os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('DOCKER_CONTAINER'):
        logger.info("🐳 Deployment mode detected")
        config = load_environment_config()
        
        # Load additional config file if exists
        for config_file in ['config.yaml', 'config.yml', 'config.json']:
            if os.path.exists(config_file):
                file_config = ConfigManager.load_config_file(config_file)
                # Merge configs (environment takes precedence)
                for key in file_config:
                    if key not in config:
                        config[key] = file_config[key]
                    elif isinstance(config[key], dict) and isinstance(file_config[key], dict):
                        for subkey in file_config[key]:
                            if subkey not in config[key]:
                                config[key][subkey] = file_config[key][subkey]
                logger.info(f"📋 Loaded additional config from {config_file}")
                break
        
        logger.info(f"⚙️  Configuration loaded: {json.dumps(config, indent=2, default=str)}")
        
        trigger = BulkAPITrigger(config)
        
        # Check for specific CSV file or use auto-discovery
        csv_file = os.getenv('CSV_FILE', 'AUTO')
        files = [csv_file] if csv_file != 'AUTO' else None
        
        job_id = trigger.trigger_webhooks(
            csv_files=files,
            job_name=config['deployment'].get('job_name'),
            skip_rows=config['deployment'].get('skip_rows', 0)
        )
        
        if config['deployment']['keep_alive']:
            keep_alive()
        else:
            logger.info("🏁 Job completed. Container will exit.")
            
    else:
        # CLI mode
        parser = argparse.ArgumentParser(description="🚀 Enhanced Bulk API Trigger Platform")
        parser.add_argument("csv_file", nargs='?', help="Path to CSV file or 'auto' for auto-discovery")
        parser.add_argument("--config", "-c", help="Configuration file path (YAML/JSON)")
        parser.add_argument("--job-name", "-n", help="Custom job name")
        parser.add_argument("--skip-rows", "-s", type=int, default=0, help="Number of rows to skip")
        parser.add_argument("--keep-alive", "-k", action="store_true", help="Keep process running")
        parser.add_argument("--workers", "-w", type=int, help="Number of parallel workers")
        parser.add_argument("--rate-limit", "-r", type=float, help="Base rate limit in seconds")
        parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
        parser.add_argument("--dry-run", "-d", action="store_true", help="Validate CSV without sending requests")
        
        args = parser.parse_args()
        
        if not args.csv_file:
            logger.error("❌ CSV file path required. Use --help for usage information.")
            return
        
        # Load configuration
        config = load_environment_config()
        
        if args.config:
            file_config = ConfigManager.load_config_file(args.config)
            # Merge configurations
            for key, value in file_config.items():
                if isinstance(value, dict) and key in config:
                    config[key].update(value)
                else:
                    config[key] = value
        
        # Override with CLI arguments
        if args.workers:
            config['rate_limiting']['max_workers'] = args.workers
        if args.rate_limit:
            config['rate_limiting']['base_rate_limit'] = args.rate_limit
            config['rate_limiting']['starting_rate_limit'] = args.rate_limit
        if args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        
        logger.info(f"🎯 Starting CLI mode with config: {args.config or 'environment/defaults'}")
        
        # Dry run mode
        if args.dry_run:
            logger.info("🧪 DRY RUN MODE - No requests will be sent")
            try:
                if args.csv_file.lower() == 'auto':
                    webhooks, stats = read_multiple_csv_files(skip_rows=args.skip_rows)
                else:
                    webhooks, stats = read_csv_with_validation(args.csv_file, skip_rows=args.skip_rows)
                
                logger.info(f"✅ Validation complete: {len(webhooks)} valid webhooks found")
                logger.info(f"📊 Statistics: {json.dumps(stats, indent=2)}")
                
                # Show sample webhooks
                if webhooks:
                    logger.info("📋 Sample webhooks:")
                    for i, webhook in enumerate(webhooks[:3]):
                        logger.info(f"  {i+1}. {webhook['name']}: {webhook['url']} [{webhook.get('method', 'GET')}]")
                    if len(webhooks) > 3:
                        logger.info(f"  ... and {len(webhooks) - 3} more")
                        
            except Exception as e:
                logger.error(f"❌ Validation failed: {e}")
            return
        
        # Execute job
        trigger = BulkAPITrigger(config)
        
        files = [args.csv_file] if args.csv_file.lower() != 'auto' else None
        
        job_id = trigger.trigger_webhooks(
            csv_files=files,
            job_name=args.job_name,
            skip_rows=args.skip_rows
        )
        
        logger.info(f"🏆 Job completed with ID: {job_id}")
        
        if args.keep_alive:
            keep_alive()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        sys.exit(1)