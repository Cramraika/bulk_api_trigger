def read_multiple_csv_files(file_patterns: List[str] = None, chunk_size: int = 1000, skip_rows: int = 0):
    """Enhanced multi-file CSV reader with pattern matching"""
    if not file_patterns:
        file_patterns = [
            '/app/data/csv/*.csv',
            '/app/data/*.csv'
        ]
    
    found_files = []
    for pattern in file_patterns:
        found_files.extend(glob.glob(pattern))
    
    # Remove duplicates and sort by modification time (newest first)
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
            webhooks, stats = read_csv_with_validation(csv_file, chunk_size, skip_rows if combined_stats['files_processed'] == 0 else 0)
            all_webhooks.extend(webhooks)
            
            combined_stats['files_processed'] += 1
            combined_stats['total_valid_rows'] += stats['valid_rows']
            combined_stats['total_invalid_rows'] += stats['invalid_rows']
            combined_stats['total_file_size'] += stats['file_size']
            combined_stats['files_stats'].append({
                'file': csv_file,
                'stats': stats
            })
            
            # Only skip rows in first file
            skip_rows = 0
            
        except Exception as e:
            logger.error(f"❌ Error processing {csv_file}: {e}")
            continue
    
    return all_webhooks, combined_stats

def generate_job_id(prefix: str = "job") -> str:
    """Generate unique job ID with timestamp and random component"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hash_part = hashlib.md5(f"{timestamp}{time.time()}".encode()).hexdigest()[:8]
    return f"{prefix}_{timestamp}_{hash_part}"

def archive_processed_file(file_path: str, config: Dict) -> bool:
    """Archive processed CSV files"""
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
        self.processing_thread = None
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
                # Single file mode
                all_webhooks, stats = read_csv_with_validation(csv_files[0], skip_rows=skip_rows)
                csv_file_path = csv_files[0]
            else:
                # Multi-file mode
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
            error_window = []
            
            # Progress tracking
            successful_count = 0
            failed_count = 0
            processed_count = 0
            
            logger.info(f"⚡ Processing {total_requests} requests with {rate_config.get('max_workers', 3)} workers")
            
            # Execute requests
            with ThreadPoolExecutor(max_workers=rate_config.get('max_workers', 3)) as executor:
                with tqdm(
                            total=total_requests,
                            desc="🌐 API Requests",
                            dynamic_ncols=False,              # avoid querying terminal width
                            ascii=True,                       # safer in non-UTF8 log renderers
                            file=sys.stdout,                  # write to stdout (your StreamHandler already uses stdout)
                            mininterval=1.0,                  # throttle redraws to avoid spam
                            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                            # If not a TTY, only show if user explicitly forces it
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
                            self.global_request_sema
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
                                    # Don't let progress Slack failures break the job
                                    pass
                            else:
                                failed_count += 1
                            
                            # Progress updates
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
            
            # Archive processed files if configured
            if csv_files and csv_files[0] != "AUTO":
                archive_processed_file(csv_files[0], self.config)
            
            # Generate summary
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
            
            # Send notifications
            self.notification_manager.send_job_completion_notification(job_stats)
            
            # Save metrics
            if self.config.get('deployment', {}).get('metrics_enabled', True):
                self.db_manager.save_metric('job_success_rate', success_rate)
                self.db_manager.save_metric('job_duration', job_stats.get('duration_seconds', 0))
                self.db_manager.save_metric('job_throughput', metrics['throughput'])
            
    
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

        # NEW: multiple file-processing workers
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

                    # Missing file?
                    if not os.path.exists(job_info['csv_file']):
                        logger.warning(f"File no longer exists: {job_info['csv_file']}")
                        self.db_manager.update_file_status(job_info['csv_file'], 'failed')
                        continue

                    # Changed during debounce? Requeue (keep inflight)
                    current_hash = self._calculate_file_hash(job_info['csv_file'])
                    if current_hash and current_hash != job_info['file_hash']:
                        logger.info(f"File changed during debounce, re-queuing: {job_info['csv_file']}")
                        job_info['file_hash'] = current_hash
                        self.job_queue.put(job_info)
                        continue

                    # Already completed?
                    if self.db_manager.is_file_processed(job_info['csv_file'], job_info['file_hash']):
                        logger.info(f"File already processed: {os.path.basename(job_info['csv_file'])}")
                        continue

                    # Optional: notify detected (if you prefer at worker time)
                    self.notification_manager.send_file_detected_notification(job_info)

                    # Processing start
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
                    # ALWAYS release inflight + task_done
                    with self.inflight_lock:
                        self.inflight_files.discard(job_info['csv_file'])
                    self.job_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in queue processing worker: {e}")
                logger.error(traceback.format_exc())


    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate file hash for duplicate detection"""
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
        
        if self.processing_thread:
            self.processing_thread.join(timeout=10)
        
        logger.info("🛑 Watchdog system stopped")
    
    def get_system_status(self) -> Dict:
        """Get comprehensive system status"""
        try:
            queue_size = self.job_queue.qsize() if hasattr(self.job_queue, 'qsize') else 0
        except:
            queue_size = 0
        
        return {
            'watchdog_enabled': self.watchdog_enabled,
            'watchdog_running': self.observer is not None and self.observer.is_alive() if self.observer else False,
            'processing_thread_running': self.processing_thread is not None and self.processing_thread.is_alive() if self.processing_thread else False,
            'queue_size': queue_size,
            'database_accessible': self._test_database_connection(),
            'timestamp': datetime.now().isoformat()
        }
    
    def _test_database_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1')
                return True
        except Exception:
            return False

def create_health_check_server():
    """Create a simple health check HTTP server"""
    try:
        from http.server import HTTPServer, BaseHTTPRequestHandler
        import threading
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
                    # Simple metrics endpoint
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    
                    try:
                        with self.trigger.db_manager.get_connection() as conn:
                            cursor = conn.cursor()
                            
                            # Get recent job stats
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
    
    # Start health check server if enabled
    health_server_starter = create_health_check_server()
    if health_server_starter and trigger.config.get('deployment', {}).get('health_check_enabled', True):
        health_thread = Thread(target=health_server_starter, args=(trigger, 8000), daemon=True)
        health_thread.start()
        logger.info("🏥 Health check server started on port 8000")
    
    # Start watchdog
    trigger.start_watchdog()
    
    logger.info("📊 Database contains job history and results for analysis")
    logger.info("👀 Watchdog is monitoring for new CSV files")
    logger.info("🏥 Container health: OK - Running with watchdog")
    
    try:
        heartbeat_count = 0
        while True:
            time.sleep(300)  # 5 minute intervals
            heartbeat_count += 1
            
            # Get system status
            status = trigger.get_system_status()
            
            logger.info(f"💓 Heartbeat #{heartbeat_count} - System Status: "
                       f"Queue: {status['queue_size']}, "
                       f"Watchdog: {'✅' if status['watchdog_running'] else '❌'}, "
                       f"DB: {'✅' if status['database_accessible'] else '❌'}")
            
            # Backup database periodically
            if heartbeat_count % 288 == 0:  # Every 24 hours (288 * 5min)
                logger.info("🗄️  Daily maintenance check...")
                try:
                    backup_database(trigger.db_manager)
                except Exception as e:
                    logger.error(f"Database backup failed: {e}")
            
            # Log queue status if not empty
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
        
        # Copy database file
        shutil.copy2(db_manager.db_path, backup_path)
        
        # Keep only last 7 backups
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
                # Calculate file hash
                file_hash = trigger._calculate_file_hash(csv_file)
                if not file_hash:
                    continue
                
                # Check if already processed
                if trigger.db_manager.is_file_processed(csv_file, file_hash):
                    logger.debug(f"File already processed: {os.path.basename(csv_file)}")
                    continue
                
                # Validate CSV
                if not os.path.getsize(csv_file) > 0:
                    continue
                
                # Add to queue for processing
                file_stats = os.stat(csv_file)
                job_info = {
                    'csv_file': csv_file,
                    'file_hash': file_hash,
                    'file_size': file_stats.st_size,
                    'row_count': 0,  # Will be calculated during processing
                    'event_type': 'startup_scan',
                    'detected_at': datetime.now().isoformat()
                }
                
                trigger.db_manager.track_file(csv_file, file_hash, file_stats.st_size, rows_count=0)
                trigger.db_manager.update_file_status(csv_file, 'queued')

                with trigger.inflight_lock:
                    trigger.inflight_files.add(csv_file)

                trigger.job_queue.put(job_info)
                logger.info(f"📄 Queued existing file: {os.path.basename(csv_file)}")   
                
def main():
    """Enhanced main function with watchdog and comprehensive error handling"""
    
    # Setup signal handlers for graceful shutdown
    import signal
    
    trigger_instance = None
    
    def signal_handler(signum, frame):
        logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
        if trigger_instance:
            trigger_instance.stop_watchdog()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check for different execution modes
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("""
🚀 Enhanced Bulk API Trigger Platform with Watchdog
==================================================

FEATURES:
✅ Automatic CSV file monitoring and processing
✅ Real-time file system watchdog
✅ Advanced rate limiting and retry logic
✅ Comprehensive job tracking and metrics
✅ Email and Slack notifications
✅ Health checks and monitoring
✅ Database backup and archiving
✅ Multi-format CSV support

USAGE MODES:
1. Deployment Mode (Auto-detected):
   - Set DEPLOYMENT_MODE=true or RAILWAY_ENVIRONMENT
   - Automatically processes CSV files and monitors for new ones
   
2. CLI Mode:
   python webhook_trigger.py <csv_file> [options]
   
3. Config Generation:
   python webhook_trigger.py --create-config
   
4. Interactive Mode:
   python webhook_trigger.py --interactive

WATCHDOG ENVIRONMENT VARIABLES:
- WATCHDOG_ENABLED=true         : Enable file monitoring
- WATCH_PATHS="/data/csv"       : Comma-separated watch directories
- AUTO_PROCESS=true             : Auto-process detected files
- DEBOUNCE_DELAY=3.0            : Wait time before processing (seconds)
- ARCHIVE_PROCESSED=true        : Archive processed files

ENHANCED FEATURES:
- HEALTH_CHECK_ENABLED=true     : Enable HTTP health endpoint
- METRICS_ENABLED=true          : Collect performance metrics
- DATABASE_BACKUP=true          : Enable automatic backups
- EMAIL_NOTIFY_FILE_DETECTED=true : Email on file detection
- SLACK_NOTIFY_FILE_DETECTED=true : Slack on file detection

ENDPOINTS (when health check enabled):
- http://localhost:8000/health  : System health status
- http://localhost:8000/metrics : Performance metrics

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
        print("🎯 Interactive Enhanced Bulk API Trigger")
        print("=" * 45)
        
        csv_file = input("CSV file path (or 'auto' for watchdog mode): ").strip()
        job_name = input("Job name (optional): ").strip()
        skip_rows = input("Rows to skip (default: 0): ").strip()
        keep_running = input("Keep container alive with watchdog? (Y/n): ").strip().lower()
        enable_watchdog = input("Enable file monitoring? (Y/n): ").strip().lower()
        
        config = load_environment_config()
        if job_name:
            config['deployment']['job_name'] = job_name
        if skip_rows.isdigit():
            config['deployment']['skip_rows'] = int(skip_rows)
        config['deployment']['keep_alive'] = keep_running not in ['n', 'no', 'false']
        config['watchdog']['enabled'] = enable_watchdog not in ['n', 'no', 'false']
        
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
    
    # Deployment mode (Coolify, Railway, Docker, etc.)
    if os.getenv('DEPLOYMENT_MODE') or os.getenv('RAILWAY_ENVIRONMENT') or os.getenv('DOCKER_CONTAINER'):
        logger.info("🐳 Enhanced deployment mode detected")
        config = load_environment_config()
        
        # Load additional config file if exists
        config_files = ['config.yaml', 'config.yml', 'config.json']
        for config_file in config_files:
            if os.path.exists(config_file):
                file_config = ConfigManager.load_config_file(config_file)
                # Deep merge configs (environment takes precedence)
                def merge_configs(base, override):
                    for key, value in override.items():
                        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                            merge_configs(base[key], value)
                        elif key not in base:
                            base[key] = value
                
                merge_configs(config, file_config)
                logger.info(f"📋 Loaded additional config from {config_file}")
                break
        
        logger.info(f"⚙️  Configuration loaded successfully")
        logger.debug(f"Config details: {json.dumps(config, indent=2, default=str)}")
        
        trigger = BulkAPITrigger(config)
        trigger_instance = trigger
        
        # Process initial files if any exist
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
            # Scan for existing files
            scan_existing_files(trigger)
        
        if config['deployment']['keep_alive']:
            keep_alive_with_watchdog(trigger)
        else:
            logger.info("🏁 Job completed. Container will exit.")
            
    else:
        # CLI mode with enhanced options
        parser = argparse.ArgumentParser(description="🚀 Enhanced Bulk API Trigger Platform")
        parser.add_argument("csv_file", nargs='?', help="Path to CSV file, 'auto' for auto-discovery, or 'watchdog' for monitoring mode")
        parser.add_argument("--config", "-c", help="Configuration file path (YAML/JSON)")
        parser.add_argument("--job-name", "-n", help="Custom job name")
        parser.add_argument("--skip-rows", "-s", type=int, default=0, help="Number of rows to skip")
        parser.add_argument("--keep-alive", "-k", action="store_true", help="Keep process running with watchdog")
        parser.add_argument("--workers", "-w", type=int, help="Number of parallel workers")
        parser.add_argument("--rate-limit", "-r", type=float, help="Base rate limit in seconds")
        parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
        parser.add_argument("--dry-run", "-d", action="store_true", help="Validate CSV without sending requests")
        parser.add_argument("--watchdog", action="store_true", help="Enable file monitoring")
        parser.add_argument("--no-watchdog", action="store_true", help="Disable file monitoring")
        parser.add_argument("--health-port", type=int, default=8000, help="Health check server port")
        
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
        if args.watchdog:
            config['watchdog']['enabled'] = True
        if args.no_watchdog:
            config['watchdog']['enabled'] = False
        
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
                logger.info(f"📊 Statistics: {json.dumps(stats, indent=2, default=str)}")
                
                # Show sample webhooks
                if webhooks:
                    logger.info("📋 Sample webhooks:")
                    for i, webhook in enumerate(webhooks[:5]):
                        logger.info(f"  {i+1}. {webhook['name']}: {webhook['url']} [{webhook.get('method', 'GET')}]")
                    if len(webhooks) > 5:
                        logger.info(f"  ... and {len(webhooks) - 5} more")
                        
            except Exception as e:
                logger.error(f"❌ Validation failed: {e}")
            return
        
        # Initialize trigger
        trigger = BulkAPITrigger(config)
        trigger_instance = trigger
        
        # Watchdog mode
        if args.csv_file.lower() == 'watchdog':
            logger.info("👀 Starting in watchdog monitoring mode")
            scan_existing_files(trigger)
            keep_alive_with_watchdog(trigger)
            return
        
        # Execute job
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
        # Install watchdog dependency at runtime if not available
        try:
            import watchdog
        except ImportError:
            logger.warning("📦 Installing watchdog dependency...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "watchdog"])
            import watchdog
            logger.info("✅ Watchdog dependency installed successfully")
        
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Process interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)def load_environment_config():
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
            'timeout': int(os.getenv('REQUEST_TIMEOUT', '30'))
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
            'path': os.getenv('DATABASE_PATH', '/app/data/webhook_results.db'),
            'backup_enabled': os.getenv('DATABASE_BACKUP', 'true').lower() == 'true'
        },
        'csv': {
            'archive_processed': os.getenv('ARCHIVE_PROCESSED', 'true').lower() == 'true',
            'archive_path': os.getenv('ARCHIVE_PATH', '/app/data/csv/processed'),
            'duplicates_path': os.getenv('DUPLICATES_PATH', '/app/data/csv/duplicates'),
            'rejected_path': os.getenv('REJECTED_PATH', '/app/data/csv/rejected'),
            'chunk_size': int(os.getenv('CSV_CHUNK_SIZE', '1000')),
            'archive_on_validation_failure': os.getenv('ARCHIVE_ON_VALIDATION_FAILURE', 'true').lower() == 'true'
        }
    }import csv
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
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass
# Configure logging with rotation
from logging.handlers import RotatingFileHandler

def _to_float(v, default=0.0):
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

def _detect_encoding(csv_path: str) -> str:
    """Detect CSV file encoding"""
    # try UTF-8 BOM first (fixes BOM issue), then UTF-8, then fallbacks
    for enc in ('utf-8-sig', 'utf-8', 'iso-8859-1', 'cp1252'):
        try:
            with open(csv_path, 'r', encoding=enc) as f:
                f.read(256)  # smoke test
            return enc
        except UnicodeDecodeError:
            continue
    # last resort
    return 'utf-8'

def _detect_delimiter(file_obj) -> str:
    """Detect CSV delimiter from file object"""
    # robust sniffer with fallback to heuristics
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
    """Normalize CSV headers by stripping BOM, trimming whitespace, and converting to lowercase"""
    return [ (h or '').lstrip('\ufeff').strip().lower() for h in (raw_headers or []) ]

def setup_logging():
    """Setup enhanced logging with rotation"""
    log_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
    )
    
    # Create logs directory
    os.makedirs('/app/data/logs', exist_ok=True)
    
    # File handler with rotation
    file_handler = RotatingFileHandler(
        '/app/data/logs/webhook_trigger.log',
        maxBytes=100*1024*1024,  # 100MB
        backupCount=5
    )
    file_handler.setFormatter(log_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_formatter)
    
    # Root logger
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root_logger = logging.getLogger()
    root_logger.setLevel(level) 
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return logging.getLogger(__name__)

logger = setup_logging()

class DatabaseManager:
    def __init__(self, db_path='/app/data/webhook_results.db'):
        self.db_path = db_path
        # ensure parent directory exists
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        # create the lock BEFORE any connection usage
        self._connection_lock = Lock()
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Thread-safe database connection context manager"""
        with self._connection_lock:
            conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
            try:
                # optional: better concurrency
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
                yield conn
            finally:
                conn.close()
    
    def init_database(self):
        """Initialize SQLite database for storing results and job history"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create tables with enhanced schema
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
            
            # Create indexes for better performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_job_id ON webhook_results(job_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_webhook_timestamp ON webhook_results(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_job_status ON job_history(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_file_status ON file_tracking(status)')
            
            conn.commit()
    
    def save_webhook_result(self, job_id: str, result: Dict):
        """Save individual webhook result with enhanced metrics"""
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
        """Record job start with enhanced tracking"""
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
        """Record job completion with metrics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Calculate average response time
            cursor.execute('''
                SELECT AVG(response_time) 
                FROM webhook_results 
                WHERE job_id = ? AND response_time IS NOT NULL
            ''', (job_id,))
            avg_response_time = cursor.fetchone()[0] or 0
            
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
                    duration_seconds = ?, status = 'completed', average_response_time = ?
                WHERE job_id = ?
            ''', (successful, failed, datetime.now().isoformat(), duration, avg_response_time, job_id))
            
            conn.commit()
    
    def track_file(self, file_path: str, file_hash: str, file_size: int, rows_count: int = 0):
        """Track processed files to avoid duplicates"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO file_tracking 
                (file_path, file_hash, file_size, rows_count, status)
                VALUES (?, ?, ?, ?, 'pending')
            ''', (file_path, file_hash, file_size, rows_count))
            
            conn.commit()
    
    def is_file_processed(self, file_path: str, file_hash: str) -> bool:
        """Check if file has been processed"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT COUNT(*) FROM file_tracking 
                WHERE file_path = ? AND file_hash = ? AND status = 'completed'
            ''', (file_path, file_hash))
            
            return cursor.fetchone()[0] > 0
    
    def update_file_status(self, file_path: str, status: str, job_id: str = None):
        """Update file processing status"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE file_tracking 
                SET status = ?, job_id = ?, last_processed = CURRENT_TIMESTAMP
                WHERE file_path = ?
            ''', (status, job_id, file_path))
            
            conn.commit()

    def get_file_status(self, file_path: str) -> Optional[str]:
        """Return current file_tracking.status for a file (or None if not tracked)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('SELECT status FROM file_tracking WHERE file_path = ?', (file_path,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_job_stats(self, job_id: str) -> Dict:
        """Get comprehensive job statistics (robust against NULL/TEXT values)."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Defaults so we never hit NameError
            job = None
            duration = 0.0
            avg_response_time = 0.0
            status_stats = {}

            # Fetch job row
            cursor.execute('SELECT * FROM job_history WHERE job_id = ?', (job_id,))
            job = cursor.fetchone()

            # Status breakdown
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
                # No row yet (should be rare). Return minimal stats.
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

            # Map columns by index from your CREATE TABLE order
            # job = (job_id, job_name, csv_file, total_requests, successful_requests, failed_requests,
            #        start_time, end_time, duration_seconds, config, status, triggered_by, average_response_time, created_at)
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
        """Save system metrics"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO system_metrics (metric_name, metric_value)
                VALUES (?, ?)
            ''', (metric_name, metric_value))
            conn.commit()

def move_file_reasoned(file_path: str, target_dir: str, reason_prefix: str = "") -> bool:
    """Move file to target directory with timestamp and reason prefix"""
    try:
        if not target_dir:
            return False  # path not configured
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

class FileWatchdog(FileSystemEventHandler):
    """Enhanced file system watchdog for CSV monitoring"""
    def __init__(self, job_queue, db_manager, config,
                 inflight_files=None, inflight_lock=None):
        super().__init__()
        self.job_queue = job_queue
        self.db_manager = db_manager
        self.config = config

        # NEW: tolerate old call-sites
        from threading import Lock
        self.inflight_files = inflight_files if inflight_files is not None else set()
        self.inflight_lock = inflight_lock if inflight_lock is not None else Lock()

        self.processed_files: Set[str] = set()
        self.file_lock = Lock()
        self._load_processed_files()

        
    def _load_processed_files(self):
        """Load previously processed files from database"""
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT file_path FROM file_tracking WHERE status = "completed"')
                self.processed_files = {row[0] for row in cursor.fetchall()}
                logger.info(f"Loaded {len(self.processed_files)} previously processed files")
        except Exception as e:
            logger.error(f"Error loading processed files: {e}")
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate file hash for duplicate detection"""
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
        """Validate CSV file structure and content (robust header/encoding/delimiter)."""
        try:
            if not file_path.lower().endswith('.csv'):
                return False

            # Ensure file isn't being written
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
                    # Optional: auto-move to rejected
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

                # Ensure at least one non-empty webhook_url row exists
                has_data = False
                name_map = {orig: norm for orig, norm in zip(raw_headers, headers)}
                for row in reader:
                    # normalize keys for safe access
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
        """Handle file creation events"""
        if event.is_directory:
            return
        
        self._handle_file_event(event.src_path, 'created')
    
    def on_modified(self, event):
        """Handle file modification events"""
        if event.is_directory:
            return
        
        # Only process if it's a new modification
        self._handle_file_event(event.src_path, 'modified')
    
    def on_moved(self, event):
        """Handle file move events"""
        if event.is_directory:
            return
        
        self._handle_file_event(event.dest_path, 'moved')
    
    def _handle_file_event(self, file_path: str, event_type: str):
        """Handle file system events with duplicate checking"""
        try:
            with self.file_lock:
                file_path = os.path.abspath(file_path)

                # Validate CSV and stable size
                if not self._is_valid_csv_file(file_path):
                    return

                # Hash
                file_hash = self._calculate_file_hash(file_path)
                if not file_hash:
                    return

                # Already completed with same hash?
                if self.db_manager.is_file_processed(file_path, file_hash):
                    logger.info(f"⏭️  Duplicate content detected for {os.path.basename(file_path)} (hash match).")
                    dup_dir = self.config.get('csv', {}).get('duplicates_path', '')
                    moved = move_file_reasoned(file_path, dup_dir, reason_prefix="dup_")
                    if moved:
                        # Optionally record status change
                        try:
                            self.db_manager.update_file_status(file_path, 'skipped_duplicate')
                        except Exception:
                            pass
                    return
                
                # Extract headers from CSV file
                enc = _detect_encoding(file_path)
                with open(file_path, 'r', encoding=enc, newline='') as f:
                    delim = _detect_delimiter(f)
                    reader = csv.DictReader(f, delimiter=delim)
                    raw_headers = reader.fieldnames or []
                    headers = _normalize_headers(raw_headers)

                # Missing webhook URL?
                if 'webhook_url' not in headers:
                    logger.warning(f"File {file_path} missing required 'webhook_url' column")
                    rej_dir = self.config.get('csv', {}).get('rejected_path', '')
                    move_file_reasoned(file_path, rej_dir, reason_prefix="invalid_")
                    return False

                # Already queued/processing in DB?
                status = self.db_manager.get_file_status(file_path)
                if status in ('queued', 'processing'):
                    logger.debug(f"Already {status} (DB): {file_path} — event {event_type} ignored")
                    return

                # In-memory short-circuit?
                with self.inflight_lock:
                    if file_path in self.inflight_files:
                        logger.debug(f"Already pending (in-memory): {file_path} — event {event_type} ignored")
                        return

                # Get file stats + rows
                file_stats = os.stat(file_path)
                file_size = file_stats.st_size
                try:
                    with open(file_path, 'r', encoding=enc) as f:
                        row_count = max(sum(1 for _ in csv.reader(f, delimiter=delim)) - 1, 0)
                except Exception:
                    row_count = 0

                # Track + set status=queued
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

                # Mark inflight and enqueue
                with self.inflight_lock:
                    self.inflight_files.add(file_path)

                self.job_queue.put(job_info)
                logger.info(f"🆕 New CSV file detected: {os.path.basename(file_path)} "
                            f"({row_count} rows, {file_size} bytes) via {event_type}")

        except Exception as e:
            logger.error(f"Error handling file event for {file_path}: {e}")
            logger.error(traceback.format_exc())


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
                use_ssl = smtp_port == 465
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

        # Slack
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

        # Email
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
        # Only Slack (keeps it lightweight)
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