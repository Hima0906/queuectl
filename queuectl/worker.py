import os
import time
import signal
import subprocess
from datetime import datetime, timedelta
from .models import Job, JobStatus, get_db_session
import logging

logger = logging.getLogger(__name__)

class Worker:
    def __init__(self, worker_id, max_jobs=10, shutdown_event=None):
        self.worker_id = worker_id
        self.max_jobs = max_jobs
        self.shutdown_event = shutdown_event or threading.Event()
        self.current_job = None
        self.running = False
        
    def start(self):
        """Start the worker process."""
        self.running = True
        logger.info(f"Worker {self.worker_id} started")
        
        while self.running and not self.shutdown_event.is_set():
            try:
                self._process_jobs()
                time.sleep(1)  # Sleep to prevent busy waiting
            except Exception as e:
                logger.error(f"Worker {self.worker_id} error: {str(e)}", exc_info=True)
                time.sleep(5)  # Sleep longer on error
                
    def stop(self):
        """Stop the worker process gracefully."""
        self.running = False
        logger.info(f"Worker {self.worker_id} stopping...")
        
    def _process_jobs(self):
        """Process pending jobs."""
        session = get_db_session()
        try:
            # Find a pending job that's ready to be processed
            job = session.query(Job).filter(
                Job.status == JobStatus.PENDING,
                (Job.next_retry_at.is_(None) | (Job.next_retry_at <= datetime.utcnow()))
            ).order_by(Job.created_at).first()
            
            if not job:
                return
                
            # Mark job as processing
            job.status = JobStatus.PROCESSING
            job.attempts += 1
            job.started_at = datetime.utcnow()
            session.commit()
            
            self.current_job = job
            self._execute_job(job, session)
            
        except Exception as e:
            logger.error(f"Error processing job: {str(e)}", exc_info=True)
            session.rollback()
        finally:
            self.current_job = None
            session.close()
    
    def _execute_job(self, job, session):
        """Execute the job command and handle the result."""
        logger.info(f"Worker {self.worker_id} executing job {job.id}: {job.command}")
        
        try:
            # Execute the command
            process = subprocess.Popen(
                job.command,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Wait for the process to complete with a timeout
            try:
                stdout, stderr = process.communicate(timeout=3600)  # 1 hour timeout
                return_code = process.returncode
                
                if return_code == 0:
                    self._handle_success(job, session, stdout)
                else:
                    self._handle_failure(job, session, f"Command failed with return code {return_code}\n{stderr}")
                    
            except subprocess.TimeoutExpired:
                process.kill()
                self._handle_failure(job, session, "Job timed out after 1 hour")
                
        except Exception as e:
            self._handle_failure(job, session, f"Error executing job: {str(e)}")
    
    def _handle_success(self, job, session, output):
        """Handle a successfully completed job."""
        job.status = JobStatus.COMPLETED
        job.completed_at = datetime.utcnow()
        session.commit()
        logger.info(f"Job {job.id} completed successfully")
    
    def _handle_failure(self, job, session, error):
        """Handle a failed job with retry logic."""
        job.error = error
        
        if job.attempts >= job.max_attempts:
            job.status = JobStatus.DEAD
            job.completed_at = datetime.utcnow()
            logger.error(f"Job {job.id} failed after {job.attempts} attempts and moved to DLQ")
        else:
            # Calculate next retry time using exponential backoff
            backoff = (job.backoff_base ** job.attempts) * 60  # in seconds
            job.next_retry_at = datetime.utcnow() + timedelta(seconds=backoff)
            job.status = JobStatus.PENDING
            logger.warning(
                f"Job {job.id} failed (attempt {job.attempts}/{job.max_attempts}). "
                f"Retrying in {backoff} seconds"
            )
        
        session.commit()

def start_workers(count=1):
    """Start multiple worker processes."""
    import threading
    
    shutdown_event = threading.Event()
    workers = []
    
    def signal_handler(sig, frame):
        logger.info("Shutting down workers...")
        shutdown_event.set()
        for worker in workers:
            if worker.is_alive():
                worker.join(timeout=5)
    
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start worker threads
    for i in range(count):
        worker = threading.Thread(
            target=Worker(i + 1, shutdown_event=shutdown_event).start,
            daemon=True
        )
        worker.start()
        workers.append(worker)
    
    # Wait for all workers to complete
    for worker in workers:
        worker.join()
    
    logger.info("All workers stopped")
