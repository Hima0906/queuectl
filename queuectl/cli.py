import click
import logging
import sys
from datetime import datetime
from .models import Job, JobStatus, get_db_session, init_db
from .worker import start_workers
import json
import os
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    "max_retries": 3,
    "backoff_base": 2,
    "default_workers": 1,
    "log_level": "INFO"
}

def get_config():
    """Load configuration from file or return defaults."""
    config_path = os.path.join(Path.home(), '.queuectl', 'config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                return {**DEFAULT_CONFIG, **json.load(f)}
        except Exception as e:
            logger.warning(f"Error loading config: {e}. Using defaults.")
    return DEFAULT_CONFIG

def save_config(config):
    """Save configuration to file."""
    config_path = os.path.join(Path.home(), '.queuectl', 'config.json')
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=2)

@click.group()
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, verbose):
    """queuectl - A CLI-based background job queue system"""
    ctx.ensure_object(dict)
    config = get_config()
    
    # Set log level
    log_level = logging.DEBUG if verbose else getattr(
        logging, config.get('log_level', 'INFO').upper()
    )
    logging.getLogger().setLevel(log_level)
    
    # Initialize database
    init_db()

@cli.command()
@click.argument('command')
@click.option('--max-retries', type=int, help='Maximum number of retry attempts')
@click.option('--backoff-base', type=int, help='Base for exponential backoff in minutes')
def add_command(command, max_retries, backoff_base):
    """Add a new command to the job queue."""
    session = get_db_session()
    try:
        config = get_config()
        job = Job(
            command=command,
            max_attempts=max_retries or config['max_retries'],
            backoff_base=backoff_base or config['backoff_base']
        )
        session.add(job)
        session.commit()
        click.echo(f"Added job {job.id}: {command}")
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding job: {str(e)}")
        raise click.ClickException("Failed to add job")
    finally:
        session.close()

@cli.command()
@click.option('--count', '-c', type=int, help='Number of worker processes to start')
def start_workers_cmd(count):
    """Start worker processes to process jobs."""
    config = get_config()
    worker_count = count or config.get('default_workers', 1)
    click.echo(f"Starting {worker_count} worker(s)... (Press Ctrl+C to stop)")
    try:
        start_workers(worker_count)
    except KeyboardInterrupt:
        click.echo("\nShutting down workers...")
    except Exception as e:
        logger.error(f"Error in worker: {str(e)}")
        raise click.ClickException("Worker failed")

@cli.command()
@click.option('--status', type=click.Choice([s.value for s in JobStatus]), 
              help='Filter jobs by status')
@click.option('--limit', type=int, default=50, help='Maximum number of jobs to show')
def list_jobs(status, limit):
    """List jobs with optional status filter."""
    session = get_db_session()
    try:
        query = session.query(Job)
        if status:
            query = query.filter(Job.status == JobStatus(status))
        
        jobs = query.order_by(Job.created_at.desc()).limit(limit).all()
        
        if not jobs:
            click.echo("No jobs found")
            return
            
        # Format and display jobs
        click.echo(f"{'ID':<5} {'Status':<12} {'Attempts':<9} {'Created At':<20} Command")
        click.echo("-" * 80)
        for job in jobs:
            click.echo(
                f"{job.id:<5} {job.status.value:<12} {job.attempts}/{job.max_attempts:<9} "
                f"{job.created_at.strftime('%Y-%m-%d %H:%M:%S'):<20} {job.command}"
            )
    finally:
        session.close()

@cli.command()
def show_dlq():
    """Show jobs in the Dead Letter Queue."""
    session = get_db_session()
    try:
        jobs = session.query(Job).filter(
            Job.status == JobStatus.DEAD
        ).order_by(Job.completed_at.desc()).all()
        
        if not jobs:
            click.echo("No jobs in Dead Letter Queue")
            return
            
        click.echo(f"{'ID':<5} {'Failed At':<20} {'Attempts':<9} Command")
        click.echo("-" * 80)
        for job in jobs:
            click.echo(
                f"{job.id:<5} {job.completed_at.strftime('%Y-%m-%d %H:%M:%S'):<20} "
                f"{job.attempts}/{job.max_attempts:<9} {job.command}"
            )
            if job.error:
                click.echo(f"     Error: {job.error.splitlines()[0]}")
    finally:
        session.close()

@cli.command()
@click.argument('job_ids', type=int, nargs=-1)
@click.option('--all', 'retry_all', is_flag=True, help='Retry all failed jobs')
def retry_dlq(job_ids, retry_all):
    """Retry failed jobs from the Dead Letter Queue."""
    if not job_ids and not retry_all:
        raise click.UsageError("You must specify job IDs or use --all")
        
    session = get_db_session()
    try:
        query = session.query(Job).filter(Job.status == JobStatus.DEAD)
        
        if not retry_all:
            query = query.filter(Job.id.in_(job_ids))
            
        jobs = query.all()
        
        if not jobs:
            click.echo("No matching jobs found in Dead Letter Queue")
            return
            
        for job in jobs:
            job.status = JobStatus.PENDING
            job.attempts = 0
            job.error = None
            job.completed_at = None
            job.next_retry_at = None
            click.echo(f"Queued job {job.id} for retry")
            
        session.commit()
        click.echo(f"Queued {len(jobs)} job(s) for retry")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error retrying jobs: {str(e)}")
        raise click.ClickException("Failed to retry jobs")
    finally:
        session.close()

@cli.command()
def status():
    """Show system status and job statistics."""
    session = get_db_session()
    try:
        # Get job counts by status
        status_counts = {
            status.value: session.query(Job).filter(Job.status == status).count()
            for status in JobStatus
        }
        
        # Get recent activity - show completed jobs first, then by creation time
        recent_jobs = session.query(Job).order_by(
            Job.completed_at.desc() if Job.status == JobStatus.COMPLETED else Job.created_at.desc()
        ).order_by(Job.created_at.desc()).limit(5).all()
        
        # Display status
        click.echo("\n=== Queue Status ===")
        for status, count in status_counts.items():
            click.echo(f"{status.upper()}: {count}")
            
        click.echo("\n=== Recent Activity ===")
        for job in recent_jobs:
            status = f"{job.status.value.upper()}"
            if job.completed_at:
                status += f" at {job.completed_at.strftime('%Y-%m-%d %H:%M:%S')}"
            click.echo(f"[{job.id}] {job.command[:50]}... ({status})")
            
    finally:
        session.close()

@cli.group()
def config():
    """Manage queuectl configuration."""
    pass

@config.command(name='set')
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """Set a configuration value."""
    config = get_config()
    
    # Validate key
    if key not in DEFAULT_CONFIG:
        raise click.UsageError(f"Invalid config key: {key}")
    
    # Convert value to correct type
    if key in ['max_retries', 'default_workers', 'backoff_base']:
        try:
            value = int(value)
            if value <= 0:
                raise ValueError("Value must be positive")
        except ValueError as e:
            raise click.UsageError(f"Invalid value for {key}: {e}")
    
    # Update and save config
    config[key] = value
    save_config(config)
    click.echo(f"Updated {key} = {value}")

@config.command(name='list')
def config_list():
    """List all configuration values."""
    config = get_config()
    click.echo("Current configuration:")
    for key, value in config.items():
        click.echo(f"{key} = {value}")

if __name__ == '__main__':
    cli()
