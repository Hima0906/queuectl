# queuectl

## Project Overview
queuectl is a robust command-line utility designed to manage and execute background jobs efficiently. It provides a simple yet powerful way to queue shell commands, process them with multiple workers, and handle failures gracefully. With built-in retry mechanisms and a Dead Letter Queue (DLQ), it ensures reliable job execution while maintaining job state persistence through SQLite storage.

## Features
- Enqueue shell commands as background jobs
- Process multiple jobs in parallel with worker processes
- Automatic retry mechanism with configurable exponential backoff
- Dead Letter Queue for handling failed jobs after max retries
- Persistent job storage using SQLite
- Graceful worker startup and shutdown
- Comprehensive CLI interface for queue management

## Installation Instructions

### Prerequisites
- Python 3.7 or higher
- pip (Python package manager)

### Installation Steps

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd queuectl
   ```

2. (Recommended) Create and activate a virtual environment:
   ```bash
   # Linux/Mac
   python -m venv venv
   source venv/bin/activate
   
   # Windows
   python -m venv venv
   .\venv\Scripts\activate
   ```

3. Install the package and dependencies:
   ```bash
   pip install -e .
   ```
   This will make the `queuectl` command available in your terminal.

### Verifying Installation
Run the following to verify the installation:
```bash
python -m queuectl --version
```

## Usage (Commands)

### Start Worker Processes
```bash
# Start 3 worker processes
python -m queuectl start-workers-cmd --count 3

# The workers will run in the foreground
# Press Ctrl+C to stop the workers gracefully
```

### Add Jobs to Queue
```bash
# Add a simple command
python -m queuectl add-command "echo 'Processing job'"

# Add a command with retry options
python -m queuectl add-command "python process_data.py" --max-retries 3
```

### Monitor and Manage Jobs
```bash
# List all jobs
python -m queuectl list-jobs

# Show only failed jobs
python -m queuectl list-jobs --status failed

# View system status
python -m queuectl status
```

### Handle Failed Jobs
```bash
# View jobs in Dead Letter Queue
python -m queuectl show-dlq

# Retry a specific failed job from DLQ
python -m queuectl retry-dlq <job_id>

# Example:
# python -m queuectl retry-dlq 4

# Retry all failed jobs in DLQ
python -m queuectl retry-dlq --all
```

## Advanced Usage Examples

### Complex Job Examples

#### Chained Commands
```bash
# Run multiple commands in sequence
python -m queuectl add-command "make clean && make build && make test"

# Command with environment variables
python -m queuectl add-command "DATABASE_URL=postgres://user:pass@localhost/db python process_data.py"

# Long-running process with output redirection
python -m queuectl add-command "python train_model.py > training.log 2>&1"
```

#### Job with Dependencies
```bash
# Add a job that depends on another job
JOB1=$(python -m queuectl add-command "process_stage1.sh" --quiet)
python -m queuectl add-command "process_stage2.sh" --after $JOB1
```

#### Scheduled Jobs
```bash
# Schedule a job to run at a specific time (using system's at command)
python -m queuectl add-command "echo 'Running at specific time' | at 23:30"
```

## Example Workflow

### 1. Add Jobs
```bash
# Add a simple job
python -m queuectl add-command "echo 'Processing data...'"

# Add a job with custom retry settings
python -m queuectl add-command "python process.py" --max-retries 5
```

### 2. Start Workers
```bash
# Start 2 worker processes
python -m queuectl start-workers-cmd --count 2
```

### 3. Monitor Jobs
```bash
# View all jobs
python -m queuectl list-jobs

# View system status
python -m queuectl status

# View failed jobs
python -m queuectl show-dlq
```

### 4. Handle Failures
```bash
# Retry a specific failed job
python -m queuectl retry-dlq 4

# Or retry all failed jobs
python -m queuectl retry-dlq --all
```

## Example Run Output

### Adding and Processing Jobs
```bash
$ python -m queuectl add-command "sleep 5 && echo 'Job 1 completed'"
Added job with ID: 1

$ python -m queuectl add-command "sleep 3 && echo 'Job 2 completed'"
Added job with ID: 2

$ python -m queuectl start-workers --count 2
Started 2 worker processes
```

### Checking Status
```bash
$ python -m queuectl status
Workers: 2 active
Jobs: 2 total (1 pending, 1 processing, 0 completed, 0 failed, 0 in DLQ)
```

### Listing Jobs
```bash
$ python -m queuectl list-jobs
ID  Command                         Status      Attempts  Next Retry
1   sleep 5 && echo 'Job 1...'      completed  1/3        -
2   sleep 3 && echo 'Job 2...'      processing 1/3        -
```

## Configuration

Configuration is stored in `config.json` and can be updated using the `config` command:

```bash
# Update retry settings
python -m queuectl config set max_retries 5

# Update exponential backoff base (in seconds)
python -m queuectl config set backoff_base 2

# View current configuration
python -m queuectl config list
```

### Configuration Options

#### Default Configuration
Configuration is stored in `~/.config/queuectl/config.json` with these defaults:
```json
{
    "max_retries": 3,
    "backoff_base": 1,
    "max_workers": "auto",  // Uses CPU count if set to "auto"
    "db_path": "~/.local/share/queuectl/queue.db",
    "log_level": "INFO"
}
```

#### Key Parameters
- `max_retries`: Maximum retry attempts before moving to DLQ (default: 3)
- `backoff_base`: Base for exponential backoff in seconds (default: 1)
- `max_workers`: Maximum worker processes (number or "auto" for CPU count)
- `db_path`: Path to SQLite database file
- `log_level`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

#### Environment Variables
All settings can be overridden with environment variables:
```bash
export QUEUECTL_MAX_RETRIES=5
export QUEUECTL_MAX_WORKERS=4
python -m queuectl status  # Will use the overridden values
```

## Project Structure

```
queuectl/
├── doc/                     # Documentation and assets
│   ├── architecture.png     # System architecture diagram
│   └── screenshots/         # Screenshots for documentation
│       ├── Add a Job.png
│       ├── Check Status.png
│       ├── List all jobs.png
│       └── Start Workers.png
│
├── queuectl/                # Main package directory
│   ├── __init__.py          # Package initialization
│   ├── __main__.py          # Entry point for python -m queuectl
│   ├── cli.py               # Command-line interface implementation
│   ├── models.py            # Database models and schema
│   └── worker.py            # Worker process implementation
│
├── .gitignore               # Git ignore file
├── README.md                # This file
├── requirements.txt         # Python dependencies
├── setup.py                 # Package installation script
└── pyproject.toml           # Build system configuration
```

## Architecture

queuectl follows a modular architecture designed for reliability and scalability:

![Architecture](doc/architecture.png)

### Key Components:

1. **CLI Layer**
   - Built with Click for intuitive command-line interactions
   - Handles user input validation and command routing
   - Provides a clean interface to interact with the job queue

2. **Core Logic**
   - Manages the job lifecycle (pending → processing → completed/failed → retried → dead)
   - Implements retry logic with exponential backoff
   - Manages the Dead Letter Queue (DLQ) for failed jobs
   - Handles worker process management

3. **Worker Pool**
   - Multiple worker processes for parallel job execution
   - Graceful startup and shutdown handling
   - Process isolation for job execution

4. **Data Storage**
   - SQLite database for persistent job storage
   - Thread-safe operations with SQLAlchemy
   - Efficient querying for job status and management

### Data Flow:
1. User submits a job via CLI
2. Job is stored in the database with 'pending' status
3. Available worker picks up the job and updates status to 'processing'
4. Worker executes the command and updates status based on the result
5. Failed jobs are automatically retried according to the retry policy
6. Jobs that exceed max retries are moved to the Dead Letter Queue

## Screenshots (Demo Output)

The following screenshots demonstrate how queuectl works in practice.

### 1. Starting Worker Processes
```bash
python -m queuectl start-workers-cmd --count 2
```

![Start Workers](doc/screenshots/Start%20Workers.png)

### 2. Adding a Job to the Queue
```bash
python -m queuectl add-command "echo 'Hello World'"
```

![Add a Job](doc/screenshots/Add%20a%20Job.png)

### 3. Listing All Jobs
```bash
python -m queuectl list-jobs
```

![List Jobs](doc/screenshots/List%20all%20jobs.png)

### 4. Checking System Status
```bash
python -m queuectl status
```

![Check Status](doc/screenshots/Check%20Status.png)


## Conclusion

queuectl provides a reliable and efficient background job processing system with a clean CLI interface, worker process management, retry handling, and persistent storage. It is suitable for automating recurring tasks, running queued shell commands, and demonstrating process orchestration concepts.
