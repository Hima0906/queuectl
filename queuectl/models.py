from datetime import datetime
from enum import Enum
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Enum as SQLEnum, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from pathlib import Path

# Create base class for models
Base = declarative_base()

class JobStatus(Enum):
    PENDING = 'pending'
    PROCESSING = 'processing'
    COMPLETED = 'completed'
    FAILED = 'failed'
    DEAD = 'dead'

class Job(Base):
    """Job model representing a command to be executed."""
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    command = Column(Text, nullable=False)
    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    attempts = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)
    error = Column(Text, nullable=True)
    backoff_base = Column(Integer, default=2)  # Base for exponential backoff
    next_retry_at = Column(DateTime, nullable=True)
    
    def __repr__(self):
        return f"<Job(id={self.id}, command='{self.command[:20]}...', status={self.status})>"

def get_db_session():
    """Create and return a database session."""
    engine = init_db()
    return scoped_session(sessionmaker(bind=engine))()

def init_db():
    """Initialize the database and return the engine."""
    db_path = os.path.join(Path.home(), '.queuectl', 'jobs.db')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return engine
