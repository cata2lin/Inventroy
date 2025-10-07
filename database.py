import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from urllib.parse import quote_plus

# Load environment variables from .env file
load_dotenv()

# --- Database Configuration ---
DB_USER = os.getenv("DB_USER", "scraper")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Scraper123#")
DB_HOST = os.getenv("DB_HOST", "38.242.226.83")
DB_NAME = os.getenv("DB_NAME", "InventorySync")

# URL-encode the password to handle special characters
encoded_password = quote_plus(DB_PASSWORD)

# Construct the final, safe database URL
DATABASE_URL = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"

# --- MODIFIED ENGINE CREATION WITH CONNECTION POOLING ---
# Create the SQLAlchemy engine with specific pool settings.
engine = create_engine(
    DATABASE_URL,
    pool_size=10,  # The number of connections to keep open in the pool.
    max_overflow=20, # The maximum number of connections to allow in addition to pool_size.
    pool_recycle=3600, # Recycle connections after 1 hour to prevent timeout issues.
    pool_pre_ping=True # Check if the connection is alive before using it.
)
# --- END OF MODIFICATION ---

# Create a SessionLocal class for creating new Session objects
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a Base class for declarative models
Base = declarative_base()

# --- Dependency for FastAPI ---
def get_db():
    """
    FastAPI dependency that provides a database session per request.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()