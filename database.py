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
DB_PASSWORD = os.getenv("DB_PASSWORD", "Scraper123#") # Raw password with special characters
DB_HOST = os.getenv("DB_HOST", "38.242.226.83")
DB_NAME = os.getenv("DB_NAME", "InventorySync")

# URL-encode the password to handle special characters like '#' safely
encoded_password = quote_plus(DB_PASSWORD)

# Construct the final, safe database URL
DATABASE_URL = f"postgresql://{DB_USER}:{encoded_password}@{DB_HOST}/{DB_NAME}"

# Create the SQLAlchemy engine with an increased connection pool size.
engine = create_engine(
    DATABASE_URL,
    pool_size=15,
    max_overflow=20
)

# Create a SessionLocal class, which will be a factory for new Session objects
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create a Base class. Our ORM models will inherit from this class.
Base = declarative_base()

# --- Dependency for FastAPI ---
def get_db():
    """
    FastAPI dependency that creates and yields a new database session
    for each request, and ensures it's closed afterward.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()