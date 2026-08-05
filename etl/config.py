import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Base Directory of Project
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Database Credentials
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "weather_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "weather_password_secure_123")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

# Pipeline Settings
LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)
FETCH_DAYS = int(os.getenv("FETCH_DAYS", "7"))

# Logs Directory
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "etl.log"

# Setup Logger
logger = logging.getLogger("OpenMeteoETL")
logger.setLevel(LOG_LEVEL)

if not logger.handlers:
    # File Handler
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] %(message)s"
    )
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# City Coordinates Metadata Catalog
CITY_CATALOG = {
    "London": {"lat": 51.5074, "lon": -0.1278, "country": "UK"},
    "New York": {"lat": 40.7128, "lon": -74.0060, "country": "USA"},
    "Tokyo": {"lat": 35.6762, "lon": 139.6503, "country": "Japan"},
    "Paris": {"lat": 48.8566, "lon": 2.3522, "country": "France"},
    "Sydney": {"lat": -33.8688, "lon": 151.2093, "country": "Australia"},
    "Delhi": {"lat": 28.6139, "lon": 77.2090, "country": "India"},
}

# Parse configured cities from env
CONFIGURED_CITIES = [
    c.strip() for c in os.getenv("CITIES", "London,New York,Tokyo,Paris,Sydney,Delhi").split(",")
    if c.strip() in CITY_CATALOG
]
