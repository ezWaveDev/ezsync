import os
import pyodbc
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Tarana API Configuration
TARANA_API_BASE_URL = "https://api.trial.cloud.taranawireless.com"
TARANA_API_VERSION = "v2"
TARANA_RADIO_ENDPOINT = f"{TARANA_API_BASE_URL}/{TARANA_API_VERSION}/network/radios"
TARANA_API_KEY = os.getenv('TARANA_API_KEY')

# Constants for RN configuration
DEFAULT_TILT = -0.3
DEFAULT_AGL = 9  # Height Above Ground Level

# Database Configuration
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT', '1433')

def get_latest_sql_driver():
    drivers = pyodbc.drivers()
    # Filter for SQL Server drivers and get the latest one
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    return sql_drivers[-1] if sql_drivers else None

# Get the latest SQL Server driver
SQL_DRIVER = get_latest_sql_driver()

# Build the connection string
if not SQL_DRIVER:
    raise Exception("No SQL Server driver found")

DB_CONNECTION_STRING = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={DB_HOST},{DB_PORT};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USER};"
    f"PWD={DB_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)
