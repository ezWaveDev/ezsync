import os
import pyodbc
from dotenv import load_dotenv
import sys
from pathlib import Path

# Define the configuration directory paths
USER_CONFIG_DIR = os.path.join(str(Path.home()), '.config', 'ezsync')
USER_CONFIG_FILE = os.path.join(USER_CONFIG_DIR, '.env')
LOCAL_CONFIG_FILE = os.path.join(os.getcwd(), '.env')

# Try to load environment variables from the user config directory first,
# then fall back to the local directory
config_loaded = False

# First try user config directory
if os.path.exists(USER_CONFIG_FILE):
    load_dotenv(USER_CONFIG_FILE)
    config_loaded = True

# Then try local directory
if not config_loaded and os.path.exists(LOCAL_CONFIG_FILE):
    load_dotenv(LOCAL_CONFIG_FILE)
    config_loaded = True

# Otherwise, just try to load from any .env in the current directory
if not config_loaded:
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

def get_config_file_path():
    """
    Determine the best location to store the configuration file.
    Tries to use the user config directory first, falls back to local directory.
    """
    # Try to create the user config directory if it doesn't exist
    try:
        if not os.path.exists(USER_CONFIG_DIR):
            os.makedirs(USER_CONFIG_DIR, exist_ok=True)
        # Test if we can write to the directory
        test_file = os.path.join(USER_CONFIG_DIR, '.write_test')
        with open(test_file, 'w') as f:
            f.write('test')
        os.remove(test_file)
        return USER_CONFIG_FILE
    except (PermissionError, OSError):
        # Fall back to local directory if we can't write to user config dir
        return LOCAL_CONFIG_FILE

def setup_config():
    """
    Check if required environment variables are set and prompt the user if they're missing.
    Creates or updates the .env file with provided values.
    """
    global TARANA_API_KEY
    
    # Determine the best config file location
    env_file_path = get_config_file_path()
    
    # Create directory if it doesn't exist (for the local case)
    os.makedirs(os.path.dirname(env_file_path) or '.', exist_ok=True)
    
    print(f"\nConfiguration will be saved to: {env_file_path}")
    
    env_vars = {}
    
    # Load existing values if .env file exists
    if os.path.exists(env_file_path):
        with open(env_file_path, 'r') as f:
            for line in f:
                if '=' in line and not line.strip().startswith('#'):
                    key, value = line.strip().split('=', 1)
                    env_vars[key] = value
    
    # Check API configuration
    print("\n=== Tarana API Configuration ===")
    if not TARANA_API_KEY:
        api_key = input("Enter your Tarana API Key: ").strip()
        env_vars['TARANA_API_KEY'] = api_key
        os.environ['TARANA_API_KEY'] = api_key
        TARANA_API_KEY = api_key
    
    cpi_id = os.getenv('CPI_ID')
    if not cpi_id:
        cpi_id = input("Enter your CPI ID (or press Enter to skip): ").strip()
        if cpi_id:
            env_vars['CPI_ID'] = cpi_id
            os.environ['CPI_ID'] = cpi_id
    
    # Database configuration
    print("\n=== Database Configuration ===")
    print("These settings are required for the '--deploy' command.")
    
    if not DB_HOST:
        db_host = input("Database Host/IP: ").strip()
        if db_host:
            env_vars['DB_HOST'] = db_host
            os.environ['DB_HOST'] = db_host
    
    if not DB_NAME:
        db_name = input("Database Name: ").strip()
        if db_name:
            env_vars['DB_NAME'] = db_name
            os.environ['DB_NAME'] = db_name
    
    if not DB_USER:
        db_user = input("Database Username: ").strip()
        if db_user:
            env_vars['DB_USER'] = db_user
            os.environ['DB_USER'] = db_user
    
    if not DB_PASSWORD:
        db_pass = input("Database Password: ").strip()
        if db_pass:
            env_vars['DB_PASSWORD'] = db_pass
            os.environ['DB_PASSWORD'] = db_pass
    
    if not DB_PORT:
        db_port = input("Database Port (default: 1433): ").strip()
        if db_port:
            env_vars['DB_PORT'] = db_port
            os.environ['DB_PORT'] = db_port
        else:
            env_vars['DB_PORT'] = '1433'
            os.environ['DB_PORT'] = '1433'
    
    # Write to .env file
    if env_vars:
        try:
            with open(env_file_path, 'w') as f:
                f.write("# Tarana API Configuration\n")
                for key in ['TARANA_API_KEY', 'CPI_ID']:
                    if key in env_vars:
                        f.write(f"{key}={env_vars[key]}\n")
                
                f.write("\n# Database Configuration\n")
                for key in ['DB_HOST', 'DB_PORT', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']:
                    if key in env_vars:
                        f.write(f"{key}={env_vars[key]}\n")
            
            print("Configuration saved successfully!")
            print("You can test your database connection with: ezsync --test-db")
        except (PermissionError, OSError) as e:
            print(f"Error saving configuration to {env_file_path}: {str(e)}")
            print("Your configuration will be used for this session but won't be saved.")
    
    return TARANA_API_KEY is not None

def get_latest_sql_driver():
    drivers = pyodbc.drivers()
    # Filter for SQL Server drivers and get the latest one
    sql_drivers = [d for d in drivers if 'SQL Server' in d]
    return sql_drivers[-1] if sql_drivers else None

# Get the latest SQL Server driver
SQL_DRIVER = get_latest_sql_driver()

# Build the connection string
if SQL_DRIVER:  # Only build if SQL driver is available
    DB_CONNECTION_STRING = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={DB_HOST},{DB_PORT};"
        f"DATABASE={DB_NAME};"
        f"UID={DB_USER};"
        f"PWD={DB_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
else:
    DB_CONNECTION_STRING = None
