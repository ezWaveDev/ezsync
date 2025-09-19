"""
Database operations for the ezSync application.
This module handles all database interactions.
"""

import socket
from ezSync.config import DB_CONNECTION_STRING, PYODBC_IMPORT_ERROR

# Optional import: only import pyodbc when actually used
try:
    import pyodbc  # type: ignore
except Exception as _e:
    pyodbc = None
    if PYODBC_IMPORT_ERROR is None:
        PYODBC_IMPORT_ERROR = _e  # Best-effort propagate

def test_connection():
    """
    Test the database connection using the configured connection string.
    
    Returns:
        tuple: (bool, str) - Success status and message
    """
    if not DB_CONNECTION_STRING:
        return False, "No database connection string configured"

    if pyodbc is None:
        return False, (
            "pyodbc is not available. On macOS, per README, install prerequisites: "
            "brew install unixodbc && brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release && "
            "brew update && brew install msodbcsql17 mssql-tools; then reinstall pyodbc in your active env."
        )
    
    # Extract server info from connection string for diagnostics
    server_info = None
    port = "1433"  # Default SQL Server port
    
    # Parse the connection string to extract server and port
    for part in DB_CONNECTION_STRING.split(';'):
        if part.strip().lower().startswith('server='):
            server_parts = part.split('=')[1].split(',')
            server_info = server_parts[0]
            if len(server_parts) > 1:
                port = server_parts[1]
    
    if not server_info:
        return False, "Could not parse server information from connection string"
    
    print(f"Testing connection to: {server_info} on port {port}")
    
    # Try to do a socket connection test first
    try:
        socket_test = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_test.settimeout(5)  # 5 second timeout
        result = socket_test.connect_ex((server_info, int(port)))
        socket_test.close()
        
        if result != 0:
            return False, f"Cannot establish TCP connection to {server_info}:{port}. Server may be unreachable or port might be blocked."
        else:
            print(f"TCP connection test successful to {server_info}:{port}")
    except socket.gaierror:
        return False, f"Hostname resolution failed for {server_info}. Check if the server name is correct."
    except Exception as e:
        print(f"Socket test warning: {str(e)}")
    
    # Now try the database connection
    try:
        print("Attempting to connect to database (timeout: 15 seconds)...")
        conn = pyodbc.connect(DB_CONNECTION_STRING, timeout=15)
        cursor = conn.cursor()
        
        # Run a simple query to verify connection is working
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return True, f"Connection successful! SQL Server version: {version}"
    except Exception as e:
        # If pyodbc provided structured errors, surface them; else generic
        return False, f"Database connection error: {str(e)}"
    except Exception as e:
        return False, f"Unexpected error: {str(e)}"

def get_customer_info(serial_number):
    """
    Retrieve customer information from the database based on the device serial number.
    
    Args:
        serial_number (str): The serial number of the RN device
        
    Returns:
        dict: Customer information including address and coordinates
    """
    query = """
    SELECT 
        c.id,
        c.name,
        c.email,
        c.phone,
        c.active,
        COALESCE(a.addr1, c.addr1) AS addr1,
        COALESCE(a.addr2, c.addr2) AS addr2,
        COALESCE(a.city, c.city) AS city,
        COALESCE(a.state, c.state) AS state,
        COALESCE(a.zip, c.zip) AS zip,
        c.storeid,
        a.latitude,
        a.longitude
    FROM 
        customer c
    LEFT JOIN 
        address a ON c.id = a.idnum AND a.type = 6
    WHERE 
        c.id = (
            SELECT TOP 1 statusDetail 
            FROM velociter.dbo.Inventory_Record 
            WHERE SerialNumber = ? 
            AND MacAddress IS NOT NULL
            AND StatusEnum = 4
        );
    """
    
    if pyodbc is None:
        print(
            "pyodbc is not available. Please install unixODBC and Microsoft ODBC Driver for SQL Server, then reinstall pyodbc."
        )
        return None

    try:
        conn = pyodbc.connect(DB_CONNECTION_STRING)
        cursor = conn.cursor()
        cursor.execute(query, (serial_number,))
        
        columns = [column[0] for column in cursor.description]
        result = cursor.fetchone()
        
        if result is None:
            return None
            
        # Convert row to dictionary
        customer_info = dict(zip(columns, result))
        
        cursor.close()
        conn.close()
        
        return customer_info
        
    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
        return None
    except Exception as e:
        print(f"Error: {str(e)}")
        return None
