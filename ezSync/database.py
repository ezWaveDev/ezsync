"""
Database operations for the ezSync application.
This module handles all database interactions.
"""

import pyodbc
from ezSync.config import DB_CONNECTION_STRING

def test_connection():
    """
    Test the database connection using the configured connection string.
    
    Returns:
        tuple: (bool, str) - Success status and message
    """
    if not DB_CONNECTION_STRING:
        return False, "No database connection string configured"
    
    try:
        # Try to establish a connection
        conn = pyodbc.connect(DB_CONNECTION_STRING, timeout=10)
        cursor = conn.cursor()
        
        # Run a simple query to verify connection is working
        cursor.execute("SELECT @@VERSION")
        version = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return True, f"Connection successful! SQL Server version: {version}"
    except pyodbc.Error as e:
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
            SELECT statusDetail 
            FROM velociter.dbo.Inventory_Record 
            WHERE SerialNumber = ?
        );
    """
    
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
