#!/usr/bin/env python
"""
Worker script for testing a single radio.
This is designed to be run as a separate process.
"""

import sys
import json
import os

def run_test(serial_number):
    """
    Run a test on a single radio and return the result.
    
    Args:
        serial_number (str): The serial number of the radio to test
        
    Returns:
        bool: True if successful, False otherwise
    """
    # Import the mock_test_radio function from operations module
    try:
        # Add the project root to path if needed
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from ezSync.operations import mock_test_radio
        
        # Run the test
        print(f"Worker script processing {serial_number}")
        result = mock_test_radio(serial_number)
        
        # Return result
        return result
    except Exception as e:
        print(f"Error in worker script: {str(e)}")
        return False

if __name__ == "__main__":
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: worker_script.py SERIAL_NUMBER")
        sys.exit(2)
    
    serial_number = sys.argv[1]
    
    # Run the test
    success = run_test(serial_number)
    
    # Output result as JSON to stdout for the parent process to capture
    result = {
        "serial_number": serial_number,
        "success": success
    }
    
    print(f"RESULT: {json.dumps(result)}")
    
    # Exit with appropriate code (0 for success, 1 for failure)
    sys.exit(0 if success else 1) 