"""
API client for interacting with the Tarana API.
This module handles all external API interactions.
"""

import os
import json
import requests
import urllib3
import time
from ezSync.config import TARANA_API_KEY, TARANA_RADIO_ENDPOINT

# Suppress SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_api_headers():
    """
    Get the standard API headers including the API key.
    
    Returns:
        dict: Headers for API requests
    """
    return {
        'accept': 'application/json',
        'x-api-key': TARANA_API_KEY.strip(),
        'Content-Type': 'application/json'
    }

def get_radio_info(serial_number):
    """
    Get radio information (RN or BN) from Tarana API.
    
    Args:
        serial_number (str): The serial number of the radio device
        
    Returns:
        dict: Radio information or None if error
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return None
    
    headers = get_api_headers()
    
    try:
        response = requests.get(
            f"{TARANA_RADIO_ENDPOINT}/{serial_number}",
            headers=headers,
            verify=False
        )
        
        if response.status_code != 200:
            print(f"API Response Status: {response.status_code}")
            print(f"API Response Text: {response.text}")
            return None
            
        return response.json().get('data', {})
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return None

def get_rn_info(serial_number):
    """
    Get RN information and verify connection status.
    
    Args:
        serial_number (str): The serial number of the RN device
        
    Returns:
        tuple: (rn_info, bn_info) or (None, None) if error
    """
    # Get RN information
    rn_data = get_radio_info(serial_number)
    if not rn_data:
        print(f"Failed to get RN information for {serial_number}")
        return None, None
    
    # Get connected BN information
    connected_bn = rn_data.get('connectedBn')
    if not connected_bn:
        print(f"Error: No connected BN found for RN {serial_number}")
        return None, None
        
    print(f"Getting information for connected BN: {connected_bn}")
    bn_data = get_radio_info(connected_bn)
    if not bn_data:
        print(f"Failed to get BN information for {connected_bn}")
        return None, None
    
    return rn_data, bn_data

def get_radio_status(serial_number):
    """
    Get detailed status information for a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        dict: Radio status information or None if error
    """
    return get_radio_info(serial_number)  # Uses the same endpoint

def reconnect_radio(serial_number):
    """
    Force a radio to reconnect to the network.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    headers = get_api_headers()
    
    # Use the v1 endpoint directly as specified
    reconnect_endpoint = f"https://api.trial.cloud.taranawireless.com/v1/network/radios/{serial_number}/reconnect"
    
    try:
        print(f"Attempting to reconnect radio: {serial_number}")
        response = requests.post(
            reconnect_endpoint,
            headers=headers,
            verify=False
        )
        
        print(f"Reconnect Request Status: {response.status_code}")
        print(f"Reconnect Response Text: {response.text}")
        
        # Accept both 200 and 202 as success status codes
        if response.status_code in [200, 202]:
            return True
            
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"Reconnect request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return False

def delete_radios(serial_numbers):
    """
    Delete radios via DELETE request.
    
    Args:
        serial_numbers (list): List of serial numbers to delete
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    headers = get_api_headers()
    
    data = {
        "serialNumbers": serial_numbers
    }
    
    print("\nDelete Request payload:")
    print(json.dumps(data, indent=2))
    
    # Use the v1 endpoint directly as specified
    delete_endpoint = "https://api.trial.cloud.taranawireless.com/v1/network/radios/delete"
    
    try:
        response = requests.post(
            delete_endpoint,
            headers=headers,
            json=data,
            verify=False
        )
        
        print(f"\nDELETE Request Status: {response.status_code}")
        print(f"DELETE Response Text: {response.text}")
        
        # Accept both 200 and 202 as success status codes
        if response.status_code in [200, 202]:
            return True
            
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"DELETE request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return False

def apply_default_config(serial_number, custom_hostname=None):
    """
    Apply default configuration to a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        custom_hostname (str, optional): Custom hostname to set. If None, uses serial_number.
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    # Get CPI_ID from environment
    cpi_id = os.getenv('CPI_ID')
    if not cpi_id:
        print("Warning: CPI_ID environment variable is not set or empty")
        print("Using empty string for cpiId")
        cpi_id = ""
    
    headers = get_api_headers()
    
    # Use custom hostname if provided, otherwise use serial number
    hostname = custom_hostname if custom_hostname is not None else serial_number
    
    # Default configuration
    data = {
        "hostName": hostname,
        "latitude": 0,
        "longitude": 0,
        "dataVlan": 0,
        "primaryBn": "",
        "heightAgl": 0,
        "tilt": 0,
        "antennaAzimuth": 0,
        "cpiId": cpi_id
    }
    
    print("\nDefault Config Request payload:")
    print(json.dumps(data, indent=2))
    
    try:
        response = requests.patch(
            f"{TARANA_RADIO_ENDPOINT}/{serial_number}",
            headers=headers,
            json=data,
            verify=False
        )
        
        print(f"\nPATCH Request Status: {response.status_code}")
        print(f"PATCH Response Text: {response.text}")
        
        # Accept both 200 and 202 as success status codes
        if response.status_code in [200, 202]:
            return True
            
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"PATCH request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return False

def apply_refurb_config(serial_number, bn_info):
    """
    Apply refurbishment configuration to a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        bn_info (dict): Information about the connected BN
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    # Fixed coordinates for refurbishment
    customer_lat = 37.79456493207615
    customer_lon = -120.9921875576708
    
    # Get CPI_ID from environment
    cpi_id = os.getenv('CPI_ID')
    if not cpi_id:
        print("Warning: CPI_ID environment variable is not set or empty")
        print("Using empty string for cpiId")
        cpi_id = ""
    
    # Calculate azimuth based on BN location
    from ezSync.utils import calculate_azimuth
    bn_lat = float(bn_info.get('latitude', 0))
    bn_lon = float(bn_info.get('longitude', 0))
    azimuth = calculate_azimuth(customer_lat, customer_lon, bn_lat, bn_lon)
    
    headers = get_api_headers()
    
    # Refurbishment configuration
    data = {
        "latitude": customer_lat,
        "longitude": customer_lon,
        "heightAgl": 1,
        "tilt": 0,
        "antennaAzimuth": azimuth,
        "hostName": "IN_REFURBISHMENT",
        "primaryBn": "",
        "cpiId": cpi_id
    }
    
    print("\nRefurbishment Config Request payload:")
    print(json.dumps(data, indent=2))
    
    try:
        response = requests.patch(
            f"{TARANA_RADIO_ENDPOINT}/{serial_number}",
            headers=headers,
            json=data,
            verify=False
        )
        
        print(f"\nPATCH Request Status: {response.status_code}")
        print(f"PATCH Response Text: {response.text}")
        
        # Check if the request was successful
        if response.status_code == 200 or response.status_code == 202:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error sending PATCH request: {str(e)}")
        return False

def apply_deploy_config(serial_number, hostname, customer_lat, customer_lon, azimuth, primary_bn):
    """
    Apply customer-specific deployment configuration to a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        hostname (str): Formatted hostname for the customer
        customer_lat (float): Customer's latitude
        customer_lon (float): Customer's longitude
        azimuth (float): Calculated antenna azimuth pointing toward BN
        primary_bn (str): Serial number of the primary BN
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    # Get CPI_ID from environment
    cpi_id = os.getenv('CPI_ID')
    if not cpi_id:
        print("Warning: CPI_ID environment variable is not set or empty")
        print("Using empty string for cpiId")
        cpi_id = ""
    
    headers = get_api_headers()
    
    # Deployment configuration with customer-specific data
    data = {
        "hostName": hostname,
        "latitude": customer_lat,
        "longitude": customer_lon,
        "dataVlan": "",
        "primaryBn": primary_bn,
        "heightAgl": 9,
        "tilt": 0,
        "antennaAzimuth": azimuth,
        "cpiId": cpi_id
    }
    
    print("\nDeployment Config Request payload:")
    print(json.dumps(data, indent=2))
    
    try:
        response = requests.patch(
            f"{TARANA_RADIO_ENDPOINT}/{serial_number}",
            headers=headers,
            json=data,
            verify=False
        )
        
        print(f"\nPATCH Request Status: {response.status_code}")
        print(f"PATCH Response Text: {response.text}")
        
        # Check if the request was successful
        if response.status_code == 200 or response.status_code == 202:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error sending PATCH request: {str(e)}")
        return False

def initiate_speed_test(serial_number):
    """
    Initiate a speed test for a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        str: Operation ID if successful, None otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return None
    
    headers = get_api_headers()
    
    # Use the v1 endpoint directly as specified
    speedtest_endpoint = f"https://api.trial.cloud.taranawireless.com/v1/network/radios/{serial_number}/speed-test"
    
    try:
        print(f"Initiating speed test for radio: {serial_number}")
        response = requests.post(
            speedtest_endpoint,
            headers=headers,
            verify=False
        )
        
        print(f"Speed Test Initiation Status: {response.status_code}")
        if response.status_code != 200:
            print(f"Speed Test Response Text: {response.text}")
            return None
            
        response_data = response.json()
        if 'data' not in response_data or 'operationId' not in response_data['data']:
            print("Error: No operation ID in response")
            return None
            
        operation_id = response_data['data']['operationId']
        print(f"Speed test initiated. Operation ID: {operation_id}")
        return operation_id
        
    except requests.exceptions.RequestException as e:
        print(f"Speed test request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return None

def poll_speed_test_results(operation_id, serial_number, check_interval=20, max_attempts=30, verbose=False):
    """
    Poll for speed test results.
    
    Args:
        operation_id (str): The operation ID returned when initiating the speed test
        serial_number (str): The serial number of the radio
        check_interval (int): Time in seconds between status checks
        max_attempts (int): Maximum number of status check attempts
        verbose (bool): Whether to print detailed debug information
        
    Returns:
        dict: Speed test results or None if error or timeout
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return None
    
    headers = get_api_headers()
    
    # Use the v1 endpoint directly as specified with serialNumber as query parameter
    results_endpoint = f"https://api.trial.cloud.taranawireless.com/v1/operations/speed-test/id/{operation_id}?serialNumber={serial_number}"
    
    print(f"\nPolling for speed test results (Operation ID: {operation_id})")
    print(f"Will check every {check_interval} seconds (maximum {max_attempts} attempts)")
    
    # Initial wait to allow the test to transition from QUEUED to RUNNING
    print(f"Waiting {check_interval} seconds before first check...")
    time.sleep(check_interval)
    
    for attempt in range(1, max_attempts + 1):
        try:
            if verbose:
                print(f"Attempt {attempt}/{max_attempts}: Checking speed test status...")
            else:
                print(f"Check {attempt}/{max_attempts}: ", end="", flush=True)
            
            response = requests.get(
                results_endpoint,
                headers=headers,
                verify=False
            )
            
            if response.status_code != 200:
                print(f"Failed! Response code: {response.status_code}")
                if verbose:
                    print(f"Response text: {response.text}")
            else:
                response_text = response.text
                if verbose:
                    print(f"Raw response: {response_text}")
                
                # Parse the response
                response_json = response.json()
                
                # The API might return the data in different formats, try to handle both
                if 'data' in response_json:
                    result_data = response_json.get('data', {})
                else:
                    result_data = response_json  # In case the data is the root object
                
                # Check for status
                status = result_data.get('status')
                
                if status:
                    print(f"Status: {status}")
                    
                    # Handle different status values
                    if status == "COMPLETED":
                        return result_data
                    elif status in ["FAILED", "CANCELLED", "TIMEOUT", "ERROR"]:
                        return result_data
                else:
                    print("Unknown status")
                    
                    # Check if we have results despite missing status
                    if 'downlinkThroughput' in result_data or 'uplinkThroughput' in result_data:
                        print("Found throughput data, assuming test is complete")
                        return result_data
            
            if attempt < max_attempts:
                time.sleep(check_interval)
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {str(e)}")
            if verbose and hasattr(e, 'response') and e.response is not None:
                print(f"Error details: {e.response.text}")
            
            if attempt < max_attempts:
                print(f"Waiting {check_interval} seconds before retrying...")
                time.sleep(check_interval)
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            
            if attempt < max_attempts:
                print(f"Waiting {check_interval} seconds before retrying...")
                time.sleep(check_interval)
    
    print(f"Maximum attempts reached. Could not get final speed test results.")
    return None

def reboot_radio(serial_number):
    """
    Reboot a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        bool: True if successful, False otherwise
    """
    if not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        return False
    
    headers = get_api_headers()
    
    # Use the v1 endpoint directly as specified
    reboot_endpoint = f"https://api.trial.cloud.taranawireless.com/v1/network/radios/{serial_number}/reboot"
    
    try:
        print(f"Attempting to reboot radio: {serial_number}")
        response = requests.post(
            reboot_endpoint,
            headers=headers,
            verify=False
        )
        
        print(f"Reboot Request Status: {response.status_code}")
        print(f"Reboot Response Text: {response.text}")
        
        # Accept both 200 and 202 as success status codes
        if response.status_code in [200, 202]:
            return True
            
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"Reboot request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error details: {e.response.text}")
        return False
