"""
Core business logic and operations for the ezSync application.
This module contains the workflows for various radio operations.
"""

import time
import random

from ezSync.api import (
    get_radio_info, get_rn_info, reconnect_radio, reboot_radio,
    apply_default_config, apply_refurb_config,
    initiate_speed_test, poll_speed_test_results, get_radio_status
)
from ezSync.utils import format_value, calculate_average_speed_test_results, calculate_azimuth

def wait_for_connection(serial_number, check_interval=30, max_attempts=20):
    """
    Wait for a radio to connect to the system.
    
    Args:
        serial_number (str): The serial number of the radio
        check_interval (int): Time in seconds between status checks
        max_attempts (int): Maximum number of attempts before giving up
        
    Returns:
        bool: True if connected, False if timed out
    """
    import sys
    
    sys.stdout.write(f"Waiting for radio {serial_number} to connect... (max {max_attempts} attempts, {check_interval}s interval)\n")
    sys.stdout.flush()
    
    for attempt in range(1, max_attempts + 1):
        sys.stdout.write(f"Connection attempt {attempt}/{max_attempts} for {serial_number}\n")
        sys.stdout.flush()
        
        rn_data = get_radio_info(serial_number)
        
        # Log what we received
        sys.stdout.write(f"Received data: {rn_data}\n")
        sys.stdout.flush()
        
        # Radio is online
        if rn_data and rn_data.get('connected') is True:
            sys.stdout.write(f"Radio {serial_number} is now connected\n")
            sys.stdout.flush()
            return True
            
        # Not connected yet, wait and retry
        if attempt < max_attempts:
            sys.stdout.write(f"Radio {serial_number} not connected yet, waiting {check_interval} seconds...\n")
            sys.stdout.flush()
            time.sleep(check_interval)
    
    sys.stdout.write(f"Radio {serial_number} connection timed out after {max_attempts} attempts\n")
    sys.stdout.flush()
    return False

def wait_for_reconnection(serial_number, check_interval=60, max_attempts=20):
    """
    Wait for a radio to reconnect after forcing reconnection.
    
    Args:
        serial_number (str): The serial number of the radio
        check_interval (int): Time in seconds between status checks
        max_attempts (int): Maximum number of status check attempts
        
    Returns:
        tuple: (bool, dict) True and radio info if radio reconnects, False and None otherwise
    """
    print(f"\nWaiting for radio {serial_number} to reconnect...")
    print(f"Radio typically takes at least 3 minutes to reconnect. Waiting...")
    
    # Initial 3-minute wait to allow the radio to complete its reconnection cycle
    initial_wait = 180  # 3 minutes in seconds
    print(f"Waiting {initial_wait} seconds before starting to check...")
    time.sleep(initial_wait)
    
    print(f"Initial wait complete. Now checking every {check_interval} seconds (maximum {max_attempts} attempts)")
    
    for attempt in range(1, max_attempts + 1):
        # Get RN information
        rn_data = get_radio_info(serial_number)
        if not rn_data:
            print(f"Attempt {attempt}/{max_attempts}: Failed to get RN information")
        elif rn_data.get('connected', False):
            print(f"Attempt {attempt}/{max_attempts}: Radio is connected!")
            
            # Get connected BN information
            connected_bn = rn_data.get('connectedBn')
            if not connected_bn:
                print(f"Error: No connected BN found for RN {serial_number}")
                return False, None
                
            print(f"Getting information for connected BN: {connected_bn}")
            bn_data = get_radio_info(connected_bn)
            if not bn_data:
                print(f"Failed to get BN information for {connected_bn}")
                return False, None
                
            return True, bn_data
        else:
            print(f"Attempt {attempt}/{max_attempts}: Radio is not connected")
        
        if attempt < max_attempts:
            print(f"Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)
    
    print(f"Maximum attempts reached. Radio {serial_number} did not reconnect within the time limit.")
    return False, None

def reset_radio(serial_number, hostname="RECLAIMED"):
    """
    Apply default config and reconnect a radio without deleting it.
    
    Args:
        serial_number (str): The serial number of the radio
        hostname (str): Hostname to set during default configuration
        
    Returns:
        bool: True if all operations were successful, False otherwise
    """
    # Step 1: Apply default configuration
    print(f"\nApplying default configuration to radio: {serial_number}")
    print(f"Setting hostname to: {hostname}")
    if not apply_default_config(serial_number, custom_hostname=hostname):
        print(f"Failed to apply default configuration to radio {serial_number}")
        return False
    
    # Step 2: Force reconnection
    print(f"Forcing reconnection for radio: {serial_number}")
    if not reconnect_radio(serial_number):
        print(f"Failed to reconnect radio {serial_number}")
        return False
    
    # Step 3: Wait 30 seconds
    print("Waiting 30 seconds after reconnection...")
    time.sleep(30)
    print("Wait complete. Radio reset successful.")
    
    print(f"Successfully reset radio {serial_number}")
    return True

def display_speed_test_results(results):
    """
    Display formatted speed test results.
    
    Args:
        results (dict): Speed test results data
    """
    if not results:
        print("No speed test results available")
        return
    
    print("\nSpeed Test Results:")
    print("=" * 50)
    
    # Basic information
    print(f"Serial Number: {results.get('serialNumber', 'N/A')}")
    print(f"Operation ID: {results.get('operationId', 'N/A')}")
    print(f"Status: {results.get('status', 'N/A')}")
    
    # Connected BN information
    if 'bnSerialNumber' in results:
        print(f"Connected BN: {results.get('bnSerialNumber', 'N/A')}")
    
    # Time information
    timestamp = results.get('timestamp')
    if timestamp:
        timestamp_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))
        print(f"Timestamp: {timestamp_str}")
    
    # Throughput information
    downlink = results.get('downlinkThroughput')
    if downlink is not None:
        # The values appear to be in Kbps, so we divide by 1000 to get Mbps
        downlink_mbps = downlink / 1000
        print(f"\nDownlink Speed: {format_value(downlink_mbps)} Mbps")
    
    uplink = results.get('uplinkThroughput')
    if uplink is not None:
        # The values appear to be in Kbps, so we divide by 1000 to get Mbps
        uplink_mbps = uplink / 1000
        print(f"Uplink Speed: {format_value(uplink_mbps)} Mbps")
    
    # Latency
    latency = results.get('latencyMillis')
    if latency is not None:
        print(f"Latency: {format_value(latency)} ms")
    
    # Signal quality
    print("\nSignal Information:")
    print(f"Downlink SNR: {format_value(results.get('downlinkSnr', 'N/A'))} dB")
    print(f"Uplink SNR: {format_value(results.get('uplinkSnr', 'N/A'))} dB")
    print(f"Path Loss: {format_value(results.get('pathloss', 'N/A'))} dB")
    
    # Link information
    print("\nLink Information:")
    print(f"Primary Frequency: {results.get('frequency0', 'N/A')/1000 if results.get('frequency0') else 'N/A'} MHz")
    print(f"Secondary Frequency: {results.get('frequency1', 'N/A')/1000 if results.get('frequency1') else 'N/A'} MHz")
    print(f"Primary Bandwidth: {results.get('bandwidth0', 'N/A')} MHz")
    print(f"Secondary Bandwidth: {results.get('bandwidth1', 'N/A')} MHz")
    print(f"RF Link Distance: {results.get('rfLinkDistance', 'N/A')} meters")
    
    # Additional information
    if results.get('failureReason'):
        print(f"\nFailure Reason: {results.get('failureReason')}")
    
    print("=" * 50)

def run_speed_tests(serial_number, num_tests=3, interval=60, max_attempts=10):
    """
    Run multiple speed tests and return the average results.
    
    Args:
        serial_number (str): The serial number of the radio
        num_tests (int): Number of successful speed tests required
        interval (int): Time in seconds between tests
        max_attempts (int): Maximum number of test attempts
        
    Returns:
        dict: Average speed test results or None if error
    """
    print(f"\nRunning speed tests until {num_tests} successful tests are completed")
    print(f"Wait time between tests: {interval} seconds")
    print(f"Maximum attempts: {max_attempts}")
    
    successful_results = []
    attempt = 0
    
    while len(successful_results) < num_tests and attempt < max_attempts:
        attempt += 1
        print(f"\nSpeed Test Attempt {attempt}/{max_attempts} (Successful: {len(successful_results)}/{num_tests})")
        
        # Initiate speed test
        operation_id = initiate_speed_test(serial_number)
        if not operation_id:
            print(f"Failed to initiate speed test attempt {attempt}")
            continue
        
        # Poll for results
        test_result = poll_speed_test_results(operation_id, serial_number)
        
        if not test_result:
            print(f"Failed to get results for speed test attempt {attempt}")
        else:
            status = test_result.get('status')
            
            # Display results regardless of status
            display_speed_test_results(test_result)
            
            # Only count successful tests
            if status == "COMPLETED":
                print(f"Speed test attempt {attempt} completed successfully")
                
                # Verify we have throughput data
                if test_result.get('downlinkThroughput') is not None:
                    successful_results.append(test_result)
                    print(f"Test added to successful results ({len(successful_results)}/{num_tests})")
                else:
                    print(f"Speed test completed but no throughput data found - not counting as successful")
            else:
                # Handle failed tests
                failure_reason = test_result.get('failureReason', 'Unknown reason')
                print(f"Speed test attempt {attempt} failed: {status} - {failure_reason}")
        
        # Wait between tests if we're not done
        if len(successful_results) < num_tests and attempt < max_attempts:
            print(f"Waiting {interval} seconds before next test...")
            time.sleep(interval)
    
    # Check if we have enough successful tests
    if len(successful_results) < num_tests:
        print(f"\nWarning: Only obtained {len(successful_results)}/{num_tests} successful speed tests")
        
        if not successful_results:
            print("No successful speed tests completed")
            return None
    
    # Display table of individual test results
    print("\n================== INDIVIDUAL TESTS =================")
    print("#  | DL (Mbps) | UL (Mbps) | Latency (ms) | DL SNR | UL SNR | Path Loss | RF Dist")
    print("-----------------------------------------------------------------------------------")
    
    for i, result in enumerate(successful_results):
        dl = result.get('downlinkThroughput', 0) / 1000
        ul = result.get('uplinkThroughput', 0) / 1000 if result.get('uplinkThroughput') is not None else 0
        latency = result.get('latencyMillis', 0)
        dl_snr = result.get('downlinkSnr', 'N/A')
        ul_snr = result.get('uplinkSnr', 'N/A')
        path_loss = result.get('pathloss', 'N/A')
        rf_dist = result.get('rfLinkDistance', 0)
        
        print(f"{i+1:<3}| {format_value(dl):^10} | {format_value(ul):^9} | {format_value(latency):^12} | {format_value(dl_snr):^6} | {format_value(ul_snr):^6} | {format_value(path_loss):^9} | {format_value(rf_dist):>5} m")
    
    print("=====================================================")
    
    # Calculate averages
    avg_results = calculate_average_speed_test_results(successful_results)
    
    print("\nAverage Speed Test Results:")
    print("=" * 50)
    print(f"Number of successful tests: {len(successful_results)}")
    
    # Throughput information
    downlink = avg_results.get('downlinkThroughput')
    if downlink is not None:
        downlink_mbps = downlink / 1000
        print(f"Average Downlink Speed: {format_value(downlink_mbps)} Mbps")
    
    uplink = avg_results.get('uplinkThroughput')
    if uplink is not None:
        uplink_mbps = uplink / 1000
        print(f"Average Uplink Speed: {format_value(uplink_mbps)} Mbps")
    
    # Latency
    latency = avg_results.get('latencyMillis')
    if latency is not None:
        print(f"Average Latency: {format_value(latency)} ms")
    
    # Signal quality
    print("\nSignal Information:")
    print(f"Average Downlink SNR: {format_value(avg_results.get('downlinkSnr', 'N/A'))} dB")
    print(f"Average Uplink SNR: {format_value(avg_results.get('uplinkSnr', 'N/A'))} dB")
    print(f"Average Path Loss: {format_value(avg_results.get('pathloss', 'N/A'))} dB")
    
    print("=" * 50)
    
    return avg_results

def refurbish_radio(serial_number, skip_speedtest=False):
    """
    Perform the full refurbishment process on a radio.
    
    Args:
        serial_number (str): The serial number of the radio
        skip_speedtest (bool): Whether to skip the speed test step
        
    Returns:
        bool: True if all operations were successful, False otherwise
    """
    # Step 1: Wait for connection with extended timeout (60 attempts)
    print(f"\nWaiting for radio {serial_number} to connect...")
    connected = wait_for_connection(serial_number, check_interval=30, max_attempts=60)
    if not connected:
        print(f"Radio {serial_number} did not connect within the time limit")
        return False
    
    # Step 2: Get RN and BN information
    rn_data, bn_data = get_rn_info(serial_number)
    if not rn_data or not bn_data:
        print(f"Failed to get radio information for {serial_number}")
        return False
    
    # Step 3: Apply refurbishment configuration
    print(f"\nApplying refurbishment configuration to radio: {serial_number}")
    if not apply_refurb_config(serial_number, bn_data):
        print(f"Failed to apply refurbishment configuration to radio {serial_number}")
        return False
    
    # Add a 30-second wait after config and before reboot
    print("Waiting 30 seconds after applying refurbishment configuration...")
    time.sleep(30)
    print("Wait complete. Proceeding with reboot.")
    
    # Step 4: Reboot the radio (replacing force reconnection)
    print(f"Rebooting radio: {serial_number}")
    if not reboot_radio(serial_number):
        print(f"Failed to reboot radio {serial_number}")
        return False
    
    # Step 5: Wait for reconnection
    print(f"Waiting for radio {serial_number} to reconnect after reboot...")
    reconnected, _ = wait_for_reconnection(serial_number)
    if not reconnected:
        print(f"Radio {serial_number} did not reconnect after reboot")
        return False
    
    # Only run speed tests if not skipped
    if not skip_speedtest:
        # Add a 1-minute settling period before starting speed tests
        settling_time = 60  # 1 minute in seconds
        print(f"\nRadio reconnected successfully. Allowing {settling_time} seconds for connection to stabilize before running speed tests...")
        time.sleep(settling_time)
        print("Settling period complete. Proceeding with speed tests.")
        
        # Step 6: Run speed tests
        print(f"Running speed tests for radio {serial_number}")
        speed_test_results = run_speed_tests(serial_number, num_tests=3, interval=60, max_attempts=10)
        if not speed_test_results:
            print(f"Failed to complete speed tests for radio {serial_number}")
            return False
    else:
        print("\nSkipping speed tests as requested.")
    
    # Step 7: Apply final default configuration with REFURBISHED hostname
    print(f"Applying final configuration with REFURBISHED hostname")
    if not apply_default_config(serial_number, custom_hostname="REFURBISHED"):
        print(f"Failed to apply final configuration to radio {serial_number}")
        return False
    
    print(f"Successfully refurbished radio {serial_number}")
    return True

# Module-level function for multiprocessing refurbishment
def mp_refurbish_radio(args):
    """
    Module-level worker function for refurbishment using multiprocessing.
    This needs to be at the module level to be picklable.
    
    Args:
        args (tuple): (serial_number, skip_speedtest)
        
    Returns:
        tuple: (serial_number, success)
    """
    # Unpack arguments
    if isinstance(args, tuple) and len(args) > 1:
        serial_number, skip_speedtest = args
    else:
        serial_number = args
        skip_speedtest = False
    
    # Print process info
    import os
    print(f"[Process {os.getpid()}] Refurbishing {serial_number}")
    
    # Call the actual refurbishment function
    success = refurbish_radio(serial_number, skip_speedtest=skip_speedtest)
    
    # Return both the serial number and success status
    return (serial_number, success)

def refurbish_radios_parallel(serial_numbers, max_workers=5, skip_speedtest=False):
    """
    Perform parallel refurbishment on multiple radios simultaneously
    using multiprocessing for reliable process management and clean exit.
    
    Args:
        serial_numbers (list): List of serial numbers to process
        max_workers (int): Maximum number of concurrent refurbishment operations
        skip_speedtest (bool): Whether to skip the speed test step
        
    Returns:
        dict: Summary of results with successful and failed operations
    """
    import multiprocessing
    import os
    import sys
    import time
    
    print(f"Starting parallel refurbishment of {len(serial_numbers)} radios with {max_workers} workers")
    
    try:
        # Use multiprocessing.Pool to create worker processes
        # This is Method A from our testing, which provided reliable exit behavior
        with multiprocessing.Pool(processes=max_workers) as pool:
            # Map all serial numbers to the module-level worker function
            print(f"Processing {len(serial_numbers)} radios, this may take some time...")
            
            # Create a list of tuples (serial_number, skip_speedtest) for each radio
            radio_args = [(sn, skip_speedtest) for sn in serial_numbers]
            
            # Process all radios using the module-level worker function
            results_list = pool.map(mp_refurbish_radio, radio_args)
            
            # Organize results
            success_list = []
            failure_list = []
            
            for sn, success in results_list:
                if success:
                    success_list.append(sn)
                else:
                    failure_list.append(sn)
            
            # Print the summary
            print("\n" + "="*20 + " REFURBISHMENT SUMMARY " + "="*20)
            print(f"Successfully refurbished: {len(success_list)}")
            print(f"Failed to refurbish: {len(failure_list)}")
            print(f"Total attempted: {len(serial_numbers)}")
            
            # Print successful radios
            if success_list:
                print("\nSuccessfully refurbished radios:")
                for serial in success_list:
                    print(f"  - {serial}")
            
            # Print failed radios
            if failure_list:
                print("\nFailed refurbishment for radios:")
                for serial in failure_list:
                    print(f"  - {serial}")
            
            print("\nRefurbishment process complete.")
            
            # Return results
            return {
                'success': success_list,
                'failure': failure_list,
                'total': len(serial_numbers)
            }
    except Exception as e:
        print(f"Error in parallel refurbishment: {str(e)}")
        return {
            'success': [],
            'failure': serial_numbers,
            'total': len(serial_numbers)
        }
    finally:
        # Ensure process exits cleanly
        # This is crucial based on our testing to prevent hanging
        print("Finishing parallel refurbishment process...")
        
        # Return control to the main process
        # No need for os._exit(0) here as multiprocessing.Pool handles cleanup

def display_radio_status(radio_data):
    """
    Display formatted radio status information.
    
    Args:
        radio_data (dict): Radio status data
    """
    if not radio_data:
        print("No radio data available to display")
        return
    
    print("\n" + "="*50)
    print(f"Radio Status: {radio_data.get('serialNumber', 'Unknown')}")
    print("="*50)
    
    status_items = [
        ("Serial Number", "serialNumber"),
        ("Hostname", "hostName"),
        ("Online", "online"),
        ("Status", "status"),
        ("Last Seen", "lastSeen"),
        ("Latitude", "latitude"),
        ("Longitude", "longitude"),
        ("Primary BN", "primaryBn"),
        ("AGL Height", "heightAgl"),
        ("Antenna Azimuth", "antennaAzimuth"),
        ("Antenna Tilt", "tilt"),
        ("CPI ID", "cpiId")
    ]
    
    for label, key in status_items:
        value = radio_data.get(key, "N/A")
        print(f"{label}: {value}")

def deploy_radio(serial_number):
    """
    Configure a radio for customer deployment using customer information from the database.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        bool: True if deployment configuration was successful, False otherwise
    """
    import time
    from ezSync.database import get_customer_info
    
    # Step 1: Wait for connection
    print(f"\nWaiting for radio {serial_number} to connect...")
    connected = wait_for_connection(serial_number, check_interval=30, max_attempts=20)
    if not connected:
        print(f"Radio {serial_number} did not connect within the time limit")
        return False
    
    # Step 2: Get customer information from database
    print(f"\nRetrieving customer information for radio: {serial_number}")
    customer_info = get_customer_info(serial_number)
    if not customer_info:
        print(f"No customer information found for radio {serial_number}")
        return False
    
    # Step 3: Get RN and BN information
    print(f"\nRetrieving radio network information for: {serial_number}")
    rn_data, bn_data = get_rn_info(serial_number)
    if not rn_data or not bn_data:
        print(f"Failed to get radio information for {serial_number}")
        return False
    
    # Step 4: Calculate antenna azimuth based on customer and BN locations
    customer_lat = customer_info.get('latitude')
    customer_lon = customer_info.get('longitude')
    
    # If customer coordinates are missing, cannot proceed
    if not customer_lat or not customer_lon:
        print(f"Customer coordinates are missing for radio {serial_number}")
        return False
    
    bn_lat = float(bn_data.get('latitude', 0))
    bn_lon = float(bn_data.get('longitude', 0))
    azimuth = calculate_azimuth(float(customer_lat), float(customer_lon), bn_lat, bn_lon)
    
    # Step 5: Format hostname based on customer name and ID
    customer_name = customer_info.get('name', '').strip()
    customer_id = customer_info.get('id', '')
    
    # Create hostname from full customer name and ID
    if customer_name:
        # Sanitize the customer name:
        # 1. Keep spaces as is
        # 2. Remove invalid hostname characters (like slashes)
        # 3. Truncate if too long to avoid hostname length limits
        sanitized_name = customer_name.upper()
        sanitized_name = sanitized_name.replace('/', ' ')  # Replace slashes with spaces
        # Keep spaces as is, only remove other invalid characters
        sanitized_name = ''.join(c for c in sanitized_name if c.isalnum() or c.isspace())
        
        # Ensure name isn't too long (allowing room for ID and separator)
        max_name_length = 50 - len(str(customer_id)) - 1  # 1 for the separator
        if len(sanitized_name) > max_name_length:
            sanitized_name = sanitized_name[:max_name_length]
        
        # Remove trailing spaces if they exist
        sanitized_name = sanitized_name.rstrip()
        
        hostname = f"{sanitized_name}-{customer_id}"
    else:
        # Fallback to ID only if no name is available
        hostname = f"CUSTOMER-{customer_id}"
    
    # Step 6: Determine Primary BN
    # Use the BN we're currently connected to
    primary_bn = bn_data.get('serialNumber', '')
    
    # Step 7: Apply deployment configuration
    print(f"\nApplying deployment configuration for customer: {customer_name} (ID: {customer_id})")
    
    # Configure with customer-specific data
    from ezSync.api import apply_deploy_config
    success = apply_deploy_config(
        serial_number=serial_number,
        hostname=hostname,
        customer_lat=float(customer_lat),
        customer_lon=float(customer_lon),
        azimuth=azimuth,
        primary_bn=primary_bn
    )
    
    if not success:
        print(f"Failed to apply deployment configuration to radio {serial_number}")
        return False
    
    print(f"\nSuccessfully configured radio {serial_number} for deployment")
    print(f"Hostname: {hostname}")
    print(f"Location: {customer_lat}, {customer_lon}")
    print(f"Azimuth: {azimuth}° (pointing toward BN: {primary_bn})")
    
    return True

def mock_test_radio(serial_number):
    """
    Mock function to simulate testing a radio with random execution time.
    
    Args:
        serial_number (str): The serial number of the radio
        
    Returns:
        bool: True if successful, randomly fails sometimes
    """
    import time
    import random
    
    # Simulate connection wait
    print(f"\nInitializing mock test for radio {serial_number}...")
    time.sleep(random.uniform(1, 3))
    
    # Random chance of failing at connection stage (10%)
    if random.random() < 0.1:
        print(f"Mock connection failed for radio {serial_number}")
        return False
    
    # Simulate configuration process
    print(f"Mock configuring radio {serial_number}...")
    time.sleep(random.uniform(2, 5))
    
    # Simulate a reboot
    print(f"Mock rebooting radio {serial_number}...")
    time.sleep(random.uniform(3, 8))
    
    # Simulate speed tests with longer wait time
    total_time = random.uniform(10, 60)  # Random time between 10-60 seconds
    print(f"Running mock speed tests for radio {serial_number} (will take {total_time:.1f} seconds)...")
    
    # Show progress during the "speed test"
    steps = random.randint(3, 6)  # Number of progress updates
    for i in range(steps):
        step_time = total_time / steps
        time.sleep(step_time)
        progress = (i + 1) * 100 / steps
        print(f"Mock speed test progress for {serial_number}: {progress:.1f}%")
    
    # Random chance of failing at speed test stage (5%)
    if random.random() < 0.05:
        print(f"Mock speed test failed for radio {serial_number}")
        return False
    
    # Simulate final configuration
    print(f"Applying final mock configuration to radio {serial_number}...")
    time.sleep(random.uniform(1, 3))
    
    print(f"Successfully completed mock test for radio {serial_number}")
    return True

# Module-level function for multiprocessing (Method A)
def mp_worker_test(serial_number):
    """
    Module-level worker function for use with multiprocessing.
    This needs to be at the module level to be picklable.
    
    Args:
        serial_number (str): The serial number of the radio to test
        
    Returns:
        tuple: (serial_number, success)
    """
    # Print process info
    import os
    import multiprocessing
    print(f"[Process {os.getpid()}] Processing {serial_number}")
    
    # Call the actual test function
    success = mock_test_radio(serial_number)
    
    # Return both the serial number and success status
    return (serial_number, success)

def test_radios_parallel(serial_numbers, max_workers=5):
    """
    Perform parallel mock testing on multiple radios simultaneously.
    
    Args:
        serial_numbers (list): List of serial numbers to process
        max_workers (int): Maximum number of concurrent test operations
        
    Returns:
        dict: Summary of results with successful and failed operations
    """
    import concurrent.futures
    from threading import Lock, current_thread
    import sys
    import os
    import threading
    
    # Lock for synchronized console output
    print_lock = Lock()
    
    # Results tracking
    results = {
        'success': [],
        'failure': [],
        'in_progress': set(serial_numbers),
        'completed': 0,
        'total': len(serial_numbers)
    }
    
    def log(message):
        """Print log messages with timestamp"""
        print(f"[LOG] {message}", flush=True)
    
    def thread_safe_print(message, serial=None):
        """Thread-safe printing with proper formatting"""
        with print_lock:
            if serial:
                prefix = f"[{serial}] "
                message = prefix + message
            print(message, flush=True)
    
    def worker_test(serial_number):
        """Worker function that tests a single radio"""
        thread_safe_print("Starting mock test process", serial=serial_number)
        
        try:
            # Call the mock test function
            success = mock_test_radio(serial_number)
            
            # Record result
            with print_lock:
                if serial_number in results['in_progress']:
                    results['in_progress'].remove(serial_number)
                    results['completed'] += 1
                    if success:
                        results['success'].append(serial_number)
                        thread_safe_print("Mock test SUCCESSFUL", serial=serial_number)
                    else:
                        results['failure'].append(serial_number)
                        thread_safe_print("Mock test FAILED", serial=serial_number)
            
            return success
                
        except Exception as e:
            with print_lock:
                if serial_number in results['in_progress']:
                    results['in_progress'].remove(serial_number)
                    results['completed'] += 1
                    results['failure'].append(serial_number)
                    thread_safe_print(f"ERROR: {str(e)}", serial=serial_number)
            
            return False
    
    # Print initial status
    print(f"Starting parallel mock testing of {len(serial_numbers)} radios with {max_workers} workers")
    
    # Most basic implementation possible
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    futures = {}
    
    try:
        # Submit all tasks and collect futures
        log("Submitting tasks to thread pool")
        for sn in serial_numbers:
            future = executor.submit(worker_test, sn)
            futures[future] = sn
        
        # Wait for all futures to complete with a timeout
        log(f"Waiting for {len(futures)} tasks to complete (with 1-hour timeout)")
        done, not_done = concurrent.futures.wait(
            futures, 
            timeout=3600,  # 1 hour timeout
            return_when=concurrent.futures.ALL_COMPLETED
        )
        
        # Log completion status
        log(f"Wait completed: {len(done)}/{len(futures)} tasks finished")
        
        # Check for any incomplete tasks
        if not_done:
            log(f"Warning: {len(not_done)} tasks did not complete within timeout")
            
    finally:
        # Force shutdown of executor
        log("Shutting down executor (wait=False)")
        executor.shutdown(wait=False)
        log("Executor shutdown complete")
    
    # Print summary after executor is done
    log("Printing summary")
    
    # All tasks are now complete - print the summary
    print("\n\n" + "="*20 + " TEST SUMMARY " + "="*20)
    print(f"Successfully tested: {len(results['success'])}")
    print(f"Failed tests: {len(results['failure'])}")
    print(f"Total attempted: {len(serial_numbers)}")
    
    # Print successful radios
    if results['success']:
        print("\nSuccessfully tested radios:")
        for serial in results['success']:
            print(f"  - {serial}")
    
    # Print failed radios
    if results['failure']:
        print("\nFailed test for radios:")
        for serial in results['failure']:
            print(f"  - {serial}")
    
    print("\nTest process complete.")
    log("Process complete, returning results")
    
    return results

def find_fix_parallel(serial_numbers, max_workers=5):
    """
    Test focused methods to solve the threading issue.
    
    Args:
        serial_numbers (list): List of serial numbers to process
        max_workers (int): Maximum number of concurrent operations
        
    Returns:
        dict: Summary of results with successful and failed operations
    """
    import concurrent.futures
    from threading import Lock, current_thread, Thread
    import sys
    import time
    import signal
    import os
    import multiprocessing
    import subprocess
    import json
    
    # Set a consistent timeout for all methods
    METHOD_TIMEOUT = 120  # seconds
    
    # Lock for synchronized console output
    print_lock = Lock()
    
    def info(message):
        """Print info messages"""
        prefix = f"[METHOD {method}] "
        with print_lock:
            print(f"{prefix}{message}", flush=True)
    
    def thread_safe_print(message, serial=None):
        """Thread-safe printing with proper formatting"""
        with print_lock:
            if serial:
                message = f"[{serial}] {message}"
            print(message, flush=True)
    
    def print_summary(results_data):
        """Print final summary of results"""
        print("\n" + "="*20 + " TEST SUMMARY " + "="*20)
        print(f"Method tested: {method}")
        print(f"Successfully tested: {len(results_data['success'])}")
        print(f"Failed tests: {len(results_data['failure'])}")
        print(f"Total attempted: {len(serial_numbers)}")
        
        # Print successful radios
        if results_data['success']:
            print("\nSuccessfully tested radios:")
            for serial in results_data['success']:
                print(f"  - {serial}")
        
        # Print failed radios
        if results_data['failure']:
            print("\nFailed test for radios:")
            for serial in results_data['failure']:
                print(f"  - {serial}")
        
        print("\nTest process complete.")
        print("="*50)
    
    # Test methods A, B, C
    for method_id in ["A", "B", "C"]:
        method = method_id
        
        print(f"\n\n{'='*20} TESTING METHOD {method} {'='*20}")
        info(f"Starting test with strict {METHOD_TIMEOUT} second timeout")
        
        # Create a wrapper process for each method
        child_pid = os.fork()
        
        if child_pid == 0:
            # This is the child process for the current method
            try:
                # Setup signal handler for immediate termination
                def handle_term_signal(signum, frame):
                    print(f"[TIMEOUT] Method {method} received termination signal")
                    # Exit immediately
                    os._exit(2)
                
                # Register signal handler
                signal.signal(signal.SIGTERM, handle_term_signal)
                
                # METHOD A: Module-Level Function with Process Pool
                if method == "A":
                    info("Using multiprocessing.Pool with module-level function")
                    
                    # Process pool - notice we're using the mp_worker_test function
                    # that was defined at the module level
                    with multiprocessing.Pool(processes=max_workers) as pool:
                        # Map all serial numbers to the worker function
                        results_list = pool.map(mp_worker_test, serial_numbers)
                        
                        # Process results
                        results = {
                            'success': [],
                            'failure': [],
                            'completed': len(results_list),
                            'total': len(serial_numbers)
                        }
                        
                        for sn, success in results_list:
                            if success:
                                results['success'].append(sn)
                                thread_safe_print(f"Test SUCCESSFUL", serial=sn)
                            else:
                                results['failure'].append(sn)
                                thread_safe_print(f"Test FAILED", serial=sn)
                    
                    # Print summary
                    print_summary(results)
                    print(f"[COMPLETE] Method {method} finished successfully")
                    
                    # Force exit - don't rely on normal termination
                    os._exit(0)
                
                # METHOD B: Watchdog Process with SIGKILL
                elif method == "B":
                    info("Using watchdog process with forced termination")
                    
                    # Create a pipe for communication between worker and watchdog
                    read_pipe, write_pipe = os.pipe()
                    
                    # Fork to create watchdog and worker processes
                    watchdog_pid = os.fork()
                    
                    if watchdog_pid == 0:
                        # This is the worker process
                        # Close the read end of the pipe
                        os.close(read_pipe)
                        write_pipe_file = os.fdopen(write_pipe, 'w')
                        
                        # Results tracking
                        results = {
                            'success': [],
                            'failure': [],
                            'in_progress': set(serial_numbers),
                            'completed': 0,
                            'total': len(serial_numbers)
                        }
                        
                        # Function to handle worker tests
                        def worker_test(serial_number):
                            """Worker function that tests a single radio"""
                            thread_name = current_thread().name
                            thread_safe_print(f"Starting test in worker process", serial=serial_number)
                            
                            try:
                                # Call the mock test function
                                success = mock_test_radio(serial_number)
                                
                                # Record result
                                with print_lock:
                                    if serial_number in results['in_progress']:
                                        results['in_progress'].remove(serial_number)
                                        results['completed'] += 1
                                        if success:
                                            results['success'].append(serial_number)
                                            thread_safe_print("Test SUCCESSFUL", serial=serial_number)
                                        else:
                                            results['failure'].append(serial_number)
                                            thread_safe_print("Test FAILED", serial=serial_number)
                                
                                return success
                                    
                            except Exception as e:
                                with print_lock:
                                    if serial_number in results['in_progress']:
                                        results['in_progress'].remove(serial_number)
                                        results['completed'] += 1
                                        results['failure'].append(serial_number)
                                        thread_safe_print(f"ERROR: {str(e)}", serial=serial_number)
                                
                                return False
                        
                        # Run all tasks in threads
                        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = [executor.submit(worker_test, sn) for sn in serial_numbers]
                            
                            # Process results as they complete
                            for future in concurrent.futures.as_completed(futures):
                                # Just ensure all exceptions are handled
                                try:
                                    future.result()
                                except Exception:
                                    pass
                        
                        # Print summary
                        print_summary(results)
                        
                        # Signal completion to watchdog
                        print("[WORKER] Signaling completion to watchdog")
                        write_pipe_file.write("DONE\n")
                        write_pipe_file.flush()
                        
                        # Wait to be killed by watchdog
                        print("[WORKER] Waiting for termination...")
                        time.sleep(60)  # This should never complete - watchdog will kill us
                        print("[WORKER] Timeout waiting for termination!")
                        os._exit(1)  # Just in case the watchdog fails
                    else:
                        # This is the watchdog process
                        # Close the write end of the pipe
                        os.close(write_pipe)
                        read_pipe_file = os.fdopen(read_pipe, 'r')
                        
                        print("[WATCHDOG] Waiting for worker to complete")
                        
                        # Wait for signal from worker
                        line = read_pipe_file.readline().strip()
                        
                        if line == "DONE":
                            print("[WATCHDOG] Received completion signal")
                            print("[WATCHDOG] Killing worker process")
                            # Kill the worker process immediately
                            os.kill(watchdog_pid, signal.SIGKILL)
                            print("[WATCHDOG] Worker process terminated")
                            print(f"[COMPLETE] Method {method} finished successfully")
                            os._exit(0)
                        else:
                            print(f"[WATCHDOG] Received unexpected signal: {line}")
                            print("[WATCHDOG] Killing worker process")
                            os.kill(watchdog_pid, signal.SIGKILL)
                            os._exit(1)
                
                # METHOD C: Externalize the Work Completely
                elif method == "C":
                    info("Using external worker script for complete isolation")
                    
                    # Get the path to the worker script
                    script_path = os.path.join(os.path.dirname(__file__), 'worker_script.py')
                    if not os.path.exists(script_path):
                        info(f"Worker script not found at {script_path}")
                        # Create a simple version directly in memory
                        script_path = os.path.join(os.path.dirname(__file__), 'temp_worker.py')
                        with open(script_path, 'w') as f:
                            f.write("""#!/usr/bin/env python
import sys, json, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ezSync.operations import mock_test_radio
result = mock_test_radio(sys.argv[1])
print(f"RESULT: {{'serial_number': sys.argv[1], 'success': {result}}}")
sys.exit(0 if result else 1)
""")
                        os.chmod(script_path, 0o755)
                    
                    # Results tracking
                    results = {
                        'success': [],
                        'failure': [],
                        'completed': 0,
                        'total': len(serial_numbers)
                    }
                    
                    # Process each radio in its own external process
                    for sn in serial_numbers:
                        info(f"Launching external process for {sn}")
                        
                        try:
                            # Run the worker script as a separate process
                            # Capture output to parse results
                            cmd = [sys.executable, script_path, sn]
                            proc = subprocess.run(
                                cmd,
                                timeout=METHOD_TIMEOUT/len(serial_numbers),
                                capture_output=True,
                                text=True
                            )
                            
                            # Check if process succeeded
                            success = proc.returncode == 0
                            
                            # Try to parse result from output
                            try:
                                # Look for the result line
                                result_line = None
                                for line in proc.stdout.splitlines():
                                    if line.startswith("RESULT:"):
                                        result_line = line[7:].strip()  # Remove "RESULT: " prefix
                                        break
                                
                                if result_line:
                                    result = json.loads(result_line)
                                    success = result.get('success', success)
                            except (json.JSONDecodeError, ValueError):
                                # Fall back to return code if parsing fails
                                pass
                            
                            # Record result
                            results['completed'] += 1
                            if success:
                                results['success'].append(sn)
                                thread_safe_print("External process SUCCESSFUL", serial=sn)
                            else:
                                results['failure'].append(sn)
                                thread_safe_print("External process FAILED", serial=sn)
                                
                        except subprocess.TimeoutExpired:
                            results['completed'] += 1
                            results['failure'].append(sn)
                            thread_safe_print("External process TIMED OUT", serial=sn)
                        except Exception as e:
                            results['completed'] += 1
                            results['failure'].append(sn)
                            thread_safe_print(f"External process ERROR: {str(e)}", serial=sn)
                    
                    # Print summary
                    print_summary(results)
                    print(f"[COMPLETE] Method {method} finished successfully")
                    
                    # Clean up temporary worker script if created
                    if script_path.endswith('temp_worker.py'):
                        try:
                            os.remove(script_path)
                        except:
                            pass
                    
                    # Force exit
                    os._exit(0)
                
            except Exception as e:
                info(f"Exception in method: {str(e)}")
                import traceback
                traceback.print_exc()
                # Exit with error
                os._exit(1)
        else:
            # This is the parent process
            # Set precise timeout for the child
            exact_timeout = time.time() + METHOD_TIMEOUT
            
            # Log start time
            print(f"[TIMEOUT] Method {method} started at {time.strftime('%H:%M:%S')}")
            print(f"[TIMEOUT] Will terminate at exactly {time.strftime('%H:%M:%S', time.localtime(exact_timeout))}")
            
            child_terminated = False
            status = None
            
            # Check periodically if child has completed
            while time.time() < exact_timeout:
                try:
                    # Non-blocking check if child has terminated
                    pid, status = os.waitpid(child_pid, os.WNOHANG)
                    if pid != 0:  # Child has terminated
                        child_terminated = True
                        elapsed = time.time() - (exact_timeout - METHOD_TIMEOUT)
                        print(f"[TIMEOUT] Method {method} completed in {elapsed:.1f} seconds")
                        break
                except OSError:
                    # Child is gone
                    child_terminated = True
                    break
                
                # Sleep briefly to avoid CPU thrashing
                time.sleep(0.1)
            
            # If child is still running when timeout is reached, forcibly terminate it
            if not child_terminated:
                print(f"\n{'='*20} TIMEOUT REACHED {'='*20}")
                print(f"[TIMEOUT] Method {method} exceeded {METHOD_TIMEOUT} seconds limit")
                
                # Send SIGTERM for clean shutdown
                try:
                    os.kill(child_pid, signal.SIGTERM)
                    print(f"[TIMEOUT] Sent SIGTERM to child process")
                    
                    # Give only 1 second to terminate gracefully
                    termination_deadline = time.time() + 1.0
                    while time.time() < termination_deadline:
                        try:
                            pid, _ = os.waitpid(child_pid, os.WNOHANG)
                            if pid != 0:  # Process terminated
                                print(f"[TIMEOUT] Child process terminated after SIGTERM")
                                child_terminated = True
                                break
                        except OSError:
                            # Process is gone
                            child_terminated = True
                            break
                        time.sleep(0.05)
                except OSError:
                    # Process might already be gone
                    child_terminated = True
                
                # If SIGTERM didn't work, use SIGKILL
                if not child_terminated:
                    try:
                        print(f"[TIMEOUT] Child process still running after SIGTERM, sending SIGKILL")
                        os.kill(child_pid, signal.SIGKILL)
                        os.waitpid(child_pid, 0)  # Clean up zombie
                        print(f"[TIMEOUT] Child process forcibly terminated with SIGKILL")
                    except OSError:
                        # Process might already be gone
                        pass
                
                print(f"[TIMEOUT] Method {method} failed due to timeout")
                print(f"{'='*20} METHOD {method} FAILED {'='*20}")
                
                # Print timeout summary
                print("\n" + "="*20 + " TIMEOUT SUMMARY " + "="*20)
                print(f"Method {method} did not complete within {METHOD_TIMEOUT} seconds")
                print(f"This indicates that Method {method} cannot exit properly")
                print("="*50)
    
    # Final recommendation
    print("\n" + "="*20 + " RECOMMENDATION " + "="*20)
    print("Based on testing, the recommended approaches are:")
    print("")
    print("METHOD A: Module-Level Function with Process Pool")
    print("  - Most reliable for proper process lifecycle management")
    print("  - Requires moving worker functions to module level")
    print("  - Clean integration with Python's multiprocessing")
    print("")
    print("METHOD B: Watchdog Process with SIGKILL")
    print("  - Forcibly terminates worker process after completion")
    print("  - Double-process architecture ensures clean exit")
    print("  - Use when threads refuse to exit naturally")
    print("")
    print("METHOD C: Externalize the Work Completely")
    print("  - Complete isolation in separate Python processes")
    print("  - Most resilient to memory/thread issues")
    print("  - Best for mission-critical applications")
    
    return {'success': True}
