"""
Worker function for parallel processing in ezSync.
This module is designed to be imported in separate processes.
"""

import time
import sys
import traceback
import queue

from ezSync.api import (
    apply_default_config, upgrade_radio_firmware, reboot_radio,
    get_radio_info, reconnect_radio
)

def wait_for_connection(serial_number, status_queue=None, check_interval=30, max_attempts=20):
    """
    Wait for a radio to connect to the system.
    
    Args:
        serial_number (str): The serial number of the radio
        status_queue (Queue): Optional queue to send status updates instead of printing
        check_interval (int): Time in seconds between status checks
        max_attempts (int): Maximum number of attempts before giving up
        
    Returns:
        bool: True if connected, False if timed out
    """
    # Initial status
    if status_queue:
        radio_info = {'firmware': '', 'connected_bn': '', 'hardware': '', 'carrier_mode': ''}
        status_queue.put(('IN_PROGRESS', f'Waiting for radio to connect (0/{max_attempts})', 1, radio_info))
    
    for attempt in range(1, max_attempts + 1):
        # Update status with current attempt
        if status_queue:
            status_queue.put(('IN_PROGRESS', f'Waiting for radio to connect ({attempt}/{max_attempts})', 1, radio_info))
        
        rn_data = get_radio_info(serial_number)
        
        # Radio is online
        if rn_data and rn_data.get('connected') is True:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Radio successfully connected', 1, radio_info))
            return True
            
        # Not connected yet, wait and retry
        if attempt < max_attempts:
            if status_queue:
                response_text = "Device not found" if not rn_data else "Device not connected"
                status_queue.put(('IN_PROGRESS', f'Waiting for connection ({attempt}/{max_attempts}): {response_text}', 1, radio_info))
            time.sleep(check_interval)
    
    # Connection timed out
    if status_queue:
        status_queue.put(('FAILED', f'Connection timed out after {max_attempts} attempts', 1, radio_info))
    
    return False

def wait_for_reconnection(serial_number, status_queue=None, check_interval=60, max_attempts=20):
    """
    Wait for a radio to reconnect after forcing reconnection.
    
    Args:
        serial_number (str): The serial number of the radio
        status_queue (Queue): Optional queue to send status updates instead of printing
        check_interval (int): Time in seconds between status checks
        max_attempts (int): Maximum number of status check attempts
        
    Returns:
        tuple: (bool, dict) True and radio info if radio reconnects, False and None otherwise
    """
    # Initial radio info for status updates
    radio_info = {'firmware': '', 'connected_bn': '', 'hardware': '', 'carrier_mode': ''}
    
    # Initial messages
    if status_queue:
        status_queue.put(('IN_PROGRESS', f'Waiting for radio to reconnect', 1, radio_info))
    else:
        print(f"\nWaiting for radio {serial_number} to reconnect...")
        print(f"Radio typically takes at least 3 minutes to reconnect. Waiting...")
    
    # Initial 3-minute wait to allow the radio to complete its reconnection cycle
    initial_wait = 180  # 3 minutes in seconds
    if status_queue:
        status_queue.put(('IN_PROGRESS', f'Initial wait period ({initial_wait}s)', 1, radio_info))
    else:
        print(f"Waiting {initial_wait} seconds before starting to check...")
    time.sleep(initial_wait)
    
    if status_queue:
        status_queue.put(('IN_PROGRESS', f'Starting reconnection checks (0/{max_attempts})', 1, radio_info))
    else:
        print(f"Initial wait complete. Now checking every {check_interval} seconds (maximum {max_attempts} attempts)")
    
    for attempt in range(1, max_attempts + 1):
        # Get RN information
        if status_queue:
            status_queue.put(('IN_PROGRESS', f'Reconnection check ({attempt}/{max_attempts})', 1, radio_info))
        
        rn_data = get_radio_info(serial_number)
        if not rn_data:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Failed to get radio info ({attempt}/{max_attempts})', 1, radio_info))
            else:
                print(f"Attempt {attempt}/{max_attempts}: Failed to get RN information")
        elif rn_data.get('connected', False):
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Radio connected! Getting BN info', 1, radio_info))
            else:
                print(f"Attempt {attempt}/{max_attempts}: Radio is connected!")
            
            # Get connected BN information
            connected_bn = rn_data.get('connectedBn')
            if not connected_bn:
                if status_queue:
                    status_queue.put(('FAILED', f'No connected BN found', 1, radio_info))
                else:
                    print(f"Error: No connected BN found for RN {serial_number}")
                return False, None
                
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Getting BN info: {connected_bn}', 1, radio_info))
            else:
                print(f"Getting information for connected BN: {connected_bn}")
            
            bn_data = get_radio_info(connected_bn)
            if not bn_data:
                if status_queue:
                    status_queue.put(('FAILED', f'Failed to get BN info', 1, radio_info))
                else:
                    print(f"Failed to get BN information for {connected_bn}")
                return False, None
                
            return True, bn_data
        else:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Radio not connected ({attempt}/{max_attempts})', 1, radio_info))
            else:
                print(f"Attempt {attempt}/{max_attempts}: Radio is not connected")
        
        if attempt < max_attempts:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Waiting for next check ({attempt}/{max_attempts})', 1, radio_info))
            else:
                print(f"Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)
    
    if status_queue:
        status_queue.put(('FAILED', f'Reconnection timed out after {max_attempts} attempts', 1, radio_info))
    else:
        print(f"Maximum attempts reached. Radio {serial_number} did not reconnect within the time limit.")
    
    return False, None

def run_speed_tests_simple(serial_number, num_tests=3, interval=60, max_attempts=10):
    """
    Run speed tests with minimal output for parallel processing.
    
    Args:
        serial_number (str): The serial number of the radio
        num_tests (int): Number of successful speed tests required
        interval (int): Time in seconds between tests
        max_attempts (int): Maximum number of test attempts
        
    Returns:
        bool: True if tests were run successfully, False otherwise
    """
    from ezSync.api import initiate_speed_test, poll_speed_test_results
    
    print(f"\nRunning speed tests for {serial_number}")
    
    successful_tests = 0
    attempt = 0
    
    while successful_tests < num_tests and attempt < max_attempts:
        attempt += 1
        print(f"Speed Test Attempt {attempt}/{max_attempts} (Successful: {successful_tests}/{num_tests})")
        
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
            
            # Only count successful tests
            if status == "COMPLETED":
                print(f"Speed test attempt {attempt} completed successfully")
                
                # Verify we have throughput data
                if test_result.get('downlinkThroughput') is not None:
                    successful_tests += 1
                    print(f"Test added to successful results ({successful_tests}/{num_tests})")
                else:
                    print(f"Speed test completed but no throughput data found - not counting as successful")
            else:
                # Handle failed tests
                failure_reason = test_result.get('failureReason', 'Unknown reason')
                print(f"Speed test attempt {attempt} failed: {status} - {failure_reason}")
        
        # Wait between tests if we're not done
        if successful_tests < num_tests and attempt < max_attempts:
            print(f"Waiting {interval} seconds before next test...")
            time.sleep(interval)
    
    return successful_tests >= num_tests

def worker_refurbish_radio(radio_serial, status_queue, skip_speedtest, skip_firmware, verbose):
    """
    Process function to refurbish a single radio and send status updates via queue
    
    Args:
        radio_serial (str): Radio serial number
        status_queue (Queue): Queue to send status updates
        skip_speedtest (bool): Flag to skip speed tests
        skip_firmware (bool): Flag to skip firmware upgrade
        verbose (bool): Flag for verbose output
    """
    # Define status steps
    STEPS = {
        'connect': 1,    # Connecting to radio
        'config': 2,     # Applying configuration
        'firmware': 3,   # Firmware upgrade
        'speedtest': 4,  # Running speed tests
        'final': 5       # Final configuration
    }
    
    # Radio info for enhanced display
    radio_info = {
        'firmware': '',
        'connected_bn': '',
        'hardware': '',
        'carrier_mode': '',
        'frequencies': ''
    }
    
    # Track completion status
    completed = False
    
    try:
        # Update status to in progress with step
        status_queue.put(('IN_PROGRESS', f'[1/5] Starting refurbishment', STEPS['connect'], radio_info))
        
        # Step 1: Connect to radio
        status_queue.put(('IN_PROGRESS', f'[1/5] Connecting to radio', STEPS['connect'], radio_info))
        connected = wait_for_connection(radio_serial, status_queue, check_interval=20, max_attempts=30)
        if not connected:
            status_queue.put(('FAILED', f'Failed to connect to radio', STEPS['connect'], radio_info))
            return
        
        # Get radio info for enhanced display
        radio_data = get_radio_info(radio_serial)
        if radio_data:
            radio_info = {
                'firmware': radio_data.get('softwareVersion', 'Unknown'),
                'connected_bn': radio_data.get('connectedBn', 'None'),
                'hardware': radio_data.get('partNumber', 'Unknown'),
                'carrier_mode': radio_data.get('multiCarrierModeRn', 'Unknown')
            }
            
            # Format carrier frequencies if available
            carriers = radio_data.get('carriers', {})
            if carriers and '0' in carriers and '1' in carriers:
                freq0 = carriers['0'].get('frequency', 0) / 1000  # Convert to MHz
                bw0 = carriers['0'].get('bandwidth', 0)
                freq1 = carriers['1'].get('frequency', 0) / 1000  # Convert to MHz
                bw1 = carriers['1'].get('bandwidth', 0)
                radio_info['frequencies'] = f"{freq0} MHz/{bw0} MHz, {freq1} MHz/{bw1} MHz"
            
            # Update status with the new info
            status_queue.put(('IN_PROGRESS', f'[1/5] Connected to radio', STEPS['connect'], radio_info))
        
        # Step 2: Apply configuration
        status_queue.put(('IN_PROGRESS', f'[2/5] Applying default configuration', STEPS['config'], radio_info))
        if not apply_default_config(radio_serial, custom_hostname="IN_REFURBISHMENT"):
            status_queue.put(('FAILED', f'Failed to apply default configuration', STEPS['config'], radio_info))
            return
        
        # Step 3: Perform firmware upgrade if needed and not skipped
        status_queue.put(('IN_PROGRESS', f'[3/5] Firmware management', STEPS['firmware'], radio_info))
        if not skip_firmware:
            status_queue.put(('IN_PROGRESS', f'[3/5] Checking firmware and upgrading if needed', STEPS['firmware'], radio_info))
            upgrade_result = upgrade_radio_firmware(radio_serial)
            
            if not upgrade_result:
                status_queue.put(('FAILED', f'Failed to initiate firmware upgrade', STEPS['firmware'], radio_info))
                return
                
            # Check if upgrade was skipped because radio already has target firmware
            if hasattr(upgrade_result, "skipped") and upgrade_result.skipped:
                status_queue.put(('IN_PROGRESS', f'[3/5] Firmware already up to date', STEPS['firmware'], radio_info))
            else:
                # An actual firmware upgrade was initiated
                status_queue.put(('IN_PROGRESS', f'[3/5] Firmware upgrade in progress', STEPS['firmware'], radio_info))
                
                # Wait for radio to upgrade and reconnect
                initial_wait = 300  # 5 minutes in seconds
                time.sleep(initial_wait)
                
                # Check periodically if the radio has reconnected
                reconnected = False
                for i in range(10):
                    if wait_for_connection(radio_serial, check_interval=5, max_attempts=2):
                        status_queue.put(('IN_PROGRESS', f'[3/5] Radio reconnected after upgrade', STEPS['firmware'], radio_info))
                        
                        # Update firmware info after upgrade
                        fresh_radio_data = get_radio_info(radio_serial)
                        if fresh_radio_data:
                            radio_info['firmware'] = fresh_radio_data.get('softwareVersion', radio_info['firmware'])
                            status_queue.put(('IN_PROGRESS', f'[3/5] Updated firmware: {radio_info["firmware"]}', STEPS['firmware'], radio_info))
                        
                        reconnected = True
                        break
                    time.sleep(60)  # Wait 1 minute before next check
                
                if not reconnected:
                    status_queue.put(('WARNING', f'[3/5] Radio did not reconnect after upgrade', STEPS['firmware'], radio_info))
        else:
            status_queue.put(('IN_PROGRESS', f'[3/5] Firmware upgrade skipped', STEPS['firmware'], radio_info))
            
            # If skipping firmware, reboot radio explicitly
            status_queue.put(('IN_PROGRESS', f'[3/5] Rebooting radio', STEPS['firmware'], radio_info))
            if not reboot_radio(radio_serial):
                status_queue.put(('FAILED', f'Failed to reboot radio', STEPS['firmware'], radio_info))
                return
                
            # Wait for reconnection after reboot
            reconnected, _ = wait_for_reconnection(radio_serial, status_queue, check_interval=20, max_attempts=30)
            if not reconnected:
                status_queue.put(('FAILED', f'Radio did not reconnect after reboot', STEPS['firmware'], radio_info))
                return
        
        # Step 4: Run speed tests if not skipped
        if not skip_speedtest:
            status_queue.put(('IN_PROGRESS', f'[4/5] Preparing for speed tests', STEPS['speedtest'], radio_info))
            # Allow connection to stabilize before speed tests
            time.sleep(60)
            status_queue.put(('IN_PROGRESS', f'[4/5] Running speed tests', STEPS['speedtest'], radio_info))
            
            # Run speed tests and capture results
            speed_test_results = run_speed_tests_with_results(radio_serial, num_tests=3, interval=60, max_attempts=10, status_queue=status_queue, step=4)
            if not speed_test_results:
                status_queue.put(('FAILED', f'Speed tests failed', STEPS['speedtest'], radio_info))
                return
                
            # Add speed test results to radio info
            if isinstance(speed_test_results, dict):
                dl = speed_test_results.get('downlinkThroughput', 0) / 1000  # Convert to Mbps
                ul = speed_test_results.get('uplinkThroughput', 0) / 1000 if speed_test_results.get('uplinkThroughput') is not None else 0
                radio_info['speed_test'] = f"{dl:.1f}/{ul:.1f} Mbps"
        else:
            status_queue.put(('IN_PROGRESS', f'[4/5] Speed tests skipped', STEPS['speedtest'], radio_info))
        
        # Step 5: Apply final configuration
        status_queue.put(('IN_PROGRESS', f'[5/5] Applying final configuration', STEPS['final'], radio_info))
        if not apply_default_config(radio_serial, custom_hostname="REFURBISHED"):
            status_queue.put(('FAILED', f'Failed to apply final configuration', STEPS['final'], radio_info))
            return
            
        # Update radio info after final configuration
        fresh_radio_data = get_radio_info(radio_serial)
        if fresh_radio_data:
            radio_info['hostname'] = fresh_radio_data.get('hostName', 'REFURBISHED')
        
        # Mark as completed
        completed = True
        status_queue.put(('COMPLETED', f'Refurbishment completed successfully', STEPS['final'], radio_info))
        
    except Exception as e:
        # Report failure with error message
        status_queue.put(('FAILED', f'Error: {str(e)}', 0, radio_info))
        if verbose:
            traceback.print_exc()
    finally:
        # Ensure we send a final status update if we haven't already marked as completed
        # This helps prevent "Process did not complete" errors for radios that actually completed
        if completed:
            # Send one more time to ensure it's received
            try:
                status_queue.put(('COMPLETED', f'Refurbishment completed successfully', STEPS['final'], radio_info), block=False)
            except queue.Full:
                pass  # Queue is full, but we already marked as completed earlier

def run_speed_tests_with_results(serial_number, num_tests=3, interval=60, max_attempts=10, status_queue=None, step=4):
    """
    Run speed tests and return results
    
    Args:
        serial_number: The serial number of the radio
        num_tests: Number of tests to run
        interval: Interval between tests
        max_attempts: Maximum attempts
        status_queue: Optional queue for status updates
        step: The step number for the status queue (default: 4)
        
    Returns:
        dict: Average speed test results or None if failed
    """
    from ezSync.api import initiate_speed_test, poll_speed_test_results
    
    # Get radio info for status updates
    radio_info = {}
    if status_queue:
        radio_data = get_radio_info(serial_number)
        if radio_data:
            radio_info = {
                'firmware': radio_data.get('softwareVersion', 'Unknown'),
                'connected_bn': radio_data.get('connectedBn', 'None'),
                'hardware': radio_data.get('partNumber', 'Unknown'),
                'carrier_mode': radio_data.get('multiCarrierModeRn', 'Unknown')
            }
            
            # Format carrier frequencies if available
            carriers = radio_data.get('carriers', {})
            if carriers and '0' in carriers and '1' in carriers:
                freq0 = carriers['0'].get('frequency', 0) / 1000  # Convert to MHz
                bw0 = carriers['0'].get('bandwidth', 0)
                freq1 = carriers['1'].get('frequency', 0) / 1000  # Convert to MHz
                bw1 = carriers['1'].get('bandwidth', 0)
                radio_info['frequencies'] = f"{freq0} MHz/{bw0} MHz, {freq1} MHz/{bw1} MHz"
    
    if status_queue:
        status_queue.put(('IN_PROGRESS', f'Starting speed tests (0/{num_tests})', step, radio_info))
    else:
        print(f"\nRunning speed tests for {serial_number}")
    
    successful_tests = 0
    attempt = 0
    
    results = []
    
    while successful_tests < num_tests and attempt < max_attempts:
        attempt += 1
        
        if status_queue:
            status_queue.put(('IN_PROGRESS', f'Speed test attempt {attempt}/{max_attempts} (Completed: {successful_tests}/{num_tests})', step, radio_info))
        else:
            print(f"Speed Test Attempt {attempt}/{max_attempts} (Successful: {successful_tests}/{num_tests})")
        
        # Initiate speed test
        operation_id = initiate_speed_test(serial_number)
        if not operation_id:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Failed to initiate test {attempt}', step, radio_info))
            else:
                print(f"Failed to initiate speed test attempt {attempt}")
            continue
        
        # Poll for results
        if status_queue:
            status_queue.put(('IN_PROGRESS', f'Waiting for test {attempt} results', step, radio_info))
            
        test_result = poll_speed_test_results(operation_id, serial_number)
        
        if not test_result:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Failed to get results for test {attempt}', step, radio_info))
            else:
                print(f"Failed to get results for speed test attempt {attempt}")
        else:
            status = test_result.get('status')
            
            # Only count successful tests
            if status == "COMPLETED":
                if status_queue:
                    status_queue.put(('IN_PROGRESS', f'Test {attempt} completed successfully', step, radio_info))
                else:
                    print(f"Speed test attempt {attempt} completed successfully")
                
                # Verify we have throughput data
                if test_result.get('downlinkThroughput') is not None:
                    successful_tests += 1
                    results.append(test_result)
                    
                    # Update status with current results
                    dl = test_result.get('downlinkThroughput', 0) / 1000  # Convert to Mbps
                    ul = test_result.get('uplinkThroughput', 0) / 1000 if test_result.get('uplinkThroughput') is not None else 0
                    
                    if status_queue:
                        radio_info['speed_test'] = f"{dl:.1f}/{ul:.1f} Mbps"
                        status_queue.put(('IN_PROGRESS', f'Test {attempt} success: {dl:.1f}/{ul:.1f} Mbps ({successful_tests}/{num_tests})', step, radio_info))
                    else:
                        print(f"Test added to successful results ({successful_tests}/{num_tests})")
                else:
                    if status_queue:
                        status_queue.put(('IN_PROGRESS', f'Test completed but no throughput data', step, radio_info))
                    else:
                        print(f"Speed test completed but no throughput data found - not counting as successful")
            else:
                # Handle failed tests
                failure_reason = test_result.get('failureReason', 'Unknown reason')
                if status_queue:
                    status_queue.put(('IN_PROGRESS', f'Test {attempt} failed: {failure_reason}', step, radio_info))
                else:
                    print(f"Speed test attempt {attempt} failed: {status} - {failure_reason}")
        
        # Wait between tests if we're not done
        if successful_tests < num_tests and attempt < max_attempts:
            if status_queue:
                status_queue.put(('IN_PROGRESS', f'Waiting {interval}s before next test', step, radio_info))
            else:
                print(f"Waiting {interval} seconds before next test...")
            time.sleep(interval)
    
    # Calculate average results from the tests
    if not results:
        if status_queue:
            status_queue.put(('FAILED', f'No successful speed tests', step, radio_info))
        return None
        
    # Just return the first result for simplicity
    # Update status with final results
    if status_queue:
        dl = results[0].get('downlinkThroughput', 0) / 1000
        ul = results[0].get('uplinkThroughput', 0) / 1000 if results[0].get('uplinkThroughput') is not None else 0
        status_queue.put(('IN_PROGRESS', f'Speed tests completed: {dl:.1f}/{ul:.1f} Mbps', step, radio_info))
        
    return results[0]