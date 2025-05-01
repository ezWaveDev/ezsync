"""
Core business logic and operations for the ezSync application.
This module contains the workflows for various radio operations.
"""

import time
import random
import multiprocessing
import threading
import queue

from ezSync.api import (
    get_radio_info,
    get_rn_info,
    reconnect_radio,
    reboot_radio,
    apply_default_config,
    apply_refurb_config,
    initiate_speed_test,
    poll_speed_test_results,
    get_radio_status,
    upgrade_radio_firmware,
)
from ezSync.utils import (
    format_value,
    calculate_average_speed_test_results,
    calculate_azimuth,
)

# Define step indicators and their meanings
STEPS = {
    "init": 0,  # Initialization
    "connect": 1,  # Connecting to radio
    "config": 2,  # Applying configuration
    "firmware": 3,  # Firmware upgrade
    "reboot": 4,  # Rebooting
    "speedtest": 5,  # Running speed tests
    "complete": 6,  # Completed
}

STEP_SYMBOLS = [
    "[1]",  # Connect
    "[2]",  # Configure
    "[3]",  # Reboot/Firmware
    "[4]",  # Speed Test
    "[5]",  # Final Config
]

# Global variables for status tracking
status_lock = None
status_board = {}
verbose_mode = False
status_stop_timer = False


def update_status(serial_number, step=None, status=None, message=None, error=None):
    """Update the status of a radio and refresh the display"""
    global status_board, status_lock, verbose_mode

    if status_lock is None:
        return

    with status_lock:
        # Update the status board
        if serial_number in status_board:
            if step is not None:
                status_board[serial_number]["step"] = step
            if status is not None:
                status_board[serial_number]["status"] = status
            if message is not None:
                status_board[serial_number]["message"] = message
            if error is not None:
                status_board[serial_number]["error"] = error

            # Only refresh display in non-verbose mode
            if not verbose_mode:
                # Clear screen and redraw status board
                print_status_board()


def print_status_board():
    """Print a status board showing progress of all radios"""
    global status_board, verbose_mode

    import sys

    # Move cursor to beginning and clear screen
    sys.stdout.write("\033[H\033[J")

    # Print header
    sys.stdout.write(f"=== Refurbishing {len(status_board)} radios in parallel ===\n")
    sys.stdout.write("\n")

    # Function to get progress indicators for a radio
    def get_progress_indicators(radio_status):
        step = radio_status["step"]
        status = radio_status["status"]
        error = radio_status["error"]

        step_value = STEPS.get(step, 0)

        # Generate progress string
        progress = []

        # Add step indicators
        for i in range(len(STEP_SYMBOLS)):
            if status == "FAILED" and i == step_value - 1:
                progress.append("[✗]")  # Failed at this step
            elif i < step_value:
                progress.append(STEP_SYMBOLS[i])  # Completed step
            elif i == step_value and status in ["RUNNING", "PENDING"]:
                # Current step (in progress)
                progress.append(f"[{i+1}]")
            else:
                # Future step
                progress.append(f"[ ]")

        # Add completion marker if successful
        if status == "SUCCESS":
            progress.append("[✓]")

        # Join with arrows
        progress_str = "→".join(progress)

        # Add status and message
        if status == "SUCCESS":
            result = f"{progress_str}  \033[92mSUCCESS\033[0m"
        elif status == "FAILED":
            result = f"{progress_str}  \033[91mFAILED\033[0m"
            if error:
                result += f" ({error})"
        else:  # RUNNING or PENDING
            result = f"{progress_str}  \033[93m{status}\033[0m"
            message = radio_status["message"]
            if message:
                result += f" - {message}"

        return result

    # Print status for each radio
    for sn in status_board.keys():
        progress_str = get_progress_indicators(status_board[sn])
        sys.stdout.write(f"{sn}:  {progress_str}\n")

    # Print legend
    sys.stdout.write(
        "\nSteps: [1]=Connect [2]=Configure [3]=Reboot/Firmware [4]=Speed Test [5]=Final Config [✓]=Complete [✗]=Failed\n\n"
    )
    sys.stdout.flush()


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
    global status_board, status_lock

    # Check if we're in parallel mode with status board
    using_status_board = status_lock is not None and serial_number in status_board

    if using_status_board:
        update_status(
            serial_number,
            step="connect",
            status="RUNNING",
            message=f"Waiting for radio to connect (0/{max_attempts})",
        )
    else:
        print(
            f"Waiting for radio {serial_number} to connect... (max {max_attempts} attempts, {check_interval}s interval)"
        )

    for attempt in range(1, max_attempts + 1):
        # Update status or print progress
        if using_status_board:
            update_status(
                serial_number,
                message=f"Waiting for connection ({attempt}/{max_attempts})",
            )
        else:
            print(f"Connection attempt {attempt}/{max_attempts} for {serial_number}")

        rn_data = get_radio_info(serial_number)

        # Radio is online
        if rn_data and rn_data.get("connected") is True:
            if using_status_board:
                update_status(serial_number, message="Radio successfully connected")
            else:
                print(f"Radio {serial_number} is now connected")
            return True

        # Not connected yet, wait and retry
        if attempt < max_attempts:
            response_text = (
                "Device not found" if not rn_data else "Device not connected"
            )
            if using_status_board:
                update_status(
                    serial_number,
                    message=f"Waiting ({attempt}/{max_attempts}): {response_text}",
                )
            else:
                print(
                    f"Radio {serial_number} not connected yet, waiting {check_interval} seconds..."
                )
            time.sleep(check_interval)

    # Connection timed out
    if using_status_board:
        update_status(
            serial_number,
            status="FAILED",
            message=f"Connection timed out after {max_attempts} attempts",
        )
    else:
        print(
            f"Radio {serial_number} connection timed out after {max_attempts} attempts"
        )

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
    global status_board, status_lock

    # Check if we're in parallel mode with status board
    using_status_board = status_lock is not None and serial_number in status_board

    # Initial message
    if using_status_board:
        update_status(
            serial_number,
            step="connect",
            status="RUNNING",
            message=f"Waiting for radio to reconnect",
        )
    else:
        print(f"\nWaiting for radio {serial_number} to reconnect...")
        print(f"Radio typically takes at least 3 minutes to reconnect. Waiting...")

    # Initial 3-minute wait to allow the radio to complete its reconnection cycle
    initial_wait = 180  # 3 minutes in seconds
    if using_status_board:
        update_status(serial_number, message=f"Initial wait period ({initial_wait}s)")
    else:
        print(f"Waiting {initial_wait} seconds before starting to check...")
    time.sleep(initial_wait)

    if using_status_board:
        update_status(
            serial_number, message=f"Starting reconnection checks (0/{max_attempts})"
        )
    else:
        print(
            f"Initial wait complete. Now checking every {check_interval} seconds (maximum {max_attempts} attempts)"
        )

    for attempt in range(1, max_attempts + 1):
        # Get RN information
        if using_status_board:
            update_status(
                serial_number, message=f"Reconnection check ({attempt}/{max_attempts})"
            )

        rn_data = get_radio_info(serial_number)
        if not rn_data:
            if using_status_board:
                update_status(
                    serial_number,
                    message=f"Failed to get radio info ({attempt}/{max_attempts})",
                )
            else:
                print(f"Attempt {attempt}/{max_attempts}: Failed to get RN information")
        elif rn_data.get("connected", False):
            if using_status_board:
                update_status(
                    serial_number, message=f"Radio connected! Getting BN info"
                )
            else:
                print(f"Attempt {attempt}/{max_attempts}: Radio is connected!")

            # Get connected BN information
            connected_bn = rn_data.get("connectedBn")
            if not connected_bn:
                if using_status_board:
                    update_status(serial_number, message=f"No connected BN found")
                else:
                    print(f"Error: No connected BN found for RN {serial_number}")
                return False, None

            if using_status_board:
                update_status(serial_number, message=f"Getting BN info: {connected_bn}")
            else:
                print(f"Getting information for connected BN: {connected_bn}")

            bn_data = get_radio_info(connected_bn)
            if not bn_data:
                if using_status_board:
                    update_status(serial_number, message=f"Failed to get BN info")
                else:
                    print(f"Failed to get BN information for {connected_bn}")
                return False, None

            return True, bn_data
        else:
            if using_status_board:
                update_status(
                    serial_number,
                    message=f"Radio not connected ({attempt}/{max_attempts})",
                )
            else:
                print(f"Attempt {attempt}/{max_attempts}: Radio is not connected")

        if attempt < max_attempts:
            if using_status_board:
                update_status(
                    serial_number,
                    message=f"Waiting for next check ({attempt}/{max_attempts})",
                )
            else:
                print(f"Waiting {check_interval} seconds before next check...")
            time.sleep(check_interval)

    if using_status_board:
        update_status(
            serial_number,
            status="FAILED",
            message=f"Reconnection timed out after {max_attempts} attempts",
        )
    else:
        print(
            f"Maximum attempts reached. Radio {serial_number} did not reconnect within the time limit."
        )

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
    if "bnSerialNumber" in results:
        print(f"Connected BN: {results.get('bnSerialNumber', 'N/A')}")

    # Time information
    timestamp = results.get("timestamp")
    if timestamp:
        timestamp_str = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(timestamp / 1000)
        )
        print(f"Timestamp: {timestamp_str}")

    # Throughput information
    downlink = results.get("downlinkThroughput")
    if downlink is not None:
        # The values appear to be in Kbps, so we divide by 1000 to get Mbps
        downlink_mbps = downlink / 1000
        print(f"\nDownlink Speed: {format_value(downlink_mbps)} Mbps")

    uplink = results.get("uplinkThroughput")
    if uplink is not None:
        # The values appear to be in Kbps, so we divide by 1000 to get Mbps
        uplink_mbps = uplink / 1000
        print(f"Uplink Speed: {format_value(uplink_mbps)} Mbps")

    # Latency
    latency = results.get("latencyMillis")
    if latency is not None:
        print(f"Latency: {format_value(latency)} ms")

    # Signal quality
    print("\nSignal Information:")
    print(f"Downlink SNR: {format_value(results.get('downlinkSnr', 'N/A'))} dB")
    print(f"Uplink SNR: {format_value(results.get('uplinkSnr', 'N/A'))} dB")
    print(f"Path Loss: {format_value(results.get('pathloss', 'N/A'))} dB")

    # Link information
    print("\nLink Information:")
    print(
        f"Primary Frequency: {results.get('frequency0', 'N/A')/1000 if results.get('frequency0') else 'N/A'} MHz"
    )
    print(
        f"Secondary Frequency: {results.get('frequency1', 'N/A')/1000 if results.get('frequency1') else 'N/A'} MHz"
    )
    print(f"Primary Bandwidth: {results.get('bandwidth0', 'N/A')} MHz")
    print(f"Secondary Bandwidth: {results.get('bandwidth1', 'N/A')} MHz")
    print(f"RF Link Distance: {results.get('rfLinkDistance', 'N/A')} meters")

    # Additional information
    if results.get("failureReason"):
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
        print(
            f"\nSpeed Test Attempt {attempt}/{max_attempts} (Successful: {len(successful_results)}/{num_tests})"
        )

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
            status = test_result.get("status")

            # Display results regardless of status
            display_speed_test_results(test_result)

            # Only count successful tests
            if status == "COMPLETED":
                print(f"Speed test attempt {attempt} completed successfully")

                # Verify we have throughput data
                if test_result.get("downlinkThroughput") is not None:
                    successful_results.append(test_result)
                    print(
                        f"Test added to successful results ({len(successful_results)}/{num_tests})"
                    )
                else:
                    print(
                        f"Speed test completed but no throughput data found - not counting as successful"
                    )
            else:
                # Handle failed tests
                failure_reason = test_result.get("failureReason", "Unknown reason")
                print(
                    f"Speed test attempt {attempt} failed: {status} - {failure_reason}"
                )

        # Wait between tests if we're not done
        if len(successful_results) < num_tests and attempt < max_attempts:
            print(f"Waiting {interval} seconds before next test...")
            time.sleep(interval)

    # Check if we have enough successful tests
    if len(successful_results) < num_tests:
        print(
            f"\nWarning: Only obtained {len(successful_results)}/{num_tests} successful speed tests"
        )

        if not successful_results:
            print("No successful speed tests completed")
            return None

    # Display table of individual test results
    print("\n================== INDIVIDUAL TESTS =================")
    print(
        "#  | DL (Mbps) | UL (Mbps) | Latency (ms) | DL SNR | UL SNR | Path Loss | RF Dist"
    )
    print(
        "-----------------------------------------------------------------------------------"
    )

    for i, result in enumerate(successful_results):
        dl = result.get("downlinkThroughput", 0) / 1000
        ul = (
            result.get("uplinkThroughput", 0) / 1000
            if result.get("uplinkThroughput") is not None
            else 0
        )
        latency = result.get("latencyMillis", 0)
        dl_snr = result.get("downlinkSnr", "N/A")
        ul_snr = result.get("uplinkSnr", "N/A")
        path_loss = result.get("pathloss", "N/A")
        rf_dist = result.get("rfLinkDistance", 0)

        print(
            f"{i+1:<3}| {format_value(dl):^10} | {format_value(ul):^9} | {format_value(latency):^12} | {format_value(dl_snr):^6} | {format_value(ul_snr):^6} | {format_value(path_loss):^9} | {format_value(rf_dist):>5} m"
        )

    print("=====================================================")

    # Calculate averages
    avg_results = calculate_average_speed_test_results(successful_results)

    print("\nAverage Speed Test Results:")
    print("=" * 50)
    print(f"Number of successful tests: {len(successful_results)}")

    # Throughput information
    downlink = avg_results.get("downlinkThroughput")
    if downlink is not None:
        downlink_mbps = downlink / 1000
        print(f"Average Downlink Speed: {format_value(downlink_mbps)} Mbps")

    uplink = avg_results.get("uplinkThroughput")
    if uplink is not None:
        uplink_mbps = uplink / 1000
        print(f"Average Uplink Speed: {format_value(uplink_mbps)} Mbps")

    # Latency
    latency = avg_results.get("latencyMillis")
    if latency is not None:
        print(f"Average Latency: {format_value(latency)} ms")

    # Signal quality
    print("\nSignal Information:")
    print(
        f"Average Downlink SNR: {format_value(avg_results.get('downlinkSnr', 'N/A'))} dB"
    )
    print(f"Average Uplink SNR: {format_value(avg_results.get('uplinkSnr', 'N/A'))} dB")
    print(f"Average Path Loss: {format_value(avg_results.get('pathloss', 'N/A'))} dB")

    print("=" * 50)

    return avg_results


def refurbish_radio(serial_number, skip_speedtest=False, skip_firmware=False):
    """
    Perform the full refurbishment process on a radio.

    Args:
        serial_number (str): The serial number of the radio
        skip_speedtest (bool): Whether to skip the speed test step
        skip_firmware (bool): Whether to skip the firmware upgrade step

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

    # Add a short wait after config before proceeding
    print("Waiting 30 seconds after applying refurbishment configuration...")
    time.sleep(30)

    # Step 4: Handle firmware upgrade or reboot based on skip_firmware flag
    if not skip_firmware:
        # Check if firmware upgrade is needed
        from ezSync.api import upgrade_radio_firmware

        upgrade_result = upgrade_radio_firmware(serial_number)

        if not upgrade_result:
            print(f"Failed to initiate firmware upgrade for radio {serial_number}")
            return False

        # Check if the radio was already running the target firmware
        if hasattr(upgrade_result, "skipped") and upgrade_result.skipped:
            print(
                f"Firmware upgrade was skipped as the radio already has the target version"
            )
            print(f"No reboot needed - continuing with next steps")

            # No reboot needed, continue with the process
        else:
            # An actual firmware upgrade was initiated
            # Wait for the radio to upgrade and reboot
            print(
                f"\nFirmware upgrade initiated. Waiting for radio to upgrade and reconnect..."
            )
            print(
                f"This process typically takes 5-15 minutes. Will start checking for reconnection after 5 minutes."
            )

            # Initial wait before starting to check for reconnection
            initial_wait = 300  # 5 minutes in seconds
            print(
                f"Waiting {initial_wait//60} minutes before first reconnection check..."
            )
            time.sleep(initial_wait)

            # Now check periodically if the radio has reconnected
            upgrade_check_interval = 60  # Check every minute
            max_upgrade_checks = 10  # Maximum 10 checks (10 additional minutes)

            print(
                f"Beginning periodic checks for radio reconnection (every {upgrade_check_interval} seconds)..."
            )
            upgrade_reconnected = False

            for check in range(1, max_upgrade_checks + 1):
                print(f"Reconnection check {check}/{max_upgrade_checks}...")

                # Check if radio has reconnected
                if wait_for_connection(serial_number, check_interval=5, max_attempts=2):
                    print(
                        f"Radio {serial_number} successfully reconnected after firmware upgrade"
                    )
                    upgrade_reconnected = True
                    break

                # Wait before next check if we haven't reached the maximum
                if check < max_upgrade_checks:
                    print(
                        f"Radio not reconnected yet. Waiting {upgrade_check_interval} seconds before next check..."
                    )
                    time.sleep(upgrade_check_interval)

            if not upgrade_reconnected:
                print(
                    f"Warning: Radio {serial_number} did not reconnect after firmware upgrade within the expected time"
                )
                print(f"Attempting to proceed with the remaining steps anyway...")
    else:
        # If we're skipping firmware, we need to explicitly reboot the radio
        print(f"Rebooting radio: {serial_number}")
        if not reboot_radio(serial_number):
            print(f"Failed to reboot radio {serial_number}")
            return False

        # Wait for reconnection after reboot
        print(f"Waiting for radio {serial_number} to reconnect after reboot...")
        reconnected, _ = wait_for_reconnection(serial_number)
        if not reconnected:
            print(f"Radio {serial_number} did not reconnect after reboot")
            return False

    # Only run speed tests if not skipped
    if not skip_speedtest:
        # Add a settling period before starting speed tests
        settling_time = 60  # 1 minute in seconds
        print(
            f"\nAllowing {settling_time} seconds for connection to stabilize before running speed tests..."
        )
        time.sleep(settling_time)
        print("Settling period complete. Proceeding with speed tests.")

        # Run speed tests
        print(f"Running speed tests for radio {serial_number}")
        speed_test_results = run_speed_tests(
            serial_number, num_tests=3, interval=60, max_attempts=10
        )
        if not speed_test_results:
            print(f"Failed to complete speed tests for radio {serial_number}")
            return False
    else:
        print("\nSkipping speed tests as requested.")

    # Apply final default configuration with REFURBISHED hostname
    print(f"Applying final configuration with REFURBISHED hostname")
    if not apply_default_config(serial_number, custom_hostname="REFURBISHED"):
        print(f"Failed to apply final configuration to radio {serial_number}")
        return False

    print(f"Successfully refurbished radio {serial_number}")
    return True


def refurbish_radios_parallel(
    radio_serial_numbers, skip_speedtest=False, skip_firmware=False, verbose=False
):
    """
    Refurbishes multiple radios in parallel

    Args:
        radio_serial_numbers (list): List of radio serial numbers to refurbish
        skip_speedtest (bool): Flag to skip speed tests during refurbishment
        skip_firmware (bool): Flag to skip firmware upgrade during refurbishment
        verbose (bool): Flag for verbose output

    Returns:
        int: Number of radios that had failures
    """
    # Using a Manager for shared state between processes
    from multiprocessing import Manager
    import time
    import queue

    # Import worker from separate module to avoid pickling issues
    from ezSync.parallel_worker import worker_refurbish_radio

    # Create a manager for shared resources
    manager = Manager()
    status_board = manager.dict()

    # Initialize status board
    for radio in radio_serial_numbers:
        status_board[radio] = {"status": "PENDING", "message": "", "step": 0}

    # Create a queue for each radio to receive status updates
    status_queues = {radio: manager.Queue() for radio in radio_serial_numbers}

    # Create and start processes for each radio
    processes = []
    for radio in radio_serial_numbers:
        process = multiprocessing.Process(
            target=worker_refurbish_radio,
            args=(radio, status_queues[radio], skip_speedtest, skip_firmware, verbose),
        )
        process.start()
        processes.append(process)

    # Start a thread to monitor the status queues
    stop_monitoring = threading.Event()

    def monitor_status():
        import time
        import queue
        
        # Keep track of finished radios to avoid marking as incomplete
        finished_radios = set()
        
        while not stop_monitoring.is_set():
            for radio, q in status_queues.items():
                # Skip radios that are already known to be finished
                if radio in finished_radios:
                    continue
                    
                # Process all available messages in the queue
                messages_processed = 0
                
                while messages_processed < 10:  # Limit to prevent infinite loop
                    try:
                        # Non-blocking queue check
                        message_data = q.get(block=False)
                        messages_processed += 1
                        
                        # Handle different message formats
                        if len(message_data) >= 4:
                            # New format with step and radio info
                            status, message, step, radio_info = message_data
                            status_board[radio] = {
                                "status": status,
                                "message": message,
                                "step": step,
                                "radio_info": radio_info,
                            }
                        elif len(message_data) == 3:
                            # Format with step information
                            status, message, step = message_data
                            status_board[radio] = {
                                "status": status,
                                "message": message,
                                "step": step,
                            }
                        else:
                            # Old format without step
                            status, message = message_data
                            status_board[radio] = {"status": status, "message": message}
                        
                        # If status is completed or failed, mark as finished
                        if status in ["COMPLETED", "FAILED"]:
                            finished_radios.add(radio)
                        
                        # Print status board after each update
                        print_status_board_parallel(status_board)
                        
                    except queue.Empty:
                        # No more messages in this queue
                        break
            
            # Short sleep to prevent CPU thrashing
            time.sleep(0.2)

    monitor_thread = threading.Thread(target=monitor_status)
    monitor_thread.daemon = True
    monitor_thread.start()

    try:
        # Wait for all processes to complete
        for process in processes:
            process.join()
            
        # Give a small grace period for final status updates to be processed
        time.sleep(1)
            
        # Check for any processes that might not have updated their status properly
        verify_process_completion(status_board, status_queues, radio_serial_numbers)
    except KeyboardInterrupt:
        print("Operation interrupted by user")
        for process in processes:
            process.terminate()
        
        # Update status for interrupted radios
        for radio, status in status_board.items():
            if status["status"] == "PENDING" or status["status"] == "IN_PROGRESS":
                current_step = status.get("step", 0)
                radio_info = status.get("radio_info", {})
                status_board[radio] = {
                    "status": "FAILED",
                    "message": "Operation interrupted by user",
                    "step": current_step,
                    "radio_info": radio_info,
                }
        print_status_board_parallel(status_board)
    finally:
        # Signal the monitoring thread to stop
        stop_monitoring.set()
        monitor_thread.join()
    
    # Count failures
    failures = sum(
        1 for status in status_board.values() if status["status"] == "FAILED"
    )
    
    return failures


def verify_process_completion(status_board, status_queues, radio_serial_numbers):
    """Check for radios that should be marked as completed but weren't properly updated"""
    
    # Check each radio's status
    for radio in radio_serial_numbers:
        if radio not in status_board:
            continue
            
        status = status_board[radio]
        current_status = status.get("status", "")
        current_step = status.get("step", 0)
        radio_info = status.get("radio_info", {})
        
        # Skip radios that are already marked as completed or failed
        if current_status in ["COMPLETED", "FAILED"]:
            continue
            
        # If status is still in progress but in a late stage
        if (current_status == "IN_PROGRESS" or current_status == "PENDING") and current_step >= 4:
            # Radio is in speed test or final configuration step
            if "speed_test" in radio_info:
                # Radio has completed speed tests, likely completed
                if current_step >= 5:
                    # In final configuration step, almost certainly completed
                    status_board[radio] = {
                        "status": "COMPLETED",
                        "message": "Refurbishment completed successfully",
                        "step": 6,  # Complete
                        "radio_info": radio_info,
                    }
                    continue
        
        # If we get here and status is still pending or in progress, mark as failed
        if current_status == "IN_PROGRESS" or current_status == "PENDING":
            status_board[radio] = {
                "status": "FAILED",
                "message": "Process did not complete",
                "step": current_step,
                "radio_info": radio_info,
            }
                    
    # One final refresh of the display
    print_status_board_parallel(status_board)


def print_status_board_parallel(status_board):
    """Print a status board showing progress of all radios"""
    import sys

    # Define step symbols
    STEP_SYMBOLS = [
        "[1]",  # Connect
        "[2]",  # Configure
        "[3]",  # Firmware
        "[4]",  # Speed Test
        "[5]",  # Final Config
    ]

    # Move cursor to beginning and clear screen
    sys.stdout.write("\033[H\033[J")

    # Print header
    sys.stdout.write(f"=== Refurbishing {len(status_board)} radios in parallel ===\n\n")

    # Print status for each radio
    for sn, status in status_board.items():
        status_str = status["status"]
        message = status.get("message", "")
        step = status.get("step", 0)
        radio_info = status.get("radio_info", {})

        # Generate progress indicators
        progress = []
        for i in range(len(STEP_SYMBOLS)):
            if status_str == "FAILED" and i == step - 1:
                progress.append("\033[91m[✗]\033[0m")  # Red X for failed step
            elif i < step:
                progress.append(
                    "\033[92m" + STEP_SYMBOLS[i] + "\033[0m"
                )  # Green for completed steps
            elif i == step - 1 and status_str in ["IN_PROGRESS"]:
                progress.append(
                    "\033[93m" + STEP_SYMBOLS[i] + "\033[0m"
                )  # Yellow for current step
            else:
                progress.append("[ ]")  # Empty for future steps

        # Add completion indicator
        if status_str == "COMPLETED":
            progress.append("\033[92m[✓]\033[0m")  # Green checkmark for completion

        # Format progress string
        progress_str = "→".join(progress)

        # Format status color
        if status_str == "COMPLETED":
            status_display = f"\033[92m{status_str}\033[0m"  # Green for completed
        elif status_str == "FAILED":
            status_display = f"\033[91m{status_str}\033[0m"  # Red for failed
        else:
            status_display = f"\033[93m{status_str}\033[0m"  # Yellow for in progress

        # Print full status line
        sys.stdout.write(f"{sn}:  {progress_str}  {status_display} - {message}\n")

        # Check if radio is connected - show actual values or placeholders
        has_connected = False
        if (
            radio_info
            and radio_info.get("connected_bn")
            and radio_info.get("connected_bn") not in ["None", ""]
        ):
            has_connected = True

        # Always show firmware info (real or placeholder)
        firmware = radio_info.get("firmware", "Unknown") if has_connected else ""
        sys.stdout.write(f"    Firmware: {firmware}\n")

        # Always show BN info (real or placeholder)
        connected_bn = radio_info.get("connected_bn", "None") if has_connected else ""
        sys.stdout.write(f"    Connected BN: {connected_bn}\n")

        # Always show hardware info (real or placeholder)
        hardware = radio_info.get("hardware", "Unknown") if has_connected else ""
        sys.stdout.write(f"    Hardware: {hardware}\n")

        # Always show carrier info (real or placeholder)
        carrier_info = []
        if has_connected:
            if radio_info.get("carrier_mode", "Unknown") != "Unknown":
                carrier_info.append(radio_info.get("carrier_mode"))
            if radio_info.get("frequencies", "Unknown") != "Unknown":
                carrier_info.append(radio_info.get("frequencies"))
            carrier_display = " - ".join(carrier_info) if carrier_info else ""
        else:
            carrier_display = ""
        sys.stdout.write(f"    Carrier: {carrier_display}\n")

        # Show speed test results if available
        if "speed_test" in radio_info:
            sys.stdout.write(f"    Speed Test: {radio_info['speed_test']}\n")

        # Show hostname after final configuration
        if "hostname" in radio_info:
            sys.stdout.write(f"    Hostname: {radio_info['hostname']}\n")

        # Add space between radio entries
        sys.stdout.write("\n")

    # Print legend
    sys.stdout.write(
        "Steps: [1]=Connect [2]=Configure [3]=Firmware [4]=Speed Test [5]=Final Config [✓]=Complete [✗]=Failed\n\n"
    )
    sys.stdout.flush()


def display_radio_status(radio_data):
    """
    Display formatted radio status information.

    Args:
        radio_data (dict): Radio status data
    """
    if not radio_data:
        print("No radio data available to display")
        return

    print("\n" + "=" * 50)
    print(f"Radio Status: {radio_data.get('serialNumber', 'Unknown')}")
    print("=" * 50)

    # Basic radio information
    status_items = [
        ("Serial Number", "serialNumber"),
        ("Hostname", "hostName"),
        ("Online", "connected"),
        ("Latitude", "latitude"),
        ("Longitude", "longitude"),
        ("Primary BN", "primaryBn"),
        ("Connected BN", "connectedBn"),
        ("BN Match", lambda d: "✓" if d.get('primaryBn') == d.get('connectedBn') else "✗"),
        ("AGL Height", "heightAgl"),
        ("Antenna Azimuth", "antennaAzimuth"),
        ("Antenna Tilt", "tilt"),
        ("CPI ID", "cpiId"),
        ("Firmware", "softwareVersion"),
        ("Hardware", "partNumber"),
        ("Carrier Mode", "multiCarrierModeRn"),
    ]

    for label, key in status_items:
        if callable(key):
            value = key(radio_data)
        else:
            value = radio_data.get(key, "N/A")
            if isinstance(value, bool):
                value = "✓" if value else "✗"
        print(f"{label}: {value}")

    # Display error if present
    error = radio_data.get('error')
    if error:
        print(f"\nError: {error}")

    # Display carrier frequencies if available
    carriers = radio_data.get('carriers', {})
    if carriers:
        print("\nCarrier Frequencies:")
        for carrier_id, carrier in carriers.items():
            freq = carrier.get('frequency', 0) / 1000  # Convert to MHz
            bw = carrier.get('bandwidth', 0)
            print(f"  Carrier {carrier_id}: {freq} MHz / {bw} MHz")

    # Display hierarchy information
    hierarchy = radio_data.get('hierarchy', {})
    if hierarchy:
        print("\nHierarchy:")
        for level in ['site', 'sector', 'cell', 'market', 'region', 'operator']:
            if level in hierarchy:
                info = hierarchy[level]
                print(f"  {level.title()}: {info.get('name', 'N/A')} (ID: {info.get('id', 'N/A')})")


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
    customer_lat = customer_info.get("latitude")
    customer_lon = customer_info.get("longitude")

    # If customer coordinates are missing, cannot proceed
    if not customer_lat or not customer_lon:
        print(f"Customer coordinates are missing for radio {serial_number}")
        return False

    bn_lat = float(bn_data.get("latitude", 0))
    bn_lon = float(bn_data.get("longitude", 0))
    azimuth = calculate_azimuth(
        float(customer_lat), float(customer_lon), bn_lat, bn_lon
    )

    # Step 5: Format hostname based on customer name and ID
    customer_name = customer_info.get("name", "").strip()
    customer_id = customer_info.get("id", "")

    # Create hostname from full customer name and ID
    if customer_name:
        # Sanitize the customer name:
        # 1. Keep spaces as is
        # 2. Remove invalid hostname characters (like slashes)
        # 3. Truncate if too long to avoid hostname length limits
        sanitized_name = customer_name.upper()
        sanitized_name = sanitized_name.replace("/", " ")  # Replace slashes with spaces
        # Keep spaces as is, only remove other invalid characters
        sanitized_name = "".join(
            c for c in sanitized_name if c.isalnum() or c.isspace()
        )

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
    primary_bn = bn_data.get("serialNumber", "")

    # Step 7: Apply deployment configuration
    print(
        f"\nApplying deployment configuration for customer: {customer_name} (ID: {customer_id})"
    )

    # Configure with customer-specific data
    from ezSync.api import apply_deploy_config

    success = apply_deploy_config(
        serial_number=serial_number,
        hostname=hostname,
        customer_lat=float(customer_lat),
        customer_lon=float(customer_lon),
        azimuth=azimuth,
        primary_bn=primary_bn,
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
    print(
        f"Running mock speed tests for radio {serial_number} (will take {total_time:.1f} seconds)..."
    )

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
        "success": [],
        "failure": [],
        "in_progress": set(serial_numbers),
        "completed": 0,
        "total": len(serial_numbers),
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
                if serial_number in results["in_progress"]:
                    results["in_progress"].remove(serial_number)
                    results["completed"] += 1
                    if success:
                        results["success"].append(serial_number)
                        thread_safe_print("Mock test SUCCESSFUL", serial=serial_number)
                    else:
                        results["failure"].append(serial_number)
                        thread_safe_print("Mock test FAILED", serial=serial_number)

            return success

        except Exception as e:
            with print_lock:
                if serial_number in results["in_progress"]:
                    results["in_progress"].remove(serial_number)
                    results["completed"] += 1
                    results["failure"].append(serial_number)
                    thread_safe_print(
                        f"ERROR: {str(e)}", serial=serial_number
                    )

            return False

    # Print initial status
    print(
        f"Starting parallel mock testing of {len(serial_numbers)} radios with {max_workers} workers"
    )

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
            return_when=concurrent.futures.ALL_COMPLETED,
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
    print("\n\n" + "=" * 20 + " TEST SUMMARY " + "=" * 20)
    print(f"Successfully tested: {len(results['success'])}")
    print(f"Failed tests: {len(results['failure'])}")
    print(f"Total attempted: {len(serial_numbers)}")

    # Print successful radios
    if results["success"]:
        print("\nSuccessfully tested radios:")
        for serial in results["success"]:
            print(f"  - {serial}")

    # Print failed radios
    if results["failure"]:
        print("\nFailed test for radios:")
        for serial in results["failure"]:
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
        print("\n" + "=" * 20 + " TEST SUMMARY " + "=" * 20)
        print(f"Method tested: {method}")
        print(f"Successfully tested: {len(results_data['success'])}")
        print(f"Failed tests: {len(results_data['failure'])}")
        print(f"Total attempted: {len(serial_numbers)}")

        # Print successful radios
        if results_data["success"]:
            print("\nSuccessfully tested radios:")
            for serial in results_data["success"]:
                print(f"  - {serial}")

        # Print failed radios
        if results_data["failure"]:
            print("\nFailed test for radios:")
            for serial in results_data["failure"]:
                print(f"  - {serial}")

        print("\nTest process complete.")
        print("=" * 50)

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
                            "success": [],
                            "failure": [],
                            "completed": len(results_list),
                            "total": len(serial_numbers),
                        }

                        for sn, success in results_list:
                            if success:
                                results["success"].append(sn)
                                thread_safe_print(f"Test SUCCESSFUL", serial=sn)
                            else:
                                results["failure"].append(sn)
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
                        write_pipe_file = os.fdopen(write_pipe, "w")

                        # Results tracking
                        results = {
                            "success": [],
                            "failure": [],
                            "in_progress": set(serial_numbers),
                            "completed": 0,
                            "total": len(serial_numbers),
                        }

                        # Function to handle worker tests
                        def worker_test(serial_number):
                            """Worker function that tests a single radio"""
                            thread_name = current_thread().name
                            thread_safe_print(
                                f"Starting test in worker process", serial=serial_number
                            )

                            try:
                                # Call the mock test function
                                success = mock_test_radio(serial_number)

                                # Record result
                                with print_lock:
                                    if serial_number in results["in_progress"]:
                                        results["in_progress"].remove(serial_number)
                                        results["completed"] += 1
                                        if success:
                                            results["success"].append(serial_number)
                                            thread_safe_print(
                                                "Test SUCCESSFUL", serial=serial_number
                                            )
                                        else:
                                            results["failure"].append(serial_number)
                                            thread_safe_print(
                                                "Test FAILED", serial=serial_number
                                            )

                                return success

                            except Exception as e:
                                with print_lock:
                                    if serial_number in results["in_progress"]:
                                        results["in_progress"].remove(serial_number)
                                        results["completed"] += 1
                                        results["failure"].append(serial_number)
                                        thread_safe_print(
                                            f"ERROR: {str(e)}", serial=serial_number
                                        )

                                return False

                        # Run all tasks in threads
                        with concurrent.futures.ThreadPoolExecutor(
                            max_workers=max_workers
                        ) as executor:
                            futures = [
                                executor.submit(worker_test, sn)
                                for sn in serial_numbers
                            ]

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
                        time.sleep(
                            60
                        )  # This should never complete - watchdog will kill us
                        print("[WORKER] Timeout waiting for termination!")
                        os._exit(1)  # Just in case the watchdog fails
                    else:
                        # This is the watchdog process
                        # Close the write end of the pipe
                        os.close(write_pipe)
                        read_pipe_file = os.fdopen(read_pipe, "r")

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
                    script_path = os.path.join(
                        os.path.dirname(__file__), "worker_script.py"
                    )
                    if not os.path.exists(script_path):
                        info(f"Worker script not found at {script_path}")
                        # Create a simple version directly in memory
                        script_path = os.path.join(
                            os.path.dirname(__file__), "temp_worker.py"
                        )
                        with open(script_path, "w") as f:
                            f.write(
                                """#!/usr/bin/env python
import sys, json, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ezSync.operations import mock_test_radio
result = mock_test_radio(sys.argv[1])
print(f"RESULT: {{'serial_number': sys.argv[1], 'success': {result}}}")
sys.exit(0 if result else 1)
"""
                            )
                        os.chmod(script_path, 0o755)

                    # Results tracking
                    results = {
                        "success": [],
                        "failure": [],
                        "completed": 0,
                        "total": len(serial_numbers),
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
                                timeout=METHOD_TIMEOUT / len(serial_numbers),
                                capture_output=True,
                                text=True,
                            )

                            # Check if process succeeded
                            success = proc.returncode == 0

                            # Try to parse result from output
                            try:
                                # Look for the result line
                                result_line = None
                                for line in proc.stdout.splitlines():
                                    if line.startswith("RESULT:"):
                                        result_line = line[
                                            7:
                                        ].strip()  # Remove "RESULT: " prefix
                                        break

                                if result_line:
                                    result = json.loads(result_line)
                                    success = result.get("success", success)
                            except (json.JSONDecodeError, ValueError):
                                # Fall back to return code if parsing fails
                                pass

                            # Record result
                            results["completed"] += 1
                            if success:
                                results["success"].append(sn)
                                thread_safe_print(
                                    "External process SUCCESSFUL", serial=sn
                                )
                            else:
                                results["failure"].append(sn)
                                thread_safe_print("External process FAILED", serial=sn)

                        except subprocess.TimeoutExpired:
                            results["completed"] += 1
                            results["failure"].append(sn)
                            thread_safe_print("External process TIMED OUT", serial=sn)
                        except Exception as e:
                            results["completed"] += 1
                            results["failure"].append(sn)
                            thread_safe_print(
                                f"External process ERROR: {str(e)}", serial=sn
                            )

                    # Print summary
                    print_summary(results)
                    print(f"[COMPLETE] Method {method} finished successfully")

                    # Clean up temporary worker script if created
                    if script_path.endswith("temp_worker.py"):
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
            print(
                f"[TIMEOUT] Will terminate at exactly {time.strftime('%H:%M:%S', time.localtime(exact_timeout))}"
            )

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
                        print(
                            f"[TIMEOUT] Method {method} completed in {elapsed:.1f} seconds"
                        )
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
                print(
                    f"[TIMEOUT] Method {method} exceeded {METHOD_TIMEOUT} seconds limit"
                )

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
                                print(
                                    f"[TIMEOUT] Child process terminated after SIGTERM"
                                )
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
                        print(
                            f"[TIMEOUT] Child process still running after SIGTERM, sending SIGKILL"
                        )
                        os.kill(child_pid, signal.SIGKILL)
                        os.waitpid(child_pid, 0)  # Clean up zombie
                        print(
                            f"[TIMEOUT] Child process forcibly terminated with SIGKILL"
                        )
                    except OSError:
                        # Process might already be gone
                        pass

                print(f"[TIMEOUT] Method {method} failed due to timeout")
                print(f"{'='*20} METHOD {method} FAILED {'='*20}")

                # Print timeout summary
                print("\n" + "=" * 20 + " TIMEOUT SUMMARY " + "=" * 20)
                print(
                    f"Method {method} did not complete within {METHOD_TIMEOUT} seconds"
                )
                print(f"This indicates that Method {method} cannot exit properly")
                print("=" * 50)

    # Final recommendation
    print("\n" + "=" * 20 + " RECOMMENDATION " + "=" * 20)
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

    return {"success": True}
