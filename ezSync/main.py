"""
Main entry point for the ezSync application.
This module provides the command-line interface for the application.
"""

import sys
import argparse

from ezSync.api import delete_radios, get_radio_status, apply_default_config
from ezSync.operations import (
    reset_radio, refurbish_radio, run_speed_tests,
    display_speed_test_results, display_radio_status,
    refurbish_radios_parallel, deploy_radio, 
    mock_test_radio, test_radios_parallel, find_fix_parallel
)
from ezSync.config import TARANA_API_KEY, setup_config

def main():
    """
    Main entry point for the ezSync application.
    Parses command-line arguments and dispatches to the appropriate operations.
    """
    parser = argparse.ArgumentParser(description='Tarana Radio Management Tool')
    parser.add_argument('--delete', action='store_true', help='Delete radios')
    parser.add_argument('--force', action='store_true', help='Force default config and reconnect before deletion (only with --delete)')
    parser.add_argument('--default', action='store_true', help='Apply default configuration to radio')
    parser.add_argument('--status', action='store_true', help='Get status information for a radio')
    parser.add_argument('--reclaim', action='store_true', help='Wait for radio to connect, then apply default config and reconnect')
    parser.add_argument('--refurb', action='store_true', help='Perform full refurbishment process on radio(s)')
    parser.add_argument('--speedtest', action='store_true', help='Run a speed test on the radio')
    parser.add_argument('--deploy', action='store_true', help='Configure radio for customer deployment using database information')
    parser.add_argument('--test', action='store_true', help='Run a mock test to verify parallel functionality')
    parser.add_argument('--findfix', action='store_true', help='Test multiple approaches to fix threading issues')
    parser.add_argument('--verbose', action='store_true', help='Show detailed debug information')
    parser.add_argument('--check-interval', type=int, default=20, help='Time in seconds between status checks (for --reclaim or --speedtest)')
    parser.add_argument('--max-attempts', type=int, default=30, help='Maximum number of status check attempts (for --reclaim or --speedtest)')
    parser.add_argument('--parallel', action='store_true', help='Process radios in parallel (for --refurb or --test)')
    parser.add_argument('--max-workers', type=int, default=5, help='Maximum number of concurrent workers for parallel processing')
    parser.add_argument('--setup', action='store_true', help='Run the setup wizard to configure API keys and database connection')
    parser.add_argument('serial_numbers', nargs='*', help='Serial number(s) of the radio(s)')
    args = parser.parse_args()
    
    # Run setup wizard if explicitly requested
    if args.setup:
        if setup_config():
            print("Setup completed successfully!")
            return
        else:
            print("Setup failed. Required configuration is missing.")
            sys.exit(1)
    
    # No need to check API key for help display
    if len(sys.argv) == 1 or (len(sys.argv) == 2 and sys.argv[1] in ['-h', '--help']):
        parser.print_help()
        return

    # Check if we have the minimum required configuration for API operations
    needs_api = args.status or args.delete or args.default or args.reclaim or args.refurb or args.speedtest or args.deploy
    
    if needs_api and not TARANA_API_KEY:
        print("Error: TARANA_API_KEY is not set or empty")
        print("\nUse one of the following options:")
        print("1. Run 'ezsync --setup' to configure your API key and save it to a .env file")
        print("2. Create a .env file in the current directory with TARANA_API_KEY=your_key_here")
        print("3. Set the TARANA_API_KEY environment variable manually")
        sys.exit(1)
    
    # Make sure serial numbers are provided when required
    if needs_api and not args.serial_numbers:
        print("Error: At least one serial number is required for this operation")
        sys.exit(1)
    
    # Handle refurbishment operation
    if args.refurb:
        if args.parallel:
            # Process radios in parallel
            print(f"Running refurbishment process in parallel with {args.max_workers} workers")
            results = refurbish_radios_parallel(args.serial_numbers, max_workers=args.max_workers)
            # The function handles its own output including the summary
        else:
            # Process radios sequentially (original behavior)
            success_count = 0
            failure_count = 0
            
            for serial_number in args.serial_numbers:
                print(f"\n{'='*20} REFURBISHING RADIO: {serial_number} {'='*20}")
                
                if refurbish_radio(serial_number):
                    success_count += 1
                else:
                    failure_count += 1
            
            print(f"\n{'='*20} REFURBISHMENT SUMMARY {'='*20}")
            print(f"Successfully refurbished: {success_count}")
            print(f"Failed to refurbish: {failure_count}")
            print(f"Total attempted: {len(args.serial_numbers)}")
        
        if failure_count > 0:
            sys.exit(1)
        return
    
    # Handle status check operation
    if args.status:
        if len(args.serial_numbers) != 1:
            print("Error: Exactly one serial number is required for status check")
            sys.exit(1)
            
        serial_number = args.serial_numbers[0]
        print(f"\nRetrieving status for radio with serial number: {serial_number}")
        status_data = get_radio_status(serial_number)
        
        if status_data:
            display_radio_status(status_data)
        else:
            print(f"Failed to retrieve status for radio {serial_number}")
            sys.exit(1)
        return
    
    # Handle speed test operation
    if args.speedtest:
        if len(args.serial_numbers) != 1:
            print("Error: Exactly one serial number is required for speed test")
            sys.exit(1)
            
        serial_number = args.serial_numbers[0]
        print(f"\nRunning speed test for radio with serial number: {serial_number}")
        
        # Run speed tests
        results = run_speed_tests(
            serial_number, 
            num_tests=3, 
            interval=60, 
            max_attempts=10
        )
        
        if not results:
            print("Failed to retrieve speed test results")
            sys.exit(1)
        return
    
    # Handle deletion operation
    if args.delete:
        if len(args.serial_numbers) < 1:
            print("Error: At least one serial number is required for deletion")
            sys.exit(1)
            
        # Handle forced default config and reconnection if requested
        if args.force:
            print(f"\nForce option enabled. Will apply default config and reconnect radios before deletion.")
            all_operations_success = True
            
            for serial_number in args.serial_numbers:
                # Apply reset process with RECLAIMED hostname
                if reset_radio(serial_number, hostname="RECLAIMED"):
                    print(f"Reset successful for {serial_number}")
                else:
                    print(f"Reset failed for {serial_number}")
                    all_operations_success = False
            
            if all_operations_success:
                print(f"All radios defaulted and reconnected successfully")
            else:
                print(f"Some operations failed")
            
        print(f"\nAttempting to delete radios with serial numbers: {', '.join(args.serial_numbers)}")
        if delete_radios(args.serial_numbers):
            print("Successfully deleted radios")
        else:
            print("Failed to delete radios")
            sys.exit(1)
        return
    
    # Validate that --force is only used with --delete
    if args.force and not args.delete:
        print("Error: The --force option can only be used with the --delete option")
        sys.exit(1)
    
    # Handle reclaim operation
    if args.reclaim:
        success_count = 0
        failure_count = 0
        
        for serial_number in args.serial_numbers:
            print(f"\n{'='*20} RECLAIMING RADIO: {serial_number} {'='*20}")
            
            # Apply reset process with RECLAIMED hostname
            if reset_radio(serial_number, hostname="RECLAIMED"):
                success_count += 1
            else:
                failure_count += 1
        
        print(f"\n{'='*20} RECLAIM SUMMARY {'='*20}")
        print(f"Successfully reclaimed: {success_count}")
        print(f"Failed to reclaim: {failure_count}")
        print(f"Total attempted: {len(args.serial_numbers)}")
        
        if failure_count > 0:
            sys.exit(1)
        return
    
    # Handle default configuration operation
    if args.default:
        if len(args.serial_numbers) != 1:
            print("Error: Exactly one serial number is required for default configuration")
            sys.exit(1)
            
        serial_number = args.serial_numbers[0]
        print(f"\nApplying default configuration to radio with serial number: {serial_number}")
        # Use serial number as hostname for --default switch
        if apply_default_config(serial_number):
            print("Successfully applied default configuration")
        else:
            print("Failed to apply default configuration")
            sys.exit(1)
        return
    
    # Handle deployment operation
    if args.deploy:
        success_count = 0
        failure_count = 0
        
        for serial_number in args.serial_numbers:
            print(f"\n{'='*20} DEPLOYING RADIO: {serial_number} {'='*20}")
            
            if deploy_radio(serial_number):
                success_count += 1
            else:
                failure_count += 1
        
        print(f"\n{'='*20} DEPLOYMENT SUMMARY {'='*20}")
        print(f"Successfully deployed: {success_count}")
        print(f"Failed to deploy: {failure_count}")
        print(f"Total attempted: {len(args.serial_numbers)}")
    
    # Handle mock test operation
    elif args.test:
        print(f"Running mock test operation for {len(args.serial_numbers)} serial numbers")
        
        if args.parallel:
            # Process tests in parallel
            print(f"Using parallel processing with {args.max_workers} workers")
            results = test_radios_parallel(args.serial_numbers, max_workers=args.max_workers)
        else:
            # Process tests sequentially
            success_count = 0
            failure_count = 0
            
            for serial_number in args.serial_numbers:
                print(f"\n{'='*20} MOCK TESTING RADIO: {serial_number} {'='*20}")
                
                if mock_test_radio(serial_number):
                    success_count += 1
                else:
                    failure_count += 1
            
            print(f"\n{'='*20} MOCK TEST SUMMARY {'='*20}")
            print(f"Successfully tested: {success_count}")
            print(f"Failed tests: {failure_count}")
            print(f"Total attempted: {len(args.serial_numbers)}")
            
    # Handle finding fix for threading issues
    elif args.findfix:
        print(f"Testing multiple approaches to fix threading issues with {len(args.serial_numbers)} radios")
        print(f"Using max {args.max_workers} workers for each approach")
        find_fix_parallel(args.serial_numbers, max_workers=args.max_workers)
    
    # Display usage if no options specified
    print("Usage for configuration: python -m ezSync.main <serial_number>")
    print("Usage for deletion: python -m ezSync.main --delete <serial_number1> [serial_number2 ...]")
    print("Usage for forced deletion: python -m ezSync.main --delete --force <serial_number1> [serial_number2 ...]")
    print("Usage for default config: python -m ezSync.main --default <serial_number>")
    print("Usage for reclaiming: python -m ezSync.main --reclaim <serial_number1> [serial_number2 ...]")
    print("Usage for refurbishing: python -m ezSync.main --refurb <serial_number1> [serial_number2 ...]")
    print("Usage for status check: python -m ezSync.main --status <serial_number>")
    print("Usage for speed test: python -m ezSync.main --speedtest <serial_number>")

if __name__ == '__main__':
    main()
