"""
Utility functions for the ezSync application.
This module contains general-purpose utility functions.
"""

from math import atan2, degrees

def format_value(value, decimal_places=2):
    """Format a value to the specified number of decimal places, if it's a number."""
    if value is None:
        return "N/A"
    
    try:
        if isinstance(value, (int, float)):
            return f"{value:.{decimal_places}f}"
        return str(value)
    except:
        return str(value)

def calculate_azimuth(customer_lat, customer_lon, bn_lat, bn_lon):
    """
    Calculate the azimuth angle from customer location to BN.
    Returns angle in degrees from true north (0-360).
    
    Args:
        customer_lat (float): Customer latitude
        customer_lon (float): Customer longitude
        bn_lat (float): BN latitude
        bn_lon (float): BN longitude
        
    Returns:
        float: Azimuth angle in degrees
    """
    # Convert lat/lon difference into azimuth
    lat_diff = bn_lat - customer_lat
    lon_diff = bn_lon - customer_lon
    
    azimuth = degrees(atan2(lon_diff, lat_diff))
    
    # Convert to 0-360 range
    azimuth = (azimuth + 360) % 360
    
    return round(azimuth, 2)

def calculate_average_speed_test_results(results):
    """
    Calculate average values from multiple speed test results.
    
    Args:
        results (list): List of speed test result dictionaries
        
    Returns:
        dict: Dictionary with average values
    """
    if not results:
        return {}
    
    avg_results = {}
    numeric_fields = [
        'downlinkThroughput', 'uplinkThroughput', 'downlinkSnr', 
        'uplinkSnr', 'pathloss', 'latencyMillis', 'rfLinkDistance'
    ]
    
    for field in numeric_fields:
        values = [r.get(field) for r in results if r.get(field) is not None]
        if values:
            avg_results[field] = sum(values) / len(values)
    
    # Copy non-averaged fields from the last result
    last_result = results[-1]
    for key, value in last_result.items():
        if key not in avg_results:
            avg_results[key] = value
    
    return avg_results

def parse_speed_test_results(output_text):
    """
    Parse speed test results from the captured output text.
    
    Args:
        output_text (str): The captured stdout from a refurbishment run
        
    Returns:
        list: List of speed test result dictionaries
    """
    import re
    
    results = []
    test_data = {}
    in_test_section = False
    current_test = None
    
    # Look for the speed test results section
    lines = output_text.splitlines()
    for i, line in enumerate(lines):
        if "INDIVIDUAL TESTS" in line:
            in_test_section = True
            continue
            
        if in_test_section:
            # Look for test number at the beginning of a result line
            test_match = re.match(r'^\s*(\d+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)\s*\|\s*([\d\.]+)', line)
            if test_match:
                # Test number starts from 1
                test_num = int(test_match.group(1)) - 1
                
                # If we already have data for this test and we encounter a new test,
                # add the previous test to results
                if current_test is not None and current_test != test_num and test_data:
                    results.append(test_data)
                    test_data = {}
                
                current_test = test_num
                
                # Extract test data
                dl = float(test_match.group(2)) * 1000  # Convert back to kbps
                ul = float(test_match.group(3)) * 1000  # Convert back to kbps
                latency = float(test_match.group(4))
                dl_snr = float(test_match.group(5))
                ul_snr = float(test_match.group(6))
                path_loss = float(test_match.group(7))
                rf_dist = float(test_match.group(8))
                
                # Create a result dict similar to what the API returns
                test_data = {
                    'downlinkThroughput': dl,
                    'uplinkThroughput': ul,
                    'latencyMillis': latency,
                    'downlinkSnr': dl_snr,
                    'uplinkSnr': ul_snr,
                    'pathloss': path_loss,
                    'rfLinkDistance': rf_dist
                }
                
                # Add to results
                results.append(test_data)
            
            # Exit when we reach a line indicating the end of test results
            elif "REFURBISHMENT SUMMARY" in line:
                in_test_section = False
                break
    
    return results
