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
