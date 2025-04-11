# ezSync - Tarana Radio Management Tool

ezSync is a command-line tool for managing Tarana radios, providing functionality for configuration, testing, and management.

## Features

- **Radio Status**: Check the status of Tarana radios
- **Configuration**: Apply default configurations to radios
- **Speed Testing**: Run speed tests on radios and view results
- **Radio Reclaiming**: Reset and reclaim radios
- **Radio Refurbishment**: Complete workflow for refurbishing radios
- **Radio Deletion**: Remove radios from the system

## Installation

```bash
# Install from the local directory
pip install -e .

# Or install directly from Git
pip install git+https://github.com/your-org/ezSync.git
```

## Configuration

Create a `.env` file in your project directory with the following variables:

```
# API Configuration
TARANA_API_KEY=your_api_key_here
CPI_ID=your_cpi_id_here

# Database Configuration
DB_HOST=your_db_host
DB_NAME=your_db_name
DB_USER=your_db_username
DB_PASSWORD=your_db_password
DB_PORT=1433
```

## Usage

### Basic Commands

```bash
# Check radio status
ezsync --status <serial_number>

# Apply default configuration
ezsync --default <serial_number>

# Run speed tests
ezsync --speedtest <serial_number>

# Delete radios
ezsync --delete <serial_number1> [serial_number2 ...]

# Delete radios with configuration reset
ezsync --delete --force <serial_number1> [serial_number2 ...]

# Reclaim radios
ezsync --reclaim <serial_number1> [serial_number2 ...]

# Refurbish radios
ezsync --refurb <serial_number1> [serial_number2 ...]

# Configure radio for customer deployment using database information
ezsync --deploy <serial_number>
```

### Advanced Options

```bash
# Configure polling interval and max attempts
ezsync --speedtest --check-interval 30 --max-attempts 20 <serial_number>

# Enable verbose output 
ezsync --speedtest --verbose <serial_number>

# Refurbish multiple radios in parallel
ezsync --refurb --parallel --max-workers 10 <serial_number1> <serial_number2> <serial_number3> ...
```

### Parallel Processing

The refurbishment process supports parallel operation, which allows multiple radios to be processed simultaneously:

```bash
# Process 3 radios with default 5 parallel workers
ezsync --refurb --parallel S150F2225218459 S150F2225218460 S150F2225218461

# Process multiple radios with 10 concurrent workers
ezsync --refurb --parallel --max-workers 10 S150F2225218459 S150F2225218460 S150F2225218461
```

This feature significantly reduces total processing time when working with multiple radios by:
- Running operations concurrently with multiple worker threads
- Providing real-time status updates for each radio
- Generating a comprehensive summary of successes and failures

## Modular Design

The ezSync package is organized into several modules:

- **api.py**: Handles all API interactions with Tarana endpoints
- **database.py**: Manages database connections and queries
- **operations.py**: Implements business logic for radio operations
- **utils.py**: Contains utility functions for formatting and calculations
- **main.py**: Provides the command-line interface

## License

Copyright (c) 2023 EZWave 