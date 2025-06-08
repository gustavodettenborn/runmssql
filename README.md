# MSSQL Client Application

A Docker-based MSSQL client application designed to connect to legacy SQL Server instances, execute SQL queries, and export results to CSV files. This application handles SSL/TLS compatibility issues with older SQL Server versions and provides multiple connection options.

## Features

- **Multiple Connection Methods**: Supports PyODBC (ODBC Driver 17/18) and PyMSSQL (FreeTDS)
- **Legacy SQL Server Support**: Configured for compatibility with older SQL Server versions
- **SSL/TLS Compatibility**: Handles legacy encryption requirements
- **CSV Export**: Execute queries and export results directly to CSV files
- **Docker Support**: Containerized deployment for consistent environments
- **Fallback Options**: Automatic fallback between different connection methods

## Target Environment

- **SQL Server**: 172.20.2.98:1433 (legacy instance)
- **SSL/TLS**: Legacy compatibility with reduced security levels
- **Authentication**: SQL Server authentication

## Quick Start

### Docker Method (Recommended)

```bash
# Build and run
docker-compose up --build -d

# Execute application
docker-compose exec mssql-client python3 run_sql_csv.py

# Test connection
docker-compose exec mssql-client python3 test_connection.py

# Clean up
docker-compose down
```

## Installation Options

### Option 1: Docker (Recommended)

1. **Build the Docker image:**
   ```bash
   docker-compose build
   ```

2. **Run the container:**
   ```bash
   docker-compose up -d
   ```

3. **Connect to the container:**
   ```bash
   docker exec -it mssql-client bash
   ```

4. **Test the installation:**
   ```bash
   python test_connection.py
   ```

### Option 2: Manual Installation (Ubuntu 20.04)

If Docker is not available or having issues, use the manual installation script:

1. **Run the installation script:**
   ```bash
   chmod +x install_mssql_client.sh
   sudo ./install_mssql_client.sh
   ```

2. **Activate the virtual environment:**
   ```bash
   source ~/mssql-venv/bin/activate
   ```

3. **Set environment variables:**
   ```bash
   export OPENSSL_CONF=/etc/ssl/openssl_legacy.cnf
   export ODBCSYSINI=/etc
   export ODBCINI=/etc/odbc.ini
   export ODBCINSTINI=/etc/odbcinst.ini
   export FREETDSCONF=/etc/freetds/freetds.conf
   export TDSVER=7.4
   ```

4. **Test the installation:**
   ```bash
   python test_connection.py
   ```

## Usage

### Environment Variables

Set the following environment variables for database connection:

```bash
export SQL_SERVER_HOST=172.20.2.98
export SQL_SERVER_PORT=1433
export SQL_SERVER_USER=your_username
export SQL_SERVER_PASSWORD=your_password
export SQL_SERVER_DATABASE=your_database
```

### Running SQL Queries

1. **Interactive mode:**
   ```bash
   python run_sql_csv.py
   ```

2. **Command line with query file:**
   ```bash
   python run_sql_csv.py --query-file queries.sql --output results.csv
   ```

3. **Direct query:**
   ```bash
   python run_sql_csv.py --query "SELECT * FROM your_table" --output results.csv
   ```

## Connection Methods

The application attempts multiple connection methods in order:

1. **ODBC Driver 17** (Primary - best legacy compatibility)
2. **ODBC Driver 18** (Secondary - with legacy SSL settings)
3. **FreeTDS/PyMSSQL** (Fallback - if PyMSSQL is available)

## Troubleshooting

### Testing Connection

Run the test script to verify all components:

```bash
python test_connection.py
```

This will check:
- Package imports (pandas, pyodbc, pymssql)
- Available ODBC drivers
- Connection string formats
- SSL configuration
- FreeTDS configuration

### Common Issues

1. **SSL/TLS Errors:**
   - The application is configured with `SECLEVEL=0` for maximum legacy compatibility
   - Encryption is disabled by default (`Encrypt=no`)
   - Trust server certificate is enabled (`TrustServerCertificate=yes`)

2. **PyMSSQL Installation Fails:**
   - The Dockerfile includes fallback strategies for PyMSSQL installation
   - The application works with PyODBC only if PyMSSQL fails to install
   - Use manual installation for better control over dependencies

3. **ODBC Driver Not Found:**
   - Check available drivers: `odbcinst -q -d`
   - Verify library paths: `echo $LD_LIBRARY_PATH`
   - Check driver files exist in `/opt/microsoft/msodbcsql*/lib64/`

4. **Connection Timeout:**
   - Legacy SQL Servers may require longer timeouts
   - Verify network connectivity: `telnet 172.20.2.98 1433`
   - Check firewall settings

## File Structure

```
runmssql/
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile                  # Main Docker image definition
├── requirements.txt            # Python dependencies
├── mssql.sh                   # ODBC Driver 18 installation script
├── run_sql_csv.py             # Main application
├── test_connection.py         # Connection testing script
├── install_mssql_client.sh    # Manual installation script
├── README.md                  # This file
└── results/                   # Output directory for CSV files
```

## Security Considerations

⚠️ **Important**: This application is configured for legacy SQL Server compatibility with reduced security levels:

- SSL/TLS encryption is disabled by default
- Security level is set to 0 (allows weak ciphers)
- Server certificate validation is disabled

These settings are necessary for connecting to legacy SQL Server instances but should not be used in production environments with sensitive data.
