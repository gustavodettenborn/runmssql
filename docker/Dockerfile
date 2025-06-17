# ==============================================================================
# MSSQL Python Client Docker Image
# Ubuntu 24.04 LTS with PyODBC + PyMSSQL dual driver support
# ==============================================================================

FROM ubuntu:24.04 AS base

# Avoid prompts from apt during build
ENV DEBIAN_FRONTEND=noninteractive

# ==============================================================================
# SYSTEM DEPENDENCIES
# ==============================================================================

# Install core system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    software-properties-common \
    apt-transport-https \
    ca-certificates \
    lsb-release \
    wget \
    unixodbc \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    gcc \
    g++ \
    libssl3 \
    libssl-dev \
    openssl \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

FROM base AS base-mssql-client

# ==============================================================================
# MICROSOFT SQL SERVER ODBC DRIVERS
# ==============================================================================

# Install Microsoft ODBC Driver 17 (primary, best legacy compatibility)
# Try Ubuntu 24.04 repository first, fallback to 22.04 if needed
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main" > /etc/apt/sources.list.d/msprod.list && \
    apt-get update && \
    (ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 || \
     (echo "Ubuntu 24.04 repo failed, trying 22.04..." && \
      echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/msprod.list && \
      apt-get update && \
      ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17)) && \
    rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 (secondary option)
COPY scripts/setup/mssql.sh /tmp/mssql.sh
RUN chmod +x /tmp/mssql.sh && /tmp/mssql.sh && rm -f /tmp/mssql.sh

# ==============================================================================
# PYTHON ENVIRONMENT AND DEPENDENCIES
# ==============================================================================

# Copy requirements for better Docker layer caching
COPY ./requirements.txt /app/requirements.txt

# Create and configure Python virtual environment
RUN python3 -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip setuptools wheel && \
    /app/venv/bin/pip install -r /app/requirements.txt

# Install PyMSSQL with fallback strategies
RUN echo "Installing PyMSSQL with fallback options..." && \
    (/app/venv/bin/pip install "Cython<3.0" && \
     /app/venv/bin/pip install "pymssql==2.2.8") || \
    (/app/venv/bin/pip install --only-binary=all "pymssql>=2.1.0,<2.3.0") || \
    (/app/venv/bin/pip install --no-cache-dir --prefer-binary pymssql) || \
    (echo "Warning: PyMSSQL installation failed. Continuing with PyODBC only..." && \
     mkdir -p /app && echo "PYMSSQL_UNAVAILABLE=true" >> /app/.env) && \
    echo "Python package installation completed."

# ==============================================================================
# SSL/TLS LEGACY CONFIGURATION
# ==============================================================================

# Create comprehensive legacy SSL/TLS configuration for old SQL Server instances
RUN cat > /etc/ssl/openssl_legacy.cnf << 'EOF'
openssl_conf = openssl_init

[openssl_init]
providers = provider_sect
ssl_conf = ssl_sect
alg_section = algorithm_sect

[provider_sect]
default = default_sect
legacy = legacy_sect

[default_sect]
activate = 1

[legacy_sect]
activate = 1

[algorithm_sect]
default_properties = default_properties

[default_properties]
fips = no

[ssl_sect]
system_default = system_default_sect

[system_default_sect]
MinProtocol = None
MaxProtocol = None
Options = UnsafeLegacyRenegotiation,UnsafeLegacyServerConnect,LegacyServerConnect
CipherString = ALL:@SECLEVEL=0:!aNULL:!eNULL
Ciphersuites =
EOF

# ==============================================================================
# ODBC CONFIGURATION
# ==============================================================================

# Configure ODBC drivers (FreeTDS + Microsoft drivers)
RUN echo "Configuring ODBC drivers..." && \
    rm -f /etc/odbcinst.ini /etc/odbc.ini && \
    mkdir -p /etc /usr/local/etc && \
    # Configure FreeTDS driver for legacy SQL Server compatibility
    echo "[FreeTDS]" > /etc/odbcinst.ini && \
    echo "Description=FreeTDS SQL Server" >> /etc/odbcinst.ini && \
    echo "Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> /etc/odbcinst.ini && \
    echo "Setup=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> /etc/odbcinst.ini && \
    echo "Threading=1" >> /etc/odbcinst.ini && \
    echo "CPTimeout=5" >> /etc/odbcinst.ini && \
    echo "CPReuse=5" >> /etc/odbcinst.ini && \
    echo "" >> /etc/odbcinst.ini && \
    # Configure Microsoft ODBC Driver 17 if available
    DRIVER17_LIB=$(find /opt/microsoft/msodbcsql17 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    if [ -n "$DRIVER17_LIB" ]; then \
        echo "[ODBC Driver 17 for SQL Server]" >> /etc/odbcinst.ini && \
        echo "Description=Microsoft ODBC Driver 17 for SQL Server" >> /etc/odbcinst.ini && \
        echo "Driver=$DRIVER17_LIB" >> /etc/odbcinst.ini && \
        echo "Threading=1" >> /etc/odbcinst.ini && \
        echo "UsageCount=1" >> /etc/odbcinst.ini && \
        echo "" >> /etc/odbcinst.ini; \
    fi && \
    # Configure Microsoft ODBC Driver 18 if available
    DRIVER18_LIB=$(find /opt/microsoft/msodbcsql18 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    if [ -n "$DRIVER18_LIB" ]; then \
        echo "[ODBC Driver 18 for SQL Server]" >> /etc/odbcinst.ini && \
        echo "Description=Microsoft ODBC Driver 18 for SQL Server" >> /etc/odbcinst.ini && \
        echo "Driver=$DRIVER18_LIB" >> /etc/odbcinst.ini && \
        echo "Threading=1" >> /etc/odbcinst.ini && \
        echo "UsageCount=1" >> /etc/odbcinst.ini; \
    fi && \
    # Create ODBC data sources file
    touch /etc/odbc.ini && \
    # Verify ODBC configuration
    echo "Verifying ODBC configuration..." && \
    cat /etc/odbcinst.ini && \
    echo "ODBC drivers available:" && \
    (odbcinst -q -d 2>/dev/null || echo "odbcinst query failed, but config file created") && \
    # Configure library search paths
    echo "/opt/microsoft/msodbcsql17/lib64" > /etc/ld.so.conf.d/mssql.conf && \
    echo "/opt/microsoft/msodbcsql18/lib64" >> /etc/ld.so.conf.d/mssql.conf && \
    echo "/usr/lib/x86_64-linux-gnu/odbc" >> /etc/ld.so.conf.d/mssql.conf && \
    ldconfig

# ==============================================================================
# FREETDS CONFIGURATION
# ==============================================================================

# Configure FreeTDS for maximum legacy SQL Server compatibility
# Note: Server configuration will be set via environment variables at runtime
RUN cat > /etc/freetds/freetds.conf << 'EOF'
[global]
# Use TDS 7.0 for maximum compatibility with legacy SQL Server
tds version = 7.0
client charset = UTF-8
text size = 2147483647
encryption = off
encrypt = false
use ntlmv2 = no
enable_krb5 = no

# Default MSSQL configuration - will be overridden by environment variables
[MSSQL]
host = localhost
port = 1433
tds version = 7.0
encryption = off
encrypt = false
EOF

# ==============================================================================
# APPLICATION SETUP
# ==============================================================================

# Create runtime configuration script
RUN cat > /app/configure_runtime.sh << 'EOF'
#!/bin/bash

# Update FreeTDS configuration with environment variables
if [ ! -z "$MSSQL_SERVER" ]; then
    echo "Updating FreeTDS configuration with server: $MSSQL_SERVER"

    # Update the FreeTDS configuration file
    sed -i "s/host = localhost/host = ${MSSQL_SERVER}/" /etc/freetds/freetds.conf

    if [ ! -z "$MSSQL_PORT" ]; then
        sed -i "s/port = 1433/port = ${MSSQL_PORT}/" /etc/freetds/freetds.conf
    fi

    if [ ! -z "$TDS_VERSION" ]; then
        sed -i "s/tds version = 7.0/tds version = ${TDS_VERSION}/" /etc/freetds/freetds.conf
    fi

    echo "FreeTDS configuration updated:"
    grep -A 5 "\[MSSQL\]" /etc/freetds/freetds.conf
fi
EOF

RUN chmod +x /app/configure_runtime.sh

# Copy Python application
COPY src/main/run_sql_csv.py /app/src/main/run_sql_csv.py
COPY src/utils/load_env.py /app/src/utils/load_env.py
COPY tests/unit/test_connection.py /app/tests/unit/test_connection.py
COPY tests/integration/test_connection_secure.py /app/tests/integration/test_connection_secure.py
COPY scripts/testing/test_automated.sh /app/scripts/testing/test_automated.sh
RUN chmod +x /app/scripts/testing/test_automated.sh

# Create __init__.py files for Python modules
RUN mkdir -p /app/src /app/src/main /app/src/utils /app/tests /app/tests/unit /app/tests/integration && \
    touch /app/src/__init__.py /app/src/main/__init__.py /app/src/utils/__init__.py /app/tests/__init__.py /app/tests/unit/__init__.py /app/tests/integration/__init__.py

WORKDIR /app

# Configure environment variables
ENV PATH="/app/venv/bin:/opt/mssql-tools17/bin:/opt/mssql-tools18/bin:${PATH}" \
    VIRTUAL_ENV="/app/venv" \
    OPENSSL_CONF="/etc/ssl/openssl_legacy.cnf" \
    ODBCSYSINI="/etc" \
    ODBCINI="/etc/odbc.ini" \
    ODBCINSTINI="/etc/odbcinst.ini" \
    FREETDSCONF="/etc/freetds/freetds.conf" \
    TDSVER="7.0" \
    LD_LIBRARY_PATH="/opt/microsoft/msodbcsql17/lib64:/opt/microsoft/msodbcsql18/lib64:/usr/lib/x86_64-linux-gnu/odbc"

# ==============================================================================
# VALIDATION AND TESTING
# ==============================================================================

# Validate installation and configuration
RUN echo "=== INSTALLATION VALIDATION ===" && \
    echo "1. ODBC Configuration:" && \
    cat /etc/odbcinst.ini && \
    echo "" && \
    echo "2. Available ODBC Drivers via odbcinst:" && \
    (odbcinst -q -d 2>/dev/null || echo "odbcinst command failed") && \
    echo "" && \
    echo "3. Available ODBC Drivers via PyODBC:" && \
    /app/venv/bin/python3 -c "import pyodbc; drivers = pyodbc.drivers(); print('PyODBC detected drivers:'); [print(f'  - {d}') for d in drivers]; print(f'Total: {len(drivers)} drivers')" && \
    echo "" && \
    echo "4. FreeTDS Configuration:" && \
    head -10 /etc/freetds/freetds.conf && \
    echo "" && \
    echo "5. Python Environment Tests:" && \
    /app/venv/bin/python3 -c "import pyodbc; print('PyODBC version:', pyodbc.version)" && \
    (/app/venv/bin/python3 -c "import pymssql; print('PyMSSQL version:', pymssql.__version__)" || echo "PyMSSQL not available") && \
    echo "" && \
    echo "6. SSL Configuration Test:" && \
    OPENSSL_CONF="/etc/ssl/openssl_legacy.cnf" /app/venv/bin/python3 -c "import ssl; print('SSL module loaded, OpenSSL version:', ssl.OPENSSL_VERSION)" && \
    echo "" && \
    echo "7. Driver Library Dependencies:" && \
    (ldd /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so | grep -E "(not found|missing)" || echo "FreeTDS libraries OK") && \
    (ldd /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1 | grep -E "(not found|missing)" || echo "MSSQL 17 driver libraries OK") && \
    (ldd /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1 | grep -E "(not found|missing)" || echo "MSSQL 18 driver libraries OK") && \
    echo "" && \
    echo "=== VALIDATION COMPLETE ==="

# Create entrypoint script that configures runtime settings
RUN cat > /app/entrypoint.sh << 'EOF'
#!/bin/bash

# Run runtime configuration
/app/configure_runtime.sh

# Execute the command passed to the container
exec "$@"
EOF

RUN chmod +x /app/entrypoint.sh

# Set entrypoint and default command
ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["/bin/bash"]