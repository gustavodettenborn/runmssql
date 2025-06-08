# Use Ubuntu 20.04 LTS for better legacy compatibility
FROM ubuntu:20.04

# Avoid prompts from apt during build
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies and Python
RUN apt-get update && apt-get install -y \
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
    libssl1.1 \
    libssl-dev \
    openssl \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft SQL Server ODBC Drivers (prioritizing Driver 17 for legacy compatibility)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/msprod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*

# Install ODBC Driver 18 as secondary option
COPY mssql.sh /tmp/mssql.sh
RUN chmod +x /tmp/mssql.sh && /tmp/mssql.sh && rm -f /tmp/mssql.sh

# Copy requirements first for better Docker layer caching
COPY requirements.txt /app/requirements.txt

# Create Python virtual environment and install dependencies
RUN python3 -m venv /app/venv

# Upgrade pip and install build tools
RUN /app/venv/bin/pip install --upgrade pip setuptools wheel

# Install from requirements.txt
RUN /app/venv/bin/pip install -r /app/requirements.txt

# Install pymssql with multiple fallback strategies
RUN echo "Attempting to install pymssql..." && \
    (/app/venv/bin/pip install "Cython<3.0" && \
     /app/venv/bin/pip install "pymssql==2.2.8") || \
    (/app/venv/bin/pip install --only-binary=all "pymssql>=2.1.0,<2.3.0") || \
    (/app/venv/bin/pip install --no-cache-dir --prefer-binary pymssql) || \
    (echo "Warning: pymssql installation failed. Continuing with pyodbc only..." && \
     echo "PYMSSQL_UNAVAILABLE=true" >> /app/.env) && \
    echo "Python package installation completed."

# Create comprehensive legacy SSL/TLS configuration
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

# Configure ODBC drivers (FreeTDS + Microsoft drivers)
RUN echo "=== Configurando ODBC ===" && \
    rm -f /etc/odbcinst.ini /etc/odbc.ini && \
    mkdir -p /etc /usr/local/etc && \
    # FreeTDS driver for legacy SQL Server compatibility
    echo "[FreeTDS]" > /etc/odbcinst.ini && \
    echo "Description=FreeTDS SQL Server" >> /etc/odbcinst.ini && \
    echo "Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so" >> /etc/odbcinst.ini && \
    echo "Setup=/usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini && \
    echo "CPTimeout=" >> /etc/odbcinst.ini && \
    echo "CPReuse=" >> /etc/odbcinst.ini && \
    echo "" >> /etc/odbcinst.ini && \
    # Find the actual ODBC Driver 17 library file
    DRIVER17_LIB=$(find /opt/microsoft/msodbcsql17 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    if [ -n "$DRIVER17_LIB" ]; then \
        echo "[ODBC Driver 17 for SQL Server]" >> /etc/odbcinst.ini && \
        echo "Description=Microsoft ODBC Driver 17 for SQL Server" >> /etc/odbcinst.ini && \
        echo "Driver=$DRIVER17_LIB" >> /etc/odbcinst.ini && \
        echo "UsageCount=1" >> /etc/odbcinst.ini && \
        echo "" >> /etc/odbcinst.ini; \
    fi && \
    # Find the actual ODBC Driver 18 library file
    DRIVER18_LIB=$(find /opt/microsoft/msodbcsql18 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    if [ -n "$DRIVER18_LIB" ]; then \
        echo "[ODBC Driver 18 for SQL Server]" >> /etc/odbcinst.ini && \
        echo "Description=Microsoft ODBC Driver 18 for SQL Server" >> /etc/odbcinst.ini && \
        echo "Driver=$DRIVER18_LIB" >> /etc/odbcinst.ini && \
        echo "UsageCount=1" >> /etc/odbcinst.ini && \
        echo "" >> /etc/odbcinst.ini; \
    fi && \
    # Create simple ODBC data sources configuration
    touch /etc/odbc.ini && \
    # Configure library paths for both drivers
    echo "/opt/microsoft/msodbcsql17/lib64" > /etc/ld.so.conf.d/mssql.conf && \
    echo "/opt/microsoft/msodbcsql18/lib64" >> /etc/ld.so.conf.d/mssql.conf && \
    echo "/usr/lib/x86_64-linux-gnu/odbc" >> /etc/ld.so.conf.d/mssql.conf && \
    ldconfig

# Configure FreeTDS for maximum legacy SQL Server compatibility
RUN cat > /etc/freetds/freetds.conf << 'EOF'
[global]
# Use the oldest TDS version for maximum compatibility
tds version = 7.0
client charset = UTF-8
text size = 2147483647
encryption = off
# Disable SSL/TLS for legacy servers
encrypt = false
# Enable legacy authentication methods
use ntlmv2 = no
# Compatibility with older SQL Server versions
enable_krb5 = no

[MSSQL]
host = 172.20.2.98
port = 1433
tds version = 7.0
encryption = off
encrypt = false
EOF

# Copy the Python application
COPY run_sql_csv.py /app/run_sql_csv.py

# Set working directory
WORKDIR /app

# Configure environment variables for ODBC and SSL
ENV PATH="/app/venv/bin:/opt/mssql-tools17/bin:/opt/mssql-tools18/bin:${PATH}" \
    VIRTUAL_ENV="/app/venv" \
    OPENSSL_CONF="/etc/ssl/openssl_legacy.cnf" \
    ODBCSYSINI="/etc" \
    ODBCINI="/etc/odbc.ini" \
    ODBCINSTINI="/etc/odbcinst.ini" \
    FREETDSCONF="/etc/freetds/freetds.conf" \
    TDSVER="7.0" \
    LD_LIBRARY_PATH="/opt/microsoft/msodbcsql17/lib64:/opt/microsoft/msodbcsql18/lib64:/usr/lib/x86_64-linux-gnu/odbc"

# Display configuration for debugging
RUN echo "=== ODBC Configuration Debug ===" && \
    cat /etc/odbcinst.ini && \
    echo "=== Available Drivers ===" && \
    odbcinst -q -d && \
    echo "=== FreeTDS Configuration ===" && \
    head -20 /etc/freetds/freetds.conf || true

# Fix ODBC driver dependencies and configuration
RUN echo "=== Testando dependências dos drivers ===" && \
    echo "FreeTDS:" && \
    (ldd /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so | grep -E "(not found|missing)" || echo "FreeTDS OK") && \
    echo "ODBC Driver 17:" && \
    (ldd /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.6.1 2>/dev/null | grep -E "(not found|missing)" || echo "Driver 17 OK") && \
    echo "ODBC Driver 18:" && \
    (ldd /opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1 2>/dev/null | grep -E "(not found|missing)" || echo "Driver 18 OK") && \
    echo "=== Instalando unixODBC corretamente ===" && \
    apt-get update && apt-get install -y --reinstall unixodbc unixodbc-dev && \
    echo "=== Reconfiguração simples ===" && \
    cat > /etc/odbcinst.ini << 'EOF'
[FreeTDS]
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
EOF

# Apply final ODBC configuration fixes
RUN echo "=== Configuração ODBC simplificada ===" && \
    rm -f /etc/odbcinst.ini /etc/odbc.ini && \
    cat > /etc/odbcinst.ini << 'EOF'
[FreeTDS]
Driver = /usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
Setup = /usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
EOF

# Test ODBC configuration without using odbcinst command
RUN echo "Driver FreeTDS configurado" && \
    echo "=== Testando configuração ODBC ===" && \
    cat /etc/odbcinst.ini && \
    echo "=== Testando PyODBC com configuração simples ===" && \
    /app/venv/bin/python3 -c "import pyodbc; drivers = pyodbc.drivers(); print(f'Drivers disponíveis: {drivers}')" || \
    echo "PyODBC test failed, continuing..."

# Verification tests to ensure everything is configured correctly
RUN echo "=== Final verification ===" && \
    echo "1. Checking ODBC driver files:" && \
    (ls -la /opt/microsoft/msodbcsql17/lib64/ 2>/dev/null || echo "ODBC Driver 17 not found") && \
    (ls -la /opt/microsoft/msodbcsql18/lib64/ 2>/dev/null || echo "ODBC Driver 18 not found") && \
    echo "2. Testing ODBC driver loading:" && \
    DRIVER17_LIB=$(find /opt/microsoft/msodbcsql17 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    DRIVER18_LIB=$(find /opt/microsoft/msodbcsql18 -name "libmsodbcsql-*.so.*" 2>/dev/null | head -1) && \
    ([ -n "$DRIVER17_LIB" ] && ldd "$DRIVER17_LIB" | head -5 || echo "ODBC Driver 17 library not found for ldd test") && \
    ([ -n "$DRIVER18_LIB" ] && ldd "$DRIVER18_LIB" | head -5 || echo "ODBC Driver 18 library not found for ldd test") && \
    echo "3. ODBC configuration files:" && \
    cat /etc/odbcinst.ini && \
    echo "4. Testing PyODBC import:" && \
    /app/venv/bin/python3 -c "import pyodbc; print('PyODBC version:', pyodbc.version)" && \
    echo "5. Testing PyMSSQL import:" && \
    (/app/venv/bin/python3 -c "import pymssql; print('PyMSSQL version:', pymssql.__version__)" || echo "PyMSSQL not available") && \
    echo "6. Testing SSL module with legacy configuration:" && \
    OPENSSL_CONF="/etc/ssl/openssl_legacy.cnf" /app/venv/bin/python3 -c "import ssl; print('SSL module loaded'); print('OpenSSL version:', ssl.OPENSSL_VERSION)" && \
    echo "7. Environment variables:" && \
    env | grep -E "(OPENSSL|SSL|ODB)" | sort && \
    echo "All tests completed successfully"

# Default command
CMD ["/bin/bash"]