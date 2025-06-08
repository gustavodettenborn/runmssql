#!/bin/bash

# install_mssql_client.sh - Manual installation script for MSSQL client
# This script can be used when Docker is not available or having issues

set -e

echo "=== MSSQL Client Manual Installation Script ==="
echo "This script will install MSSQL client tools and dependencies on Ubuntu 20.04"

# Check if running on Ubuntu
if ! grep -q "Ubuntu 20.04" /etc/os-release 2>/dev/null; then
    echo "Warning: This script is designed for Ubuntu 20.04. Proceeding anyway..."
fi

# Update package list
echo "1. Updating package lists..."
sudo apt-get update

# Install system dependencies
echo "2. Installing system dependencies..."
sudo apt-get install -y \
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
    libffi-dev

# Install Microsoft ODBC drivers
echo "3. Installing Microsoft ODBC drivers..."
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list | sudo tee /etc/apt/sources.list.d/msprod.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql17 msodbcsql18

# Create Python virtual environment
echo "4. Creating Python virtual environment..."
python3 -m venv ~/mssql-venv
source ~/mssql-venv/bin/activate

# Install Python packages
echo "5. Installing Python packages..."
pip install --upgrade pip setuptools wheel
pip install pandas pyodbc

# Try to install pymssql
echo "6. Attempting to install pymssql..."
if pip install "Cython<3.0" && pip install "pymssql==2.2.8"; then
    echo "✓ pymssql installed successfully"
elif pip install --only-binary=all "pymssql>=2.1.0,<2.3.0"; then
    echo "✓ pymssql installed with binary package"
elif pip install --no-cache-dir --prefer-binary pymssql; then
    echo "✓ pymssql installed with fallback method"
else
    echo "⚠️ pymssql installation failed. Continuing with pyodbc only..."
fi

# Configure ODBC
echo "7. Configuring ODBC drivers..."
sudo bash -c 'cat > /etc/odbcinst.ini << EOF
[FreeTDS]
Description=FreeTDS SQL Server
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so
Setup=/usr/lib/x86_64-linux-gnu/odbc/libtdsS.so
CPTimeout=
CPReuse=

[ODBC Driver 17 for SQL Server]
Description=Microsoft ODBC Driver 17 for SQL Server
Driver=/opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.10.so.2.1
UsageCount=1

[ODBC Driver 18 for SQL Server]
Description=Microsoft ODBC Driver 18 for SQL Server
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1
UsageCount=1
EOF'

# Configure FreeTDS
echo "8. Configuring FreeTDS..."
sudo bash -c 'cat > /etc/freetds/freetds.conf << EOF
[global]
tds version = 7.4
client charset = UTF-8
text size = 2147483647
encryption = off

[MSSQL]
host = ${MSSQL_SERVER:-ipdatabase}
port = 1433
tds version = 7.4
encryption = off
EOF'

# Configure OpenSSL for legacy compatibility
echo "9. Configuring OpenSSL for legacy compatibility..."
sudo bash -c 'cat > /etc/ssl/openssl_legacy.cnf << EOF
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
EOF'

# Update library paths
echo "10. Updating library paths..."
sudo bash -c 'echo "/opt/microsoft/msodbcsql17/lib64" > /etc/ld.so.conf.d/mssql.conf'
sudo bash -c 'echo "/opt/microsoft/msodbcsql18/lib64" >> /etc/ld.so.conf.d/mssql.conf'
sudo bash -c 'echo "/usr/lib/x86_64-linux-gnu/odbc" >> /etc/ld.so.conf.d/mssql.conf'
sudo ldconfig

# Copy the application script
echo "11. Copying application script..."
cp run_sql_csv.py ~/mssql-client/

echo ""
echo "=== Installation completed! ==="
echo ""
echo "To use the MSSQL client:"
echo "1. Activate the virtual environment: source ~/mssql-venv/bin/activate"
echo "2. Set environment variables:"
echo "   export OPENSSL_CONF=/etc/ssl/openssl_legacy.cnf"
echo "   export ODBCSYSINI=/etc"
echo "   export ODBCINI=/etc/odbc.ini"
echo "   export ODBCINSTINI=/etc/odbcinst.ini"
echo "   export FREETDSCONF=/etc/freetds/freetds.conf"
echo "   export TDSVER=7.4"
echo "3. Run the application: python ~/mssql-client/run_sql_csv.py"
echo ""
echo "Test the installation:"
echo "python -c \"import pyodbc; print('PyODBC version:', pyodbc.version)\""
echo "odbcinst -q -d"
