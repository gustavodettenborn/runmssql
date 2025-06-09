#!/bin/bash
set -e

echo "=== Installing Microsoft SQL Server ODBC Driver 18 ==="

# The Microsoft repository is already set up in the Dockerfile
# Just update package lists and install driver 18
echo "Updating package lists..."
apt-get update

echo "Installing MSSQL ODBC Driver 18..."
ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    mssql-tools18

echo "Cleaning up package cache..."
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

echo "=== MSSQL ODBC Driver 18 installation completed successfully ==="