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
    mssql-tools18 || \
(echo "Failed to install from current repo, checking if fallback is needed..." && \
 if ! grep -q "22.04" /etc/apt/sources.list.d/msprod.list; then \
   echo "Switching to Ubuntu 22.04 repository for compatibility..." && \
   echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/ubuntu/22.04/prod jammy main" > /etc/apt/sources.list.d/msprod.list && \
   apt-get update && \
   ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 mssql-tools18; \
 else \
   echo "Repository fallback already attempted, installation failed."; \
   exit 1; \
 fi)

echo "Cleaning up package cache..."
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

echo "=== MSSQL ODBC Driver 18 installation completed successfully ==="