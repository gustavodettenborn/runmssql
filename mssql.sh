#!/bin/bash
set -e
set +u

echo "=== Installing Microsoft SQL Server ODBC Driver 18 ==="

# Verificação de versão otimizada
version=$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)
echo "Detected Ubuntu version: $version"

if ! [[ "20.04 22.04 24.04 24.10" == *"$version"* ]]; then
    echo "Ubuntu $version is not currently supported."
    exit 1
fi

echo "Ubuntu version $version is supported"

# Instalar Microsoft repo e drivers em uma única operação
echo "Downloading Microsoft packages configuration..."
curl -sSL -O https://packages.microsoft.com/config/ubuntu/$version/packages-microsoft-prod.deb

echo "Installing Microsoft packages configuration..."
dpkg -i packages-microsoft-prod.deb

echo "Cleaning up downloaded package..."
rm packages-microsoft-prod.deb

echo "Updating package lists..."
apt-get update

echo "Installing MSSQL ODBC drivers..."
ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    mssql-tools18 \
    unixodbc-dev

echo "Cleaning up package cache..."
apt-get autoremove -y
apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

echo "=== MSSQL ODBC Driver installation completed successfully ==="