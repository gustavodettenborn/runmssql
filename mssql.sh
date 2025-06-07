#!/bin/bash
set -e

# Verificação de versão otimizada
version=$(grep VERSION_ID /etc/os-release | cut -d '"' -f 2)
if ! [[ "20.04 22.04 24.04 24.10" == *"$version"* ]]; then
    echo "Ubuntu $version is not currently supported."
    exit 1
fi

# Instalar Microsoft repo e drivers em uma única operação
curl -sSL -O https://packages.microsoft.com/config/ubuntu/$version/packages-microsoft-prod.deb && \
dpkg -i packages-microsoft-prod.deb && \
rm packages-microsoft-prod.deb && \
apt-get update && \
ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql18 \
    mssql-tools18 \
    unixodbc-dev && \
apt-get autoremove -y && \
apt-get clean && \
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*