#!/bin/bash
set -e

echo "=== Rebuilding Docker Container ==="
cd /home/gustavodettenborn/developer/python/runmssql

echo "1. Stopping existing containers..."
docker-compose down || true

echo "2. Pruning Docker system..."
docker system prune -f

echo "3. Building new image (no cache)..."
docker-compose build --no-cache --progress=plain

echo "4. Starting container..."
docker-compose up -d mssql-client

echo "5. Waiting for container to be ready..."
sleep 5

echo "6. Checking container status..."
docker-compose ps

echo "7. Testing ODBC configuration..."
docker-compose exec mssql-client /bin/bash -c "
echo '=== ODBC Configuration Test ==='
echo 'Environment variables:'
env | grep -E '(ODBC|OPENSSL)' | sort
echo ''
echo 'ODBC drivers from odbcinst:'
odbcinst -q -d
echo ''
echo 'PyODBC drivers:'
python3 -c 'import pyodbc; print(\"Available drivers:\", pyodbc.drivers())'
echo ''
echo 'PyODBC version:'
python3 -c 'import pyodbc; print(\"PyODBC version:\", pyodbc.version)'
echo ''
echo 'OpenSSL configuration:'
openssl version -a
echo ''
echo 'Testing connection (dry run):'
python3 run_sql_csv.py || echo 'Connection test completed (expected to fail without SQL)'
"

echo "=== Rebuild Complete ==="
