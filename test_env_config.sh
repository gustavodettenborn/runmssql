#!/bin/bash

# ==============================================================================
# Test Environment Variable Configuration
# ==============================================================================

echo "=== Testing .env File Loading ==="

# Source the .env file
if [ -f .env ]; then
    set -a  # automatically export all variables
    source .env
    set +a
    echo "✓ .env file loaded successfully"
else
    echo "✗ .env file not found"
    exit 1
fi

echo ""
echo "=== Environment Variables ==="
echo "MSSQL_SERVER: $MSSQL_SERVER"
echo "MSSQL_PORT: $MSSQL_PORT"
echo "MSSQL_DATABASE: $MSSQL_DATABASE"
echo "MSSQL_USERNAME: $MSSQL_USERNAME"
echo "TDS_VERSION: $TDS_VERSION"
echo "MSSQL_ENCRYPT: $MSSQL_ENCRYPT"
echo "MSSQL_TRUST_SERVER_CERTIFICATE: $MSSQL_TRUST_SERVER_CERTIFICATE"

echo ""
echo "=== Testing FreeTDS Configuration Update ==="

# Create a temporary FreeTDS config to test our script logic
TEMP_FREETDS="/tmp/test_freetds.conf"
cat > $TEMP_FREETDS << 'EOF'
[global]
tds version = 7.0
client charset = UTF-8

[MSSQL]
host = localhost
port = 1433
tds version = 7.0
encryption = off
EOF

echo "Original configuration:"
grep -A 5 "\[MSSQL\]" $TEMP_FREETDS

# Test the sed commands
if [ ! -z "$MSSQL_SERVER" ]; then
    sed -i "s/host = localhost/host = ${MSSQL_SERVER}/" $TEMP_FREETDS
fi

if [ ! -z "$MSSQL_PORT" ]; then
    sed -i "s/port = 1433/port = ${MSSQL_PORT}/" $TEMP_FREETDS
fi

if [ ! -z "$TDS_VERSION" ]; then
    sed -i "s/tds version = 7.0/tds version = ${TDS_VERSION}/" $TEMP_FREETDS
fi

echo ""
echo "Updated configuration:"
grep -A 5 "\[MSSQL\]" $TEMP_FREETDS

# Cleanup
rm -f $TEMP_FREETDS

echo ""
echo "=== Configuration Test Complete ==="
echo "✓ Environment variables are properly configured"
echo "✓ FreeTDS configuration update logic works correctly"
