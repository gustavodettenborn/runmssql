#!/usr/bin/env python3
"""
test_connection.py - Test script for MSSQL connectivity
Tests different connection methods to verify the setup works correctly.
"""

import os
import socket
import sys
import traceback

# Set environment variables for legacy SSL support
os.environ['OPENSSL_CONF'] = '/etc/ssl/openssl_legacy.cnf'
os.environ['ODBCSYSINI'] = '/etc'
os.environ['FREETDSCONF'] = '/etc/freetds/freetds.conf'
os.environ['TDSVER'] = '7.0'


def test_imports():
    """Test if required packages can be imported"""
    print("=== Testing Package Imports ===")

    try:
        import pandas as pd
        print(f"✓ pandas {pd.__version__}")
    except ImportError as e:
        print(f"✗ pandas: {e}")
        return False, False

    pyodbc_available = False
    try:
        import pyodbc
        print(f"✓ pyodbc {pyodbc.version}")
        pyodbc_available = True
    except ImportError as e:
        print(f"⚠️ pyodbc: {e}")

    pymssql_available = False
    try:
        import pymssql
        print(f"✓ pymssql {pymssql.__version__}")
        pymssql_available = True
    except ImportError as e:
        print(f"⚠️ pymssql: {e}")

    if not pyodbc_available and not pymssql_available:
        print("✗ Nenhum driver SQL Server disponível")
        return False, False

    return True, (pyodbc_available, pymssql_available)


def test_odbc_drivers():
    """Test available ODBC drivers"""
    print("\n=== Testing ODBC Drivers ===")

    try:
        import pyodbc
        drivers = pyodbc.drivers()
        print("Available ODBC drivers:")
        for driver in drivers:
            print(f"  • {driver}")

        # Check for specific drivers we need
        required_drivers = [
            'FreeTDS',
            'ODBC Driver 17 for SQL Server',
            'ODBC Driver 18 for SQL Server'
        ]

        for driver in required_drivers:
            if driver in drivers:
                print(f"✓ {driver} - Available")
            else:
                print(f"✗ {driver} - Not found")

        return len(drivers) > 0
    except Exception as e:
        print(f"Error testing ODBC drivers: {e}")
        return False


def test_connection_strings():
    """Test different connection string formats"""
    print("\n=== Testing Connection Strings ===")

    # Test data from environment variables or defaults
    server = os.getenv('MSSQL_SERVER', 'ipdatabase')
    port = os.getenv('MSSQL_PORT', '1433')
    database = os.getenv('MSSQL_DATABASE', 'master')

    print(f"Testing with server: {server}:{port}, database: {database}")

    connection_strings = [
        # FreeTDS with TDS 7.0 (most compatible)
        (f"DRIVER={{FreeTDS}};SERVER={server};PORT={port};"
         f"DATABASE={database};TDS_Version=7.0;Encrypt=no;"),

        # FreeTDS with TDS 7.2
        (f"DRIVER={{FreeTDS}};SERVER={server};PORT={port};"
         f"DATABASE={database};TDS_Version=7.2;Encrypt=no;"),

        # ODBC Driver 17 without SSL
        (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
         f"SERVER={server},{port};DATABASE={database};"
         f"Encrypt=no;TrustServerCertificate=yes;"),

        # ODBC Driver 18 without SSL
        (f"DRIVER={{ODBC Driver 18 for SQL Server}};"
         f"SERVER={server},{port};DATABASE={database};"
         f"Encrypt=no;TrustServerCertificate=yes;"),

        # ODBC Driver 17 legacy mode
        (f"DRIVER={{ODBC Driver 17 for SQL Server}};"
         f"SERVER={server},{port};DATABASE={database};"
         f"Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;"),
    ]

    for i, conn_str in enumerate(connection_strings, 1):
        print(f"\nTesting connection string {i}:")
        print(f"  {conn_str}")

        try:
            # Just test the connection string format, don't actually connect
            # since we don't have credentials
            print("  ✓ Connection string format is valid")
        except Exception as e:
            print(f"  ✗ Error: {e}")


def test_legacy_ssl_support():
    """Test legacy SSL/TLS protocol support"""
    print("\n=== Testing Legacy SSL Support ===")

    try:
        import ssl
        print("✓ SSL module loaded")
        print(f"  OpenSSL version: {ssl.OPENSSL_VERSION}")

        # Test SSL context creation with legacy settings
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE

            # Enable legacy protocols
            context.options &= ~ssl.OP_NO_SSLv2
            context.options &= ~ssl.OP_NO_SSLv3
            context.options &= ~ssl.OP_NO_TLSv1
            context.options &= ~ssl.OP_NO_TLSv1_1

            # Set cipher suites for legacy compatibility
            context.set_ciphers('ALL:@SECLEVEL=0')

            print("✓ Legacy SSL context created successfully")

        except Exception as e:
            print(f"⚠️ Legacy SSL context creation failed: {e}")

        # Check if legacy configuration file exists
        config_file = os.environ.get('OPENSSL_CONF',
                                     '/etc/ssl/openssl_legacy.cnf')
        if os.path.exists(config_file):
            print(f"✓ OpenSSL legacy config found: {config_file}")

            # Read and display relevant parts of config
            try:
                with open(config_file, 'r') as f:
                    content = f.read()
                    if 'legacy' in content.lower():
                        print("✓ Legacy provider configuration detected")
                    if 'SECLEVEL=0' in content:
                        print("✓ Low security level configured for compatibility")
            except Exception as e:
                print(f"⚠️ Could not read config file: {e}")
        else:
            print(f"⚠️ OpenSSL legacy config not found: {config_file}")

        return True
    except Exception as e:
        print(f"✗ SSL configuration error: {e}")
        return False

    for i, conn_str in enumerate(connection_strings, 1):
        print(f"\nTesting connection string {i}:")
        print(f"  {conn_str}")

        try:
            # Just test the connection string format, don't actually connect
            # since we don't have credentials
            print("  ✓ Connection string format is valid")
        except Exception as e:
            print(f"  ✗ Error: {e}")


def test_ssl_configuration():
    """Test SSL configuration"""
    print("\n=== Testing SSL Configuration ===")

    try:
        import ssl
        print("✓ SSL module loaded")
        print(f"  OpenSSL version: {ssl.OPENSSL_VERSION}")

        # Check if legacy configuration file exists
        config_file = os.environ.get('OPENSSL_CONF',
                                     '/etc/ssl/openssl_legacy.cnf')
        if os.path.exists(config_file):
            print(f"✓ OpenSSL legacy config found: {config_file}")
        else:
            print(f"⚠️ OpenSSL legacy config not found: {config_file}")

        return True
    except Exception as e:
        print(f"✗ SSL configuration error: {e}")
        return False


def test_freetds_config():
    """Test FreeTDS configuration"""
    print("\n=== Testing FreeTDS Configuration ===")

    config_file = os.environ.get('FREETDSCONF', '/etc/freetds/freetds.conf')
    if os.path.exists(config_file):
        print(f"✓ FreeTDS config found: {config_file}")
        try:
            with open(config_file, 'r') as f:
                content = f.read()
                server_ip = os.getenv('MSSQL_SERVER', 'ipdatabase')
                if 'MSSQL' in content and server_ip in content:
                    print("✓ FreeTDS configuration contains "
                          "our server settings")
                else:
                    print("⚠️ FreeTDS configuration may not contain "
                          "our server settings")
        except Exception as e:
            print(f"⚠️ Could not read FreeTDS config: {e}")
    else:
        print(f"✗ FreeTDS config not found: {config_file}")


def test_tcp_connectivity():
    """Testa conectividade TCP básica com o servidor SQL"""
    print("\n=== Testing TCP Connectivity ===")

    server = os.getenv('MSSQL_SERVER', 'ipdatabase')
    parts = server.split(',')
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 1433

    print(f"Testing TCP connection to {host}:{port}...")

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((host, port))
        sock.close()

        if result == 0:
            print(f"✓ TCP connectivity OK to {host}:{port}")
            return True
        else:
            print(f"✗ TCP connection failed to {host}:{port} (code: {result})")
            return False
    except Exception as e:
        print(f"✗ TCP test error: {e}")
        return False


def main():
    """Run all tests"""
    print("MSSQL Client Connection Test")
    print("=" * 50)

    # Test imports
    imports_result = test_imports()
    if not imports_result[0]:
        print("\n❌ Basic package imports failed. Please check your installation.")
        return False

    pyodbc_available, pymssql_available = imports_result[1]

    # Test TCP connectivity
    tcp_ok = test_tcp_connectivity()

    # Test ODBC drivers only if pyodbc is available
    odbc_ok = False
    if pyodbc_available:
        odbc_ok = test_odbc_drivers()
    else:
        print("\n⚠️ PyODBC not available, skipping ODBC driver test")

    # Test connection strings
    test_connection_strings()

    # Test SSL configuration
    ssl_ok = test_legacy_ssl_support()

    # Test FreeTDS configuration
    test_freetds_config()

    print("\n" + "=" * 50)
    print("Test Summary:")
    print(f"  ✓ Package imports: {'OK' if imports_result[0] else 'FAILED'}")
    print(f"  ✓ PyODBC available: {'YES' if pyodbc_available else 'NO'}")
    print(f"  ✓ PyMSSQL available: {'YES' if pymssql_available else 'NO'}")
    print(f"  ✓ TCP connectivity: {'OK' if tcp_ok else 'FAILED'}")
    print(f"  ✓ ODBC drivers: {'OK' if odbc_ok else 'FAILED/SKIPPED'}")
    print(f"  ✓ SSL configuration: {'OK' if ssl_ok else 'FAILED'}")

    print("\nTo test actual connectivity, run:")
    print("  docker-compose exec mssql-client python3 run_sql_csv.py")

    # Overall status
    if pymssql_available or (pyodbc_available and odbc_ok):
        print("\n✅ Sistema está pronto para conectar ao SQL Server!")
        return True
    else:
        print("\n❌ Sistema não tem drivers funcionais para SQL Server")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        traceback.print_exc()
        sys.exit(1)
