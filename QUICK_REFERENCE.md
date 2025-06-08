# MSSQL Docker Client - Quick Reference

## 🚀 **Quick Start Commands**

### **Build & Run**
```bash
# Build the Docker image
docker build -t mssql-client:latest .

# Run with Docker Compose (recommended)
docker-compose up --build

# Run directly with environment file
docker run --rm --env-file .env mssql-client:latest

# Interactive shell
docker run -it --env-file .env mssql-client:latest bash
```

### **Testing Commands**
```bash
# Test environment configuration
./test_env_config.sh

# Test Python drivers
docker run --rm --env-file .env mssql-client:latest python3 -c "
import pyodbc, pymssql, os
print(f'Server: {os.getenv(\"MSSQL_SERVER\")}')
print(f'PyODBC: {pyodbc.version}')
print(f'PyMSSQL: {pymssql.__version__}')
"

# Test connection capabilities
docker run --rm --env-file .env mssql-client:latest python3 /app/test_connection.py
```

## ⚙️ **Configuration Quick Edit**

### **Change Server**
```bash
# Edit .env file
nano .env

# Update these lines:
MSSQL_SERVER=YOUR_SERVER_IP
MSSQL_PORT=1433
MSSQL_DATABASE=YOUR_DATABASE
MSSQL_USERNAME=YOUR_USERNAME
MSSQL_PASSWORD=YOUR_PASSWORD
```

### **Driver Preferences**
```bash
# Use PyODBC (recommended for legacy servers)
PREFERRED_DRIVER=pyodbc
FALLBACK_ENABLED=true

# Use PyMSSQL (faster for modern servers)
PREFERRED_DRIVER=pymssql
FALLBACK_ENABLED=true

# Disable fallback (single driver only)
FALLBACK_ENABLED=false
```

### **TDS Protocol Versions**
```bash
# Legacy SQL Server (2000, 2005)
TDS_VERSION=7.0

# Standard SQL Server (2008, 2012)
TDS_VERSION=7.2

# Modern SQL Server (2014+)
TDS_VERSION=7.4
```

## 🔧 **Troubleshooting**

### **Connection Issues**
```bash
# Test TCP connectivity
docker run --rm --env-file .env mssql-client:latest bash -c "
telnet \$MSSQL_SERVER \$MSSQL_PORT
"

# Check FreeTDS configuration
docker run --rm --env-file .env mssql-client:latest bash -c "
grep -A 10 '\[MSSQL\]' /etc/freetds/freetds.conf
"

# Verify ODBC drivers
docker run --rm --env-file .env mssql-client:latest odbcinst -q -d
```

### **SSL/TLS Issues**
```bash
# For legacy servers, ensure these settings:
MSSQL_ENCRYPT=false
MSSQL_TRUST_SERVER_CERTIFICATE=true
OPENSSL_LEGACY_MODE=true

# Test OpenSSL configuration
docker run --rm --env-file .env mssql-client:latest openssl version -a
```

## 📝 **Environment Variables Reference**

### **Essential Variables**
| Variable | Description | Example |
|----------|-------------|---------|
| `MSSQL_SERVER` | SQL Server hostname/IP | `ipdatabase` |
| `MSSQL_PORT` | SQL Server port | `1433` |
| `MSSQL_DATABASE` | Database name | `GESP` |
| `MSSQL_USERNAME` | Username | `consulta_athenas` |
| `MSSQL_PASSWORD` | Password | `your_password` |

### **Protocol Variables**
| Variable | Description | Default |
|----------|-------------|---------|
| `TDS_VERSION` | TDS protocol version | `7.0` |
| `MSSQL_ENCRYPT` | Enable encryption | `false` |
| `MSSQL_TRUST_SERVER_CERTIFICATE` | Trust server cert | `true` |
| `MSSQL_CONNECTION_TIMEOUT` | Connection timeout | `30` |

### **Advanced Variables**
| Variable | Description | Default |
|----------|-------------|---------|
| `PREFERRED_DRIVER` | Primary driver | `pyodbc` |
| `FALLBACK_ENABLED` | Enable fallback | `true` |
| `DEBUG_MODE` | Enable debugging | `false` |
| `VERBOSE_LOGGING` | Verbose output | `false` |

## 🐳 **Docker Commands**

### **Management**
```bash
# Remove all containers
docker-compose down

# Rebuild from scratch
docker-compose build --no-cache

# View logs
docker-compose logs mssql-client

# Clean up images
docker rmi mssql-client:latest
```

### **Development**
```bash
# Mount local directory for development
docker run -it --env-file .env \
  -v $(pwd):/app/workspace \
  mssql-client:latest bash

# Run specific Python script
docker run --rm --env-file .env \
  -v $(pwd)/scripts:/app/scripts \
  mssql-client:latest python3 /app/scripts/your_script.py
```

## 📁 **File Structure**
```
runmssql/
├── .env                     # Environment configuration
├── Dockerfile              # Multi-stage Docker build
├── docker-compose.yml      # Container orchestration
├── run_sql_csv.py          # Main application
├── test_connection.py      # Connection testing
├── test_env_config.sh      # Environment testing
├── requirements.txt        # Python dependencies
├── mssql.sh               # ODBC driver installation
└── IMPLEMENTATION_SUMMARY.md # Complete documentation
```

## 🔍 **Status Check Commands**

### **Validate Setup**
```bash
# Check if everything is working
docker run --rm --env-file .env mssql-client:latest bash -c "
echo '=== Environment Check ==='
echo 'Server:' \$MSSQL_SERVER
echo 'Database:' \$MSSQL_DATABASE
echo 'TDS Version:' \$TDS_VERSION

echo '=== Driver Check ==='
python3 -c 'import pyodbc; print(\"PyODBC:\", pyodbc.version)'
python3 -c 'import pymssql; print(\"PyMSSQL:\", pymssql.__version__)'

echo '=== Configuration Check ==='
grep -A 5 '\[MSSQL\]' /etc/freetds/freetds.conf
"
```

---

**💡 TIP**: For production use, store sensitive variables in Docker secrets or external secret management systems instead of `.env` files.
