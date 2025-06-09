# MSSQL Client Application

Cliente Python para SQL Server legado co## 🛠️ Troubleshooting

```bash
# Teste completo e seguro (sem expor credenciais)
docker-compose exec mssql-client python3 test_connection_secure.py

# Teste automatizado com validações
docker-compose exec mssql-client ./test_automated.sh

# Verificar drivers ODBC
docker-compose exec mssql-client odbcinst -q -d

# Verificar configuração FreeTDS
docker-compose exec mssql-client cat /etc/freetds/freetds.conf

# Validar apenas variáveis de ambiente
python3 load_env.py --validate-only
```

### Scripts de Teste Disponíveis

- **`test_connection_secure.py`**: Teste completo sem expor credenciais
- **`test_automated.sh`**: Script automatizado para CI/CD
- **`load_env.py`**: Carregador seguro de variáveis .env
- **`test_connection.py`**: Teste original (pode expor credenciais)plos (PyODBC + PyMSSQL), configuração via variáveis de ambiente e compatibilidade SSL/TLS legado. **Ubuntu 22.04 LTS**.

## ✨ Características

- **Dual-Driver**: PyODBC (primário) + PyMSSQL (fallback) com troca automática
- **Configuração `.env`**: Sem valores hardcoded
- **SQL Server Legado**: Suporte para versões antigas (2000, 2005, 2008+)
- **SSL/TLS Legacy**: SECLEVEL=0 para máxima compatibilidade
- **Docker Ready**: Ubuntu 22.04 LTS containerizado
- **Testes Seguros**: Scripts de teste que não expõem credenciais

## 🚀 Início Rápido

```bash
# Configure ambiente
cp .env.example .env
nano .env  # Preencha as variáveis obrigatórias

# Execute
docker-compose up --build

# Teste conexão SEGURO (sem expor credenciais)
docker-compose exec mssql-client python3 test_connection_secure.py

# Teste automatizado completo
docker-compose exec mssql-client ./test_automated.sh

# Execute consultas
docker-compose exec mssql-client python3 run_sql_csv.py
```

### Ambiente Local (sem Docker)

```bash
# Carregue variáveis do .env
python3 load_env.py --show-values

# Teste conexão
python3 test_connection_secure.py
```

## ⚙️ Configuração

### Variáveis Obrigatórias (.env)
```bash
MSSQL_SERVER=seu_servidor
MSSQL_DATABASE=sua_database
MSSQL_USERNAME=seu_usuario
MSSQL_PASSWORD=sua_senha
RESULT_DIR=/caminho/para/resultados
SQL_SCRIPT=/caminho/para/scripts
```

### Variáveis Opcionais
```bash
MSSQL_PORT=1433
TDS_VERSION=7.0
MSSQL_ENCRYPT=false
MSSQL_TRUST_SERVER_CERTIFICATE=true
PREFERRED_DRIVER=pyodbc
```

## 🔧 Drivers e Fallback

1. **ODBC Driver 17** → 2. **ODBC Driver 18** → 3. **FreeTDS/PyMSSQL**

## 🛠️ Troubleshooting

```bash
# Teste completo
docker-compose exec mssql-client python3 test_connection.py

# Verificar drivers
docker-compose exec mssql-client odbcinst -q -d

# Verificar configuração FreeTDS
docker-compose exec mssql-client cat /etc/freetds/freetds.conf
```

### Problemas Comuns
- **SSL/TLS**: Configurado com SECLEVEL=0 para compatibilidade legada
- **PyMSSQL**: Fallback automático se instalação falhar
- **Timeout**: Ajuste `MSSQL_CONNECTION_TIMEOUT` para servidores lentos
- **Ubuntu 22.04**: Atualizado de 20.04 para melhor suporte a drivers

## 📁 Arquivos Principais

```
├── .env.example              # Template de configuração
├── docker-compose.yml        # Orquestração Docker
├── Dockerfile               # Imagem Ubuntu 22.04 + drivers
├── run_sql_csv.py           # Aplicação principal
├── test_connection_secure.py # Teste seguro (recomendado)
├── test_automated.sh        # Script automação CI/CD
├── load_env.py              # Carregador seguro .env
├── test_connection.py       # Teste original
└── requirements.txt         # Dependências Python
```

## ⚠️ Segurança

Configurado para **SQL Server legado** com segurança reduzida:
- SSL/TLS desabilitado (SECLEVEL=0)
- Certificados não validados
- **Não usar em produção com dados sensíveis**
