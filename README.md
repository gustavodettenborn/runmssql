# MSSQL Client Application

Cliente Python para SQL Server legado com múltiplos drivers (PyODBC + PyMSSQL), configuração via variáveis de ambiente e compatibilidade SSL/TLS legado. **Ubuntu 22.04 LTS**.

## ✨ Características

- **Dual-Driver**: PyODBC (primário) + PyMSSQL (fallback) com troca automática
- **Configuração `.env`**: Sem valores hardcoded
- **SQL Server Legado**: Suporte para versões antigas (2000, 2005, 2008+)
- **SSL/TLS Legacy**: SECLEVEL=0 para máxima compatibilidade
- **Docker Ready**: Ubuntu 22.04 LTS containerizado
- **Testes Seguros**: Scripts de teste que não expõem credenciais
- **🔄 Controle de Execução**: Sistema inteligente que executa apenas scripts novos/modificados/com falha
- **🐳 Docker Swarm**: Suporte para execução em cluster com alta disponibilidade
- **📊 Relatórios Detalhados**: Status completo e estatísticas de execução
- **⚙️ CLI Arguments**: Controle via linha de comando ou variáveis de ambiente

## 🚀 Início Rápido

### Docker Compose (Desenvolvimento)

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

### Docker Swarm (Produção)

```bash
# Inicialize o swarm (se necessário)
docker swarm init

# Deploy da stack
./swarm-deploy.sh deploy

# Verificar status
./swarm-deploy.sh status

# Executar scripts SQL
./swarm-deploy.sh exec
python3 /app/run_sql_csv.py

# Ver logs
./swarm-deploy.sh logs
```

### Ambiente Local (sem Docker)

```bash
# Carregue variáveis do .env
python3 load_env.py --show-values

# Teste conexão
python3 test_connection_secure.py

# Execute scripts com controle
python3 run_sql_csv.py --help
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

### Variáveis de Controle de Execução
```bash
FORCE_ALL=true/false               # Força execução de todos os scripts
STATUS_ONLY=true/false             # Apenas mostra status sem executar
RESET_LOG=true/false               # Limpa log de execução
SCRIPTS_DIR=/path/to/scripts       # Diretório dos scripts SQL
```

## 🔄 Sistema de Controle de Execução

### Funcionalidades
- **Controle inteligente**: Executa apenas scripts novos, modificados ou que falharam
- **Log de execução**: Histórico completo com status e timestamps
- **Detecção de mudanças**: Hash MD5 para detectar alterações nos scripts
- **Relatórios detalhados**: Status completo e estatísticas de execução

### Estados dos Scripts
- 🆕 **Never Executed**: Nunca foi executado → será executado
- ✅ **Success**: Executado com sucesso → será ignorado
- ❌ **Failed**: Falhou na execução → será executado novamente
- 🔄 **Modified**: Foi alterado desde a última execução → será executado

### Exemplos de Uso via CLI

```bash
# Execução normal (apenas scripts pendentes)
python3 run_sql_csv.py

# Ver apenas status dos scripts
python3 run_sql_csv.py --status

# Forçar execução de todos os scripts
python3 run_sql_csv.py --force-all

# Limpar log e executar
python3 run_sql_csv.py --reset-log

# Usar diretório customizado
python3 run_sql_csv.py --scripts-dir ./queries

# Combinações
python3 run_sql_csv.py --force-all --scripts-dir /custom/path
```

### Exemplos de Uso via Variáveis de Ambiente

```bash
# Docker Compose
docker-compose exec -e STATUS_ONLY=true mssql-client python3 run_sql_csv.py
docker-compose exec -e FORCE_ALL=true mssql-client python3 run_sql_csv.py

# Docker Swarm
./swarm-deploy.sh exec -e STATUS_ONLY=true
./swarm-deploy.sh exec -e FORCE_ALL=true -e RESET_LOG=true
```

## 🐳 Docker Swarm

### Diferenças entre Docker Compose e Docker Swarm

| Aspecto | Docker Compose | Docker Swarm |
|---------|----------------|-------------|
| **Escopo** | Máquina única | Cluster (múltiplas máquinas) |
| **Volumes** | Bind mounts diretos | Volumes gerenciados |
| **Uso** | Desenvolvimento | Produção |
| **Alta Disponibilidade** | ❌ | ✅ |
| **Load Balancing** | ❌ | ✅ Automático |

### Comandos do Docker Swarm

```bash
# Deploy completo
./swarm-deploy.sh deploy

# Deploy para desenvolvimento (bind mounts)
./swarm-deploy.sh deploy-dev

# Redeploy com rebuild
./swarm-deploy.sh redeploy

# Verificar status
./swarm-deploy.sh status

# Ver logs
./swarm-deploy.sh logs

# Entrar no container
./swarm-deploy.sh exec

# Remover stack
./swarm-deploy.sh remove
```

## 🔧 Gerenciamento de Atualizações Python

### Modo Produção (Recomendado)
```bash
# Para mudanças em scripts Python
./swarm-deploy.sh redeploy
```

### Modo Desenvolvimento (Mais Rápido)
```bash
# Deploy inicial com bind mounts
./swarm-deploy.sh deploy-dev

# Edite scripts Python diretamente no host
# Mudanças são refletidas imediatamente
vim run_sql_csv.py

# Execute dentro do container
./swarm-deploy.sh exec
python3 /app/run_sql_csv.py
```

### Fluxo de Trabalho Recomendado

1. **Desenvolvimento Ativo**: Use `deploy-dev` uma vez, depois edite diretamente
2. **Teste Final/Produção**: Use `redeploy` para rebuild completo

## 🔧 Drivers e Fallback

1. **ODBC Driver 17** → 2. **ODBC Driver 18** → 3. **FreeTDS/PyMSSQL**

## 🛠️ Troubleshooting

### Scripts de Teste Disponíveis

- **`test_connection_secure.py`**: Teste completo sem expor credenciais
- **`test_automated.sh`**: Script automatizado para CI/CD
- **`load_env.py`**: Carregador seguro de variáveis .env
- **`test_connection.py`**: Teste original (pode expor credenciais)

### Comandos de Diagnóstico

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

### Problemas Comuns

#### ❌ **Mudanças não aparecem no Docker Swarm**

**Modo Produção:**
```bash
# Força rebuild completo
docker image rm mssql-pyodbc-client:latest
./swarm-deploy.sh redeploy
```

**Modo Desenvolvimento:**
```bash
# Verifica se está usando a stack correta
docker stack ps mssql-client-stack

# Verifica os mounts
./swarm-deploy.sh exec
mount | grep app
```

#### ❌ **Container não inicia**
```bash
# Ver logs detalhados
./swarm-deploy.sh logs

# Verificar recursos
docker service ls
docker service ps mssql-client-stack_mssql-client
```

#### ❌ **Problemas de Conexão**
- **SSL/TLS**: Configurado com SECLEVEL=0 para compatibilidade legada
- **PyMSSQL**: Fallback automático se instalação falhar
- **Timeout**: Ajuste `MSSQL_CONNECTION_TIMEOUT` para servidores lentos
- **Ubuntu 22.04**: Atualizado de 20.04 para melhor suporte a drivers

## 📁 Arquivos Principais

```
├── .env.example              # Template de configuração
├── docker-compose.yml        # Orquestração Docker (desenvolvimento)
├── docker-stack.yml          # Stack Docker Swarm (produção)
├── docker-stack-dev.yml      # Stack Docker Swarm (desenvolvimento)
├── swarm-deploy.sh           # Script de deploy e gerenciamento Swarm
├── Dockerfile               # Imagem Ubuntu 22.04 + drivers
├── run_sql_csv.py           # Aplicação principal com controle de execução
├── test_connection_secure.py # Teste seguro (recomendado)
├── test_automated.sh        # Script automação CI/CD
├── load_env.py              # Carregador seguro .env
├── requirements.txt         # Dependências Python
├── EXECUTION-CONTROL.md     # Documentação do sistema de controle
├── DOCKER-SWARM.md         # Documentação Docker Swarm
├── PYTHON-UPDATES.md       # Processo de atualização de scripts
└── README.md               # Este arquivo
```

## 📚 Documentação Adicional

- **[EXECUTION-CONTROL.md](EXECUTION-CONTROL.md)**: Sistema completo de controle de execução
- **[DOCKER-SWARM.md](DOCKER-SWARM.md)**: Configuração e uso do Docker Swarm
- **[PYTHON-UPDATES.md](PYTHON-UPDATES.md)**: Processo de atualização de scripts Python

## ⚠️ Segurança

Configurado para **SQL Server legado** com segurança reduzida:
- SSL/TLS desabilitado (SECLEVEL=0)
- Certificados não validados
- **Não usar em produção com dados sensíveis**
