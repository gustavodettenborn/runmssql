#!/bin/bash

# ==============================================================================
# Script de Teste Automatizado - MSSQL Connection Test
# Executa testes de conexão de forma segura sem expor credenciais
# ==============================================================================

set -e  # Exit on any error

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para log colorido
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Verificar se estamos em um ambiente Docker
check_environment() {
    log_info "Verificando ambiente de execução..."

    if [ -f /.dockerenv ] || [ -d /app/venv ]; then
        log_info "Ambiente Docker detectado"
        PYTHON_CMD="/app/venv/bin/python3"
        ENV_FILE="/app/.env"
    else
        log_info "Ambiente local detectado"
        PYTHON_CMD="python3"
        ENV_FILE="./.env"
    fi

    # Verificar se Python está disponível
    if ! command -v $PYTHON_CMD &> /dev/null; then
        log_error "Python não encontrado: $PYTHON_CMD"
        exit 1
    fi

    log_success "Python encontrado: $($PYTHON_CMD --version)"
}

# Verificar arquivo .env
check_env_file() {
    log_info "Verificando arquivo de configuração..."

    if [ ! -f "$ENV_FILE" ]; then
        log_warning "Arquivo .env não encontrado em $ENV_FILE"
        log_info "Verificando variáveis de ambiente do sistema..."

        # Verificar se as variáveis essenciais estão definidas
        required_vars=("MSSQL_SERVER" "MSSQL_DATABASE")
        missing_vars=()

        for var in "${required_vars[@]}"; do
            if [ -z "${!var}" ]; then
                missing_vars+=("$var")
            fi
        done

        if [ ${#missing_vars[@]} -gt 0 ]; then
            log_error "Variáveis de ambiente obrigatórias não definidas: ${missing_vars[*]}"
            log_error "Defina as variáveis ou crie um arquivo .env"
            return 1
        fi
    else
        log_success "Arquivo .env encontrado: $ENV_FILE"

        # Verificar se o arquivo não está vazio
        if [ ! -s "$ENV_FILE" ]; then
            log_warning "Arquivo .env está vazio"
            return 1
        fi

        # Contar variáveis definidas (não comentadas)
        var_count=$(grep -c '^[^#]*=' "$ENV_FILE" 2>/dev/null || echo 0)
        log_info "Variáveis definidas no .env: $var_count"
    fi

    return 0
}

# Carregar variáveis de ambiente do arquivo .env (se existir e não estivermos no Docker)
load_env_variables() {
    if [ -f "$ENV_FILE" ] && [ ! -f /.dockerenv ]; then
        log_info "Carregando variáveis do arquivo .env..."

        # Usar uma forma segura de carregar o .env
        set -a  # automatically export all variables
        source "$ENV_FILE" 2>/dev/null || {
            log_error "Erro ao carregar arquivo .env"
            return 1
        }
        set +a

        log_success "Variáveis de ambiente carregadas"
    fi
}

# Testar importação de pacotes Python
test_python_packages() {
    log_info "Testando importação de pacotes Python..."

    # Lista de pacotes para testar
    packages=("pyodbc" "pymssql" "pandas")
    available_packages=()
    missing_packages=()

    for package in "${packages[@]}"; do
        if $PYTHON_CMD -c "import $package" 2>/dev/null; then
            available_packages+=("$package")
            log_success "Pacote $package: Disponível"
        else
            missing_packages+=("$package")
            log_warning "Pacote $package: Não disponível"
        fi
    done

    log_info "Pacotes disponíveis: ${#available_packages[@]}/${#packages[@]}"

    # Verificar se pelo menos um driver de conexão está disponível
    if [[ " ${available_packages[*]} " =~ " pyodbc " ]] || [[ " ${available_packages[*]} " =~ " pymssql " ]]; then
        log_success "Pelo menos um driver de conexão está disponível"
        return 0
    else
        log_error "Nenhum driver de conexão (pyodbc/pymssql) está disponível"
        return 1
    fi
}

# Executar teste de conexão seguro
run_connection_test() {
    log_info "Executando teste de conexão seguro..."

    # Verificar se o script de teste existe
    if [ -f "test_connection_secure.py" ]; then
        TEST_SCRIPT="test_connection_secure.py"
    elif [ -f "/app/test_connection_secure.py" ]; then
        TEST_SCRIPT="/app/test_connection_secure.py"
    else
        log_error "Script de teste seguro não encontrado"
        return 1
    fi

    log_info "Executando: $PYTHON_CMD $TEST_SCRIPT"

    # Executar o teste e capturar o código de saída
    if $PYTHON_CMD "$TEST_SCRIPT"; then
        log_success "Teste de conexão passou!"
        return 0
    else
        log_error "Teste de conexão falhou!"
        return 1
    fi
}

# Teste de validação de drivers ODBC
test_odbc_drivers() {
    log_info "Verificando drivers ODBC disponíveis..."

    if command -v odbcinst &> /dev/null; then
        log_info "odbcinst encontrado, listando drivers..."

        # Listar drivers disponíveis
        drivers=$(odbcinst -q -d 2>/dev/null || echo "")

        if [ -n "$drivers" ]; then
            log_success "Drivers ODBC encontrados:"
            echo "$drivers" | while read -r line; do
                if [ -n "$line" ]; then
                    log_info "  • $line"
                fi
            done
        else
            log_warning "Nenhum driver ODBC encontrado"
        fi
    else
        log_warning "odbcinst não disponível"
    fi
}

# Função principal
main() {
    echo "=============================================="
    echo "🔍 TESTE AUTOMATIZADO DE CONEXÃO MSSQL"
    echo "=============================================="

    local exit_code=0

    # Executar verificações em sequência
    check_environment || exit_code=1

    if [ $exit_code -eq 0 ]; then
        check_env_file || exit_code=1
    fi

    if [ $exit_code -eq 0 ]; then
        load_env_variables || exit_code=1
    fi

    if [ $exit_code -eq 0 ]; then
        test_python_packages || exit_code=1
    fi

    if [ $exit_code -eq 0 ]; then
        test_odbc_drivers
    fi

    if [ $exit_code -eq 0 ]; then
        run_connection_test || exit_code=1
    fi

    echo ""
    echo "=============================================="
    if [ $exit_code -eq 0 ]; then
        log_success "🎉 TODOS OS TESTES PASSARAM!"
        echo "✅ A aplicação está pronta para uso"
    else
        log_error "💥 ALGUNS TESTES FALHARAM!"
        echo "❌ Verifique a configuração antes de usar"
    fi
    echo "=============================================="

    exit $exit_code
}

# Verificar se o script está sendo executado diretamente
if [ "${BASH_SOURCE[0]}" == "${0}" ]; then
    main "$@"
fi
