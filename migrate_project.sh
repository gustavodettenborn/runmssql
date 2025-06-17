#!/bin/bash

# =============================================================================
# Script de Migração Automática - Reorganização do Projeto
# =============================================================================

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configurações
BACKUP_DIR="backup_$(date +%Y%m%d_%H%M%S)"
LOG_FILE="migration_$(date +%Y%m%d_%H%M%S).log"

# Função para log
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

# Função para criar backup
create_backup() {
    log "Criando backup em $BACKUP_DIR..."
    mkdir -p "$BACKUP_DIR"
    cp -r . "$BACKUP_DIR/" 2>/dev/null || true
    log_success "Backup criado com sucesso"
}

# Função para criar estrutura de diretórios
create_directory_structure() {
    log "Criando nova estrutura de diretórios..."

    # Diretórios principais
    mkdir -p src/{main,utils,config}
    mkdir -p tests/{unit,integration,demo,fixtures}
    mkdir -p scripts/{deployment,setup,testing}
    mkdir -p docker/{stacks,configs,healthchecks}
    mkdir -p docs/{docker,development,examples}
    mkdir -p data/{results,sql_scripts/{migrations,queries},logs,temp}
    mkdir -p config/{database,docker}
    mkdir -p tools/{migration,monitoring,development}

    # Criar arquivos __init__.py
    touch src/__init__.py
    touch src/main/__init__.py
    touch src/utils/__init__.py
    touch src/config/__init__.py
    touch tests/__init__.py
    touch tests/unit/__init__.py
    touch tests/integration/__init__.py
    touch tests/demo/__init__.py

    # Criar arquivos .gitkeep
    touch data/results/.gitkeep
    touch data/sql_scripts/.gitkeep
    touch data/logs/.gitkeep
    touch data/temp/.gitkeep

    log_success "Estrutura de diretórios criada"
}

# Função para mover arquivos Python
migrate_python_files() {
    log "Migrando arquivos Python..."

    # Aplicação principal
    if [ -f "run_sql_csv.py" ]; then
        mv run_sql_csv.py src/main/
        log_success "Movido: run_sql_csv.py -> src/main/"
    fi

    if [ -f "run_sql_csv_pymssql.py" ]; then
        mv run_sql_csv_pymssql.py src/main/
        log_success "Movido: run_sql_csv_pymssql.py -> src/main/"
    fi

    # Utilitários
    if [ -f "load_env.py" ]; then
        mv load_env.py src/utils/
        log_success "Movido: load_env.py -> src/utils/"
    fi

    # Testes
    if [ -f "test_connection.py" ]; then
        mv test_connection.py tests/unit/
        log_success "Movido: test_connection.py -> tests/unit/"
    fi

    if [ -f "test_connection_secure.py" ]; then
        mv test_connection_secure.py tests/integration/
        log_success "Movido: test_connection_secure.py -> tests/integration/"
    fi

    if [ -f "demo_secure_testing.py" ]; then
        mv demo_secure_testing.py tests/demo/
        log_success "Movido: demo_secure_testing.py -> tests/demo/"
    fi
}

# Função para mover scripts shell
migrate_shell_scripts() {
    log "Migrando scripts shell..."

    # Scripts de deployment
    if [ -f "swarm-deploy.sh" ]; then
        mv swarm-deploy.sh scripts/deployment/
        log_success "Movido: swarm-deploy.sh -> scripts/deployment/"
    fi

    # Scripts de setup
    if [ -f "mssql.sh" ]; then
        mv mssql.sh scripts/setup/
        log_success "Movido: mssql.sh -> scripts/setup/"
    fi

    # Scripts de teste
    if [ -f "test_automated.sh" ]; then
        mv test_automated.sh scripts/testing/
        log_success "Movido: test_automated.sh -> scripts/testing/"
    fi

    if [ -f "test_env_config.sh" ]; then
        mv test_env_config.sh scripts/testing/
        log_success "Movido: test_env_config.sh -> scripts/testing/"
    fi
}

# Função para mover arquivos Docker
migrate_docker_files() {
    log "Migrando arquivos Docker..."

    # Dockerfile
    if [ -f "Dockerfile" ]; then
        mv Dockerfile docker/
        log_success "Movido: Dockerfile -> docker/"
    fi

    # Docker Stacks
    if [ -f "docker-stack.yml" ]; then
        mv docker-stack.yml docker/stacks/
        log_success "Movido: docker-stack.yml -> docker/stacks/"
    fi

    if [ -f "docker-stack-dev.yml" ]; then
        mv docker-stack-dev.yml docker/stacks/
        log_success "Movido: docker-stack-dev.yml -> docker/stacks/"
    fi

    if [ -f "docker-stack-volumes.yml" ]; then
        mv docker-stack-volumes.yml docker/stacks/
        log_success "Movido: docker-stack-volumes.yml -> docker/stacks/"
    fi

    # Configurações
    if [ -f "java.security" ]; then
        mv java.security docker/configs/
        log_success "Movido: java.security -> docker/configs/"
    fi
}

# Função para mover documentação
migrate_documentation() {
    log "Migrando documentação..."

    if [ -f "CHANGELOG.md" ]; then
        mv CHANGELOG.md docs/
        log_success "Movido: CHANGELOG.md -> docs/"
    fi

    # Manter README.md na raiz como link simbólico
    if [ -f "README.md" ]; then
        cp README.md docs/
        log_success "Copiado: README.md -> docs/"
    fi
}

# Função para mover configurações
migrate_configurations() {
    log "Migrando configurações..."

    # Mover .env.example para config/
    if [ -f ".env.example" ]; then
        cp .env.example config/
        log_success "Copiado: .env.example -> config/"
    fi

    # Criar arquivos de ambiente específicos
    if [ -f ".env.example" ]; then
        cp .env.example config/.env.development
        cp .env.example config/.env.production
        cp .env.example config/.env.testing
        log_success "Criados: arquivos .env específicos por ambiente"
    fi
}

# Função para atualizar imports Python
update_python_imports() {
    log "Atualizando imports Python..."

    # Atualizar imports nos arquivos movidos
    find src/ -name "*.py" -type f -exec sed -i 's/from load_env/from src.utils.load_env/g' {} \;
    find tests/ -name "*.py" -type f -exec sed -i 's/from load_env/from src.utils.load_env/g' {} \;
    find src/ -name "*.py" -type f -exec sed -i 's/import load_env/import src.utils.load_env as load_env/g' {} \;

    log_success "Imports Python atualizados"
}

# Função para atualizar caminhos em scripts shell
update_shell_scripts() {
    log "Atualizando caminhos em scripts shell..."

    # Atualizar referências de caminhos nos scripts
    find scripts/ -name "*.sh" -type f -exec sed -i 's|python3 run_sql_csv.py|python3 src/main/run_sql_csv.py|g' {} \;
    find scripts/ -name "*.sh" -type f -exec sed -i 's|python3 test_connection_secure.py|python3 tests/integration/test_connection_secure.py|g' {} \;
    find scripts/ -name "*.sh" -type f -exec sed -i 's|python3 load_env.py|python3 src/utils/load_env.py|g' {} \;

    log_success "Caminhos em scripts shell atualizados"
}

# Função para atualizar arquivos Docker
update_docker_files() {
    log "Atualizando arquivos Docker..."

    # Atualizar Dockerfile
    if [ -f "docker/Dockerfile" ]; then
        sed -i 's|COPY \.|COPY . /app/|g' docker/Dockerfile
        sed -i 's|COPY requirements.txt|COPY ./requirements.txt|g' docker/Dockerfile
    fi

    log_success "Arquivos Docker atualizados"
}

# Função para criar scripts de conveniência
create_convenience_scripts() {
    log "Criando scripts de conveniência..."

    # Script para executar aplicação principal
    cat > run_app.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
python3 -m src.main.run_sql_csv "$@"
EOF
    chmod +x run_app.sh

    # Script para executar testes
    cat > run_tests.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
python3 -m pytest tests/ "$@"
EOF
    chmod +x run_tests.sh

    # Script para desenvolvimento
    cat > dev_setup.sh << 'EOF'
#!/bin/bash
cd "$(dirname "$0")"
echo "Configurando ambiente de desenvolvimento..."
pip install -r requirements.txt
echo "Ambiente configurado!"
EOF
    chmod +x dev_setup.sh

    log_success "Scripts de conveniência criados"
}

# Função para validar migração
validate_migration() {
    log "Validando migração..."

    local errors=0

    # Verificar se arquivos foram movidos corretamente
    if [ ! -f "src/main/run_sql_csv.py" ]; then
        log_error "Arquivo principal não encontrado: src/main/run_sql_csv.py"
        ((errors++))
    fi

    if [ ! -f "src/utils/load_env.py" ]; then
        log_error "Utilitário não encontrado: src/utils/load_env.py"
        ((errors++))
    fi

    if [ ! -f "docker/Dockerfile" ]; then
        log_error "Dockerfile não encontrado: docker/Dockerfile"
        ((errors++))
    fi

    if [ ! -f "scripts/deployment/swarm-deploy.sh" ]; then
        log_error "Script de deploy não encontrado: scripts/deployment/swarm-deploy.sh"
        ((errors++))
    fi

    if [ $errors -eq 0 ]; then
        log_success "Validação concluída com sucesso!"
        return 0
    else
        log_error "Validação falhou com $errors erros"
        return 1
    fi
}

# Função para rollback
rollback_migration() {
    log_warning "Executando rollback..."

    if [ -d "$BACKUP_DIR" ]; then
        # Remover arquivos atuais
        rm -rf src/ tests/ scripts/ docker/ docs/ data/ config/ tools/
        rm -f run_app.sh run_tests.sh dev_setup.sh

        # Restaurar backup
        cp -r "$BACKUP_DIR"/* .
        rm -rf "$BACKUP_DIR"

        log_success "Rollback executado com sucesso"
    else
        log_error "Backup não encontrado. Rollback não pode ser executado."
    fi
}

# Função principal
main() {
    echo -e "${BLUE}=== Script de Migração - Reorganização do Projeto ===${NC}"
    echo -e "${YELLOW}Este script irá reorganizar a estrutura do projeto.${NC}"
    echo -e "${YELLOW}Um backup será criado automaticamente.${NC}"
    echo ""

    read -p "Deseja continuar? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Migração cancelada."
        exit 0
    fi

    log "Iniciando migração..."

    # Executar migração
    create_backup
    create_directory_structure
    migrate_python_files
    migrate_shell_scripts
    migrate_docker_files
    migrate_documentation
    migrate_configurations
    update_python_imports
    update_shell_scripts
    update_docker_files
    create_convenience_scripts

    # Validar migração
    if validate_migration; then
        log_success "Migração concluída com sucesso!"
        echo ""
        echo -e "${GREEN}✅ Projeto reorganizado com sucesso!${NC}"
        echo -e "${BLUE}📁 Nova estrutura criada${NC}"
        echo -e "${BLUE}📋 Log salvo em: $LOG_FILE${NC}"
        echo -e "${BLUE}💾 Backup salvo em: $BACKUP_DIR${NC}"
        echo ""
        echo -e "${YELLOW}Próximos passos:${NC}"
        echo "1. Teste a aplicação: ./run_app.sh"
        echo "2. Execute os testes: ./run_tests.sh"
        echo "3. Verifique o Docker Swarm: ./scripts/deployment/swarm-deploy.sh deploy"
        echo "4. Revise e atualize documentação conforme necessário"
    else
        log_error "Migração falhou na validação"
        echo ""
        echo -e "${RED}❌ Migração falhou!${NC}"
        read -p "Deseja executar rollback? (Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Rollback cancelado. Verifique os erros manualmente."
        else
            rollback_migration
        fi
    fi
}

# Executar função principal
main "$@"
