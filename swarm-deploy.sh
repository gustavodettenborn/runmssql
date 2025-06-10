#!/bin/bash

# =============================================================================
# Docker Swarm Deploy Script for MSSQL Client
# =============================================================================

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configurações
STACK_NAME="mssql-client-stack"
IMAGE_NAME="mssql-pyodbc-client:latest"
STACK_FILE="docker-stack.yml"

echo -e "${BLUE}=== Docker Swarm Deploy Script ===${NC}"

# Função para verificar se Docker Swarm está ativo
check_swarm() {
    echo -e "${YELLOW}Verificando Docker Swarm...${NC}"
    if ! docker info | grep -q "Swarm: active"; then
        echo -e "${RED}Docker Swarm não está ativo. Inicializando...${NC}"
        docker swarm init
        echo -e "${GREEN}Docker Swarm inicializado!${NC}"
    else
        echo -e "${GREEN}Docker Swarm já está ativo!${NC}"
    fi
}

# Função para build da imagem
build_image() {
    echo -e "${YELLOW}Fazendo build da imagem Docker...${NC}"
    docker build -t $IMAGE_NAME .
    echo -e "${GREEN}Build concluído!${NC}"
}

# Função para verificar se os diretórios existem
check_directories() {
    echo -e "${YELLOW}Verificando diretórios...${NC}"

    # Carregar variáveis do .env (excluindo variáveis readonly do sistema)
    if [ -f .env ]; then
        export $(cat .env | grep -v '^#' | grep -v '^UID=' | grep -v '^GID=' | xargs)
    fi

    # Verificar RESULT_DIR
    if [ ! -d "$RESULT_DIR" ]; then
        echo -e "${YELLOW}Criando diretório: $RESULT_DIR${NC}"
        mkdir -p "$RESULT_DIR"
    fi

    # Verificar SQL_SCRIPT
    if [ ! -d "$SQL_SCRIPT" ]; then
        echo -e "${YELLOW}Criando diretório: $SQL_SCRIPT${NC}"
        mkdir -p "$SQL_SCRIPT"
    fi

    echo -e "${GREEN}Diretórios verificados!${NC}"
}

# Função para deploy no swarm
deploy_stack() {
    echo -e "${YELLOW}Fazendo deploy da stack no Docker Swarm...${NC}"

    # Carregar variáveis do .env para o deploy (excluindo variáveis readonly)
    set -a
    if [ -f .env ]; then
        source <(cat .env | grep -v '^#' | grep -v '^UID=' | grep -v '^GID=')
    fi
    set +a

    docker stack deploy -c $STACK_FILE $STACK_NAME
    echo -e "${GREEN}Stack deployada com sucesso!${NC}"
}

# Função para deploy de desenvolvimento (sem rebuild)
deploy_dev() {
    echo -e "${YELLOW}Deploy de desenvolvimento (scripts Python via bind mount)...${NC}"

    # Carregar variáveis do .env para o deploy (excluindo variáveis readonly)
    set -a
    if [ -f .env ]; then
        source <(cat .env | grep -v '^#' | grep -v '^UID=' | grep -v '^GID=')
    fi
    set +a

    docker stack deploy -c docker-stack-dev.yml $STACK_NAME
    echo -e "${GREEN}Stack de desenvolvimento deployada!${NC}"
    echo -e "${BLUE}📝 Scripts Python serão lidos diretamente do host${NC}"
    echo -e "${BLUE}   Mudanças nos .py serão refletidas imediatamente${NC}"
}

# Função para mostrar status
show_status() {
    echo -e "${BLUE}=== Status da Stack ===${NC}"
    docker stack ls
    echo ""
    echo -e "${BLUE}=== Serviços da Stack ===${NC}"
    docker stack services $STACK_NAME
    echo ""
    echo -e "${BLUE}=== Tasks da Stack ===${NC}"
    docker stack ps $STACK_NAME
}

# Função para logs
show_logs() {
    echo -e "${BLUE}=== Logs do Serviço ===${NC}"
    SERVICE_NAME="${STACK_NAME}_mssql-client"
    docker service logs $SERVICE_NAME --tail 50 --follow
}

# Função para remover stack
remove_stack() {
    echo -e "${YELLOW}Removendo stack...${NC}"
    docker stack rm $STACK_NAME
    echo -e "${GREEN}Stack removida!${NC}"
}

# Função para entrar no container
exec_container() {
    echo -e "${YELLOW}Procurando container ativo...${NC}"
    CONTAINER_ID=$(docker ps --filter "label=com.docker.swarm.service.name=${STACK_NAME}_mssql-client" --format "{{.ID}}" | head -1)

    if [ -z "$CONTAINER_ID" ]; then
        echo -e "${RED}Nenhum container ativo encontrado!${NC}"
        exit 1
    fi

    echo -e "${GREEN}Entrando no container: $CONTAINER_ID${NC}"
    docker exec -it $CONTAINER_ID /bin/bash
}

# Menu principal
case "${1:-help}" in
    "build")
        build_image
        ;;
    "deploy")
        check_swarm
        check_directories
        build_image
        deploy_stack
        show_status
        ;;
    "deploy-dev")
        check_swarm
        check_directories
        build_image
        deploy_dev
        show_status
        ;;
    "status")
        show_status
        ;;
    "logs")
        show_logs
        ;;
    "exec")
        exec_container
        ;;
    "remove")
        remove_stack
        ;;
    "redeploy")
        remove_stack
        sleep 5
        check_swarm
        check_directories
        build_image
        deploy_stack
        show_status
        ;;
    "redeploy-dev")
        remove_stack
        sleep 5
        check_swarm
        check_directories
        build_image
        deploy_dev
        show_status
        ;;
    "help"|*)
        echo -e "${BLUE}Uso: $0 {build|deploy|deploy-dev|status|logs|exec|remove|redeploy|redeploy-dev}${NC}"
        echo ""
        echo -e "${YELLOW}Comandos disponíveis:${NC}"
        echo "  build       - Faz build da imagem Docker"
        echo "  deploy      - Deploy completo da stack no Swarm"
        echo "  deploy-dev  - Deploy para desenvolvimento (bind mount dos scripts Python)"
        echo "  status      - Mostra status da stack"
        echo "  logs        - Mostra logs do serviço"
        echo "  exec        - Entra no container ativo"
        echo "  remove      - Remove a stack"
        echo "  redeploy    - Remove e refaz deploy da stack"
        echo "  redeploy-dev- Remove e refaz deploy de desenvolvimento"
        echo "  help        - Mostra esta ajuda"
        echo ""
        echo -e "${BLUE}💡 Modo Desenvolvimento:${NC}"
        echo "   Use deploy-dev para editar scripts Python sem rebuild"
        echo "   Mudanças em .py serão refletidas imediatamente"
        ;;
esac
