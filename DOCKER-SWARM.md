# Docker Swarm Configuration for MSSQL Client

Este projeto foi adaptado para funcionar com Docker Swarm, oferecendo melhor escalabilidade e gerenciamento em cluster.

## Arquivos Criados

- `docker-stack.yml` - Stack principal com bind mounts
- `docker-stack-volumes.yml` - Stack alternativa com volumes nomeados
- `swarm-deploy.sh` - Script de deploy e gerenciamento
- `.env.swarm` - Configurações específicas do Swarm

## Diferenças entre Docker Compose e Docker Swarm

### Docker Compose
- Execução em máquina única
- Bind mounts diretos
- Ideal para desenvolvimento

### Docker Swarm
- Execução em cluster (múltiplas máquinas)
- Volumes gerenciados
- Ideal para produção
- Alta disponibilidade
- Load balancing automático

## Como Usar

### 1. Deploy Completo
```bash
./swarm-deploy.sh deploy
```

### 2. Verificar Status
```bash
./swarm-deploy.sh status
```

### 3. Ver Logs
```bash
./swarm-deploy.sh logs
```

### 4. Entrar no Container
```bash
./swarm-deploy.sh exec
```

### 5. Remover Stack
```bash
./swarm-deploy.sh remove
```

### 6. Redeploy (útil para atualizações)
```bash
./swarm-deploy.sh redeploy
```

## Comandos Manuais

### Inicializar Swarm
```bash
docker swarm init
```

### Deploy Manual
```bash
# Carregar variáveis de ambiente
set -a && source .env && set +a

# Deploy da stack
docker stack deploy -c docker-stack.yml mssql-client-stack
```

### Verificar Serviços
```bash
docker stack services mssql-client-stack
docker stack ps mssql-client-stack
```

### Logs
```bash
docker service logs mssql-client-stack_mssql-client
```

### Escalar Serviço
```bash
docker service scale mssql-client-stack_mssql-client=3
```

## Configurações de Volumes

### Opção 1: Bind Mounts (docker-stack.yml)
- Usa diretórios locais do host
- Mais simples para desenvolvimento
- Limitado a uma máquina

### Opção 2: Volumes Nomeados (docker-stack-volumes.yml)
- Volumes gerenciados pelo Docker
- Melhor para produção em cluster
- Pode usar drivers distribuídos (NFS, GlusterFS, etc.)

## Configurações de Rede

- Usa rede overlay para comunicação entre nodes
- Rede criptografada por padrão
- Permite comunicação segura entre containers em diferentes hosts

## Recursos e Limites

- Memory Limit: 512MB
- Memory Reservation: 256MB
- CPU Limit: 0.5 cores
- CPU Reservation: 0.25 cores

## Estratégias de Deploy

- **Placement**: Executa apenas em nodes manager
- **Update Strategy**: Stop-first com rollback automático
- **Restart Policy**: On-failure com limite de tentativas

## Monitoramento

### Verificar Saúde dos Serviços
```bash
docker service ls
docker service ps mssql-client-stack_mssql-client
```

### Verificar Uso de Recursos
```bash
docker stats $(docker ps -q --filter "label=com.docker.swarm.service.name=mssql-client-stack_mssql-client")
```

## Troubleshooting

### Container não inicia
```bash
docker service logs mssql-client-stack_mssql-client
```

### Problemas de conectividade
```bash
docker network ls
docker network inspect mssql-client-stack_mssql-network
```

### Verificar placement constraints
```bash
docker node ls
docker service ps mssql-client-stack_mssql-client
```

## Migração do Docker Compose

Para migrar do Docker Compose para Swarm:

1. **Pare o Docker Compose**:
   ```bash
   docker-compose down
   ```

2. **Use o script de deploy**:
   ```bash
   ./swarm-deploy.sh deploy
   ```

3. **Verifique se tudo funcionou**:
   ```bash
   ./swarm-deploy.sh status
   ```

## Notas Importantes

- Docker Swarm requer que as imagens estejam disponíveis em todos os nodes
- Para clusters multi-node, considere usar um registry privado
- Volumes bind mount só funcionam se o caminho existir em todos os nodes
- Para produção, use volumes nomeados com drivers distribuídos
