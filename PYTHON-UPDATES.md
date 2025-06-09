# Processo de Atualização de Scripts Python no Docker Swarm

## Resumo Rápido ⚡

**Para mudanças terem efeito após editar scripts Python:**

### Produção (Recomendado)
```bash
./swarm-deploy.sh redeploy
```

### Desenvolvimento (Mais Rápido)
```bash
./swarm-deploy.sh deploy-dev
```

---

## Detalhamento dos Processos

### 🔧 **Modo Produção**

#### Como funciona:
- Scripts Python estão **dentro** da imagem Docker
- Mudanças requerem **rebuild** da imagem
- Processo mais seguro e consistente

#### Comandos:
```bash
# Rebuild + redeploy completo
./swarm-deploy.sh redeploy

# Ou passo a passo:
./swarm-deploy.sh build     # Rebuild da imagem
./swarm-deploy.sh deploy    # Deploy da stack
```

#### Quando usar:
- ✅ Deploy de produção
- ✅ Mudanças finalizadas
- ✅ Teste de comportamento em ambiente isolado

---

### 🚀 **Modo Desenvolvimento**

#### Como funciona:
- Scripts Python são montados via **bind mount**
- Mudanças são refletidas **imediatamente**
- Não requer rebuild para cada mudança

#### Comandos:
```bash
# Deploy de desenvolvimento
./swarm-deploy.sh deploy-dev

# Redeploy de desenvolvimento
./swarm-deploy.sh redeploy-dev
```

#### Quando usar:
- ✅ Durante desenvolvimento ativo
- ✅ Testes rápidos de mudanças
- ✅ Debug e iterações frequentes

---

## Fluxo de Trabalho Recomendado

### 1. **Desenvolvimento Ativo**
```bash
# Primeira vez ou após mudanças na imagem base
./swarm-deploy.sh deploy-dev

# Edite os scripts Python no host
vim run_sql_csv.py

# Execute dentro do container (mudanças já estão ativas)
./swarm-deploy.sh exec
python3 /app/run_sql_csv.py
```

### 2. **Teste Final / Produção**
```bash
# Deploy final com imagem atualizada
./swarm-deploy.sh redeploy
```

---

## Verificação de Mudanças

### Ver se a stack está ativa:
```bash
./swarm-deploy.sh status
```

### Ver logs do serviço:
```bash
./swarm-deploy.sh logs
```

### Entrar no container:
```bash
./swarm-deploy.sh exec
```

### Verificar qual arquivo está sendo usado:
```bash
# Dentro do container
cat /app/run_sql_csv.py | head -10
ls -la /app/run_sql_csv.py
```

---

## Arquivos de Stack

| Arquivo | Descrição | Scripts Python |
|---------|-----------|----------------|
| `docker-stack.yml` | Produção | Dentro da imagem |
| `docker-stack-dev.yml` | Desenvolvimento | Bind mount |
| `docker-stack-volumes.yml` | Produção com volumes nomeados | Dentro da imagem |

---

## Troubleshooting

### ❌ **Mudanças não aparecem**

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

### ❌ **Container não inicia**

```bash
# Ver logs detalhados
./swarm-deploy.sh logs

# Verificar recursos
docker service ls
docker service ps mssql-client-stack_mssql-client
```

### ❌ **Bind mount não funciona**

```bash
# Verificar se arquivo existe no host
ls -la ./run_sql_csv.py

# Verificar permissões
chmod 644 ./run_sql_csv.py
```

---

## Resumo dos Comandos

| Comando | Descrição | Rebuild | Tempo |
|---------|-----------|---------|-------|
| `./swarm-deploy.sh deploy` | Deploy produção | ✅ | ~2-3 min |
| `./swarm-deploy.sh deploy-dev` | Deploy desenvolvimento | ✅ (primeira vez) | ~2-3 min |
| `./swarm-deploy.sh redeploy` | Redeploy produção | ✅ | ~2-3 min |
| `./swarm-deploy.sh redeploy-dev` | Redeploy desenvolvimento | ✅ | ~2-3 min |

**💡 Dica:** Use `deploy-dev` uma vez, depois edite os scripts diretamente - mudanças são instantâneas!

---

## 🎯 **Novo Sistema de Controle de Execução**

### Funcionalidades Adicionadas:
- ✅ **Controle inteligente**: Executa apenas scripts novos, modificados ou que falharam
- ✅ **Log de execução**: Histórico completo com status e timestamps
- ✅ **Detecção de mudanças**: Hash MD5 para detectar alterações nos scripts
- ✅ **Relatórios detalhados**: Status completo e estatísticas de execução
- ✅ **Configuração por variáveis de ambiente**: Controle sem rebuild

### Controle por Variáveis de Ambiente:

#### **Execução normal** (apenas novos/modificados/com falha):
```bash
./swarm-deploy.sh exec
python3 /app/run_sql_csv.py
```

#### **Ver apenas status** dos scripts:
```bash
./swarm-deploy.sh exec -e STATUS_ONLY=true
python3 /app/run_sql_csv.py
```

#### **Forçar execução** de todos os scripts:
```bash
./swarm-deploy.sh exec -e FORCE_ALL=true
python3 /app/run_sql_csv.py
```

#### **Limpar log** e recomeçar do zero:
```bash
./swarm-deploy.sh exec -e RESET_LOG=true
python3 /app/run_sql_csv.py
```

#### **Combinações úteis**:
```bash
# Limpar log + executar tudo
./swarm-deploy.sh exec -e RESET_LOG=true -e FORCE_ALL=true
python3 /app/run_sql_csv.py

# Ver status após limpar log
./swarm-deploy.sh exec -e RESET_LOG=true -e STATUS_ONLY=true
python3 /app/run_sql_csv.py
```

### Estados dos Scripts:
- 🆕 **Never Executed**: Nunca foi executado → será executado
- ✅ **Success**: Executado com sucesso → será ignorado
- ❌ **Failed**: Falhou na execução → será executado novamente
- 🔄 **Modified**: Foi alterado desde a última execução → será executado

### Log de Execução:
- Arquivo: `/app/results/execution_log.json`
- Contém: hash dos arquivos, timestamps, status, mensagens de erro
- Persiste entre execuções do container

### Documentação Completa:
Ver arquivo `EXECUTION-CONTROL.md` para detalhes completos.

---
