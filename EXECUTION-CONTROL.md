# Sistema de Controle de Execução SQL

Este documento explica como usar o novo sistema de controle de execução que foi adicionado ao script `run_sql_csv.py`. O sistema permite controlar quais scripts SQL são executados, evitando re-execução desnecessária e fornecendo relatórios detalhados.

## Funcionalidades

### 1. Controle de Execução Inteligente
- **Detecção de mudanças**: Usa hash MD5 para detectar se um script foi modificado
- **Log de execução**: Mantém histórico do que foi executado com sucesso/falha
- **Execução seletiva**: Por padrão, executa apenas scripts novos, modificados ou que falharam

### 2. Variáveis de Ambiente de Controle

O sistema é controlado através de variáveis de ambiente:

#### `FORCE_ALL`
- **Valores**: `true`, `false` (padrão: `false`)
- **Função**: Força execução de TODOS os scripts, mesmo os já executados com sucesso
- **Uso**: Quando você quer re-executar todo o lote de scripts

#### `STATUS_ONLY`
- **Valores**: `true`, `false` (padrão: `false`)
- **Função**: Apenas exibe o status dos scripts sem executar nada
- **Uso**: Para verificar quais scripts foram executados e quando

#### `RESET_LOG`
- **Valores**: `true`, `false` (padrão: `false`)
- **Função**: Limpa o log de execução, fazendo todos os scripts serem considerados "nunca executados"
- **Uso**: Para recomeçar do zero o controle de execução

#### `SCRIPTS_DIR`
- **Valor**: Caminho do diretório (padrão: `/app/sql_scripts`)
- **Função**: Define onde buscar os scripts SQL
- **Uso**: Para usar um diretório diferente do padrão

## Como Usar no Docker Swarm

### 1. Execução Normal (apenas scripts novos/modificados/com falha)
```bash
docker service exec legacymssql_python_app.1 python run_sql_csv.py
```

### 2. Ver apenas o status dos scripts
```bash
docker service exec -e STATUS_ONLY=true legacymssql_python_app.1 python run_sql_csv.py
```

### 3. Forçar execução de todos os scripts
```bash
docker service exec -e FORCE_ALL=true legacymssql_python_app.1 python run_sql_csv.py
```

### 4. Limpar log e recomeçar
```bash
docker service exec -e RESET_LOG=true legacymssql_python_app.1 python run_sql_csv.py
```

### 5. Combinações úteis
```bash
# Limpar log e executar todos os scripts
docker service exec -e RESET_LOG=true -e FORCE_ALL=true legacymssql_python_app.1 python run_sql_csv.py

# Ver status após limpar o log
docker service exec -e RESET_LOG=true -e STATUS_ONLY=true legacymssql_python_app.1 python run_sql_csv.py

# Usar diretório específico
docker service exec -e SCRIPTS_DIR=/app/custom_scripts legacymssql_python_app.1 python run_sql_csv.py
```

## Estados dos Scripts

O sistema classifica cada script SQL em um dos seguintes estados:

### 🆕 Never Executed (Nunca executado)
- Script encontrado no diretório mas nunca foi executado
- **Ação**: Será executado na próxima execução

### ✅ Success (Sucesso)
- Script foi executado com sucesso e não foi modificado desde então
- **Ação**: Será ignorado na próxima execução (exceto se `FORCE_ALL=true`)

### ❌ Failed (Falhou)
- Script foi executado mas falhou (erro SQL ou conexão)
- **Ação**: Será executado novamente na próxima execução

### 🔄 Modified (Modificado)
- Script foi executado com sucesso anteriormente, mas foi modificado depois
- **Ação**: Será executado novamente na próxima execução

## Log de Execução

O sistema mantém um arquivo de log em `/app/results/execution_log.json` com as seguintes informações:

```json
{
  "01_usuarios.sql": {
    "file_path": "/app/sql_scripts/01_usuarios.sql",
    "csv_output": "01_usuarios.csv",
    "file_hash": "abc123def456...",
    "success": true,
    "execution_time": "2024-01-15T10:30:45.123456",
    "error_message": null
  }
}
```

### Campos do Log:
- **file_path**: Caminho completo do script SQL
- **csv_output**: Nome do arquivo CSV gerado
- **file_hash**: Hash MD5 do conteúdo do script
- **success**: `true` se executado com sucesso, `false` se falhou
- **execution_time**: Data/hora da execução no formato ISO
- **error_message**: Mensagem de erro (se houver)

## Relatórios

### Relatório de Status (`STATUS_ONLY=true`)
```
======================================================================
STATUS DOS SCRIPTS SQL
======================================================================
📊 Resumo: 5 scripts encontrados
   ✅ Executados com sucesso: 3
   ❌ Falharam: 1
   🔄 Modificados: 0
   📄 Nunca executados: 1

✅ 01_usuarios.sql                        | Sucesso         | 15/01/2024 10:30
❌ 02_produtos.sql                        | Falhou          | 15/01/2024 10:32
✅ 03_pedidos.sql                         | Sucesso         | 15/01/2024 10:35
📄 04_relatorios.sql                      | Nunca executado | Nunca executado
🔄 05_backup.sql                          | Modificado      | 14/01/2024 09:15

🔄 Scripts que serão executados na próxima execução (2):
   • 02_produtos.sql
   • 04_relatorios.sql
```

### Relatório de Execução
```
======================================================================
RELATÓRIO FINAL
======================================================================
📊 Estatísticas:
   • Scripts executados: 2
   • Sucessos: 1
   • Falhas: 1
   • Ignorados (já executados): 3

📁 Arquivos CSV gerados com sucesso (1):
   📄 04_relatorios.csv

❌ Scripts que falharam (1):
   ❌ 02_produtos.sql

💡 Próximos passos:
   • Execute novamente para tentar os 1 scripts que falharam
   • Use STATUS_ONLY=true para ver detalhes dos erros
```

## Casos de Uso Comuns

### 1. Execução Diária Automatizada
Configure um cron job ou task scheduler para executar apenas os scripts novos/modificados:
```bash
docker service exec legacymssql_python_app.1 python run_sql_csv.py
```

### 2. Re-processamento Completo
Quando você quer garantir que todos os dados sejam regenerados:
```bash
docker service exec -e FORCE_ALL=true legacymssql_python_app.1 python run_sql_csv.py
```

### 3. Verificação de Status
Para ver o que foi executado recentemente sem executar nada:
```bash
docker service exec -e STATUS_ONLY=true legacymssql_python_app.1 python run_sql_csv.py
```

### 4. Recuperação de Falhas
O sistema automaticamente re-executa scripts que falharam na execução anterior. Você apenas precisa executar normalmente:
```bash
docker service exec legacymssql_python_app.1 python run_sql_csv.py
```

### 5. Nova Implementação
Quando você quer começar do zero (limpar histórico):
```bash
docker service exec -e RESET_LOG=true legacymssql_python_app.1 python run_sql_csv.py
```

## Boas Práticas

1. **Nomenclatura de Scripts**: Use prefixos numéricos (01_, 02_, etc.) para garantir ordem de execução
2. **Monitoramento**: Use `STATUS_ONLY=true` regularmente para monitorar o estado
3. **Backup**: O arquivo de log é importante - considere fazer backup do diretório `/app/results`
4. **Teste**: Teste scripts novos individualmente antes de adicionar ao lote
5. **Documentação**: Mantenha documentação dos scripts e sua função

## Troubleshooting

### Scripts não são encontrados
- Verifique se o diretório `/app/sql_scripts` existe e tem scripts `.sql`
- Confirm que o bind mount está configurado corretamente no Docker Swarm
- Use `SCRIPTS_DIR=/caminho/custom` se necessário

### Scripts sempre re-executam
- Verifique se o arquivo de log está sendo persistido (volume montado corretamente)
- Se arquivos são editados fora do container, hash será diferente

### Log não persiste
- Confirme que o volume `/app/results` está montado corretamente
- Verifique permissões de escrita no diretório

### Scripts falham constantemente
- Use `STATUS_ONLY=true` para ver mensagens de erro detalhadas
- Verifique credenciais e conectividade com SQL Server
- Teste scripts individualmente no SQL Server Management Studio
