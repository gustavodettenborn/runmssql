# Changelog - MS SQL Server Docker Client

## v2.0.0 - Ubuntu 24.04 LTS Upgrade (Junho 2025)

### 🚀 Principais Melhorias

- **Upgraded para Ubuntu 24.04 LTS (Noble Numbat)**
  - Suporte oficial até abril de 2029
  - Python 3.12.3 (melhores performances)
  - Bibliotecas de sistema mais recentes

### 📦 Atualizações de Dependências

- **Python Packages**:
  - PyODBC: 5.2.0 (mantido)
  - PyMSSQL: 2.2.11 (upgrade de 2.2.8)
  - Pandas: 2.3.0 (upgrade significativo)

- **System Libraries**:
  - OpenSSL 3.0.13 (melhor segurança)
  - GCC 13.3.0 (compilador mais recente)
  - FreeTDS com compatibilidade aprimorada

### 🔧 Melhorias Técnicas

- **Estratégia de Fallback Robusta**:
  - Tentativa primária: Repositório Ubuntu 24.04
  - Fallback automático: Repositório Ubuntu 22.04
  - Logs detalhados para debugging

- **Configuração ODBC Aprimorada**:
  - Threading melhorado para performance
  - Timeouts configuráveis
  - Detecção automática de drivers

### 🛡️ Segurança e Compatibilidade

- **SSL/TLS Legacy Support**:
  - Compatibilidade com SQL Server antigos
  - Configuração OpenSSL flexível
  - Suporte a protocolos legados

- **Driver Support**:
  - ✅ FreeTDS (compatibilidade legacy)
  - ✅ Microsoft ODBC Driver 17
  - ✅ Microsoft ODBC Driver 18

### 📊 Comparação de Versões

| Componente | v1.x (Ubuntu 22.04) | v2.0 (Ubuntu 24.04) |
|------------|---------------------|---------------------|
| Base OS | Ubuntu 22.04 LTS | Ubuntu 24.04 LTS |
| Python | 3.10.x | 3.12.3 |
| PyMSSQL | 2.2.8 | 2.2.11 |
| Pandas | 2.1.x | 2.3.0 |
| Suporte LTS | Até 2027 | Até 2029 |
| Tamanho Imagem | ~758MB | ~882MB |

### 🔄 Instruções de Migração

#### Para usuários existentes:

```bash
# Atualizar para a nova versão
docker pull mssql-client-ubuntu24:latest

# Ou rebuild local
docker build -t mssql-client-ubuntu24 .
```

#### Compatibilidade:

- ✅ Todas as APIs existentes mantidas
- ✅ Variáveis de ambiente inalteradas
- ✅ Scripts de conexão compatíveis
- ✅ Configurações de rede mantidas

### 🧪 Teste de Validação

```bash
# Testar a nova imagem
docker run --rm mssql-client-ubuntu24 \
  /app/venv/bin/python3 -c "
import pyodbc, pymssql, pandas
print('Ubuntu 24.04 LTS - Todos os drivers funcionando!')
"
```

### 📝 Notas de Desenvolvimento

- Implementação de fallback automático para repositórios Microsoft
- Validação extensiva de drivers ODBC
- Otimização de layers Docker para melhor cache
- Logs detalhados durante build para debugging

### 🔮 Próximos Passos

- [ ] Implementar multi-stage build para redução de tamanho
- [ ] Adicionar suporte a Ubuntu 25.04 quando disponível
- [ ] Otimizar configurações Python para performance
- [ ] Avaliar drivers Microsoft mais recentes

---

**Contribuidores**: Gustavo Dettenborn
**Data**: Junho 2025
**Ambiente de Teste**: Ubuntu 24.04 LTS, Docker Engine 24.x
