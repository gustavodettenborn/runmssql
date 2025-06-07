import os
import socket

import pandas as pd
import pyodbc


class SQLToCsv:
    def __init__(self):
        """Inicializa a conexão com SQL Server usando variáveis de ambiente"""

        # Lê variáveis de ambiente
        self.server = os.getenv('MSSQL_SERVER', 'localhost')
        self.database = os.getenv('MSSQL_DATABASE', 'master')
        self.username = os.getenv('MSSQL_USERNAME')
        self.password = os.getenv('MSSQL_PASSWORD')
        self.trusted_connection = os.getenv(
            'MSSQL_TRUSTED_CONNECTION', 'false').lower() == 'true'

        self.connection = None

        print("Configuração carregada:")
        print(f"  Servidor: {self.server}")
        print(f"  Database: {self.database}")
        print(f"  Trusted Connection: {self.trusted_connection}")
        if not self.trusted_connection:
            print(f"  Username: {self.username}")

    def list_available_drivers(self):
        """Lista drivers ODBC disponíveis"""
        print("\n--- Drivers ODBC Disponíveis ---")
        try:
            drivers = pyodbc.drivers()
            sql_server_drivers = [d for d in drivers if 'SQL Server' in d]

            if sql_server_drivers:
                for driver in sql_server_drivers:
                    print(f"✓ {driver}")
            else:
                print("✗ Nenhum driver SQL Server encontrado")
                print("Drivers disponíveis:", drivers)

            return sql_server_drivers
        except Exception as e:
            print(f"✗ Erro ao listar drivers: {e}")
            return []

    def connect(self):
        """Estabelece conexão com o banco usando múltiplas estratégias"""
        print("\n--- Iniciando Conexão ---")
        print(f"Servidor: {self.server}")
        print(f"Database: {self.database}")
        print(f"Usuário: {self.username}")

        # Verifica se as credenciais foram fornecidas
        if not self.username or not self.password:
            print("✗ ERRO: Usuário ou senha não fornecidos")
            print("Verifique as variáveis MSSQL_USERNAME e MSSQL_PASSWORD")
            return False

        # Lista de drivers para testar
        drivers_to_test = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server"
        ]

        # Lista de configurações SSL/TLS para testar
        ssl_configs = [
            {"name": "Sem SSL", "encrypt": "no", "trust_cert": "yes"},
            {"name": "SSL Opcional", "encrypt": "optional",
             "trust_cert": "yes"},
            {"name": "SSL Obrigatório", "encrypt": "yes",
             "trust_cert": "yes"},
            {"name": "SSL Strict", "encrypt": "yes", "trust_cert": "no"}
        ]

        # Testa todas as combinações de driver e SSL
        for driver in drivers_to_test:
            if driver not in pyodbc.drivers():
                print(f"⚠️  Driver {driver} não está disponível")
                continue

            print(f"\n🔄 Testando {driver}...")

            for ssl_config in ssl_configs:
                print(f"  └─ Configuração: {ssl_config['name']}")

                try:
                    # Monta string de conexão baseada no driver
                    if driver == "ODBC Driver 17 for SQL Server":
                        # Driver 17 - configuração mais simples
                        conn_str = (f"DRIVER={{{driver}}};"
                                    f"SERVER={self.server};"
                                    f"DATABASE={self.database};"
                                    f"UID={self.username};"
                                    f"PWD={self.password};"
                                    f"ConnectionTimeout=30;"
                                    f"LoginTimeout=30;")
                    else:
                        # Driver 18 - controle explícito de SSL
                        conn_str = (f"DRIVER={{{driver}}};"
                                    f"SERVER={self.server};"
                                    f"DATABASE={self.database};"
                                    f"UID={self.username};"
                                    f"PWD={self.password};"
                                    f"Encrypt={ssl_config['encrypt']};"
                                    f"TrustServerCertificate="
                                    f"{ssl_config['trust_cert']};"
                                    f"ConnectionTimeout=30;"
                                    f"LoginTimeout=30;")

                    print(f"Servidor: {self.server}, Database: "
                          f"{self.database}")
                    safe_conn = (conn_str.replace(f"PWD={self.password}",
                                                  "PWD=***")
                                 if self.password else conn_str)
                    print(f"    String de conexão: {safe_conn}")

                    # Tenta a conexão
                    self.connection = pyodbc.connect(conn_str)
                    print(f"✅ SUCESSO! Conectado usando {driver} com "
                          f"{ssl_config['name']}")
                    return True

                except pyodbc.Error as e:
                    error_code = (getattr(e, 'args', [''])[0]
                                  if hasattr(e, 'args') and e.args
                                  else str(e))
                    print(f"    ❌ Falhou: {error_code}")

                    # Diagnóstico específico por tipo de erro
                    if "28000" in str(e) or "18456" in str(e):
                        print("    🔍 ERRO DE AUTENTICAÇÃO DETECTADO:")
                        print(f"       - Usuário: {self.username}")
                        print("       - Verificações necessárias:")
                        print("         1. Usuário existe no SQL Server?")
                        print("         2. Senha está correta?")
                        print("         3. SQL Server Authentication está "
                              "habilitado?")
                        print("         4. Usuário tem permissão no database?")
                        print("         5. Usuário não está "
                              "bloqueado/desabilitado?")

                    elif "08001" in str(e):
                        print("    🔍 ERRO DE CONECTIVIDADE:")
                        print("       - Servidor não acessível ou porta "
                              "bloqueada")
                        print("       - Verifique firewall e configuração "
                              "de rede")

                    elif "SSL" in str(e).upper() or "TLS" in str(e).upper():
                        print("    🔍 ERRO DE SSL/TLS:")
                        print("       - Problema na configuração de "
                              "criptografia")
                        print("       - Tente outras configurações SSL")

                except Exception as e:
                    print(f"    ❌ Erro inesperado: {e}")

        print("❌ FALHA: Não foi possível conectar com nenhuma configuração")
        return False

    def execute_sql_to_csv(self, sql_query, csv_filename, chunk_size=10000):
        """Executa query SQL e salva resultado em CSV"""
        if not self.connection:
            print("✗ Sem conexão com o banco")
            return False

        try:
            print("\nExecutando query...")
            print(f"Query: {sql_query[:100]}...")

            # Executa a query usando cursor para ter controle total dos tipos
            cursor = self.connection.cursor()
            cursor.execute(sql_query)

            # Obtém os nomes das colunas
            columns = [column[0] for column in cursor.description]

            # Coleta todos os dados como strings
            rows = []
            for row in cursor:
                # Converte cada valor para string, tratando None/NULL
                string_row = []
                for value in row:
                    if value is None:
                        string_row.append('')
                    else:
                        str_value = str(value)
                        # Remove .0 de números inteiros que viraram float
                        if (str_value.endswith('.0') and
                                str_value.replace('.0', '').replace(
                                    '-', '').isdigit()):
                            str_value = str_value[:-2]
                        string_row.append(str_value)
                rows.append(string_row)

            cursor.close()

            # Cria DataFrame com todos os dados como string
            df = pd.DataFrame(rows, columns=columns, dtype=str)

            # Salva em CSV
            output_path = f"/app/results/{csv_filename}"
            df.to_csv(output_path, index=False, encoding='utf-8', quoting=1)

            print("✓ Query executada com sucesso!")
            print(f"✓ {len(df)} registros salvos em {output_path}")
            print("✓ Todos os valores mantidos como string "
                  "(0/1 não convertidos)")
            return True

        except Exception as e:
            print(f"✗ Erro ao executar query: {e}")
            return False

    def execute_sql_file_to_csv(self, sql_file_path, csv_filename,
                                chunk_size=10000):
        """Lê arquivo SQL e executa, salvando resultado em CSV"""
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_query = file.read()

            print(f"✓ Arquivo SQL lido: {sql_file_path}")
            return self.execute_sql_to_csv(sql_query, csv_filename, chunk_size)

        except Exception as e:
            print(f"✗ Erro ao ler arquivo SQL: {e}")
            return False

    def batch_process(self, scripts_config):
        """Processa múltiplos scripts SQL

        Args:
            scripts_config: Lista de dicionários com 'sql_file' e 'csv_output'

        """
        results = []

        for config in scripts_config:
            sql_file = config.get('sql_file')
            csv_output = config.get('csv_output')

            if not sql_file or not csv_output:
                print(f"✗ Configuração inválida: {config}")
                continue

            print(f"\n--- Processando {sql_file} ---")
            success = self.execute_sql_file_to_csv(sql_file, csv_output)

            results.append({
                'sql_file': sql_file,
                'csv_output': csv_output,
                'success': success
            })

        return results

    def test_network_connectivity(self):
        """Testa conectividade de rede com o servidor"""
        print("\n--- Teste de Conectividade de Rede ---")

        # Extrai o servidor e porta
        server_parts = self.server.split(',')
        server_host = server_parts[0]
        server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433

        print(f"Testando conectividade para {server_host}:{server_port}")

        # Teste de conectividade TCP
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((server_host, server_port))
            sock.close()

            if result == 0:
                print(f"✓ Conectividade TCP OK para "
                      f"{server_host}:{server_port}")
                return True
            else:
                print(f"✗ Não foi possível conectar TCP para "
                      f"{server_host}:{server_port}")
                return False

        except Exception as e:
            print(f"✗ Erro no teste de conectividade: {e}")
            return False

    def test_connection(self):
        """Testa a conexão executando uma query simples"""
        if not self.connection:
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 as test")
            cursor.fetchone()
            cursor.close()
            print("✓ Teste de conexão com banco OK")
            return True
        except Exception as e:
            print(f"✗ Erro no teste de conexão: {e}")
            return False

    def close(self):
        """Fecha a conexão com o banco"""
        if self.connection:
            self.connection.close()
            print("✓ Conexão fechada")


# Função principal
def main():
    print("=" * 50)
    print("MSSQL to CSV Converter")
    print("=" * 50)

    # Inicializa a conexão
    db = SQLToCsv()

    # Lista drivers disponíveis
    db.list_available_drivers()

    # Conecta ao banco
    if not db.connect():
        print("Falha na conexão. Executando diagnósticos...")

        # Testa conectividade de rede
        if db.test_network_connectivity():
            print("\n✓ Conectividade de rede OK")
            print("- Problema pode ser nas credenciais ou "
                  "configuração do SQL Server")
        else:
            print("\n✗ Problema de conectividade de rede detectado")

        print("\nDicas de solução:")
        print("1. Verifique se o servidor está correto no .env")
        print("2. Para SQL Azure: use 'servidor.database.windows.net'")
        print("3. Para instâncias nomeadas: use 'servidor\\instancia'")
        print("4. Verifique se o firewall permite conexões na porta 1433")
        print("5. Para containers locais, use 'host.docker.internal'")
        print("6. Verifique se o usuário 'GESP_ABR' existe e tem permissões")
        print("7. Teste se SQL Server Authentication está habilitado")
        return

    # Testa a conexão
    if not db.test_connection():
        print("Falha no teste de conexão.")
        db.close()
        return

    try:
        # Exemplo de query simples para teste
        test_query = """
        SELECT
            'Teste' as tipo,
            GETDATE() as data_execucao,
            @@VERSION as versao_sql
        """

        print("\n--- Executando query de teste ---")
        db.execute_sql_to_csv(test_query, "teste_conexao.csv")

        # Processa scripts SQL em lote do diretório /app/sql_scripts
        sql_scripts_dir = "/app/sql_scripts"

        if os.path.exists(sql_scripts_dir) and os.path.isdir(sql_scripts_dir):
            print(f"\n--- Processamento em Lote ---")
            print(f"Buscando scripts SQL em: {sql_scripts_dir}")

            # Lista todos os arquivos .sql no diretório
            sql_files = []
            for file in os.listdir(sql_scripts_dir):
                if file.lower().endswith('.sql'):
                    sql_files.append(file)

            if sql_files:
                print(f"✓ Encontrados {len(sql_files)} arquivo(s) SQL:")
                for file in sorted(sql_files):
                    print(f"  - {file}")

                # Prepara configuração para batch_process
                scripts_config = []
                for sql_file in sorted(sql_files):
                    # Remove extensão .sql e adiciona .csv
                    csv_filename = sql_file[:-4] + ".csv"
                    scripts_config.append({
                        'sql_file': os.path.join(sql_scripts_dir, sql_file),
                        'csv_output': csv_filename
                    })

                # Executa processamento em lote
                print("\n--- Iniciando Processamento em Lote ---")
                results = db.batch_process(scripts_config)

                # Relatório final
                print("\n--- Relatório Final ---")
                total_scripts = len(results)
                successful_scripts = sum(1 for r in results if r['success'])
                failed_scripts = total_scripts - successful_scripts

                print(f"Total de scripts processados: {total_scripts}")
                print(f"✓ Sucessos: {successful_scripts}")
                print(f"✗ Falhas: {failed_scripts}")

                if failed_scripts > 0:
                    print("\nScripts que falharam:")
                    for result in results:
                        if not result['success']:
                            print(f"  ✗ {os.path.basename(result['sql_file'])}")

                if successful_scripts > 0:
                    print("\nCSVs gerados com sucesso:")
                    for result in results:
                        if result['success']:
                            print(f"  ✓ {result['csv_output']}")
            else:
                print("✗ Nenhum arquivo .sql encontrado no diretório")
        else:
            print(f"\n⚠️  Diretório {sql_scripts_dir} não encontrado")
            print("Para usar o processamento em lote:")
            print("1. Crie o diretório sql_scripts na raiz do projeto")
            print("2. Adicione seus arquivos .sql neste diretório")
            print("3. Execute novamente o container")

    except Exception as e:
        print(f"✗ Erro durante execução: {e}")

    finally:
        db.close()


if __name__ == "__main__":
    main()
