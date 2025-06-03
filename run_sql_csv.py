import os
from datetime import datetime

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
        self.trusted_connection = os.getenv('MSSQL_TRUSTED_CONNECTION', 'false').lower() == 'true'

        self.connection = None

        print(f"Configuração carregada:")
        print(f"  Servidor: {self.server}")
        print(f"  Database: {self.database}")
        print(f"  Trusted Connection: {self.trusted_connection}")
        if not self.trusted_connection:
            print(f"  Username: {self.username}")

    def connect(self):
        """Estabelece conexão com o banco"""
        try:
            if self.trusted_connection:
                # Autenticação Windows
                conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes;TrustServerCertificate=yes;ConnectionTimeout=30;LoginTimeout=30;"
            else:
                # Autenticação SQL Server
                conn_str = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};TrustServerCertificate=yes;ConnectionTimeout=30;LoginTimeout=30;"

            print(f"Tentando conectar com: SERVER={self.server}, DATABASE={self.database}")
            print("String de conexão (sem senha):", conn_str.replace(f"PWD={self.password}", "PWD=***"))

            self.connection = pyodbc.connect(conn_str)
            print(f"✓ Conectado ao banco {self.database} no servidor {self.server}")
            return True

        except Exception as e:
            print(f"✗ Erro ao conectar: {e}")
            print("Possíveis causas:")
            print("  1. Servidor inacessível ou nome incorreto")
            print("  2. Porta 1433 bloqueada por firewall")
            print("  3. Credenciais incorretas")
            print("  4. Rede do container sem acesso ao servidor")
            return False

    def execute_sql_to_csv(self, sql_query, csv_filename, chunk_size=10000):
        """Executa query SQL e salva resultado em CSV"""
        if not self.connection:
            print("✗ Sem conexão com o banco")
            return False

        try:
            print(f"\nExecutando query...")
            print(f"Query: {sql_query[:100]}...")

            # Executa a query usando apenas cursor pyodbc
            cursor = self.connection.cursor()
            cursor.execute(sql_query)

            # Obtém os nomes das colunas
            columns = [column[0] for column in cursor.description]

            # Prepara o arquivo CSV manualmente
            output_path = f"/app/results/{csv_filename}"

            with open(output_path, 'w', encoding='utf-8', newline='') as csvfile:
                import csv
                writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

                # Escreve o cabeçalho
                writer.writerow(columns)

                # Processa linha por linha
                row_count = 0
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break

                    for row in rows:
                        # Converte cada valor para string preservando tipos originais
                        clean_row = []
                        for value in row:
                            # print(f"Valor original: {value} (tipo: {type(value)})")
                            if value is None:
                                clean_row.append('')
                            elif isinstance(value, bool):
                                # Preserva booleanos como True/False
                                # clean_row.append(str(value))
                                if value:
                                    clean_row.append("1")
                                else:
                                    clean_row.append("0")
                            elif isinstance(value, (int, float)):
                                # Para números, converte diretamente
                                if isinstance(value, float) and value.is_integer():
                                    clean_row.append(str(int(value)))
                                else:
                                    clean_row.append(str(value))
                            else:
                                # Para outros tipos, converte para string
                                clean_row.append(str(value))

                        writer.writerow(clean_row)
                        row_count += 1

            cursor.close()

            print(f"✓ Query executada com sucesso!")
            print(f"✓ {row_count} registros salvos em {output_path}")
            print(f"✓ Valores preservados sem conversão automática")
            return True

        except Exception as e:
            print(f"✗ Erro ao executar query: {e}")
            return False

    def execute_sql_file_to_csv(self, sql_file_path, csv_filename, chunk_size=10000):
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
            sql_file = config["sql_file"]
            csv_output = config["csv_output"]

            print(f"\n--- Processando {sql_file} ---")
            success = self.execute_sql_file_to_csv(sql_file, csv_output)

            results.append(
                {
                    "sql_file": sql_file,
                    "csv_output": csv_output,
                    "success": success,
                    "timestamp": datetime.now(),
                },
            )

        return results

    def test_network_connectivity(self):
        """Testa conectividade de rede com o servidor"""
        import socket
        import subprocess

        print(f"\n--- Teste de Conectividade de Rede ---")

        # Extrai o servidor e porta
        server_parts = self.server.split(',')
        server_host = server_parts[0]
        server_port = int(server_parts[1]) if len(server_parts) > 1 else 1433

        print(f"Testando conectividade para {server_host}:{server_port}")

        # Teste de resolução DNS
        try:
            import socket
            ip = socket.gethostbyname(server_host)
            print(f"✓ DNS resolvido: {server_host} -> {ip}")
        except Exception as e:
            print(f"✗ Erro na resolução DNS: {e}")
            return False

        # Teste de conectividade TCP
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((server_host, server_port))
            sock.close()

            if result == 0:
                print(f"✓ Porta {server_port} acessível")
                return True
            else:
                print(f"✗ Porta {server_port} inacessível (código: {result})")
                return False
        except Exception as e:
            print(f"✗ Erro no teste de conectividade: {e}")
            return False
        """Testa a conexão executando uma query simples"""
        if not self.connection:
            print("Conexão não estabelecida.")
            return False

        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT @@VERSION as version, GETDATE() as current_time")
            row = cursor.fetchone()
            print(f"✓ Teste de conexão bem-sucedido!")
            print(f"  Versão: {row.version}")
            print(f"  Data/Hora: {row.current_time}")
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

    # Conecta ao banco
    if not db.connect():
        print("Falha na conexão. Executando diagnósticos...")

        # Testa conectividade de rede
        if db.test_network_connectivity():
            print("\n✓ Conectividade de rede OK - problema pode ser nas credenciais ou configuração do SQL Server")
        else:
            print("\n✗ Problema de conectividade de rede detectado")

        print("\nDicas de solução:")
        print("1. Verifique se o servidor está correto no .env")
        print("2. Para SQL Azure: use 'servidor.database.windows.net'")
        print("3. Para instâncias nomeadas: use 'servidor\\instancia' ou 'servidor,porta'")
        print("4. Verifique se o firewall permite conexões na porta 1433")
        print("5. Para containers locais, use 'host.docker.internal' em vez de 'localhost'")
        return

    # Testa a conexão
    if not db.test_network_connectivity():
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

        print(f"\n--- Executando query de teste ---")
        db.execute_sql_to_csv(test_query, "teste_conexao.csv")

        # Processa scripts SQL se existirem
        sql_scripts_dir = "/app/sql_scripts"
        if os.path.exists(sql_scripts_dir):
            sql_files = [f for f in os.listdir(sql_scripts_dir) if f.endswith('.sql')]

            if sql_files:
                print(f"\n--- Encontrados {len(sql_files)} scripts SQL ---")
                scripts_config = []

                for sql_file in sql_files:
                    sql_path = os.path.join(sql_scripts_dir, sql_file)
                    csv_name = sql_file.replace('.sql', '.csv')
                    scripts_config.append({
                        "sql_file": sql_path,
                        "csv_output": csv_name
                    })

                results = db.batch_process(scripts_config)

                print("\n=== RESUMO DO PROCESSAMENTO ===")
                for result in results:
                    status = "✓ Sucesso" if result["success"] else "✗ Erro"
                    sql_name = os.path.basename(result['sql_file'])
                    print(f"{sql_name} -> {result['csv_output']}: {status}")
            else:
                print(f"\nNenhum script SQL encontrado em {sql_scripts_dir}")
        else:
            print(f"\nDiretório {sql_scripts_dir} não encontrado")

    except Exception as e:
        print(f"✗ Erro durante execução: {e}")

    finally:
        # Fecha conexão
        db.close()
        print("\n" + "=" * 50)
        print("Processamento concluído!")
        print("Verifique os resultados na pasta ./results")
        print("=" * 50)


if __name__ == "__main__":
    main()