#!/usr/bin/env python3
"""
run_sql_csv.py - Versão atualizada com fallback PyMSSQL
"""

import os
import socket
import warnings
from typing import Optional

import pandas as pd

# Tentar importar ambos os drivers
try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False
    pyodbc = None

try:
    import pymssql
    PYMSSQL_AVAILABLE = True
except ImportError:
    PYMSSQL_AVAILABLE = False
    pymssql = None

# Configurar legacy SSL para compatibilidade com SQL Server antigos
os.environ.setdefault('OPENSSL_CONF', '/etc/ssl/openssl_legacy.cnf')
os.environ.setdefault('FREETDSCONF', '/etc/freetds/freetds.conf')
os.environ.setdefault('TDSVER', '7.0')

# Suprimir warnings
warnings.filterwarnings('ignore', category=UserWarning)


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
        self.connection_type = None  # 'pyodbc' ou 'pymssql'

        print("==================================================")
        print("MSSQL to CSV Converter")
        print("==================================================")
        print("Configuração carregada:")
        print(f"  Servidor: {self.server}")
        print(f"  Database: {self.database}")
        print(f"  Trusted Connection: {self.trusted_connection}")
        if not self.trusted_connection:
            print(f"  Username: {self.username}")

        print(f"\nDrivers disponíveis:")
        print(f"  PyODBC: {'✓' if PYODBC_AVAILABLE else '✗'}")
        print(f"  PyMSSQL: {'✓' if PYMSSQL_AVAILABLE else '✗'}")

    def connect(self):
        """Estabelece conexão com o banco usando múltiplas estratégias"""
        print("\n--- Iniciando Conexão ---")

        # Verifica se as credenciais foram fornecidas
        if not self.username or not self.password:
            print("✗ ERRO: Usuário ou senha não fornecidos")
            print("Verifique as variáveis MSSQL_USERNAME e MSSQL_PASSWORD")
            return False

        # Tentar PyODBC primeiro, depois PyMSSQL
        if self._try_pyodbc_connection():
            return True

        if self._try_pymssql_connection():
            return True

        print("❌ FALHA: Não foi possível conectar com nenhum driver")
        return False

    def _try_pyodbc_connection(self):
        """Tenta conexão usando PyODBC"""
        print("\n🔄 Tentando PyODBC...")

        if not pyodbc:
            print("✗ PyODBC não disponível")
            return False

        # Lista drivers disponíveis
        drivers = pyodbc.drivers()
        print(f"Drivers ODBC encontrados: {len(drivers)}")

        if not drivers:
            print("✗ Nenhum driver ODBC disponível")
            return False

        # Tentar conexão com drivers disponíveis
        test_drivers = [
            "FreeTDS",
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server"
        ]

        for driver in test_drivers:
            if driver not in drivers:
                continue

            print(f"  Testando {driver}...")
            try:
                conn_str = self._build_pyodbc_connection_string(driver)
                self.connection = pyodbc.connect(conn_str, timeout=30)
                self.connection_type = 'pyodbc'
                print(f"✅ SUCESSO com {driver}")
                return True
            except Exception as e:
                print(f"  ✗ Falhou: {str(e)[:100]}...")

        return False

    def _try_pymssql_connection(self):
        """Tenta conexão usando PyMSSQL"""
        print("\n🔄 Tentando PyMSSQL...")

        if not pymssql:
            print("✗ PyMSSQL não disponível")
            return False

        # Extrair host e porta
        server_parts = self.server.split(',')
        host = server_parts[0]
        port = int(server_parts[1]) if len(server_parts) > 1 else 1433

        # Estratégias de TDS para máxima compatibilidade
        tds_versions = ['7.0', '7.2', '7.4']

        for tds_ver in tds_versions:
            print(f"  Testando TDS version {tds_ver}...")
            try:
                self.connection = pymssql.connect(
                    server=host,
                    port=port,
                    user=self.username,
                    password=self.password,
                    database=self.database,
                    timeout=30,
                    login_timeout=30,
                    as_dict=False,  # Compatibilidade com cursor padrão
                    tds_version=tds_ver
                )
                self.connection_type = 'pymssql'
                print(f"✅ SUCESSO com PyMSSQL (TDS {tds_ver})")

                # Teste rápido da conexão
                cursor = self.connection.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]
                cursor.close()
                print(f"    SQL Server: {version[:50]}...")

                return True
            except Exception as e:
                print(f"  ✗ TDS {tds_ver} falhou: {str(e)[:100]}...")

        return False

    def _build_pyodbc_connection_string(self, driver):
        """Constrói string de conexão PyODBC"""
        if 'FreeTDS' in driver:
            return (f"DRIVER={{{driver}}};"
                   f"SERVER={self.server};"
                   f"DATABASE={self.database};"
                   f"UID={self.username};"
                   f"PWD={self.password};"
                   f"TDS_Version=7.0;"
                   f"Port=1433;"
                   f"Encrypt=no;")
        else:
            return (f"DRIVER={{{driver}}};"
                   f"SERVER={self.server};"
                   f"DATABASE={self.database};"
                   f"UID={self.username};"
                   f"PWD={self.password};"
                   f"Encrypt=no;"
                   f"TrustServerCertificate=yes;")

    def test_tcp_connectivity(self):
        """Testa conectividade TCP básica"""
        print(f"\n--- Teste de Conectividade TCP ---")
        try:
            # Extrai host e porta
            parts = self.server.split(',')
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 1433

            print(f"Testando TCP {host}:{port}...")

            # Teste de socket TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((host, port))
            sock.close()

            if result == 0:
                print("✅ Conectividade TCP OK")
                return True
            else:
                print(f"❌ TCP Falhou (código: {result})")
                return False

        except Exception as e:
            print(f"❌ Erro no teste TCP: {e}")
            return False

    def execute_sql_to_csv(self, sql_query, csv_filename, chunk_size=10000):
        """Executa query SQL e salva resultado em CSV"""
        if not self.connection:
            print("✗ Sem conexão com o banco")
            return False

        try:
            # print("\nExecutando query...")
            # print(f"Query: {sql_query[:100]}...")

            # Executa a query
            cursor = self.connection.cursor()
            cursor.execute(sql_query)

            # Obtém os nomes das colunas
            columns = [column[0] for column in cursor.description]

            # Coleta todos os dados
            rows = []
            for row in cursor:
                # Converte cada valor para string, tratando None/NULL
                string_row = []
                for value in row:
                    if value is None:
                        string_row.append('')
                    elif type(value) is bool:
                        string_row.append(1 if value else 0)
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
            print("✓ Todos os valores mantidos como string")
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
        """Processa múltiplos scripts SQL"""
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


def main():
    """Função principal"""
    # Inicializa a conexão
    db = SQLToCsv()

    # Conecta ao banco
    if not db.connect():
        print("Falha na conexão. Executando diagnósticos...")

        # Testa conectividade de rede
        if db.test_tcp_connectivity():
            print("\n✓ Conectividade de rede OK")
            print("- Problema pode ser nas credenciais ou configuração do SQL Server")
        else:
            print("\n✗ Problema de conectividade de rede detectado")

        print("\nDicas de solução:")
        print("1. Verifique se o servidor está correto no .env")
        print("2. Verifique se o firewall permite conexões na porta 1433")
        print("3. Verifique se o usuário e senha estão corretos")
        print("4. Teste se SQL Server Authentication está habilitado")
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
            print("\n--- Processamento em Lote ---")
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
