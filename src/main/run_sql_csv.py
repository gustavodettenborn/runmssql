#!/usr/bin/env python3
"""
run_sql_csv.py - Versão atualizada com fallback PyMSSQL e controle de execução
"""

import argparse
import hashlib
import json
import os
import socket
import traceback
import warnings
from datetime import datetime
from typing import Dict, List, Optional

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
    def __init__(self, execution_log_path=None):
        """Inicializa a conexão com SQL Server usando variáveis de ambiente"""
        if execution_log_path is None:
            execution_log_path = os.getenv('EXECUTION_LOG_PATH', '/app/results/execution_log.json')

        # Lê variáveis de ambiente
        self.server = os.getenv('MSSQL_SERVER', 'localhost')
        self.database = os.getenv('MSSQL_DATABASE', 'master')
        self.username = os.getenv('MSSQL_USERNAME')
        self.password = os.getenv('MSSQL_PASSWORD')
        self.trusted_connection = os.getenv(
            'MSSQL_TRUSTED_CONNECTION', 'false').lower() == 'true'

        self.connection = None
        self.connection_type = None  # 'pyodbc' ou 'pymssql'

        # Sistema de controle de execução
        self.execution_log_path = execution_log_path
        self.execution_log = self._load_execution_log()

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
                                chunk_size=10000, log_execution=True):
        """Lê arquivo SQL e executa, salvando resultado em CSV"""
        success = False
        error_msg = ""

        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_query = file.read()

            print(f"✓ Arquivo SQL lido: {sql_file_path}")
            success = self.execute_sql_to_csv(sql_query, csv_filename, chunk_size)

        except Exception as e:
            error_msg = str(e)
            print(f"✗ Erro ao ler arquivo SQL: {e}")
            success = False

        # Log da execução se solicitado
        if log_execution:
            self._log_execution_result(sql_file_path, csv_filename, success, error_msg)

        return success

    def batch_process(self, scripts_config, force_all=False):
        """Processa múltiplos scripts SQL com controle de execução"""
        results = []
        skipped = []

        for config in scripts_config:
            sql_file = config.get('sql_file')
            csv_output = config.get('csv_output')

            if not sql_file or not csv_output:
                print(f"✗ Configuração inválida: {config}")
                continue

            # Verifica se deve executar o script
            if not self._should_execute_script(sql_file, force_all):
                skipped.append({
                    'sql_file': sql_file,
                    'csv_output': csv_output,
                    'success': True,
                    'skipped': True
                })
                continue

            print(f"\n--- Processando {os.path.basename(sql_file)} ---")
            success = self.execute_sql_file_to_csv(sql_file, csv_output)

            results.append({
                'sql_file': sql_file,
                'csv_output': csv_output,
                'success': success,
                'skipped': False
            })

        return results, skipped

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

    def _load_execution_log(self) -> Dict:
        """Carrega o log de execução dos scripts"""
        try:
            if os.path.exists(self.execution_log_path):
                with open(self.execution_log_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                return {}
        except Exception as e:
            print(f"⚠️  Erro ao carregar log de execução: {e}")
            return {}

    def _save_execution_log(self):
        """Salva o log de execução dos scripts"""
        try:
            # Garante que o diretório existe
            os.makedirs(os.path.dirname(self.execution_log_path), exist_ok=True)

            with open(self.execution_log_path, 'w', encoding='utf-8') as f:
                json.dump(self.execution_log, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️  Erro ao salvar log de execução: {e}")

    def _get_file_hash(self, file_path: str) -> str:
        """Calcula hash do arquivo para detectar mudanças"""
        try:
            with open(file_path, 'rb') as f:
                return hashlib.md5(f.read()).hexdigest()
        except Exception:
            return ""

    def _should_execute_script(self, sql_file_path: str, force_all: bool = False) -> bool:
        """Determina se um script deve ser executado"""
        if force_all:
            return True

        file_name = os.path.basename(sql_file_path)
        current_hash = self._get_file_hash(sql_file_path)

        if file_name not in self.execution_log:
            print(f"  📄 {file_name}: Primeira execução")
            return True

        log_entry = self.execution_log[file_name]

        # Se o arquivo mudou, executar
        if log_entry.get('file_hash') != current_hash:
            print(f"  🔄 {file_name}: Arquivo modificado")
            return True

        # Se a última execução falhou, executar
        if not log_entry.get('success', False):
            print(f"  ❌ {file_name}: Última execução falhou")
            return True

        # Script já foi executado com sucesso e não mudou
        print(f"  ✅ {file_name}: Já executado com sucesso")
        return False

    def _log_execution_result(self, sql_file_path: str, csv_filename: str, success: bool, error_msg: str = ""):
        """Registra o resultado da execução de um script"""
        file_name = os.path.basename(sql_file_path)
        current_hash = self._get_file_hash(sql_file_path)

        self.execution_log[file_name] = {
            'file_path': sql_file_path,
            'csv_output': csv_filename,
            'file_hash': current_hash,
            'success': success,
            'execution_time': datetime.now().isoformat(),
            'error_message': error_msg
        }

        self._save_execution_log()

    def get_execution_summary(self) -> Dict:
        """Retorna resumo das execuções"""
        total = len(self.execution_log)
        successful = sum(1 for entry in self.execution_log.values() if entry.get('success', False))
        failed = total - successful

        return {
            'total_scripts': total,
            'successful': successful,
            'failed': failed,
            'last_executions': self.execution_log
        }

    def list_scripts_status(self, sql_scripts_dir: str) -> List[Dict]:
        """Lista status de todos os scripts SQL"""
        scripts_status = []

        if not os.path.exists(sql_scripts_dir):
            return scripts_status

        for file in os.listdir(sql_scripts_dir):
            if file.lower().endswith('.sql'):
                parts = file.split('_', 1)
                if len(parts) >= 2 and parts[0].isdigit():
                    file_path = os.path.join(sql_scripts_dir, file)
                    current_hash = self._get_file_hash(file_path)

                    if file in self.execution_log:
                        log_entry = self.execution_log[file]
                        status = "success" if log_entry.get('success', False) else "failed"

                        # Verifica se arquivo foi modificado
                        if log_entry.get('file_hash') != current_hash:
                            status = "modified"
                    else:
                        status = "never_executed"

                    scripts_status.append({
                        'file_name': file,
                        'file_path': file_path,
                        'status': status,
                        'last_execution': self.execution_log.get(file, {}).get('execution_time'),
                        'csv_output': self.execution_log.get(file, {}).get('csv_output')
                    })

        return sorted(scripts_status, key=lambda x: x['file_name'])


def get_execution_config():
    """Obtém configuração de execução a partir de argumentos CLI e variáveis de ambiente"""

    # Configura parser de argumentos
    parser = argparse.ArgumentParser(
        description='Sistema de Execução SQL com Controle de Estado',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  %(prog)s                           # Execução normal (apenas scripts pendentes)
  %(prog)s --force-all               # Executa todos os scripts
  %(prog)s --status                  # Mostra apenas o status dos scripts
  %(prog)s --reset-log               # Limpa o log e executa normalmente
  %(prog)s --scripts-dir ./queries   # Usa diretório customizado
  %(prog)s --force-all --scripts-dir /custom/path  # Combina opções

Variáveis de ambiente (sobrescritas pelos argumentos CLI):
  FORCE_ALL=true/false               # Força execução de todos os scripts
  STATUS_ONLY=true/false             # Apenas mostra status
  RESET_LOG=true/false               # Limpa log de execução
  SCRIPTS_DIR=/path/to/scripts       # Diretório dos scripts SQL
        """
    )

    parser.add_argument(
        '--version',
        action='version',
        version='%(prog)s 2.0 - Sistema de Execução SQL com Controle de Estado'
    )

    parser.add_argument(
        '--force-all',
        action='store_true',
        help='Força execução de todos os scripts, ignorando histórico'
    )

    parser.add_argument(
        '--status',
        action='store_true',
        help='Apenas mostra o status dos scripts sem executar'
    )

    parser.add_argument(
        '--reset-log',
        action='store_true',
        help='Limpa o log de execução antes de executar'
    )

    parser.add_argument(
        '--scripts-dir',
        type=str,
        default=None,
        help='Diretório contendo os scripts SQL (padrão: variável SCRIPTS_DIR ou /app/sql_scripts)'
    )

    # Parse dos argumentos
    args = parser.parse_args()

    class Config:
        def __init__(self, args):
            # Valores padrão
            default_scripts_dir = os.getenv('SCRIPTS_DIR', '/app/sql_scripts')

            # Prioridade: Argumentos CLI > Variáveis de ambiente > Padrão

            # FORCE_ALL
            if args.force_all:
                self.force_all = True
            else:
                force_all_env = os.getenv('FORCE_ALL', 'false').lower()
                self.force_all = force_all_env in ['true', '1', 'yes', 'on']

            # STATUS
            if args.status:
                self.status = True
            else:
                status_env = os.getenv('STATUS_ONLY', 'false').lower()
                self.status = status_env in ['true', '1', 'yes', 'on']

            # RESET_LOG
            if args.reset_log:
                self.reset_log = True
            else:
                reset_env = os.getenv('RESET_LOG', 'false').lower()
                self.reset_log = reset_env in ['true', '1', 'yes', 'on']

            # SCRIPTS_DIR
            if args.scripts_dir:
                self.scripts_dir = args.scripts_dir
            else:
                self.scripts_dir = os.getenv('SCRIPTS_DIR', default_scripts_dir)

            # Log das configurações para debug
            print(f"🔧 Configurações de execução:")
            print(f"   • FORCE_ALL: {self.force_all} {'(CLI)' if args.force_all else '(ENV)' if os.getenv('FORCE_ALL') else '(padrão)'}")
            print(f"   • STATUS_ONLY: {self.status} {'(CLI)' if args.status else '(ENV)' if os.getenv('STATUS_ONLY') else '(padrão)'}")
            print(f"   • RESET_LOG: {self.reset_log} {'(CLI)' if args.reset_log else '(ENV)' if os.getenv('RESET_LOG') else '(padrão)'}")
            print(f"   • SCRIPTS_DIR: {self.scripts_dir} {'(CLI)' if args.scripts_dir else '(ENV)' if os.getenv('SCRIPTS_DIR') else '(padrão)'}")

    return Config(args)


def display_scripts_status(db: SQLToCsv, sql_scripts_dir: str):
    """Exibe status detalhado dos scripts"""
    print("\n" + "="*70)
    print("STATUS DOS SCRIPTS SQL")
    print("="*70)

    scripts_status = db.list_scripts_status(sql_scripts_dir)

    if not scripts_status:
        print("❌ Nenhum script SQL encontrado")
        return

    # Contadores
    never_executed = sum(1 for s in scripts_status if s['status'] == 'never_executed')
    success = sum(1 for s in scripts_status if s['status'] == 'success')
    failed = sum(1 for s in scripts_status if s['status'] == 'failed')
    modified = sum(1 for s in scripts_status if s['status'] == 'modified')

    print(f"📊 Resumo: {len(scripts_status)} scripts encontrados")
    print(f"   ✅ Executados com sucesso: {success}")
    print(f"   ❌ Falharam: {failed}")
    print(f"   🔄 Modificados: {modified}")
    print(f"   📄 Nunca executados: {never_executed}")
    print()

    # Lista detalhada
    status_icons = {
        'success': '✅',
        'failed': '❌',
        'modified': '🔄',
        'never_executed': '📄'
    }

    status_names = {
        'success': 'Sucesso',
        'failed': 'Falhou',
        'modified': 'Modificado',
        'never_executed': 'Nunca executado'
    }

    for script in scripts_status:
        icon = status_icons.get(script['status'], '❓')
        status_name = status_names.get(script['status'], 'Desconhecido')

        print(f"{icon} {script['file_name']:40} | {status_name:15}", end="")

        if script['last_execution']:
            # Formata data
            try:
                dt = datetime.fromisoformat(script['last_execution'].replace('Z', '+00:00'))
                date_str = dt.strftime('%d/%m/%Y %H:%M')
                print(f" | {date_str}")
            except:
                print(f" | {script['last_execution']}")
        else:
            print(" | Nunca executado")

    print()

    # Scripts que serão executados na próxima vez
    to_execute = [s for s in scripts_status if s['status'] in ['never_executed', 'failed', 'modified']]
    if to_execute:
        print(f"🔄 Scripts que serão executados na próxima execução ({len(to_execute)}):")
        for script in to_execute:
            print(f"   • {script['file_name']}")
    else:
        print("✅ Todos os scripts foram executados com sucesso")

    print("="*70)


def main():
    """Função principal com controle de execução usando variáveis de ambiente"""
    print("\n" + "="*70)
    print("SISTEMA DE EXECUÇÃO SQL COM CONTROLE DE ESTADO")
    print("="*70)

    # Obtém configuração a partir de variáveis de ambiente
    config = get_execution_config()

    # Inicializa a conexão com SQL Server
    db = SQLToCsv()

    # Limpa log se solicitado
    if config.reset_log:
        print("\n🗑️  Limpando log de execução...")
        if os.path.exists(db.execution_log_path):
            os.remove(db.execution_log_path)
            print("✅ Log de execução limpo!")
        else:
            print("ℹ️  Log de execução não existe")
        db.execution_log = {}

    # Diretório de scripts SQL
    sql_scripts_dir = config.scripts_dir

    # Exibe status dos scripts e sai se STATUS_ONLY foi solicitado
    if config.status:
        display_scripts_status(db, sql_scripts_dir)
        return

    # Verifica se o diretório de scripts existe
    if not os.path.exists(sql_scripts_dir) or not os.path.isdir(sql_scripts_dir):
        print(f"\n❌ Diretório {sql_scripts_dir} não encontrado")
        print("Para usar o processamento em lote:")
        print("1. Crie o diretório sql_scripts na raiz do projeto")
        print("2. Adicione seus arquivos .sql neste diretório")
        print("3. Execute novamente o container")
        return

    # Tenta conectar ao banco antes de prosseguir
    print("\n--- Conectando ao SQL Server ---")
    if not db.connect():
        print("❌ Falha na conexão. Executando diagnósticos...")

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
        print("❌ Falha no teste de conexão.")
        db.close()
        return

    try:
        # Mostra resumo antes da execução
        scripts_status = db.list_scripts_status(sql_scripts_dir)
        if not scripts_status:
            print("❌ Nenhum script SQL encontrado no diretório")
            return

        # Contadores do status atual
        never_executed = sum(1 for s in scripts_status if s['status'] == 'never_executed')
        success = sum(1 for s in scripts_status if s['status'] == 'success')
        failed = sum(1 for s in scripts_status if s['status'] == 'failed')
        modified = sum(1 for s in scripts_status if s['status'] == 'modified')

        print(f"\n📊 Status atual: {len(scripts_status)} scripts encontrados")
        print(f"   ✅ Executados com sucesso: {success}")
        print(f"   ❌ Falharam: {failed}")
        print(f"   🔄 Modificados: {modified}")
        print(f"   📄 Nunca executados: {never_executed}")

        # Determina quais scripts executar
        if config.force_all:
            print(f"\n🔄 Modo FORCE_ALL ativado - executando TODOS os {len(scripts_status)} scripts")
            scripts_to_execute = [s['file_path'] for s in scripts_status]
        else:
            # Executa apenas scripts que precisam ser executados
            scripts_to_execute = []
            for script in scripts_status:
                if script['status'] in ['never_executed', 'failed', 'modified']:
                    scripts_to_execute.append(script['file_path'])

            if scripts_to_execute:
                print(f"\n⚡ Executando {len(scripts_to_execute)} scripts que precisam ser processados")
            else:
                print("\n✅ Todos os scripts já foram executados com sucesso!")
                print("💡 Use FORCE_ALL=true para re-executar todos os scripts")
                return

        # Lista dos scripts que serão executados
        print("\n📋 Scripts a serem executados:")
        for script_file in scripts_to_execute:
            script_name = os.path.basename(script_file)
            script_status_info = next((s for s in scripts_status if s['file_path'] == script_file), None)
            if script_status_info:
                status_emoji = {
                    'never_executed': '🆕',
                    'failed': '🔄',
                    'modified': '📝',
                    'success': '✅'
                }.get(script_status_info['status'], '❓')
                print(f"   {status_emoji} {script_name} ({script_status_info['status']})")

        print(f"\n🚀 Iniciando execução...")

        # Prepara configuração para batch_process
        scripts_config = []
        for script_file in scripts_to_execute:
            script_name = os.path.basename(script_file)
            # Remove extensão .sql e adiciona .csv
            csv_filename = script_name[:-4] + ".csv" if script_name.endswith('.sql') else script_name + ".csv"
            scripts_config.append({
                'sql_file': script_file,
                'csv_output': csv_filename
            })

        # Executa processamento em lote
        executed_results, skipped_scripts = db.batch_process(
            scripts_config,
            force_all=config.force_all
        )

        # Estatísticas finais
        executed_count = len(executed_results)
        success_count = sum(1 for r in executed_results if r['success'])
        failed_count = executed_count - success_count
        skipped_count = len(skipped_scripts)

        print(f"\n" + "="*70)
        print("RELATÓRIO FINAL")
        print("="*70)
        print(f"📊 Estatísticas:")
        print(f"   • Scripts executados: {executed_count}")
        print(f"   • Sucessos: {success_count}")
        print(f"   • Falhas: {failed_count}")
        print(f"   • Ignorados (já executados): {skipped_count}")

        # Lista de scripts executados com sucesso
        successful_csvs = [r['csv_output'] for r in executed_results if r['success'] and r.get('csv_output')]
        if successful_csvs:
            print(f"\n📁 Arquivos CSV gerados com sucesso ({len(successful_csvs)}):")
            for csv_file in successful_csvs:
                print(f"   📄 {csv_file}")

        # Lista de scripts que falharam
        failed_scripts = [r for r in executed_results if not r['success']]
        if failed_scripts:
            print(f"\n❌ Scripts que falharam ({len(failed_scripts)}):")
            for result in failed_scripts:
                script_name = os.path.basename(result['sql_file'])
                print(f"   ❌ {script_name}")

        # Recomendações finais
        print(f"\n💡 Próximos passos:")
        if failed_count > 0:
            print(f"   • Execute novamente para tentar os {failed_count} scripts que falharam")
            print("   • Use STATUS_ONLY=true para ver detalhes dos erros")
        elif executed_count > 0:
            print("   • Todos os scripts foram executados com sucesso!")

        if skipped_count > 0:
            print("   • Use FORCE_ALL=true para re-executar todos os scripts")

        print("   • Use STATUS_ONLY=true para ver o status completo de todos os scripts")

    except Exception as e:
        print(f"❌ Erro durante execução: {e}")
        print(f"📋 Detalhes do erro:\n{traceback.format_exc()}")

    finally:
        db.close()
        print("\n" + "="*70)


if __name__ == "__main__":
    main()
