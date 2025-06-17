#!/usr/bin/env python3
"""
test_connection_secure.py - Teste de conexão seguro com variáveis de ambiente
Testa a conectividade sem expor credenciais sensíveis
"""

import os
import socket
import sys
import warnings
from typing import Any, Dict, Optional

# Suprimir warnings desnecessários
warnings.filterwarnings('ignore', category=UserWarning)

# Tentar importar drivers
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

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


class SecureConnectionTester:
    """Testa conexões MSSQL de forma segura sem expor credenciais"""

    def __init__(self):
        """Inicializa o testador com variáveis de ambiente"""
        self.server = os.getenv('MSSQL_SERVER')
        self.port = os.getenv('MSSQL_PORT', '1433')
        self.database = os.getenv('MSSQL_DATABASE')
        self.username = os.getenv('MSSQL_USERNAME')
        self.password = os.getenv('MSSQL_PASSWORD')
        self.trusted_connection = os.getenv('MSSQL_TRUSTED_CONNECTION', 'false').lower() == 'true'

        # Configurações opcionais
        self.tds_version = os.getenv('TDS_VERSION', '7.0')
        self.encrypt = os.getenv('MSSQL_ENCRYPT', 'false').lower() == 'true'
        self.trust_cert = os.getenv('MSSQL_TRUST_SERVER_CERTIFICATE', 'true').lower() == 'true'
        self.connection_timeout = int(os.getenv('MSSQL_CONNECTION_TIMEOUT', '30'))

        self.results = {
            'environment_check': False,
            'network_connectivity': False,
            'driver_availability': {},
            'pyodbc_connection': False,
            'pymssql_connection': False,
            'query_execution': False
        }

    def mask_sensitive_info(self, text: str, sensitive_value: str) -> str:
        """Mascara informações sensíveis para logs seguros"""
        if not sensitive_value or len(sensitive_value) < 3:
            return text

        masked = sensitive_value[:2] + '*' * (len(sensitive_value) - 4) + sensitive_value[-2:]
        return text.replace(sensitive_value, masked)

    def safe_print(self, message: str):
        """Print seguro que mascara credenciais"""
        safe_message = message
        if self.password:
            safe_message = self.mask_sensitive_info(safe_message, self.password)
        if self.username and len(self.username) > 4:
            safe_message = self.mask_sensitive_info(safe_message, self.username)
        print(safe_message)

    def check_environment_variables(self) -> bool:
        """Verifica se as variáveis de ambiente necessárias estão definidas"""
        print("=== Verificação de Variáveis de Ambiente ===")

        required_vars = ['MSSQL_SERVER', 'MSSQL_DATABASE']
        if not self.trusted_connection:
            required_vars.extend(['MSSQL_USERNAME', 'MSSQL_PASSWORD'])

        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)

        if missing_vars:
            print(f"❌ Variáveis obrigatórias ausentes: {', '.join(missing_vars)}")
            return False

        # Mostrar configuração de forma segura
        server_masked = self.mask_sensitive_info("", self.server) if self.server else "NOT_SET"
        database_masked = self.mask_sensitive_info("", self.database) if self.database else "NOT_SET"

        print(f"✅ Servidor: {server_masked}")
        print(f"✅ Porta: {self.port}")
        print(f"✅ Database: {database_masked}")
        print(f"✅ TDS Version: {self.tds_version}")
        print(f"✅ Encrypt: {self.encrypt}")
        print(f"✅ Trust Certificate: {self.trust_cert}")

        if not self.trusted_connection:
            print(f"✅ Username: {'*' * len(self.username) if self.username else 'NOT_SET'}")
            print(f"✅ Password: {'*' * len(self.password) if self.password else 'NOT_SET'}")
        else:
            print("✅ Using Trusted Connection")

        self.results['environment_check'] = True
        return True

    def test_network_connectivity(self) -> bool:
        """Testa conectividade de rede com o servidor"""
        print("\n=== Teste de Conectividade de Rede ===")

        if not self.server:
            print("❌ Servidor não definido")
            return False

        try:
            # Test basic network connectivity
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)
            result = sock.connect_ex((self.server, int(self.port)))
            sock.close()

            if result == 0:
                server_masked = self.mask_sensitive_info("", self.server)
                print(f"✅ Conectividade de rede OK para {server_masked}:{self.port}")
                self.results['network_connectivity'] = True
                return True
            else:
                server_masked = self.mask_sensitive_info("", self.server)
                print(f"❌ Falha na conectividade para {server_masked}:{self.port}")
                return False

        except Exception as e:
            print(f"❌ Erro na conectividade: {str(e)}")
            return False

    def check_driver_availability(self) -> Dict[str, bool]:
        """Verifica disponibilidade dos drivers"""
        print("\n=== Verificação de Drivers ===")

        drivers = {
            'pyodbc': PYODBC_AVAILABLE,
            'pymssql': PYMSSQL_AVAILABLE,
            'pandas': PANDAS_AVAILABLE
        }

        for driver, available in drivers.items():
            status = "✅" if available else "❌"
            print(f"{status} {driver}: {'Disponível' if available else 'Não disponível'}")

        # Verificar drivers ODBC se pyodbc estiver disponível
        if PYODBC_AVAILABLE:
            try:
                odbc_drivers = pyodbc.drivers()
                print(f"\n📋 Drivers ODBC encontrados ({len(odbc_drivers)}):")
                for driver in odbc_drivers:
                    print(f"   • {driver}")
                drivers['odbc_drivers'] = odbc_drivers
            except Exception as e:
                print(f"❌ Erro ao listar drivers ODBC: {e}")
                drivers['odbc_drivers'] = []

        self.results['driver_availability'] = drivers
        return drivers

    def test_pyodbc_connection(self) -> bool:
        """Testa conexão usando PyODBC"""
        print("\n=== Teste de Conexão PyODBC ===")

        if not PYODBC_AVAILABLE:
            print("❌ PyODBC não disponível")
            return False

        if not self.username or not self.password:
            print("❌ Credenciais não fornecidas")
            return False

        # Testar diferentes drivers ODBC
        drivers_to_test = [
            "ODBC Driver 17 for SQL Server",
            "ODBC Driver 18 for SQL Server",
            "FreeTDS"
        ]

        for driver in drivers_to_test:
            try:
                print(f"🔄 Testando driver: {driver}")

                conn_str = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={self.server},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"Encrypt={'yes' if self.encrypt else 'no'};"
                    f"TrustServerCertificate={'yes' if self.trust_cert else 'no'};"
                    f"Connection Timeout={self.connection_timeout};"
                )

                # Conectar e testar query simples
                with pyodbc.connect(conn_str, timeout=self.connection_timeout) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT @@VERSION")
                    version = cursor.fetchone()[0]

                    # Mascarar informações sensíveis na versão
                    version_safe = self.mask_sensitive_info(version, self.server) if self.server else version
                    print(f"✅ Conexão PyODBC bem-sucedida com {driver}")
                    print(f"   SQL Server Version: {version_safe[:100]}...")

                    self.results['pyodbc_connection'] = True
                    return True

            except Exception as e:
                error_msg = str(e)
                # Mascarar credenciais em mensagens de erro
                if self.password:
                    error_msg = error_msg.replace(self.password, "***")
                if self.username:
                    error_msg = error_msg.replace(self.username, "***")
                print(f"❌ Falha com {driver}: {error_msg}")
                continue

        print("❌ Falha em todos os drivers PyODBC")
        return False

    def test_pymssql_connection(self) -> bool:
        """Testa conexão usando PyMSSQL"""
        print("\n=== Teste de Conexão PyMSSQL ===")

        if not PYMSSQL_AVAILABLE:
            print("❌ PyMSSQL não disponível")
            return False

        if not self.username or not self.password:
            print("❌ Credenciais não fornecidas")
            return False

        try:
            print("🔄 Testando conexão PyMSSQL...")

            with pymssql.connect(
                server=self.server,
                port=int(self.port),
                user=self.username,
                password=self.password,
                database=self.database,
                timeout=self.connection_timeout,
                login_timeout=self.connection_timeout,
                tds_version=self.tds_version
            ) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT @@VERSION")
                version = cursor.fetchone()[0]

                # Mascarar informações sensíveis
                version_safe = self.mask_sensitive_info(version, self.server) if self.server else version
                print(f"✅ Conexão PyMSSQL bem-sucedida")
                print(f"   SQL Server Version: {version_safe[:100]}...")

                self.results['pymssql_connection'] = True
                return True

        except Exception as e:
            error_msg = str(e)
            # Mascarar credenciais em mensagens de erro
            if self.password:
                error_msg = error_msg.replace(self.password, "***")
            if self.username:
                error_msg = error_msg.replace(self.username, "***")
            print(f"❌ Falha PyMSSQL: {error_msg}")
            return False

    def test_query_execution(self) -> bool:
        """Testa execução de query simples"""
        print("\n=== Teste de Execução de Query ===")

        if not (self.results['pyodbc_connection'] or self.results['pymssql_connection']):
            print("❌ Nenhuma conexão disponível para teste de query")
            return False

        try:
            # Usar o driver que funcionou
            if self.results['pyodbc_connection'] and PYODBC_AVAILABLE:
                print("🔄 Testando query com PyODBC...")
                conn_str = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={self.server},{self.port};"
                    f"DATABASE={self.database};"
                    f"UID={self.username};"
                    f"PWD={self.password};"
                    f"Encrypt={'yes' if self.encrypt else 'no'};"
                    f"TrustServerCertificate={'yes' if self.trust_cert else 'no'};"
                )

                with pyodbc.connect(conn_str) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT GETDATE() as CurrentTime, DB_NAME() as DatabaseName")
                    result = cursor.fetchone()
                    print(f"✅ Query executada: {result[0]}, Database: {result[1]}")

            elif self.results['pymssql_connection'] and PYMSSQL_AVAILABLE:
                print("🔄 Testando query com PyMSSQL...")
                with pymssql.connect(
                    server=self.server,
                    port=int(self.port),
                    user=self.username,
                    password=self.password,
                    database=self.database
                ) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT GETDATE() as CurrentTime, DB_NAME() as DatabaseName")
                    result = cursor.fetchone()
                    print(f"✅ Query executada: {result[0]}, Database: {result[1]}")

            self.results['query_execution'] = True
            return True

        except Exception as e:
            error_msg = str(e)
            if self.password:
                error_msg = error_msg.replace(self.password, "***")
            print(f"❌ Falha na execução de query: {error_msg}")
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Executa todos os testes de forma sequencial"""
        print("🔍 TESTE DE CONEXÃO SEGURO MSSQL")
        print("=" * 50)

        # Executar testes em ordem
        self.check_environment_variables()
        self.test_network_connectivity()
        self.check_driver_availability()
        self.test_pyodbc_connection()
        self.test_pymssql_connection()
        self.test_query_execution()

        # Resumo final
        print("\n" + "=" * 50)
        print("📊 RESUMO DOS TESTES")
        print("=" * 50)

        total_tests = 0
        passed_tests = 0

        for test_name, result in self.results.items():
            if isinstance(result, bool):
                total_tests += 1
                if result:
                    passed_tests += 1
                status = "✅ PASSOU" if result else "❌ FALHOU"
                print(f"{test_name.replace('_', ' ').title()}: {status}")

        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        print(f"\n📈 Taxa de Sucesso: {passed_tests}/{total_tests} ({success_rate:.1f}%)")

        if self.results['pyodbc_connection'] or self.results['pymssql_connection']:
            print("🎉 CONEXÃO MSSQL ESTABELECIDA COM SUCESSO!")
        else:
            print("💥 FALHA NA CONEXÃO MSSQL")

        return self.results


def main():
    """Função principal"""
    # Verificar se estamos em um ambiente Docker ou local
    is_docker = os.path.exists('/app/venv')

    if is_docker:
        print("🐳 Executando em ambiente Docker")
    else:
        print("💻 Executando em ambiente local")

    # Verificar arquivo .env
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"✅ Arquivo {env_file} encontrado")
        # Carregar .env se não estiver em Docker
        if not is_docker:
            try:
                with open(env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#') and '=' in line:
                            key, value = line.split('=', 1)
                            os.environ[key] = value
                print("✅ Variáveis do .env carregadas")
            except Exception as e:
                print(f"❌ Erro ao carregar .env: {e}")
    else:
        print(f"⚠️ Arquivo {env_file} não encontrado")

    # Executar testes
    tester = SecureConnectionTester()
    results = tester.run_all_tests()

    # Retornar código de saída apropriado
    if results['pyodbc_connection'] or results['pymssql_connection']:
        sys.exit(0)  # Sucesso
    else:
        sys.exit(1)  # Falha


if __name__ == "__main__":
    main()
