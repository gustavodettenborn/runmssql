#!/usr/bin/env python3
"""
Script de Validação Pós-Migração
Verifica se todos os arquivos foram movidos corretamente e se as funcionalidades ainda funcionam
"""

import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class MigrationValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.success_count = 0
        self.total_checks = 0

    def log_success(self, message: str):
        print(f"✅ {message}")
        self.success_count += 1

    def log_warning(self, message: str):
        print(f"⚠️  {message}")
        self.warnings.append(message)

    def log_error(self, message: str):
        print(f"❌ {message}")
        self.errors.append(message)

    def check_file_exists(self, filepath: str, description: str = None) -> bool:
        """Verifica se um arquivo existe"""
        self.total_checks += 1
        if os.path.exists(filepath):
            desc = description or filepath
            self.log_success(f"Arquivo encontrado: {desc}")
            return True
        else:
            desc = description or filepath
            self.log_error(f"Arquivo não encontrado: {desc}")
            return False

    def check_directory_structure(self) -> bool:
        """Verifica se a estrutura de diretórios foi criada corretamente"""
        print("\n🏗️  Verificando estrutura de diretórios...")

        required_dirs = [
            "src/main",
            "src/utils",
            "src/config",
            "tests/unit",
            "tests/integration",
            "tests/demo",
            "scripts/deployment",
            "scripts/setup",
            "scripts/testing",
            "docker/stacks",
            "docker/configs",
            "docs",
            "data/results",
            "data/sql_scripts",
            "config"
        ]

        all_exist = True
        for directory in required_dirs:
            self.total_checks += 1
            if os.path.exists(directory) and os.path.isdir(directory):
                self.log_success(f"Diretório: {directory}")
            else:
                self.log_error(f"Diretório não encontrado: {directory}")
                all_exist = False

        return all_exist

    def check_python_files(self) -> bool:
        """Verifica se os arquivos Python foram movidos corretamente"""
        print("\n🐍 Verificando arquivos Python...")

        python_files = [
            ("src/main/run_sql_csv.py", "Aplicação principal"),
            ("src/utils/load_env.py", "Utilitário de ambiente"),
            ("tests/integration/test_connection_secure.py", "Teste de conexão segura"),
            ("tests/demo/demo_secure_testing.py", "Demo de testes")
        ]

        all_exist = True
        for filepath, description in python_files:
            if not self.check_file_exists(filepath, description):
                all_exist = False

        return all_exist

    def check_shell_scripts(self) -> bool:
        """Verifica se os scripts shell foram movidos corretamente"""
        print("\n🖥️  Verificando scripts shell...")

        shell_scripts = [
            ("scripts/deployment/swarm-deploy.sh", "Script de deploy Swarm"),
            ("scripts/setup/mssql.sh", "Script de instalação MSSQL"),
            ("scripts/testing/test_automated.sh", "Script de teste automatizado")
        ]

        all_exist = True
        for filepath, description in shell_scripts:
            if not self.check_file_exists(filepath, description):
                all_exist = False
            else:
                # Verificar se o script é executável
                self.total_checks += 1
                if os.access(filepath, os.X_OK):
                    self.log_success(f"Script executável: {description}")
                else:
                    self.log_warning(f"Script não executável: {description}")

        return all_exist

    def check_docker_files(self) -> bool:
        """Verifica se os arquivos Docker foram movidos corretamente"""
        print("\n🐳 Verificando arquivos Docker...")

        docker_files = [
            ("docker/Dockerfile", "Dockerfile principal"),
            ("docker/stacks/docker-stack.yml", "Docker Stack principal"),
            ("docker/stacks/docker-stack-dev.yml", "Docker Stack desenvolvimento")
        ]

        all_exist = True
        for filepath, description in docker_files:
            if not self.check_file_exists(filepath, description):
                all_exist = False

        return all_exist

    def check_python_imports(self) -> bool:
        """Verifica se os imports Python ainda funcionam"""
        print("\n📦 Verificando imports Python...")

        # Adicionar src ao path para testar imports
        src_path = os.path.abspath("src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        imports_to_test = [
            ("src.utils.load_env", "Utilitário load_env"),
            ("src.main.run_sql_csv", "Aplicação principal")
        ]

        all_imports_work = True
        for module_name, description in imports_to_test:
            self.total_checks += 1
            try:
                # Verificar se o arquivo existe primeiro
                module_parts = module_name.split('.')
                module_path = os.path.join(*module_parts) + '.py'

                if not os.path.exists(module_path):
                    self.log_error(f"Arquivo não encontrado: {module_path}")
                    all_imports_work = False
                    continue

                # Tentar importar
                spec = importlib.util.spec_from_file_location(module_name, module_path)
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                    self.log_success(f"Import funcionando: {description}")
                else:
                    self.log_error(f"Não foi possível criar spec para: {description}")
                    all_imports_work = False

            except Exception as e:
                self.log_error(f"Erro ao importar {description}: {str(e)}")
                all_imports_work = False

        return all_imports_work

    def check_convenience_scripts(self) -> bool:
        """Verifica se os scripts de conveniência foram criados"""
        print("\n⚙️  Verificando scripts de conveniência...")

        scripts = [
            ("run_app.sh", "Script para executar aplicação"),
            ("run_tests.sh", "Script para executar testes"),
            ("dev_setup.sh", "Script de configuração de desenvolvimento")
        ]

        all_exist = True
        for script, description in scripts:
            if not self.check_file_exists(script, description):
                all_exist = False
            else:
                # Verificar se é executável
                self.total_checks += 1
                if os.access(script, os.X_OK):
                    self.log_success(f"Script executável: {description}")
                else:
                    self.log_warning(f"Script não executável: {description}")

        return all_exist

    def check_configuration_files(self) -> bool:
        """Verifica se os arquivos de configuração estão corretos"""
        print("\n⚙️  Verificando arquivos de configuração...")

        config_files = [
            ("requirements.txt", "Dependências Python"),
            ("config/.env.example", "Template de configuração"),
            (".env.example", "Template original (deve existir)"),
            ("runmssql.code-workspace", "Workspace VS Code")
        ]

        all_exist = True
        for filepath, description in config_files:
            if not self.check_file_exists(filepath, description):
                all_exist = False

        return all_exist

    def generate_report(self) -> None:
        """Gera relatório final da validação"""
        print("\n" + "="*60)
        print("📊 RELATÓRIO DE VALIDAÇÃO")
        print("="*60)

        print(f"✅ Verificações bem-sucedidas: {self.success_count}")
        print(f"⚠️  Avisos: {len(self.warnings)}")
        print(f"❌ Erros: {len(self.errors)}")
        print(f"📊 Total de verificações: {self.total_checks}")

        success_rate = (self.success_count / self.total_checks * 100) if self.total_checks > 0 else 0
        print(f"📈 Taxa de sucesso: {success_rate:.1f}%")

        if self.warnings:
            print(f"\n⚠️  AVISOS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"   • {warning}")

        if self.errors:
            print(f"\n❌ ERROS ({len(self.errors)}):")
            for error in self.errors:
                print(f"   • {error}")

        print("\n" + "="*60)

        if len(self.errors) == 0:
            print("🎉 MIGRAÇÃO VALIDADA COM SUCESSO!")
            print("✅ Todos os arquivos foram movidos corretamente")
            print("✅ A estrutura está funcionando corretamente")

            if len(self.warnings) > 0:
                print(f"⚠️  {len(self.warnings)} avisos encontrados - verifique se necessário")
        else:
            print("❌ MIGRAÇÃO POSSUI ERROS!")
            print("🔧 Corrija os erros listados acima")
            print("💡 Execute o script de rollback se necessário")

        return len(self.errors) == 0

    def run_validation(self) -> bool:
        """Executa todas as validações"""
        print("🔍 INICIANDO VALIDAÇÃO PÓS-MIGRAÇÃO")
        print("="*60)

        # Executar todas as verificações
        checks = [
            self.check_directory_structure,
            self.check_python_files,
            self.check_shell_scripts,
            self.check_docker_files,
            self.check_python_imports,
            self.check_convenience_scripts,
            self.check_configuration_files
        ]

        for check in checks:
            try:
                check()
            except Exception as e:
                self.log_error(f"Erro durante verificação: {str(e)}")

        # Gerar relatório
        return self.generate_report()

def main():
    """Função principal"""
    validator = MigrationValidator()

    # Verificar se estamos no diretório correto
    if not os.path.exists("requirements.txt"):
        print("❌ Execute este script no diretório raiz do projeto!")
        sys.exit(1)

    # Executar validação
    success = validator.run_validation()

    # Retornar código de saída apropriado
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
