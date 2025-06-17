#!/usr/bin/env python3
"""
Exemplo de uso dos scripts de teste seguro
Demonstra como usar os novos scripts sem expor credenciais
"""

import os
import subprocess
import sys
from pathlib import Path


def test_local_environment():
    """Testa o ambiente local"""
    print("🧪 TESTANDO AMBIENTE LOCAL")
    print("=" * 50)

    # Verificar se os arquivos existem
    required_files = [
        'test_connection_secure.py',
        'test_automated.sh',
        'load_env.py',
        '.env.example'
    ]

    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)

    if missing_files:
        print(f"❌ Arquivos faltando: {', '.join(missing_files)}")
        return False

    print("✅ Todos os arquivos necessários estão presentes")
    return True


def demonstrate_load_env():
    """Demonstra o uso do carregador de .env"""
    print("\n📋 DEMONSTRAÇÃO: load_env.py")
    print("-" * 30)

    try:
        # Listar variáveis sem mostrar valores
        result = subprocess.run(
            ['python3', 'load_env.py', '--env-file', '.env.example'],
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print("✅ Carregamento do .env.example:")
            print(result.stdout)
        else:
            print("❌ Erro no carregamento:")
            print(result.stderr)

    except Exception as e:
        print(f"❌ Erro ao executar load_env.py: {e}")


def demonstrate_secure_test():
    """Demonstra o teste seguro de conexão"""
    print("\n🔒 DEMONSTRAÇÃO: test_connection_secure.py")
    print("-" * 40)

    if not os.path.exists('.env'):
        print("⚠️ Arquivo .env não encontrado")
        print("   Copiando .env.example para demonstração...")
        try:
            with open('.env.example', 'r') as src:
                content = src.read()

            # Criar um .env temporário com valores de exemplo
            temp_content = content.replace('=""', '="exemplo"')
            with open('.env.temp', 'w') as dst:
                dst.write(temp_content)

            print("✅ Arquivo .env.temp criado para demonstração")

            # Executar teste com arquivo temporário
            env = os.environ.copy()
            env['PYTHONPATH'] = '.'

            # Simular algumas variáveis para o teste
            env.update({
                'MSSQL_SERVER': 'servidor_exemplo',
                'MSSQL_DATABASE': 'database_exemplo',
                'MSSQL_USERNAME': 'usuario_exemplo',
                'MSSQL_PASSWORD': 'senha_exemplo'
            })

            result = subprocess.run(
                ['python3', 'test_connection_secure.py'],
                env=env,
                capture_output=True,
                text=True
            )

            print("📋 Resultado do teste seguro:")
            print(result.stdout)

            if result.stderr:
                print("⚠️ Avisos/Erros:")
                print(result.stderr)

            # Limpar arquivo temporário
            if os.path.exists('.env.temp'):
                os.remove('.env.temp')

        except Exception as e:
            print(f"❌ Erro na demonstração: {e}")
    else:
        print("✅ Arquivo .env encontrado - executando teste real...")
        try:
            result = subprocess.run(
                ['python3', 'test_connection_secure.py'],
                capture_output=True,
                text=True,
                timeout=30
            )

            print("📋 Resultado do teste:")
            print(result.stdout[:1000])  # Limitar output

            if result.returncode == 0:
                print("✅ Teste concluído com sucesso")
            else:
                print("❌ Teste falhou (esperado sem servidor real)")

        except subprocess.TimeoutExpired:
            print("⏰ Teste interrompido por timeout (normal sem servidor)")
        except Exception as e:
            print(f"❌ Erro: {e}")


def demonstrate_automated_test():
    """Demonstra o script de teste automatizado"""
    print("\n🤖 DEMONSTRAÇÃO: test_automated.sh")
    print("-" * 35)

    try:
        # Verificar se o script é executável
        script_path = Path('test_automated.sh')
        if not os.access(script_path, os.X_OK):
            print("⚠️ Script não é executável, tornando executável...")
            os.chmod(script_path, 0o755)

        print("🔄 Executando script de teste automatizado...")
        print("   (Pode falhar na conexão real - isso é esperado)")

        result = subprocess.run(
            ['./test_automated.sh'],
            capture_output=True,
            text=True,
            timeout=30
        )

        print("📋 Resultado do teste automatizado:")
        print(result.stdout[:1500])  # Limitar output

        if result.returncode == 0:
            print("✅ Script executado com sucesso")
        else:
            print("❌ Script reportou falhas (esperado sem configuração real)")

    except subprocess.TimeoutExpired:
        print("⏰ Script interrompido por timeout")
    except Exception as e:
        print(f"❌ Erro ao executar script automatizado: {e}")


def show_security_features():
    """Mostra as funcionalidades de segurança implementadas"""
    print("\n🛡️ FUNCIONALIDADES DE SEGURANÇA")
    print("=" * 40)

    security_features = [
        "✅ Mascaramento de credenciais em logs",
        "✅ Não exposição de senhas no terminal",
        "✅ Validação segura de variáveis de ambiente",
        "✅ Testes que funcionam sem credenciais reais",
        "✅ Logs estruturados sem informações sensíveis",
        "✅ Fallback automático entre drivers",
        "✅ Timeouts configuráveis para evitar travamentos",
        "✅ Mensagens de erro sanitizadas"
    ]

    for feature in security_features:
        print(f"  {feature}")

    print("\n📋 Comandos seguros disponíveis:")
    commands = [
        "python3 load_env.py --show-values        # Carrega .env com valores mascarados",
        "python3 load_env.py --validate-only      # Apenas valida variáveis obrigatórias",
        "python3 test_connection_secure.py        # Teste completo sem expor credenciais",
        "./test_automated.sh                      # Script automatizado para CI/CD"
    ]

    for cmd in commands:
        print(f"  {cmd}")


def main():
    """Função principal"""
    print("🔍 DEMONSTRAÇÃO DOS NOVOS SCRIPTS DE TESTE SEGURO")
    print("=" * 60)
    print("Este script demonstra as novas funcionalidades de teste")
    print("que NÃO expõem credenciais durante a execução.")
    print("=" * 60)

    # Verificar ambiente
    if not test_local_environment():
        print("\n❌ Ambiente não está pronto para demonstração")
        return 1

    # Executar demonstrações
    demonstrate_load_env()
    demonstrate_secure_test()
    demonstrate_automated_test()
    show_security_features()

    print("\n" + "=" * 60)
    print("🎉 DEMONSTRAÇÃO CONCLUÍDA!")
    print("Agora você pode usar os scripts de forma segura.")
    print("=" * 60)

    return 0


if __name__ == "__main__":
    sys.exit(main())
