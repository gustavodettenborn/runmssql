#!/usr/bin/env python3
"""
load_env.py - Carregador seguro de variáveis de ambiente
Carrega variáveis do .env sem expor valores sensíveis no terminal
"""

import os
import sys
from pathlib import Path


def load_env_file(env_file_path: str = '.env') -> dict:
    """
    Carrega variáveis de ambiente de um arquivo .env

    Args:
        env_file_path: Caminho para o arquivo .env

    Returns:
        dict: Dicionário com as variáveis carregadas
    """
    env_vars = {}

    if not os.path.exists(env_file_path):
        print(f"⚠️ Arquivo {env_file_path} não encontrado")
        return env_vars

    try:
        with open(env_file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                line = line.strip()

                # Pular linhas vazias e comentários
                if not line or line.startswith('#'):
                    continue

                # Verificar se a linha contém uma atribuição
                if '=' not in line:
                    print(f"⚠️ Linha {line_num} ignorada (formato inválido): {line}")
                    continue

                # Separar chave e valor
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip()

                # Remover aspas se presentes
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]

                # Definir variável de ambiente
                os.environ[key] = value
                env_vars[key] = value

        print(f"✅ {len(env_vars)} variáveis carregadas de {env_file_path}")
        return env_vars

    except Exception as e:
        print(f"❌ Erro ao carregar {env_file_path}: {e}")
        return {}


def mask_sensitive_value(key: str, value: str) -> str:
    """
    Mascara valores sensíveis para exibição segura

    Args:
        key: Nome da variável
        value: Valor da variável

    Returns:
        str: Valor mascarado se sensível, original caso contrário
    """
    sensitive_keys = {
        'PASSWORD', 'PWD', 'SECRET', 'TOKEN', 'KEY', 'PRIVATE'
    }

    # Verificar se a chave contém termos sensíveis
    key_upper = key.upper()
    is_sensitive = any(sensitive in key_upper for sensitive in sensitive_keys)

    if is_sensitive and value:
        if len(value) <= 4:
            return '*' * len(value)
        else:
            return value[:2] + '*' * (len(value) - 4) + value[-2:]

    return value


def print_loaded_variables(env_vars: dict, show_values: bool = False):
    """
    Exibe as variáveis carregadas de forma segura

    Args:
        env_vars: Dicionário com as variáveis
        show_values: Se deve mostrar os valores (mascarados para valores sensíveis)
    """
    if not env_vars:
        return

    print("\n📋 Variáveis de ambiente carregadas:")
    print("-" * 50)

    for key, value in sorted(env_vars.items()):
        if show_values:
            masked_value = mask_sensitive_value(key, value)
            print(f"{key}={masked_value}")
        else:
            print(f"{key}")

    print("-" * 50)


def validate_required_variables(required_vars: list) -> bool:
    """
    Valida se as variáveis obrigatórias estão definidas

    Args:
        required_vars: Lista de variáveis obrigatórias

    Returns:
        bool: True se todas estão definidas, False caso contrário
    """
    missing_vars = []

    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"❌ Variáveis obrigatórias não definidas: {', '.join(missing_vars)}")
        return False

    print(f"✅ Todas as variáveis obrigatórias estão definidas")
    return True


def main():
    """Função principal"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Carregador seguro de variáveis de ambiente do arquivo .env"
    )
    parser.add_argument(
        '--env-file',
        default='.env',
        help='Caminho para o arquivo .env (padrão: .env)'
    )
    parser.add_argument(
        '--show-values',
        action='store_true',
        help='Mostrar valores das variáveis (mascarados para valores sensíveis)'
    )
    parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Apenas validar se as variáveis obrigatórias estão definidas'
    )
    parser.add_argument(
        '--required',
        nargs='*',
        default=['MSSQL_SERVER', 'MSSQL_DATABASE'],
        help='Lista de variáveis obrigatórias a serem validadas'
    )

    args = parser.parse_args()

    # Carregar variáveis do arquivo .env
    if not args.validate_only:
        env_vars = load_env_file(args.env_file)
        print_loaded_variables(env_vars, args.show_values)

    # Validar variáveis obrigatórias
    if args.required:
        print(f"\n🔍 Validando variáveis obrigatórias: {', '.join(args.required)}")
        is_valid = validate_required_variables(args.required)

        if not is_valid:
            sys.exit(1)

    print("\n✅ Carregamento concluído com sucesso!")


if __name__ == "__main__":
    main()
