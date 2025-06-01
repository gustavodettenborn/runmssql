FROM ubuntu:24.10

# Evita prompts interativos durante a instalação
ENV DEBIAN_FRONTEND=noninteractive

# Atualiza o sistema e instala dependências básicas
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    software-properties-common \
    python3 \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia o script de instalação do MSSQL
COPY mssql.sh /tmp/mssql.sh
RUN chmod +x /tmp/mssql.sh

# Executa o script de instalação do MSSQL
RUN /tmp/mssql.sh

# Adiciona o PATH do mssql-tools ao bashrc global
RUN echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> /etc/bash.bashrc

# Cria um usuário não-root para executar a aplicação
RUN useradd -m -s /bin/bash appuser

# Cria um diretório de trabalho
WORKDIR /app

# Cria ambiente virtual Python
RUN python3 -m venv /app/venv

# Ativa o ambiente virtual e instala as dependências
RUN /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install --no-cache-dir \
    pandas \
    pyodbc

# Copia o script Python
COPY run_sql_csv.py /app/

# Cria diretório para os resultados e define permissões
RUN mkdir -p /app/results && \
    chown -R appuser:appuser /app

# Muda para o usuário não-root
USER appuser

# Define variáveis de ambiente para usar o venv
ENV PATH="/app/venv/bin:$PATH"
ENV VIRTUAL_ENV="/app/venv"