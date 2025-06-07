FROM ubuntu:24.10

# Configurações para build otimizado
ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Instala dependências base e MSSQL em camadas combinadas
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    python3 \
    python3-pip \
    python3-venv \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Instala MSSQL drivers
COPY mssql.sh /tmp/mssql.sh
RUN chmod +x /tmp/mssql.sh && \
    /tmp/mssql.sh && \
    rm -f /tmp/mssql.sh

# Configura usuário, diretórios e ambiente Python em uma única camada
RUN useradd -m -s /bin/bash appuser && \
    mkdir -p /app/results && \
    python3 -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install pandas pyodbc && \
    chown -R appuser:appuser /app && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> /etc/bash.bashrc && \
    printf "\n[openssl_init]\nssl_conf = ssl_sect\n[ssl_sect]\nsystem_default = system_default_sect\n[system_default_sect]\nMinProtocol = TLSv1.0\nCipherString = DEFAULT@SECLEVEL=1\n" >> /etc/ssl/openssl.cnf

# Copia o script Python para o container
COPY run_sql_csv.py /app/run_sql_csv.py

WORKDIR /app
USER appuser

# Configurações de ambiente otimizadas
ENV PATH="/app/venv/bin:/opt/mssql-tools18/bin:$PATH" \
    VIRTUAL_ENV="/app/venv" \
    OPENSSL_CONF="/etc/ssl/openssl.cnf"