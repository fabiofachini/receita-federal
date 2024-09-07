# Usa a imagem base do Python
FROM python:3.9-slim

# Instala dependências adicionais e o driver ODBC SQL Server 18
RUN apt-get update && apt-get install -y \
    curl \
    apt-transport-https \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Copia os arquivos necessários para o diretório de trabalho no contêiner
COPY . /app

# Atualiza o pip
RUN pip install --upgrade pip

# Instala as dependências do projeto a partir do arquivo requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Limpa o cache do pip
RUN pip cache purge

# Define a variável de ambiente para o profiles.yml
ENV DBT_PROFILES_DIR=/app

# Comando padrão a ser executado quando o contêiner for iniciado
CMD ["python3", "/app/run_pipeline.py"]
