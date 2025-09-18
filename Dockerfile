FROM python:3.11-slim

# Instala dependências do sistema
RUN apt-get update && apt-get install -y     gcc     libssl-dev     && rm -rf /var/lib/apt/lists/*

# Diretório de trabalho
WORKDIR /app

# Copia arquivos de dependência
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante dos arquivos
COPY . .

# Expõe a porta do FastAPI
EXPOSE 8000
