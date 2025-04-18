FROM python:3.13-slim

# Установка необходимых пакетов
RUN apt-get update && apt-get install -y \
    git \
    curl \
    build-essential \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Настройка рабочей директории
WORKDIR /workspace

# Установка Python инструментов
RUN pip install --upgrade pip uv

# Установка конфигурации для сертификатов
ENV CLICKHOUSE_CA_CERTS=false
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# Предустановка общих зависимостей
RUN pip install mcp[cli]>=1.3.0 python-dotenv>=1.0.1 uvicorn>=0.34.0 clickhouse-connect>=0.8.0 pip-system-certs>=4.0 pandas>=2.2.3 tabulate>=0.9.0 ruff pytest

# Установка локали
ENV LANG=C.UTF-8 