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

# Копирование всех файлов проекта
COPY . .

# Установка зависимостей из pyproject.toml
RUN pip install -e .[dev]

# Установка локали
ENV LANG=C.UTF-8 