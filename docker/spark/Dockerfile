FROM python:3.11-slim-bookworm

# Java 17 para o Spark; procps porque os scripts do Spark chamam ps
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app \
    PYTHONUNBUFFERED=1
