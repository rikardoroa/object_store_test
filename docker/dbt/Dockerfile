FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive

# Dependencias necesarias para PyHive + SASL
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    libkrb5-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

#Versiones PINNED y compatibles
RUN pip install "dbt-core==1.3.1" \
    && pip install "dbt-spark[PyHive]==1.3.0"

WORKDIR /usr/app

# dbt leerá /usr/app/profiles gracias a DBT_PROFILES_DIR en docker-compose
CMD ["tail", "-f", "/dev/null"]
