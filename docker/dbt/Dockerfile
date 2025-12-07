FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive

# Dependencias para PyHive / SASL
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
    libkrb5-dev \
    build-essential \
    git \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

# 🔒 Fijamos versiones compatibles de dbt-core y dbt-spark
RUN pip install \
    "dbt-core==1.5.11" \
    "dbt-spark[PyHive]==1.5.0"

WORKDIR /usr/app

CMD ["tail", "-f", "/dev/null"]
