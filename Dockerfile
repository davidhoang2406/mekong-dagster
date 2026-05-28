FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/dagster/dagster_home/storage

COPY dagster.yaml /opt/dagster/dagster_home/dagster.yaml
COPY . /opt/dagster/app/

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app
EXPOSE 3000
