FROM python:3.11-slim

WORKDIR /app

RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' \
        /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless git \
    && git config --system core.autocrlf true \
    && rm -rf /var/lib/apt/lists/*

COPY dagster/requirements.txt ./requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

COPY dagster/ ./dagster/
COPY scripts/ ./scripts/
COPY docs/postgres_pipeline_catalog_schema.sql ./docs/postgres_pipeline_catalog_schema.sql

ENV DAGSTER_HOME=/app/dagster_home

RUN mkdir -p /app/dagster_home

WORKDIR /app/dagster

EXPOSE 3000

CMD ["sh", "-c", "cp /app/dagster/dagster.yaml /app/dagster_home/dagster.yaml && exec dagster dev -f definitions.py -h 0.0.0.0 -p 3000"]
