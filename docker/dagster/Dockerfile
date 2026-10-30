FROM python:3.11-slim

LABEL org.opencontainers.image.source=https://github.com/kestra-io/plugin-dagster
LABEL org.opencontainers.image.description="Dagster image for Kestra plugin testing"

RUN apt-get update && apt-get install -y git curl && apt-get clean

# sqlalchemy 2.1 defaults postgresql:// to psycopg 3, dagster-postgres ships psycopg2 only
RUN pip install --no-cache-dir \
    dagster==1.13.27 \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    "sqlalchemy<2.1"

# Set $DAGSTER_HOME
ENV DAGSTER_HOME=/opt/dagster/dagster_home/

RUN mkdir -p $DAGSTER_HOME /opt/dagster/app

COPY dagster.yaml workspace.yaml $DAGSTER_HOME/

WORKDIR $DAGSTER_HOME
