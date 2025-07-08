# infra/airflow/Dockerfile

FROM apache/airflow:2.10.4

USER root

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    gcc \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /opt/airflow/pyproject.toml
COPY poetry.lock /opt/airflow/poetry.lock

RUN chown -R airflow /opt/airflow

USER airflow

WORKDIR /opt/airflow

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-root --with dev
