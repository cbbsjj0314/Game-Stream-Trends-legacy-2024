# infra/duckdb/Dockerfile

FROM debian:latest

RUN apt-get update && apt-get install -y curl unzip gettext \
    && curl -LO https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin/ \
    && rm duckdb_cli-linux-amd64.zip

COPY init.sql.template /root/init.sql

ENTRYPOINT ["/bin/sh", "-c", "envsubst < /root/init.sql > /root/.duckdbrc && exec duckdb"]
