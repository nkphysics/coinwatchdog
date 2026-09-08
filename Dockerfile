FROM python:3.12-slim

RUN apt-get update && apt-get install -y libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir /mnt/coinwatchdog
WORKDIR /coinwatchdog
COPY . .

RUN python -m pip install --upgrade pip \
    && pip install .

ENTRYPOINT ["coinwatchdog", "--backend", "timescaledb", \
            "--tsdb-host", "timescaledb", "--tsdb-name", "coinwatchdog"]