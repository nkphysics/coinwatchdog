FROM python:3.12-slim

RUN mkdir /mnt/coinwatchdog
WORKDIR /coinwatchdog
COPY . .

RUN apt-get update && apt-get upgrade -y \
  && python -m pip install --upgrade pip \
  && pip install .

ENTRYPOINT ["coinwatchdog", "--database", "/mnt/coinwatchdog/coinwatchdog.db", "--log", "/mnt/coinwatchdog/coinwatchdog.log"]