FROM python:3.9.6-slim-buster

RUN apt-get update

RUN pip install pip --upgrade
RUN pip install "dbt==0.20.0"

WORKDIR /usr/src/app
RUN cd /usr/src/app

RUN dbt deps

COPY . .
