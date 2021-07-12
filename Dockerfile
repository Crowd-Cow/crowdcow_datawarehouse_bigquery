FROM python:3.8

RUN apt-get update

RUN pip install pip --upgrade
RUN pip install "dbt==0.19.2"

WORKDIR /usr/src/app
RUN cd /usr/src/app

COPY . .
