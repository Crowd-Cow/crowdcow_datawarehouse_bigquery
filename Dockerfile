FROM python:3.8

RUN apt-get update

RUN apt-get install -y python-pip netcat python-dev python3-dev vim

RUN pip install pip --upgrade
RUN pip install "dbt==0.19.0"

WORKDIR /usr/src/app
RUN cd /usr/src/app

COPY . .
