FROM python:3.6

# RUN apt-get update -y

RUN pip install pip --upgrade
RUN pip install "dbt==0.19.6"

WORKDIR /usr/src/app
RUN cd /usr/src/app

COPY . .

RUN dbt deps
