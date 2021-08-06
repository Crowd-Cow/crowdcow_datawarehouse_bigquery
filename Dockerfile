FROM python:3.9.6

# RUN apt-get update -y
# RUN apt-get install vim -y

RUN curl -O https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/1.2/linux_x86_64/snowsql-1.2.17-linux_x86_64.bash
RUN SNOWSQL_DEST=~/bin SNOWSQL_LOGIN_SHELL=~/.profile bash snowsql-1.2.17-linux_x86_64.bash

RUN pip install pip --upgrade
RUN pip install "dbt==0.20.0"

WORKDIR /usr/src/app
RUN cd /usr/src/app

COPY . .

RUN dbt deps
