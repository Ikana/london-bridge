FROM apache/airflow:2.3.3

COPY . .

USER root
RUN apt-get update
RUN apt-get install telnet -yqq
USER airflow
RUN pip install -r requirements.txt