FROM apache/airflow:2.3.3

COPY . .

RUN apt-get update

RUN apt-get install telnet -y

RUN pip install -r requirements.txt