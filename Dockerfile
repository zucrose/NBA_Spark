
FROM apache/airflow:2.10.5


LABEL authors="uzrhm"

USER root

RUN apt update  && \
    apt-get install -y --fix-missing default-jre && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow
COPY requirements.txt /

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt