FROM bitnami/spark

USER root

RUN pip install nba-api

ENV HADOOP_HOME=${HADOOP_HOME:-"/opt/bitnami/hadoop"}
RUN mkdir -p ${HADOOP_HOME}
