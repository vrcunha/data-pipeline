FROM apache/airflow:2.11.0

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jre-headless && \
    apt-get clean

COPY requirements.txt .

RUN groupadd -g 1001 docker-host || true
RUN usermod -aG docker-host airflow

USER airflow

RUN pip install -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_VERSION=3.5
ENV PYDEEQU_DEEQU_MAVEN_COORD=com.amazon.deequ:deequ:2.0.7-spark-3.5
