FROM apache/spark:3.5.6-scala2.12-java11-python3-r-ubuntu

USER root
RUN apt-get update && \
    apt-get install -y curl wget && \
    apt-get clean

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar && \
    wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar

RUN wget -P /opt/spark/jars/ \
    https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.7-spark-3.5/deequ-2.0.7-spark-3.5.jar

RUN pip install --no-cache-dir pydeequ==1.5.0

RUN mkdir -p /opt/spark/conf

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

ENV PATH="/opt/spark/bin:/opt/spark/sbin:${PATH}"

USER 185
