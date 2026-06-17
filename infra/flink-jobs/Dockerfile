FROM flink:1.18-scala_2.12-java11

USER root

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.18.1 requests==2.31.0

RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

RUN mkdir -p /opt/flink/usrlib
COPY fraud_stream_job.py /opt/flink/usrlib/fraud_stream_job.py

USER flink
