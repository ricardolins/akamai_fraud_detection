FROM apache/spark-py:v3.5.1

USER root

# Download Iceberg, Nessie and S3A JARs into the Spark jars directory
RUN curl -fsSL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar && \
    curl -fsSL -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.80.0.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie/nessie-spark-extensions-3.5_2.12/0.80.0/nessie-spark-extensions-3.5_2.12-0.80.0.jar && \
    curl -fsSL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -fsSL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /app

COPY silver_etl.py .
COPY gold_features.py .

USER ${spark_uid}
