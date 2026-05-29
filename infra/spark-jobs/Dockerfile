FROM apache/spark:3.5.8-python3

USER root

COPY iceberg-spark-runtime-3.5_2.12-1.5.2.jar       /opt/spark/jars/
COPY nessie-spark-extensions-3.5_2.12-0.80.0.jar    /opt/spark/jars/
COPY hadoop-aws-3.3.4.jar                           /opt/spark/jars/
COPY aws-java-sdk-bundle-1.12.262.jar               /opt/spark/jars/

WORKDIR /app

COPY silver_etl.py .
COPY gold_features.py .

USER spark
