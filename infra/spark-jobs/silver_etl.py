#!/usr/bin/env python3
"""
Silver ETL — transforms bronze JSONL into an Iceberg silver table.

Source : s3a://fraud-datalake/bronze/claims/  (partitioned JSONL, raw/PHI-present)
Target : nessie.silver.claims                  (Iceberg, partitioned by processing_date)

Transformations applied:
  - Tokenize PHI columns (member_id, provider_npi) with SHA-256 prefix
  - Normalize event_time to a proper timestamp
  - Drop malformed records (null claim_id / member_id / provider_npi / amount)
  - Add processing_date partition column

Run daily via ScheduledSparkApplication (see 13-spark-jobs.yaml).
"""

import hashlib
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie.data.svc.cluster.local:19120/api/v1")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_S3", "")
WAREHOUSE = "s3a://fraud-datalake/warehouse"


def _tokenize(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


tokenize_udf = F.udf(_tokenize, StringType())


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("silver-etl")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
    )
    if S3_ENDPOINT:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
        )
    return builder.getOrCreate()


def main() -> None:
    spark = build_spark()
    processing_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_path = "s3a://fraud-datalake/bronze/claims/"

    df = spark.read.json(bronze_path)
    if df.rdd.isEmpty():
        print(f"[silver-etl] No bronze records found — nothing to do for {processing_date}")
        spark.stop()
        return

    raw_count = df.count()
    print(f"[silver-etl] Read {raw_count} records from {bronze_path}")

    df_silver = (
        df
        .withColumn("event_time", F.to_timestamp("event_time"))
        .filter(
            F.col("claim_id").isNotNull()
            & F.col("member_id").isNotNull()
            & F.col("provider_npi").isNotNull()
            & F.col("amount").isNotNull()
        )
        .withColumn("member_id_token", tokenize_udf(F.col("member_id")))
        .withColumn("provider_npi_token", tokenize_udf(F.col("provider_npi")))
        .drop("member_id", "provider_npi")
        .withColumn("processing_date", F.lit(processing_date))
        .withColumn("ingested_at", F.current_timestamp())
    )

    dropped = raw_count - df_silver.count()
    if dropped:
        print(f"[silver-etl] Dropped {dropped} malformed records")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    df_silver.writeTo("nessie.silver.claims") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .partitionedBy(F.col("processing_date")) \
        .createOrReplace()

    total = spark.table("nessie.silver.claims").count()
    print(f"[silver-etl] Done. Silver table total rows: {total}")
    spark.stop()


if __name__ == "__main__":
    main()
