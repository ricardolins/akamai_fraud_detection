#!/usr/bin/env python3
"""
Gold Features — materialises ML-ready feature tables from the silver Iceberg table.

Source : nessie.silver.claims
Targets:
  nessie.gold.provider_features  — per-provider aggregates over rolling windows
  nessie.gold.member_features    — per-member aggregates over rolling windows
  nessie.gold.claim_features     — per-claim feature vector (joins both, used by ML)

Run weekly (Sunday 04:00 UTC) via ScheduledSparkApplication (see 13-spark-jobs.yaml).
"""

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie.data.svc.cluster.local:19120/api/v1")
S3_ENDPOINT = os.environ.get("AWS_ENDPOINT_URL_S3", "")
WAREHOUSE = "s3a://fraud-datalake/warehouse"

WINDOW_30D = 30
WINDOW_90D = 90


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("gold-features")
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


def build_provider_features(df):
    """Aggregated provider stats over a 30-day rolling window."""
    w = (
        Window.partitionBy("provider_npi_token")
        .orderBy(F.col("event_time").cast("long"))
        .rangeBetween(-WINDOW_30D * 86400, 0)
    )
    return (
        df
        .withColumn("claim_count_30d", F.count("claim_id").over(w))
        .withColumn("avg_amount_30d", F.avg("amount").over(w))
        .withColumn("stddev_amount_30d", F.stddev("amount").over(w))
        .withColumn("max_amount_30d", F.max("amount").over(w))
        .withColumn("distinct_procedures_30d", F.approx_count_distinct("procedure_code").over(w))
        .groupBy("provider_npi_token")
        .agg(
            F.last("claim_count_30d").alias("claim_count_30d"),
            F.last("avg_amount_30d").alias("avg_amount_30d"),
            F.last("stddev_amount_30d").alias("stddev_amount_30d"),
            F.last("max_amount_30d").alias("max_amount_30d"),
            F.last("distinct_procedures_30d").alias("distinct_procedures_30d"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )


def build_member_features(df):
    """Aggregated member stats over a 90-day rolling window."""
    w = (
        Window.partitionBy("member_id_token")
        .orderBy(F.col("event_time").cast("long"))
        .rangeBetween(-WINDOW_90D * 86400, 0)
    )
    return (
        df
        .withColumn("claim_count_90d", F.count("claim_id").over(w))
        .withColumn("total_amount_90d", F.sum("amount").over(w))
        .withColumn("distinct_providers_90d", F.approx_count_distinct("provider_npi_token").over(w))
        .groupBy("member_id_token")
        .agg(
            F.last("claim_count_90d").alias("claim_count_90d"),
            F.last("total_amount_90d").alias("total_amount_90d"),
            F.last("distinct_providers_90d").alias("distinct_providers_90d"),
        )
        .withColumn("computed_at", F.current_timestamp())
    )


def build_claim_features(df, provider_feat, member_feat):
    """Per-claim feature vector: raw claim fields + provider/member aggregates."""
    return (
        df
        .withColumn("hour_of_day", F.hour("event_time"))
        .withColumn("is_weekend", (F.dayofweek("event_time").isin([1, 7])).cast("int"))
        .join(provider_feat, on="provider_npi_token", how="left")
        .join(member_feat, on="member_id_token", how="left")
        .withColumn(
            "amount_ratio",
            F.when(F.col("avg_amount_30d") > 0, F.col("amount") / F.col("avg_amount_30d"))
            .otherwise(1.0),
        )
        .select(
            "claim_id",
            "provider_npi_token",
            "member_id_token",
            "amount",
            "procedure_code",
            "diagnosis_code",
            "event_time",
            "hour_of_day",
            "is_weekend",
            "claim_count_30d",
            "avg_amount_30d",
            "stddev_amount_30d",
            "amount_ratio",
            "max_amount_30d",
            "claim_count_90d",
            "total_amount_90d",
            "distinct_providers_90d",
            "processing_date",
        )
        .withColumn("feature_date", F.current_timestamp())
    )


def main() -> None:
    spark = build_spark()
    run_ts = datetime.now(timezone.utc).isoformat()
    print(f"[gold-features] Starting at {run_ts}")

    df = spark.table("nessie.silver.claims")
    row_count = df.count()
    print(f"[gold-features] Silver rows read: {row_count}")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")

    provider_feat = build_provider_features(df)
    member_feat = build_member_features(df)
    claim_feat = build_claim_features(df, provider_feat, member_feat)

    provider_feat.writeTo("nessie.gold.provider_features") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()

    member_feat.writeTo("nessie.gold.member_features") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .createOrReplace()

    claim_feat.writeTo("nessie.gold.claim_features") \
        .tableProperty("write.format.default", "parquet") \
        .tableProperty("write.parquet.compression-codec", "snappy") \
        .partitionedBy(F.col("processing_date")) \
        .createOrReplace()

    print(f"[gold-features] provider_features: {spark.table('nessie.gold.provider_features').count()} rows")
    print(f"[gold-features] member_features:   {spark.table('nessie.gold.member_features').count()} rows")
    print(f"[gold-features] claim_features:    {spark.table('nessie.gold.claim_features').count()} rows")
    spark.stop()


if __name__ == "__main__":
    main()
