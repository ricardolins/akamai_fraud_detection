#!/usr/bin/env python3
"""
Bronze Consumer — persists raw Redpanda messages to Akamai Object Storage.

Reads : raw.claims.new  (Redpanda, consumer group bronze-consumer)
Writes: s3://fraud-datalake/bronze/claims/year=YYYY/month=MM/day=DD/{HHmmss}_{offset}.jsonl

Records are written as NDJSON (one JSON object per line) in Hive-style date partitions.
No transformation is applied — this is the immutable raw layer with full PHI fidelity.
Spark ETL (silver_etl.py) reads from here daily to produce the tokenized silver table.
"""

import io
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("bronze-consumer")

KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC           = "raw.claims.new"
GROUP_ID        = "bronze-consumer"

BUCKET          = os.environ.get("BUCKET", "fraud-datalake")
S3_ENDPOINT     = os.environ.get("AWS_ENDPOINT_URL_S3", "")
ACCESS_KEY      = os.environ.get("AWS_ACCESS_KEY_ID", "")
SECRET_KEY      = os.environ.get("AWS_SECRET_ACCESS_KEY", "")

BATCH_SIZE      = int(os.environ.get("BATCH_SIZE", "200"))
FLUSH_INTERVAL  = int(os.environ.get("FLUSH_INTERVAL", "30"))

_running = True


def _stop(*_):
    global _running
    log.info("Received shutdown signal — draining current batch...")
    _running = False


signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)


def _s3():
    kwargs = {
        "aws_access_key_id": ACCESS_KEY,
        "aws_secret_access_key": SECRET_KEY,
        "config": Config(signature_version="s3v4"),
    }
    if S3_ENDPOINT:
        kwargs["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **kwargs)


def _s3_key(dt: datetime, start_offset: int) -> str:
    return (
        f"bronze/claims/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"{dt.strftime('%H%M%S')}_{start_offset}.jsonl"
    )


def _flush(s3, batch: list, start_offset: int) -> None:
    if not batch:
        return
    dt = datetime.now(timezone.utc)
    key = _s3_key(dt, start_offset)
    body = "\n".join(batch).encode("utf-8")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body,
        ContentType="application/x-ndjson",
    )
    log.info("Flushed %d records → s3://%s/%s", len(batch), BUCKET, key)


def main():
    s3 = _s3()
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    log.info(
        "Subscribed to %s | batch_size=%d flush_interval=%ds → s3://%s/bronze/",
        TOPIC, BATCH_SIZE, FLUSH_INTERVAL, BUCKET,
    )

    batch: list[str] = []
    start_offset = 0
    last_flush = time.monotonic()

    try:
        while _running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                raise KafkaException(msg.error())
            else:
                if not batch:
                    start_offset = msg.offset()
                # Ensure the value is valid UTF-8 JSON before appending
                raw = msg.value()
                try:
                    json.loads(raw)  # validate
                    batch.append(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError) as exc:
                    log.warning("Skipping unparseable message offset=%d: %s", msg.offset(), exc)

            elapsed = time.monotonic() - last_flush
            if len(batch) >= BATCH_SIZE or (batch and elapsed >= FLUSH_INTERVAL):
                _flush(s3, batch, start_offset)
                consumer.commit(asynchronous=False)
                batch.clear()
                last_flush = time.monotonic()

    finally:
        if batch:
            _flush(s3, batch, start_offset)
            try:
                consumer.commit(asynchronous=False)
            except Exception:
                pass
        consumer.close()
        log.info("Bronze consumer stopped cleanly.")


if __name__ == "__main__":
    main()
