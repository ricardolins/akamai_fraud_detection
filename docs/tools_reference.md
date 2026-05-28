# Tool Reference Guide — Healthcare Fraud Detection Platform

Each tool used in the architecture is documented here with its origin, purpose, license, and a concrete example of how it fits into this specific platform.

---

## Table of Contents

1. [Streaming & Ingestion](#1-streaming--ingestion)
   - Redpanda · Debezium · Vector · Redpanda Connect
2. [Stream Processing](#2-stream-processing)
   - Apache Flink
3. [Batch Processing & Orchestration](#3-batch-processing--orchestration)
   - Apache Spark · Apache Airflow
4. [Data Lake & Storage](#4-data-lake--storage)
   - Apache Iceberg · Project Nessie
5. [Query Engine](#5-query-engine)
   - Trino · DuckDB
6. [Feature Store](#6-feature-store)
   - Feast · Redis
7. [ML Platform](#7-ml-platform)
   - MLflow · Ray · BentoML
8. [Serving Layer](#8-serving-layer)
   - FastAPI
9. [Security](#9-security)
   - HashiCorp Vault · Keycloak · Open Policy Agent · Linkerd · Falco
10. [Observability](#10-observability)
    - Prometheus · Grafana · Loki · Grafana Tempo · OpenTelemetry · Fluentbit
11. [Platform & GitOps](#11-platform--gitops)
    - Kubernetes (LKE) · Helm · ArgoCD

---

## 1. Streaming & Ingestion

---

### Redpanda

**Created by:** Alex Gallego and the Vectorized Inc. team, founded in 2019. Gallego previously worked at Akamai and Pure Storage, where he identified that Apache Kafka, despite being the industry standard, wasted significant CPU cycles due to its JVM runtime and ZooKeeper dependency. He built Redpanda from scratch in C++ to solve this.

**Open source:** Partially. The core broker is under the **Business Source License (BSL 1.1)** — free for internal use, becomes Apache 2.0 after 4 years. The Enterprise edition adds RBAC, tiered storage, audit logs, and remote read replicas under a commercial license.

**Purpose:** Redpanda is a Kafka-compatible streaming data platform. It is a drop-in replacement for Apache Kafka but with no ZooKeeper, no JVM, and no separate Schema Registry — all packed into a single binary written in C++. It stores ordered, durable streams of events in topics, and allows multiple consumers to read from those topics at different speeds without affecting each other.

**Why it was created:** Kafka requires running ZooKeeper alongside it (a separate distributed coordination system), a JVM (which brings garbage collection pauses), and a separate Confluent Schema Registry for schema management. For teams that don't want to manage three systems to run one message bus, Redpanda offers the same Kafka API in a single, operationally simpler package with better tail latency.

**Architecture example — fraud platform:**

```
                    CLAIM SUBMITTED
                         │
              ┌──────────▼──────────┐
              │  Claims System DB   │
              │  (PostgreSQL)       │
              └──────────┬──────────┘
                         │ CDC via Debezium
              ┌──────────▼──────────────────────────┐
              │         REDPANDA CLUSTER             │
              │                                      │
              │  Topic: raw.claims.new               │
              │  ├── Partition 0 (provider_id 0-999) │
              │  ├── Partition 1 (provider_id 1000+) │
              │  └── ...                             │
              │                                      │
              │  Schema Registry (built-in):         │
              │  ClaimEvent v1 → Avro schema stored  │
              │                                      │
              │  Tiered Storage:                     │
              │  hot (< 7 days) → broker disk        │
              │  cold (> 7 days) → Object Storage    │
              └──────────────────────────────────────┘
                    │                    │
              Flink consumer       Spark consumer
              (real-time scoring)  (batch training)
```

**Key configuration for this platform:**

```yaml
# redpanda cluster config
redpanda:
  kafka_api:
    - address: 0.0.0.0:9092
  schema_registry:
    - address: 0.0.0.0:8081
  cloud_storage_enabled: true          # tiered storage
  cloud_storage_bucket: fraud-datalake
  retention_bytes: 10737418240         # 10GB local per partition
  retention_ms: 604800000              # 7 days local, then tiered
```

---

### Debezium

**Created by:** Randall Hauch at **Red Hat**, first released in 2016. Hauch was working on event sourcing patterns and needed a reliable way to capture every row-level change in a relational database and stream it to Kafka without modifying the application code.

**Open source:** Yes — **Apache License 2.0**. Fully open, maintained by Red Hat and a large community.

**Purpose:** Debezium is a Change Data Capture (CDC) platform. It reads the internal transaction log of a database (the binary log in MySQL, WAL in PostgreSQL, redo log in Oracle) and converts every INSERT, UPDATE, and DELETE into a structured event that it publishes to a Kafka/Redpanda topic. The result is that every change to the source database becomes a stream of events, in order, with the before and after state of each row.

**Why it was created:** The traditional approach to syncing a database with a data warehouse is scheduled batch queries (`SELECT * WHERE updated_at > ?`). This has problems: it misses deletes, creates load spikes on the source DB, and has latency of minutes to hours. CDC reads the replication log instead — the same mechanism databases use for their own replication — which is non-intrusive, captures deletes, and has millisecond latency.

**Architecture example — fraud platform:**

```
PostgreSQL (claims system)
  └── WAL (Write-Ahead Log)
        │
        │  Debezium PostgreSQL Connector
        │  reads replication slot: "debezium_slot"
        ▼
  ┌─────────────────────────────────────────────┐
  │  Debezium event (JSON / Avro):              │
  │  {                                          │
  │    "op": "u",          ← UPDATE             │
  │    "before": {                              │
  │      "claim_id": "CLM-001",                 │
  │      "status": "PENDING"                    │
  │    },                                       │
  │    "after": {                               │
  │      "claim_id": "CLM-001",                 │
  │      "status": "APPROVED",                  │
  │      "amount": 1250.00                      │
  │    },                                       │
  │    "ts_ms": 1748432400000                   │
  │  }                                          │
  └──────────────────┬──────────────────────────┘
                     │
              Redpanda topic:
              raw.claims.status
```

**Supported sources:** PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, Db2, Cassandra, Vitess, Spanner (via connector).

---

### Vector

**Created by:** Timber.io, a log management startup, in 2019. **Datadog acquired Timber.io in 2021** and Vector became one of its core open source projects. The team found that existing log shippers (Logstash, Fluentd) were JVM-based, memory-hungry, and slow. They built Vector in **Rust** for minimal memory footprint and high throughput.

**Open source:** Yes — **Mozilla Public License 2.0 (MPL-2.0)**.

**Purpose:** Vector is a high-performance observability data pipeline. It collects logs, metrics, and traces from any source (files, syslog, Kafka, HTTP, Docker, Kubernetes) and routes, transforms, and ships them to any destination. In this platform, Vector runs as a DaemonSet on every Kubernetes node to collect all container logs and forward them to Redpanda.

**Why it was created:** Logstash (Elastic) and Fluentd require a JVM or Ruby runtime, consuming 200–500MB RAM per node just for log collection. On a Kubernetes cluster with 20 nodes that is 4–10GB of RAM dedicated to log shipping. Vector uses ~5–20MB per node for the same workload.

**Architecture example — fraud platform:**

```
┌────────────────────────────────────────────┐
│  Kubernetes Node                           │
│                                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐   │
│  │ Flink pod│ │Spark pod │ │ API pod  │   │
│  │ stdout   │ │ stdout   │ │ stdout   │   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘   │
│       │            │            │          │
│  ┌────▼────────────▼────────────▼──────┐   │
│  │          Vector (DaemonSet)         │   │
│  │                                     │   │
│  │  sources:                           │   │
│  │    kubernetes_logs:                 │   │
│  │      type: kubernetes_logs          │   │
│  │  transforms:                        │   │
│  │    parse_json:                      │   │
│  │      type: remap                    │   │
│  │      source: |                      │   │
│  │        . = parse_json!(.message)    │   │
│  │  sinks:                             │   │
│  │    redpanda:                        │   │
│  │      type: kafka                    │   │
│  │      topic: raw.logs.app            │   │
│  └─────────────────────────────────────┘   │
└────────────────────────────────────────────┘
                    │
             Redpanda topic:
             raw.logs.app
             raw.logs.audit
```

---

### Redpanda Connect (formerly Benthos)

**Created by:** Ashley Jeffs, a British software engineer, who started it as a personal side project in 2016 and open-sourced it. The project was called **Benthos** for years. **Redpanda acquired it in 2023** and renamed it Redpanda Connect.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Redpanda Connect is a declarative data streaming and transformation tool. It defines pipelines in YAML: read from a source, apply transformations (filter, map, enrich, split), write to a sink. It has 200+ built-in connectors (S3, HTTP, databases, Kafka, SFTP, email, etc.) and supports custom processors in Python or Go. In this platform, it handles file ingestion: polling S3 buckets for new EDI 837 / HL7 / CSV files, parsing them, and publishing structured events to Redpanda.

**Why it was created:** Kafka Connect (the official Kafka connector framework) requires Java and has a complex plugin deployment model. For teams that need to connect non-standard sources to Kafka, writing a Kafka Connect plugin in Java is heavyweight. Benthos/Connect lets you write the same pipeline in 20 lines of YAML.

**Architecture example — fraud platform:**

```yaml
# pipeline: ingest EDI 837 claim files from Object Storage
input:
  aws_s3:
    bucket: fraud-datalake
    prefix: incoming/edi837/
    codec: lines

pipeline:
  processors:
    - mapping: |
        root.claim_id     = this.CLM01
        root.provider_npi = this.NM109
        root.amount       = this.CLM02.number()
        root.icd10_codes  = this.HI.map_each(v -> v.HI01)
        root.ingested_at  = now()
    - catch:
        - log:
            message: "EDI parse error: ${! error() }"

output:
  kafka:
    addresses: [ redpanda-0:9092 ]
    topic: raw.files.edi837
    key: ${! json("claim_id") }
```

---

## 2. Stream Processing

---

### Apache Flink

**Created by:** A research project at **Technische Universität Berlin** (TU Berlin), Germany, starting around 2010 under the name "Stratosphere". It was donated to the **Apache Software Foundation** in 2014, graduated as a top-level Apache project in 2015, and was later commercially developed by **Ververica** (founded by the original Flink creators, acquired by Alibaba in 2019).

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Apache Flink is a distributed stream processing engine. It processes continuous streams of events (from Kafka/Redpanda) with stateful computations, time-based windows, and exactly-once guarantees. It is designed to maintain internal state (e.g., "how many claims has provider X submitted in the last 30 days?") in a fault-tolerant way, checkpointing state to durable storage so it can recover from failures without reprocessing all history.

**Why it was created:** Before Flink, the dominant processing approach was either pure batch (Hadoop MapReduce, slow) or micro-batch (Spark Streaming, adds latency and is not truly event-driven). Flink introduced genuine event-time processing: the ability to handle late-arriving events correctly using watermarks, and stateful computation with millisecond latency and exactly-once semantics. These are non-negotiable for fraud detection — a claim can arrive out of order (EDI files are batched by providers), and the system must correctly place it in the right time window.

**Architecture example — fraud platform:**

```java
// Flink job: RuleBasedScoringJob
// Detects providers billing > 95th percentile claim amount in a 30-day window

DataStream<EnrichedClaim> claims = env
    .fromSource(redpandaSource, WatermarkStrategy
        .<EnrichedClaim>forBoundedOutOfOrderness(Duration.ofMinutes(5))
        .withTimestampAssigner((e, ts) -> e.getClaimDate().toEpochMilli()),
        "Redpanda enriched.claims");

// Stateful keyed stream: one state per provider_id
claims
    .keyBy(EnrichedClaim::getProviderId)
    .window(SlidingEventTimeWindows.of(Time.days(30), Time.hours(1)))
    .aggregate(new ClaimStatsAggregator(), new FraudRuleEvaluator())
    .addSink(redpandaSink("scored.claims"));

// FraudRuleEvaluator applies:
// Rule 1: claim_amount > provider_30d_avg * 3.0 → flag
// Rule 2: same procedure billed > 5x in 24h → flag
// Rule 3: impossible procedure+diagnosis combination → flag
```

```
Redpanda: enriched.claims
         │
         │  keyBy(provider_id)
         ▼
┌────────────────────────────────────┐
│  Flink TaskManager                 │
│                                    │
│  Provider State (RocksDB):         │
│  provider_id=NPI-1234:             │
│    claims_30d: [CLM-001, CLM-002]  │
│    avg_amount_30d: 850.00          │
│    procedure_counts: {99213: 12}   │
│                                    │
│  New claim arrives:                │
│    amount: 4200.00                 │
│    → 4200 > 850 * 3.0 → FLAG       │
└────────────┬───────────────────────┘
             │
             ▼
  Redpanda: scored.claims
  { "claim_id": "CLM-099",
    "rule_score": 0.87,
    "triggered_rules": ["HIGH_AMOUNT_DEVIATION"] }
```

**Checkpointing (fault tolerance):**

```
Every 60 seconds:
  Flink → snapshot RocksDB state → Linode Object Storage
              s3://fraud-datalake/flink-checkpoints/job-id/chk-1234/

On failure:
  Flink restarts TaskManagers
  Restores state from last checkpoint
  Replays Redpanda events from checkpoint offset
  → Zero data loss, exactly-once semantics
```

---

## 3. Batch Processing & Orchestration

---

### Apache Spark

**Created by:** Matei Zaharia as part of his PhD research at the **AMPLab, UC Berkeley**, around 2009. The core insight was that Hadoop MapReduce was too slow for iterative algorithms (like ML training) because it wrote intermediate results to disk between every step. Spark kept data in memory across steps. Zaharia later co-founded **Databricks** (2013) to commercialize Spark.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Apache Spark is a distributed computing engine for large-scale data processing. It runs batch jobs (ETL, aggregations, ML training) across a cluster of machines, dividing the work into tasks that execute in parallel. A Spark job can process terabytes of Parquet files in minutes by parallelizing across dozens of executors. In this platform, Spark handles the bronze→silver→gold ETL pipeline and ML model training.

**Why it was created:** Hadoop MapReduce required writing every intermediate result to HDFS, making iterative ML algorithms (which need 100+ passes over data) prohibitively slow. Spark's RDD (Resilient Distributed Dataset) abstraction keeps data in-memory across iterations, making iterative workloads 10–100x faster.

**Architecture example — fraud platform:**

```python
# Spark job: silver_etl.py
# Reads raw claims from bronze zone, cleans and tokenizes PHI, writes to silver

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, col, to_timestamp

spark = SparkSession.builder \
    .config("spark.sql.catalog.nessie", "org.projectnessie.spark.extensions.NessieSpark32CatalogPlugin") \
    .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
    .getOrCreate()

# Read from bronze (Iceberg table, full fidelity)
bronze = spark.read.format("iceberg").load("nessie.bronze.claims")

# Clean and tokenize PHI
silver = bronze \
    .filter(col("claim_id").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn("member_token", sha2(concat_ws("|", col("cpf"), col("dob")), 256)) \
    .withColumn("provider_token", sha2(col("provider_npi"), 256)) \
    .drop("cpf", "name", "dob", "address") \   # PHI removed in silver
    .withColumn("claim_date", to_timestamp("claim_date_str", "yyyy-MM-dd"))

# Write to silver zone (Iceberg, partitioned by month)
silver.writeTo("nessie.silver.claims") \
    .partitionedBy("months(claim_date)") \
    .createOrReplace()
```

```
Linode Object Storage
  bronze/claims/  (raw, PHI present)
        │
        │  Spark job runs daily at 02:00 (Airflow trigger)
        │  ~45 min for 10M rows
        ▼
  silver/claims/  (clean, PHI tokenized)
        │
        │  Spark feature engineering job (Sunday 04:00)
        ▼
  gold/features/provider_profiles/  (ML-ready, no PHI)
```

---

### Apache Airflow

**Created by:** Maxime Beauchemin at **Airbnb** in 2014, open-sourced in 2015, donated to the Apache Software Foundation in 2016, and graduated as a top-level Apache project in 2019. Beauchemin needed to orchestrate complex, multi-step data pipelines with dependency management and scheduling — none of the existing tools (cron, Oozie) were expressive enough.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Airflow is a workflow orchestration platform. Pipelines are written as Python code (DAGs — Directed Acyclic Graphs), where each node is a task (run a Spark job, query a database, call an API, send a notification) and edges define dependencies. Airflow schedules these DAGs, monitors their execution, retries failed tasks, sends alerts on failure, and provides a visual UI to inspect pipeline history.

**Why it was created:** cron jobs can't express task dependencies ("run task B only after task A succeeds, and run task C and D in parallel after B"). Oozie (Hadoop's workflow tool) was XML-based and Java-heavy. Beauchemin wanted pipelines-as-code with a clean Python API, a visual UI, and first-class retry/alerting semantics.

**Architecture example — fraud platform:**

```python
# DAG: daily_silver_etl
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="daily_silver_etl",
    schedule_interval="0 2 * * *",   # 02:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
) as dag:

    bronze_to_silver = SparkKubernetesOperator(
        task_id="bronze_to_silver",
        application="s3://fraud-datalake/jobs/silver_etl.py",
        kubernetes_conn_id="lke_cluster",
    )

    silver_to_gold = SparkKubernetesOperator(
        task_id="silver_to_gold",
        application="s3://fraud-datalake/jobs/gold_features.py",
        kubernetes_conn_id="lke_cluster",
    )

    notify_success = SlackWebhookOperator(
        task_id="notify_success",
        message="ETL pipeline completed. Gold features updated.",
    )

    bronze_to_silver >> silver_to_gold >> notify_success
```

```
Airflow Scheduler (runs on LKE)
  │
  ├── 02:00: trigger bronze_to_silver Spark job → LKE
  │         wait for completion (~45 min)
  ├── 02:45: trigger silver_to_gold Spark job → LKE
  │         wait for completion (~90 min)
  └── 04:15: notify Slack "#data-platform" → success
```

---

## 4. Data Lake & Storage

---

### Apache Iceberg

**Created by:** Ryan Blue and Daniel Weeks at **Netflix** around 2017–2018, open-sourced and donated to the Apache Software Foundation, graduating as a top-level project in 2020. Netflix was struggling with correctness problems on their data lake: concurrent writers corrupting Hive tables, queries reading inconsistent snapshots, schema changes breaking downstream jobs.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Iceberg is an open table format for large analytic datasets stored in object storage (S3, GCS, ADLS, Linode Object Storage). It adds ACID transactions, snapshot isolation (time travel), schema evolution, and partition evolution on top of plain Parquet files. The key insight is that Iceberg tracks metadata in manifest files — it knows exactly which files belong to a table at any point in time, enabling consistent reads even while writers are adding new files.

**Why it was created:** Hive tables on S3 had critical problems: (1) there was no atomic multi-file commit, so a failed write left the table in a corrupt state; (2) queries read whatever files were in the S3 prefix at scan time, meaning a concurrent writer could add a file mid-query and return inconsistent results; (3) changing a partition scheme required rewriting all data. Iceberg solved all three with a metadata layer.

**Architecture example — fraud platform:**

```
Linode Object Storage: s3://fraud-datalake/

Iceberg table: nessie.silver.claims

Metadata layer (Iceberg manages this):
  metadata/
    v1.metadata.json          ← table schema, partition spec
    v2.metadata.json          ← schema evolution: added icd10_version column
    snap-001.avro             ← snapshot 1: files from 2026-01-01
    snap-002.avro             ← snapshot 2: files from 2026-01-02

Data layer (Parquet files, written by Spark/Flink):
  data/
    claim_date_month=2026-01/
      00000-0-abc.parquet
      00001-0-def.parquet
    claim_date_month=2026-02/
      00000-0-ghi.parquet

Time travel query (audit requirement):
  SELECT * FROM nessie.silver.claims
  TIMESTAMP AS OF '2026-01-15 00:00:00'
  WHERE claim_id = 'CLM-001'
  -- Returns the row as it existed on Jan 15, even if it was later deleted
```

**LGPD right-to-erasure with Iceberg:**
```sql
-- Mark claim as deleted (Iceberg position delete file)
DELETE FROM nessie.silver.claims WHERE member_token = 'abc123';

-- Iceberg writes a delete file, does NOT rewrite data files immediately
-- Old snapshots still accessible for audit (within retention period)
-- After retention expires: expire_snapshots() removes old data permanently
```

---

### Project Nessie

**Created by:** The engineering team at **Dremio**, a data lakehouse query engine company, released in 2020. Dremio needed a way to allow data engineers to work on the same data lake tables without stepping on each other — the same way software engineers use Git branches.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Project Nessie is a transactional catalog for data lakes. It provides Git-like semantics over Iceberg (and Delta Lake) tables: branches, commits, merges, and tags. Instead of writing directly to the production table, a data engineer creates a branch, transforms data on that branch, reviews the result, and merges it to main — exactly like a code pull request. Nessie also provides the Iceberg catalog API that Spark, Flink, and Trino use to discover and resolve table metadata.

**Why it was created:** Without Nessie (or a similar catalog), Iceberg tables require a central Hive Metastore (a heavyweight Java service backed by a relational database that becomes a bottleneck). More critically, there was no way to do multi-table atomic operations on a data lake or to experiment with transformations in isolation.

**Architecture example — fraud platform:**

```
Production branch: main
  nessie.silver.claims          (live data, read by Flink)
  nessie.gold.provider_profiles (live features, read by BentoML)

ML experiment workflow:
  1. Data scientist creates branch: "feature/new-fraud-features"
     git equivalent: git checkout -b feature/new-fraud-features

  2. Runs Spark job on branch:
     spark.conf.set("spark.sql.catalog.nessie.ref", "feature/new-fraud-features")
     # reads silver.claims, writes gold.provider_profiles_v2
     # does NOT affect main branch

  3. Reviews output in DuckDB / Trino:
     SELECT * FROM nessie.gold.provider_profiles_v2@feature/new-fraud-features

  4. Merges to main after validation:
     nessie_client.merge_branch("feature/new-fraud-features", "main")
     # atomic: all tables updated together, or none

  5. Flink and BentoML automatically see new features on next read
```

---

## 5. Query Engine

---

### Trino

**Created by:** Martin Traverso, Dain Sundstrom, David Phillips, and Eric Hwang at **Facebook** in 2012 under the name **Presto**, to allow analysts to query Facebook's Hive data warehouse interactively (Hive's MapReduce backend took hours; Facebook needed seconds). After Facebook restricted the Presto name, the original founders forked the project and renamed it **Trino** in 2020 and founded **Trinodb.io**.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Trino is a distributed SQL query engine that can query data where it lives — Iceberg tables on object storage, PostgreSQL, Redis, Kafka, Elasticsearch — without moving data first. An analyst writes a single SQL query that JOINs an Iceberg table on Object Storage with a live PostgreSQL database and gets results in seconds. It is the "query anything" engine for the data lake.

**Why it was created:** Facebook had petabytes of data in Hive that took hours to query. They needed sub-minute interactive SQL without moving data to a specialized database. Presto/Trino achieves this by pushing computation to where data lives and pipelining intermediate results in memory rather than writing to disk.

**Architecture example — fraud platform:**

```sql
-- Fraud investigator query: find all claims from a provider
-- that match a suspected fraud pattern, joining live and historical data

SELECT
    c.claim_id,
    c.procedure_code,
    c.amount,
    p.provider_name,         -- from live PostgreSQL
    p.license_status,
    f.fraud_score,           -- from Iceberg gold zone
    f.triggered_rules
FROM
    iceberg.silver.claims c              -- Iceberg on Object Storage
    JOIN postgresql.providers.info p     -- live PostgreSQL
      ON c.provider_token = p.npi_token
    JOIN iceberg.gold.fraud_scores f     -- Iceberg on Object Storage
      ON c.claim_id = f.claim_id
WHERE
    c.claim_date >= DATE '2026-01-01'
    AND f.fraud_score > 0.75
    AND p.license_status = 'ACTIVE'
ORDER BY f.fraud_score DESC;

-- Trino executes this in ~3-8 seconds across 100M claim rows
-- No data movement, no ETL, no staging table
```

---

### DuckDB

**Created by:** Mark Raasveldt and Hannes Mühleisen at the **Centrum Wiskunde & Informatica (CWI)** research institute in Amsterdam, Netherlands, released in 2019. They recognized that most analytical queries run on laptops or single servers, not clusters — and existing tools (SQLite for OLTP, Spark for large-scale) left a gap for fast, in-process analytical SQL.

**Open source:** Yes — **MIT License**.

**Purpose:** DuckDB is an in-process OLAP SQL database. It runs inside a Python script, a Jupyter notebook, or a command line, with no server to deploy. It reads Parquet files, Iceberg tables, CSV, and JSON directly from local disk or S3 and runs analytical queries with columnar vectorized execution. It is exceptionally fast for single-machine workloads (up to ~100GB) and ideal for data exploration and development.

**Why it was created:** Analysts often need to explore data quickly on their laptops without spinning up a Spark cluster or connecting to Trino. DuckDB enables full analytical SQL (window functions, aggregations, LATERAL JOINs) on local files with zero infrastructure.

**Architecture example — fraud platform:**

```python
# Data scientist: local exploration of gold zone features
import duckdb

con = duckdb.connect()
con.execute("""
    INSTALL iceberg; LOAD iceberg;
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='us-east-1.linodeobjects.com';
    SET s3_access_key_id='...';
    SET s3_secret_access_key='...';
""")

# Query Iceberg table directly from Linode Object Storage — no cluster needed
result = con.execute("""
    SELECT
        provider_token,
        AVG(amount)             AS avg_claim,
        COUNT(*)                AS claim_count,
        COUNT(DISTINCT member_token) AS unique_members,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95_amount
    FROM iceberg_scan('s3://fraud-datalake/silver/claims/')
    WHERE claim_date >= '2026-01-01'
    GROUP BY provider_token
    HAVING claim_count > 100
    ORDER BY avg_claim DESC
    LIMIT 50
""").df()
```

---

## 6. Feature Store

---

### Feast

**Created by:** The data science team at **Gojek** (Indonesian super-app, similar to Uber + DoorDash), open-sourced in 2019. Gojek had dozens of ML models (fraud, ETA, pricing, recommendations) all computing the same features independently, leading to inconsistency: a feature computed slightly differently for training vs serving caused model performance to degrade silently in production.

**Open source:** Yes — **Apache License 2.0**. Now maintained by the Feast community and **Tecton** (commercial).

**Purpose:** Feast is an open source feature store. It solves two problems: (1) **training-serving skew** — ensuring that the same feature values computed for model training are also available at serving time; (2) **feature reuse** — defining a feature once and sharing it across multiple models. Feast has an offline store (for training, backed by Iceberg) and an online store (for real-time serving, backed by Redis) and keeps them synchronized.

**Why it was created:** Without a feature store, each ML model team writes its own feature pipeline (often as a Spark job). Model A computes "provider claim count in 30 days" with one definition; Model B computes the same feature with a slightly different window. When models are retrained on offline data but served with live data, subtle differences in feature computation cause the model to underperform. Feast enforces a single feature definition used consistently everywhere.

**Architecture example — fraud platform:**

```python
# Feature definition: provider_stats
from feast import FeatureView, Entity, Field, FileSource
from feast.types import Float64, Int64

provider = Entity(name="provider", join_keys=["provider_token"])

provider_stats_source = FileSource(
    path="s3://fraud-datalake/gold/features/provider_profiles/",
    file_format=ParquetFormat(),
    timestamp_field="feature_date",
)

provider_stats_view = FeatureView(
    name="provider_stats_30d",
    entities=[provider],
    ttl=timedelta(days=1),
    schema=[
        Field(name="claim_count_30d",      dtype=Int64),
        Field(name="avg_amount_30d",        dtype=Float64),
        Field(name="unique_members_30d",    dtype=Int64),
        Field(name="denial_rate_30d",       dtype=Float64),
        Field(name="procedure_entropy_30d", dtype=Float64),
    ],
    source=provider_stats_source,
)

# Training: point-in-time correct feature retrieval
# (no future leakage — uses only features known at claim_date)
training_df = store.get_historical_features(
    entity_df=claims_with_labels,       # claim_id, provider_token, claim_date, label
    features=["provider_stats_30d:claim_count_30d",
              "provider_stats_30d:avg_amount_30d"],
).to_df()

# Serving: real-time feature lookup via Redis (<5ms)
features = store.get_online_features(
    features=["provider_stats_30d:claim_count_30d"],
    entity_rows=[{"provider_token": "abc123"}],
).to_dict()
```

---

### Redis

**Created by:** Salvatore Sanfilippo (known as "antirez"), a Sicilian software engineer, in 2009. He was building a real-time web analytics system and found that PostgreSQL was too slow for the workload. He created Redis — Remote Dictionary Server — as an in-memory key-value store that could handle millions of operations per second.

**Open source:** Yes (historically) — **BSD 3-Clause**. In 2024 Redis Ltd. changed the license to **RSAL + SSPL** (not OSI-approved). The community forked it as **Valkey** under Apache 2.0, maintained by the Linux Foundation. Either works as a drop-in replacement here.

**Purpose:** Redis is an in-memory data structure store used as a database, cache, and message broker. In this platform, Redis serves two purposes: (1) as the online store for Feast — feature vectors for real-time ML inference are pre-computed and stored in Redis for sub-millisecond lookup; (2) as a cache for the fraud score API — recently scored claims are cached to avoid redundant inference calls.

**Architecture example — fraud platform:**

```
Feast materializes features to Redis daily:
  feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

Redis key structure:
  provider_stats_30d:abc123 →
    HSET {
      claim_count_30d:    "142",
      avg_amount_30d:     "870.50",
      unique_members_30d: "38",
      denial_rate_30d:    "0.12"
    }
    TTL: 86400 (1 day)

Real-time inference flow:
  1. Flink receives claim for provider NPI-1234
  2. Flink calls: HGETALL provider_stats_30d:NPI-1234  → ~0.3ms
  3. Flink attaches features to claim event
  4. Flink calls BentoML inference endpoint with features → ~15ms
  5. Fraud score written to scored.claims topic
  Total latency: ~20ms from claim arrival to score
```

---

## 7. ML Platform

---

### MLflow

**Created by:** Matei Zaharia (also creator of Spark) and the team at **Databricks**, released as open source in 2018. The problem: ML experiments are chaotic. A data scientist runs 50 experiments over two weeks, changes hyperparameters, tries different feature sets, and at the end cannot remember which combination produced the best model.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** MLflow is an open source ML lifecycle management platform with four components: (1) **Tracking** — log parameters, metrics, and artifacts for every experiment run; (2) **Models** — a standard format to package ML models with their dependencies for deployment; (3) **Model Registry** — a versioned catalog of models with staging/production promotion workflow; (4) **Projects** — a format to package ML code for reproducible execution. In this platform, MLflow is the single source of truth for all trained models and their performance history.

**Why it was created:** Before MLflow, data scientists used spreadsheets, notebooks, and folders named "model_final_v3_USE_THIS" to track experiments. Reproducing a model from 3 months ago was often impossible. MLflow brought software engineering discipline to ML workflows.

**Architecture example — fraud platform:**

```python
# Training run: claim fraud classifier
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.metrics import roc_auc_score, average_precision_score

mlflow.set_tracking_uri("http://mlflow.platform.svc.cluster.local:5000")
mlflow.set_experiment("claim-fraud-classifier")

with mlflow.start_run(run_name="xgb-v3-with-network-features"):

    # Log parameters
    mlflow.log_params({
        "max_depth": 8,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "feature_set": "provider_stats_30d + member_stats_90d + network_v2",
    })

    model = xgb.XGBClassifier(max_depth=8, learning_rate=0.05, n_estimators=500)
    model.fit(X_train, y_train)

    # Log metrics
    y_pred = model.predict_proba(X_test)[:, 1]
    mlflow.log_metrics({
        "auc_roc":       roc_auc_score(y_test, y_pred),      # 0.943
        "avg_precision": average_precision_score(y_test, y_pred),  # 0.721
        "precision_at_10pct": precision_at_k(y_test, y_pred, 0.10),
    })

    # Register model
    mlflow.xgboost.log_model(model, "model",
        registered_model_name="claim-fraud-classifier")

# Promote to production via registry:
# Staging → (shadow test for 48h) → Production
client.transition_model_version_stage(
    name="claim-fraud-classifier", version=7, stage="Production"
)
```

---

### Ray

**Created by:** The **RISELab at UC Berkeley** (successor to AMPLab, which created Spark), led by Ion Stoica, Philipp Moritz, Robert Nishihara, and others, first released in 2018. The team was working on reinforcement learning and found that existing distributed computing frameworks (Spark, Dask) were designed for batch data processing, not for the irregular, task-graph-based computation patterns that ML training requires.

**Open source:** Yes — **Apache License 2.0**. Commercially developed by **Anyscale**.

**Purpose:** Ray is a distributed computing framework for Python, designed specifically for ML workloads. Its key abstractions are: (1) **Ray Core** — a task and actor model for general parallel Python; (2) **Ray Train** — distributed model training (PyTorch, TensorFlow, XGBoost); (3) **Ray Tune** — hyperparameter search with early stopping; (4) **Ray Serve** — scalable model serving. In this platform, Ray is used for distributed model training and hyperparameter tuning jobs that would be too slow on a single machine.

**Architecture example — fraud platform:**

```python
# Hyperparameter tuning for the claim fraud classifier using Ray Tune
from ray import tune
from ray.tune.schedulers import ASHAScheduler
import xgboost as xgb

def train_fraud_model(config):
    # Loads features from Feast offline store
    df = feast_store.get_historical_features(...)

    model = xgb.XGBClassifier(
        max_depth=config["max_depth"],
        learning_rate=config["lr"],
        subsample=config["subsample"],
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)])

    auc = roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])
    tune.report(auc=auc)

# Search 200 configurations in parallel across 10 LKE pods
analysis = tune.run(
    train_fraud_model,
    config={
        "max_depth":  tune.randint(4, 12),
        "lr":         tune.loguniform(0.01, 0.3),
        "subsample":  tune.uniform(0.6, 1.0),
    },
    num_samples=200,
    scheduler=ASHAScheduler(metric="auc", mode="max"),
    resources_per_trial={"cpu": 4, "memory": 8 * 1024**3},
)

best_config = analysis.best_config
# → {"max_depth": 7, "lr": 0.042, "subsample": 0.85}
```

---

### BentoML

**Created by:** Chaoyu Yang and the team at **BentoML Inc.**, first released in 2019. The problem: data scientists train models in Jupyter notebooks with no path to production. Packaging a model for serving requires writing a Flask app, a Dockerfile, Kubernetes manifests, and health check endpoints — work that is repeated for every model.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** BentoML is a model serving framework that turns a trained ML model into a production-ready REST API or streaming service. A data scientist writes a simple `@bentoml.service` class, and BentoML handles batching, serialization, multi-model composition, health checks, metrics, Dockerfile generation, and Kubernetes deployment. Models from any framework (XGBoost, PyTorch, sklearn, ONNX) are supported.

**Architecture example — fraud platform:**

```python
# BentoML service: fraud scoring endpoint
import bentoml
import numpy as np
from bentoml.io import NumpyNdarray, JSON

fraud_runner = bentoml.xgboost.get("claim-fraud-classifier:production").to_runner()

@bentoml.service(runners=[fraud_runner])
class FraudScoringService:

    @bentoml.api(input=JSON(), output=JSON())
    async def score(self, claim_features: dict) -> dict:
        features = np.array([[
            claim_features["claim_count_30d"],
            claim_features["avg_amount_30d"],
            claim_features["procedure_entropy_30d"],
            claim_features["member_claim_velocity_90d"],
            # ... 40 more features
        ]])

        fraud_prob = await fraud_runner.predict_proba.async_run(features)

        return {
            "claim_id":    claim_features["claim_id"],
            "fraud_score": float(fraud_prob[0][1]),
            "model_version": "claim-fraud-classifier:7",
        }
```

```
Flink MLScoringJob → POST http://fraud-scorer.serving.svc:3000/score
                         {features...}
                      ← {"fraud_score": 0.87, "model_version": "7"}
                      ~15ms p50, ~45ms p99

BentoML on LKE:
  HPA: min 3 pods, max 20 pods
  Trigger: CPU > 70% or requests/s > 500
  → scales to 20 pods in ~90 seconds
```

---

## 8. Serving Layer

---

### FastAPI

**Created by:** Sebastián Ramírez (known as "tiangolo"), a Colombian software engineer, in 2018. He was building APIs professionally and found that existing Python frameworks (Flask, Django) required boilerplate code for request validation, serialization, and documentation. He built FastAPI on top of Starlette (async HTTP) and Pydantic (data validation) to auto-generate all of that from Python type annotations.

**Open source:** Yes — **MIT License**.

**Purpose:** FastAPI is a modern, high-performance Python web framework for building REST APIs. It auto-generates OpenAPI (Swagger) documentation from type annotations, validates request and response schemas automatically with Pydantic, and handles async I/O natively (critical for an inference API that calls Redis and multiple ML services concurrently). In this platform, FastAPI powers the external fraud score API used by claims adjudication systems.

**Architecture example — fraud platform:**

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis.asyncio as redis
import httpx

app = FastAPI(title="Fraud Detection API", version="1.0")

class ClaimScoreRequest(BaseModel):
    claim_id: str
    provider_token: str
    amount: float
    procedure_code: str

class ClaimScoreResponse(BaseModel):
    claim_id: str
    fraud_score: float          # 0.0 → 1.0
    risk_level: str             # LOW / MEDIUM / HIGH / CRITICAL
    triggered_rules: list[str]
    model_version: str

@app.post("/v1/score", response_model=ClaimScoreResponse)
async def score_claim(request: ClaimScoreRequest):
    # 1. Check cache (avoid re-scoring same claim)
    cached = await redis_client.get(f"score:{request.claim_id}")
    if cached:
        return ClaimScoreResponse.parse_raw(cached)

    # 2. Fetch features from Feast online store (Redis)
    features = await feast_client.get_online_features(request.provider_token)

    # 3. Call BentoML inference endpoint
    async with httpx.AsyncClient() as client:
        resp = await client.post("http://fraud-scorer:3000/score",
                                 json={**features, "claim_id": request.claim_id})

    score = resp.json()["fraud_score"]
    risk  = "CRITICAL" if score > 0.9 else "HIGH" if score > 0.7 else "MEDIUM" if score > 0.4 else "LOW"

    result = ClaimScoreResponse(
        claim_id=request.claim_id,
        fraud_score=score,
        risk_level=risk,
        triggered_rules=resp.json().get("rules", []),
        model_version=resp.json()["model_version"],
    )

    # 4. Cache result for 1 hour
    await redis_client.setex(f"score:{request.claim_id}", 3600, result.json())
    return result
```

---

## 9. Security

---

### HashiCorp Vault

**Created by:** Mitchell Hashimoto and Armon Dadgar at **HashiCorp**, released in 2015. HashiCorp recognized that secrets (database passwords, API keys, TLS certificates) were typically stored in config files, environment variables, and spreadsheets — none of which are auditable, rotatable, or revocable.

**Open source:** Partially. Community edition is **Mozilla Public License 2.0**. Enterprise adds HSM support, replication, and advanced governance. Note: in 2023 HashiCorp changed the license to **BUSL 1.1** for new versions; the community forked it as **OpenBao** under Mozilla Public License.

**Purpose:** Vault is a secrets management and encryption-as-a-service platform. It stores and controls access to tokens, passwords, certificates, and encryption keys. Critically, Vault generates **dynamic secrets** — temporary database credentials that exist only for the duration of a job and are automatically revoked. This means there are no long-lived passwords to rotate or leak. Vault also provides the **Transit engine** for application-layer encryption without exposing keys.

**Architecture example — fraud platform:**

```
Problem without Vault:
  Spark job config: database.password = "prod_password_123"
  → stored in Airflow variable, visible to all Airflow users
  → committed to git by accident in 2019
  → never rotated because "it's too risky"

Solution with Vault dynamic secrets:
  1. Spark job authenticates to Vault via Kubernetes service account
  2. Vault validates Kubernetes JWT token (no password needed)
  3. Vault generates temporary PostgreSQL credentials:
     {
       "username": "spark-etl-20260528-ab12",
       "password": "Vh8k...randomized...9Xp",
       "lease_duration": "1h",
       "lease_id": "database/creds/spark-etl/abc123"
     }
  4. Spark job uses credentials for 1 hour, then they're auto-revoked
  5. PostgreSQL audit log shows exactly which job accessed which tables

Vault Transit (PHI tokenization):
  plaintext CPF: "123.456.789-00"
  → vault.encrypt("transit/encrypt/phi-key", plaintext)
  → ciphertext: "vault:v1:AbCdEf..."
  → stored in bronze zone
  → only services with "transit/decrypt/phi-key" policy can reverse it
```

---

### Keycloak

**Created by:** Bill Burke and the team at **Red Hat**, released in 2013, now managed by the **Cloud Native Computing Foundation (CNCF)**. Red Hat needed an open source Identity Provider that could handle SSO, OAuth2, and OpenID Connect for their enterprise middleware products.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Keycloak is an Identity and Access Management (IAM) platform. It provides: single sign-on (SSO) across all platform services; OAuth2 / OpenID Connect for service-to-service authentication; user federation (LDAP/Active Directory sync); fine-grained authorization policies; and MFA. In this platform, Keycloak is the single identity authority — every human user and every service authenticates through it.

**Architecture example — fraud platform:**

```
Human users:
  Data scientist → logs into Jupyter/MLflow via Keycloak SSO
  Fraud analyst  → logs into Trino/Grafana via Keycloak SSO
  Platform admin → logs into ArgoCD/Airflow via Keycloak SSO

Service accounts (machine-to-machine):
  Flink jobs       → Keycloak client credentials → JWT token
                   → used to authenticate to Redpanda (SASL/OAUTHBEARER)
  Airflow DAGs     → Keycloak → JWT → Vault (for dynamic DB secrets)
  FastAPI          → Validates user JWTs from Keycloak public key

RBAC example:
  Realm: fraud-platform
  Roles:
    fraud-analyst:     read Trino (silver/gold), read Grafana
    data-engineer:     write Iceberg (bronze/silver), manage Airflow DAGs
    ml-engineer:       read/write MLflow, deploy BentoML models
    platform-admin:    full access
  Users:
    alice@company.com → [fraud-analyst]
    bob@company.com   → [data-engineer, ml-engineer]
```

---

### Open Policy Agent (OPA)

**Created by:** Torin Sandall and Tim Hinrichs at **Styra**, released in 2016, donated to the **CNCF** in 2018, graduated in 2021. As microservices proliferated, authorization logic was duplicated across dozens of services. Styra built OPA to externalize policy decisions into a single, auditable policy engine.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** OPA is a general-purpose policy engine. Services ask OPA "is this action allowed?" by sending a JSON context (who is the user, what are they trying to do, what resource). OPA evaluates a policy written in its language **Rego** and returns allow/deny. This decouples policy logic from application code — policies can be updated without redeploying services. In this platform, OPA enforces fine-grained access control on Trino queries (preventing analysts from reading columns containing PHI in the silver zone).

**Architecture example — fraud platform:**

```rego
# OPA policy: trino-data-access.rego
# Prevents non-PHI-authorized roles from reading PHI columns

package trino.data

import future.keywords.if
import future.keywords.in

# PHI columns that require explicit authorization
phi_columns := {"cpf", "name", "dob", "address", "phone"}

# Default deny
default allow = false

# Allow if user has phi_authorized role and requests PHI column
allow if {
    input.action.operation == "SELECT"
    input.action.resource.column in phi_columns
    "phi_authorized" in input.context.identity.groups
}

# Allow all non-PHI column access for authenticated users
allow if {
    input.action.operation == "SELECT"
    not input.action.resource.column in phi_columns
    input.context.identity.user != ""
}

# Allow data engineers to write
allow if {
    input.action.operation in {"INSERT", "CREATE TABLE", "DROP TABLE"}
    "data-engineer" in input.context.identity.groups
}
```

---

### Linkerd

**Created by:** William Morgan and Oliver Gould at **Buoyant**, released in 2016 as the first service mesh, donated to the **CNCF**, graduated in 2021. As microservices multiplied, implementing mutual TLS, retries, circuit breakers, and observability in every service became impractical. Buoyant externalized this into a sidecar proxy.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Linkerd is a service mesh for Kubernetes. It injects a lightweight proxy (written in Rust) as a sidecar into every pod. This proxy handles: mutual TLS (mTLS) for all service-to-service communication — without any code changes; automatic retries and timeouts; traffic metrics (success rate, latency, request volume) per service pair; and traffic splitting (for canary deployments). In this platform, Linkerd ensures that every connection between Flink, BentoML, Feast, and FastAPI is encrypted and mutually authenticated.

**Architecture example — fraud platform:**

```
Without Linkerd:
  Flink pod → plaintext HTTP → BentoML pod
  (traffic visible to any process on the same node)

With Linkerd:
  Flink pod [linkerd-proxy sidecar]
    → mTLS (certificate from Linkerd CA)
    → encrypted TCP
    → mTLS termination
  BentoML pod [linkerd-proxy sidecar]
  (no application code change required)

Automatic certificate rotation:
  Linkerd CA issues certificates with 24h TTL
  Proxies auto-renew → no manual cert management

Traffic metrics (exported to Prometheus):
  route: flink → bentoml/score
    success_rate: 99.94%
    p50_latency:  14ms
    p99_latency:  43ms
    rps:          320

Canary deployment of new fraud model:
  linkerd traffic-split:
    backend-v7: 90%    ← current production
    backend-v8: 10%    ← new model candidate
  → compare fraud_score distributions before full rollout
```

---

### Falco

**Created by:** Loris Degioanni (also creator of Wireshark) at **Sysdig**, first released in 2016, donated to the **CNCF** in 2018, graduated in 2020. Degioanni applied the packet inspection principle (watching network traffic for anomalies) to kernel system calls — watching what processes do at the OS level to detect intrusions.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Falco is a runtime security tool for Linux and Kubernetes. It hooks into the Linux kernel (via eBPF or kernel module) and observes every system call made by every process in real time. It compares observed behavior against a rule set and raises alerts for anomalies: a container that opens a shell, a process that reads `/etc/shadow`, a pod that establishes an unexpected outbound network connection, a process that writes to `/tmp` and executes it. In this platform, Falco detects if a compromised pod attempts to access PHI outside its authorized scope.

**Architecture example — fraud platform:**

```yaml
# Falco rule: detect if a Flink or Spark pod reads PHI raw files directly
# (should only access silver/gold, never bronze)
- rule: Unauthorized PHI file access from ML workload
  desc: A processing pod is reading bronze zone PHI data directly
  condition: >
    spawned_process and
    container.image.repository contains "flink" or
    container.image.repository contains "spark" and
    (fd.name startswith "/bronze/claims" or
     evt.arg.path contains "s3://fraud-datalake/bronze/claims") and
    not proc.name in (silver_etl_authorized_processes)
  output: >
    PHI access from unauthorized process
    (user=%user.name cmd=%proc.cmdline file=%fd.name
     container=%container.id pod=%k8s.pod.name)
  priority: CRITICAL
  tags: [phi, lgpd, hipaa, pci]

# Alert goes to:
#   → Falco Sidekick → Slack #security-alerts
#   → Falco Sidekick → PagerDuty (P1 incident)
#   → Loki (audit log)
```

---

## 10. Observability

---

### Prometheus

**Created by:** Julius Volz and Björn Rabenstein at **SoundCloud**, written in Go, released in 2012, donated to the **CNCF** in 2016, graduated in 2018. SoundCloud needed a monitoring system that could handle the dynamic, ephemeral nature of microservices — where instances come and go and IP addresses change constantly. Existing tools (Nagios, Graphite) required static configuration.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Prometheus is a time-series metrics database and monitoring system. Services expose metrics on an HTTP endpoint (`/metrics`); Prometheus scrapes them on a schedule. Metrics are stored as time series (value + timestamp + labels). Prometheus evaluates alert rules continuously and fires alerts to Alertmanager. In this platform, Prometheus collects metrics from Redpanda, Flink, Spark, BentoML, Keycloak, and all LKE nodes.

**Architecture example — fraud platform:**

```yaml
# prometheus.yml — scrape configuration
scrape_configs:
  - job_name: redpanda
    static_configs:
      - targets: ['redpanda-0:9644', 'redpanda-1:9644', 'redpanda-2:9644']
    metrics_path: /public_metrics

  - job_name: flink
    kubernetes_sd_configs:
      - role: pod
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        regex: flink-taskmanager
        action: keep

# Alert rule: Redpanda consumer lag too high (Flink falling behind)
groups:
  - name: fraud_platform
    rules:
      - alert: FlinkConsumerLagHigh
        expr: >
          redpanda_kafka_consumer_group_lag{
            group="flink-scoring-job",
            topic="enriched.claims"
          } > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Flink scoring job is falling behind"
          description: "Consumer lag {{ $value }} on enriched.claims"
```

---

### Grafana

**Created by:** Torkel Ödegaard, a Norwegian software engineer, in 2013 as a fork of Kibana (Elastic's visualization tool). He wanted a visualization layer that was not tied to a single data source — one dashboard that could show Prometheus metrics, Loki logs, and Tempo traces side by side.

**Open source:** Yes — **GNU Affero General Public License v3 (AGPL-3.0)**. Commercially developed by **Grafana Labs**.

**Purpose:** Grafana is a visualization and dashboarding platform. It connects to dozens of data sources (Prometheus, Loki, Tempo, PostgreSQL, Elasticsearch, BigQuery) and builds interactive dashboards with graphs, tables, heatmaps, and alerts. It is the operational "single pane of glass" for the entire platform.

**Architecture example — fraud platform:**

```
Grafana dashboards:

1. Fraud Platform Overview
   ├── Redpanda: messages/sec per topic (last 24h)
   ├── Flink: consumer lag by job (alert band at 10k)
   ├── BentoML: inference latency p50/p99 (alert at 100ms)
   ├── Fraud scores: distribution histogram by risk level
   └── Alert rate: HIGH/CRITICAL fraud alerts per hour

2. ML Model Performance
   ├── Daily precision / recall / AUC (from MLflow → Prometheus)
   ├── Feature drift PSI score by feature (alert at 0.2)
   ├── Model version currently serving
   └── Canary vs production score comparison

3. Security
   ├── Falco: alert count by rule (last 7 days)
   ├── Vault: secret access count by service
   ├── Keycloak: failed login attempts
   └── OPA: policy deny count by policy name
```

---

### Grafana Loki

**Created by:** Tom Wilkie at **Grafana Labs**, released in 2018. Wilkie observed that Elasticsearch (the dominant log storage solution) was extremely expensive because it indexed every word in every log line. For most use cases, you don't need full-text search — you need to filter logs by Kubernetes labels (namespace, pod, container) and then grep the results.

**Open source:** Yes — **AGPL-3.0**.

**Purpose:** Loki is a log aggregation system inspired by Prometheus. Instead of indexing log content (like Elasticsearch), Loki only indexes the **labels** on each log stream (pod name, namespace, container, log level). The raw log text is stored compressed in object storage. Queries use **LogQL** to filter by labels and then grep the content. This makes Loki 10–100x cheaper to operate than Elasticsearch for the same volume of logs, at the cost of slower full-text search.

**Architecture example — fraud platform:**

```
Fluentbit (DaemonSet) → Loki

LogQL queries in Grafana:

# Show all ERROR logs from Flink in the last 1 hour:
{namespace="processing", app="flink-taskmanager"} |= "ERROR"

# Show fraud alert events with claim details:
{namespace="serving", app="fraud-api"}
  | json
  | fraud_score > 0.9
  | line_format "claim={{.claim_id}} score={{.fraud_score}} provider={{.provider_token}}"

# Count Vault PHI decrypt calls per service (security audit):
{namespace="platform", app="vault"}
  | logfmt
  | operation="transit/decrypt"
  | count_over_time[1h]) by (service_account)
```

---

### Grafana Tempo

**Created by:** The **Grafana Labs** team, released in 2020. Jaeger and Zipkin (existing distributed tracing tools) required Elasticsearch or Cassandra as backends, making them expensive to operate at scale. Tempo stores traces in object storage (S3/GCS) with no indexing — dramatically reducing cost.

**Open source:** Yes — **AGPL-3.0**.

**Purpose:** Tempo is a distributed tracing backend. Distributed tracing follows a request across multiple services — from the API endpoint through Flink, Feast, Redis, and BentoML — and records how long each hop took. This is essential for diagnosing latency issues in a multi-service inference pipeline. Tempo stores traces in object storage and queries them by trace ID; Grafana visualizes the trace as a waterfall diagram.

**Architecture example — fraud platform:**

```
A single claim scoring request produces a trace:

Trace ID: abc-123-def
  │
  ├── FastAPI /v1/score                        2ms
  │    ├── Redis HGETALL (feature lookup)      0.4ms
  │    ├── BentoML /score                      44ms
  │    │    ├── Feature deserialization        1ms
  │    │    ├── XGBoost predict               38ms
  │    │    └── Response serialization         5ms
  │    └── Redis SETEX (cache result)          0.3ms
  └── Total                                   46.7ms

Slow trace detected (p99 > 100ms):
  → Grafana Tempo shows: BentoML XGBoost predict = 95ms (normally 38ms)
  → Root cause: model loaded from disk (pod restarted), first inference cold start
  → Fix: add model pre-warming on pod startup
```

---

### OpenTelemetry

**Created by:** The **Cloud Native Computing Foundation (CNCF)**, formed in 2019 by merging two competing projects: **OpenTracing** (a tracing standard from Ben Sigelman at LightStep) and **OpenCensus** (a metrics+tracing library from Google). Both projects recognized they were solving the same problem and unified.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** OpenTelemetry is a vendor-neutral observability framework. It provides a single SDK to instrument application code and emit traces, metrics, and logs in a standard format. The **OTel Collector** receives telemetry from all sources and routes it to multiple backends (Prometheus for metrics, Tempo for traces, Loki for logs) from a single pipeline. The critical benefit: instrument once, change backends without code changes.

**Architecture example — fraud platform:**

```python
# FastAPI service instrumented with OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

tracer = trace.get_tracer("fraud-api")

@app.post("/v1/score")
async def score_claim(request: ClaimScoreRequest):
    with tracer.start_as_current_span("score_claim") as span:
        span.set_attribute("claim.id",           request.claim_id)
        span.set_attribute("claim.amount",        request.amount)
        span.set_attribute("provider.token",      request.provider_token)

        with tracer.start_as_current_span("fetch_features"):
            features = await feast_client.get_online_features(...)

        with tracer.start_as_current_span("ml_inference"):
            score = await bentoml_client.score(features)

        span.set_attribute("fraud.score", score)
        return score

# Spans → OTel Collector → Tempo (traces) + Prometheus (metrics)
```

---

### Fluentbit

**Created by:** Eduardo Silva at **Treasure Data** (a Japanese data management company), released in 2015 as a lightweight companion to **Fluentd** (which Treasure Data also created). Fluentd is written in Ruby and uses ~40MB RAM per node; Fluentbit is written in C and uses ~1MB RAM — designed for IoT devices and edge nodes where memory is scarce.

**Open source:** Yes — **Apache License 2.0**. Now maintained by **Calyptia** (acquired by Chronosphere).

**Purpose:** Fluentbit is a lightweight log processor and forwarder. It runs as a DaemonSet on every Kubernetes node, tails all container log files, parses them (JSON, multi-line stack traces, etc.), enriches them with Kubernetes metadata (pod name, namespace, labels), and forwards them to a destination (Loki, Elasticsearch, Kafka). It is the log collection agent of choice for Kubernetes environments.

**Architecture example — fraud platform:**

```yaml
# fluent-bit configmap
[INPUT]
    Name              tail
    Path              /var/log/containers/*.log
    Parser            docker
    Tag               kube.*
    Refresh_Interval  5
    Mem_Buf_Limit     50MB

[FILTER]
    Name                kubernetes
    Match               kube.*
    Kube_URL            https://kubernetes.default.svc:443
    Merge_Log           On
    K8S-Logging.Parser  On

[FILTER]
    Name   record_modifier
    Match  kube.*
    Record cluster fraud-platform-lke

[OUTPUT]
    Name            loki
    Match           kube.*
    Host            loki.platform.svc.cluster.local
    Port            3100
    Labels          job=fluentbit, namespace=$kubernetes['namespace_name'],
                    pod=$kubernetes['pod_name'], app=$kubernetes['labels']['app']
    Auto_Kubernetes_Labels On
```

---

## 11. Platform & GitOps

---

### Kubernetes (Linode LKE)

**Created by:** Joe Beda, Brendan Burns, and Craig McLuckie at **Google**, released as open source in 2014. It was based on Google's internal cluster management system **Borg**, which had run Google's production workloads for a decade. Google donated Kubernetes to the newly formed **CNCF** in 2015.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Kubernetes is a container orchestration platform. It manages the deployment, scaling, and lifecycle of containerized workloads across a cluster of machines. It handles: scheduling containers on available nodes, restarting failed containers, scaling pods in or out based on load, managing network routing between services, and storing secrets. **Linode LKE** is a managed Kubernetes offering — Linode manages the control plane (API server, scheduler, etcd); the team manages worker nodes and workloads.

**Architecture example — fraud platform:**

```yaml
# Kubernetes namespace strategy
namespaces:
  ingestion:    Redpanda Connect, Debezium, Vector
  processing:   Flink, Spark, Airflow
  ml:           MLflow, Ray, BentoML, Feast
  serving:      FastAPI, Redis
  platform:     Vault, Keycloak, ArgoCD, Prometheus, Grafana, Loki, Tempo
  security:     Falco, OPA, Linkerd control plane

# Example: BentoML Deployment with HPA
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud-scorer
  namespace: ml
spec:
  replicas: 3
  selector:
    matchLabels:
      app: fraud-scorer
  template:
    spec:
      containers:
        - name: fraud-scorer
          image: registry.company.com/fraud-scorer:v7
          resources:
            requests: { cpu: "2", memory: "4Gi" }
            limits:   { cpu: "4", memory: "8Gi" }
          readinessProbe:
            httpGet: { path: /healthz, port: 3000 }
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: fraud-scorer-hpa
  namespace: ml
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: fraud-scorer
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

---

### Helm

**Created by:** The team at **Deis** (a container platform startup acquired by Microsoft in 2017), released in 2015, donated to the **CNCF**, graduated in 2020. Deploying a complex application on Kubernetes requires dozens of YAML files (Deployments, Services, ConfigMaps, Secrets, RBAC rules). Helm packages these into a versioned, configurable bundle.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** Helm is the package manager for Kubernetes. A **Helm chart** is a template of Kubernetes manifests with configurable values. Deploying MLflow becomes `helm install mlflow mlflow/mlflow -f values.yaml` instead of maintaining 15 YAML files. Helm also manages upgrades (helm upgrade) and rollbacks (helm rollback) and maintains a release history.

**Architecture example — fraud platform:**

```bash
# Install Redpanda with custom values
helm repo add redpanda https://charts.redpanda.com
helm install redpanda redpanda/redpanda \
  --namespace ingestion \
  --values redpanda-values.yaml

# redpanda-values.yaml
statefulset:
  replicas: 3
resources:
  cpu:    { cores: 8 }
  memory: { container: { max: "32Gi" } }
storage:
  tieredConfig:
    cloud_storage_enabled: true
    cloud_storage_bucket: fraud-datalake
    cloud_storage_region: us-east-1

# Upgrade with zero downtime (rolling update)
helm upgrade redpanda redpanda/redpanda \
  --namespace ingestion \
  --values redpanda-values.yaml \
  --set statefulset.updateStrategy.type=RollingUpdate
```

---

### ArgoCD

**Created by:** The team at **Intuit** (financial software company, maker of TurboTax and QuickBooks), released as open source in 2018, donated to the **CNCF** in 2020, graduated in 2022. Intuit had hundreds of microservices deployed on Kubernetes and needed a way to ensure that what ran in the cluster exactly matched what was declared in Git — the GitOps principle.

**Open source:** Yes — **Apache License 2.0**.

**Purpose:** ArgoCD is a GitOps continuous delivery tool for Kubernetes. It watches a Git repository for changes to Kubernetes manifests or Helm charts and automatically applies those changes to the cluster. If the cluster state drifts from what Git declares (e.g., someone manually edits a deployment), ArgoCD detects it and alerts or auto-heals. Every deployment is a Git commit — providing a complete, auditable history of every change to the platform.

**Architecture example — fraud platform:**

```
Git repository: github.com/company/fraud-platform-infra
  │
  ├── apps/
  │    ├── redpanda/         helm chart values
  │    ├── flink/            flink operator CRDs
  │    ├── mlflow/           deployment + service
  │    ├── bentoml/          deployment + hpa
  │    └── airflow/          helm chart values
  └── environments/
       ├── staging/          overrides for staging
       └── production/       overrides for production

Workflow:
  1. Engineer opens PR: "bump BentoML fraud-scorer to v8"
  2. CI runs: lint, test, security scan
  3. PR approved → merge to main
  4. ArgoCD detects change in apps/bentoml/deployment.yaml
  5. ArgoCD syncs: applies rolling update to fraud-scorer pods in LKE
  6. ArgoCD health check: waits for all pods Ready
  7. ArgoCD sends Slack notification: "fraud-scorer v8 deployed ✓"

If deployment fails health check:
  ArgoCD marks sync as degraded
  → Slack alert: "fraud-scorer v8 degraded, manual rollback required"
  Engineer: argocd app rollback fraud-scorer
```

---

## Summary: Tool Relationships

```
Data flows through the platform:

[Sources] → Debezium/Vector/RedpandaConnect
           → REDPANDA (streaming backbone)
           → Flink (stream) / Spark (batch)
           → Iceberg on Object Storage (Nessie catalog)
           → Feast (features) ←→ Redis (online)
           → MLflow (model registry)
           → BentoML (serving)
           → FastAPI (external API)

Every service is:
  Deployed by:    Helm + ArgoCD (on LKE/Kubernetes)
  Monitored by:   Prometheus + Grafana
  Logged by:      Fluentbit → Loki → Grafana
  Traced by:      OpenTelemetry → Tempo → Grafana
  Secured by:     Vault (secrets) + Keycloak (identity)
                  + OPA (policy) + Linkerd (mTLS) + Falco (runtime)
```
