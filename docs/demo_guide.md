# End-to-End Demo — Healthcare Fraud Detection Platform

This guide covers two runnable environments:

- **Local (Docker Compose)** — 13 services on your laptop, no cloud account needed.
- **LKE (Linode Kubernetes Engine)** — full medallion data lake on Akamai Object Storage, Nessie catalog, and Spark ETL. See [linode_deploy.md](linode_deploy.md) for provisioning; this document covers the walkthrough once the cluster is live.

---

## 1. What the Demo Shows

### 1.1 Real-time scoring pipeline (both environments)

A synthetic healthcare claims system generates 2 claims per second. ~8% are fraudulent. The pipeline detects fraud in real time:

```
Claims system (PostgreSQL)
        │
        │  INSERT  →  Debezium CDC (WAL)
        │  +  direct publish (demo reliability path)
        ▼
   REDPANDA  ─── raw.claims.new
        │
        ├─────────────────────────────────────────────────────┐
        ▼                                                     ▼
  Stream Processor                                   Bronze Consumer
    ├── Enrichment       → provider/member stats      │  (LKE only)
    ├── Rule scoring     → 4 deterministic rules      │  batches raw JSONL
    ├── ML scoring       → Fraud Scorer API (GBM)     │  to Object Storage
    └── Alert generation → score ≥ 0.65               ▼
        │                               s3://fraud-datalake/bronze/
        ├─── scored.claims                             │
        └─── alerts.fraud              Spark silver-etl (daily 02:00)
                │                                     │ tokenize PHI
        Grafana  ←  Prometheus         Iceberg nessie.silver.claims
                                                       │
                                  Spark gold-features (Sunday 04:00)
                                                       │ rolling aggregates
                                       ┌───────────────┴───────────────┐
                               gold.provider_features  gold.member_features
                                       └───────────────┬───────────────┘
                                               gold.claim_features
                                               (ML feature vectors)
```

### 1.2 Fraud scenarios injected by the generator

| Scenario | Trigger | Expected rule | Expected score |
|---|---|---|---|
| **Upcoding** | Orthopedic surgery billed by a cardiology clinic (NPI-001) | `PROCEDURE_DIAGNOSIS_MISMATCH` | ≥ 0.90 |
| **Excess amount** | Claim 5–10× provider average | `AMOUNT_3X_PROVIDER_AVG` | ≥ 0.78 |
| **Phantom billing** | Very high provider volume | `HIGH_VOLUME_PROVIDER` | ≥ 0.62 |
| **Suspect Clinic (NPI-005)** | Every claim from this provider is fraudulent | mixed | ≥ 0.70 |

### 1.3 Medallion zones (LKE)

| Zone | Path | Contents | PHI |
|---|---|---|---|
| **Bronze** | `s3://fraud-datalake/bronze/claims/year=…/month=…/day=…/` | Raw NDJSON, full fidelity, immutable | Present |
| **Silver** | Iceberg `nessie.silver.claims` | PHI tokenized (SHA-256), normalized timestamps, bad records dropped | None |
| **Gold** | Iceberg `nessie.gold.{provider,member,claim}_features` | ML-ready aggregates — 30-day provider rolling window, 90-day member rolling window | None |

---

## 2. Architecture Mapping

| Demo component | Production equivalent |
|---|---|
| `data-generator` Python script | Real claims adjudication system |
| Single Redpanda node | 3-node Redpanda Enterprise cluster on Linode VMs |
| `stream-processor` Python consumer | Apache Flink on LKE (4 jobs) |
| `fraud-scorer` FastAPI + GBM (synthetic) | BentoML serving XGBoost from MLflow registry |
| Redis (in-memory provider stats) | Feast online store + Redis on LKE |
| Prometheus + Grafana | Full observability stack (Loki, Tempo, OTel, Falco) |
| PostgreSQL single instance | Managed PostgreSQL cluster on Linode |
| Debezium standalone | Debezium on LKE (Kafka Connect cluster) |
| Local filesystem (MLflow) | Linode Object Storage (MLflow artifact store) |
| **`bronze-consumer` Deployment (LKE)** | **Reads `raw.claims.new`, writes NDJSON to Object Storage** |
| **Nessie catalog (LKE)** | **Git-like versioning for all Iceberg tables (branches, commits, merges)** |
| **Spark `silver-etl` (LKE, daily)** | **Tokenizes PHI, normalizes schema, writes Iceberg silver table** |
| **Spark `gold-features` (LKE, weekly)** | **Computes provider/member/claim feature aggregates for ML** |
| **Akamai Object Storage** | **S3-compatible data lake — bronze JSONL + Iceberg warehouse** |

---

## 3. Prerequisites

### Local (Docker Compose)

| Requirement | Minimum version |
|---|---|
| Docker Desktop or Docker Engine | 24.x |
| Docker Compose plugin | v2.x (`docker compose`) |
| Free RAM | 4 GB |
| Free disk | 3 GB |
| Internet access | Required to pull images on first run |

### LKE (Linode)

| Requirement | Notes |
|---|---|
| Terraform ≥ 1.6 | Provisions LKE cluster + Object Storage bucket |
| kubectl + Helm ≥ 3.14 | Applied by `deploy.sh` |
| Linode API token | `export TF_VAR_linode_token="..."` |
| Container registry | GHCR recommended — run `build-push.sh` before `deploy.sh` |

---

## 4. Quick Start

### Local

```bash
cd demo/
make start
# or: docker compose up --build -d
```

First run takes ~3–5 minutes (pulls ~2 GB of images, builds 3 Python services).

```bash
make status          # wait until all services show "running"
docker compose ps
```

Expected when ready:
```
NAME                STATUS
data-generator      running
debezium            running (healthy)
debezium-init       exited (0)     ← one-shot, exit 0 = success
fraud-scorer        running (healthy)
grafana             running
mlflow              running
postgres            running (healthy)
prometheus          running
redis               running (healthy)
redpanda            running (healthy)
redpanda-console    running
redpanda-init       exited (0)     ← one-shot, exit 0 = success
stream-processor    running
```

### LKE

```bash
export TF_VAR_linode_token="<your-token>"
export REGISTRY="ghcr.io/ricardolins/akamai_fraud_detection"
export TAG="demo"

# Build and push images (only needed once or after code changes)
cd infra && ./build-push.sh

# Provision cluster + deploy everything
./deploy.sh
```

See [linode_deploy.md](linode_deploy.md) for full LKE walkthrough and cost breakdown.

---

## 5. Access Points

### Local

| Service | URL | Credentials |
|---|---|---|
| **Grafana** (main dashboard) | http://localhost:3000 | admin / admin |
| **Redpanda Console** | http://localhost:8080 | — |
| **Fraud Scorer API** | http://localhost:8000/docs | — |
| **MLflow** | http://localhost:5000 | — |
| **Prometheus** | http://localhost:9090 | — |
| **Debezium REST API** | http://localhost:8083/connectors | — |

### LKE (NodePort)

| Service | URL | Credentials |
|---|---|---|
| **Grafana** | `http://<NODE_IP>:30300` | admin / admin |
| **Redpanda Console** | `http://<NODE_IP>:30808` | — |
| **Fraud Scorer API** | `http://<NODE_IP>:30800/docs` | — |
| **MLflow** | `http://<NODE_IP>:30500` | — |
| **Prometheus** | `http://<NODE_IP>:30909` | — |

`deploy.sh` prints the `NODE_IP` at the end of the run.

---

## 6. Demo Walkthrough

### Step 1 — Verify data is flowing into Redpanda

Open **Redpanda Console** → **Topics**

| Topic | Partitions | Expected activity |
|---|---|---|
| `raw.claims.new` | 4 | ~2 messages/sec |
| `scored.claims` | 4 | ~2 messages/sec |
| `alerts.fraud` | 1 | ~1 message every 6–10 sec |

Click `raw.claims.new` → **Messages** tab:

```json
{
  "claim_id": "CLM-A3F2B891",
  "provider_npi": "NPI-001",
  "provider_name": "Cardio Clinic SP",
  "member_id": "MBR-4521",
  "procedure_code": "93000",
  "diagnosis_code": "I10",
  "amount": 362.50,
  "service_date": "2026-05-28",
  "submitted_at": "2026-05-28T14:22:10.432"
}
```

Switch to `alerts.fraud`:

```json
{
  "claim_id": "CLM-D9E4A102",
  "provider_npi": "NPI-005",
  "amount": 8750.00,
  "fraud_score": 0.9312,
  "risk_level": "CRITICAL",
  "triggered_rules": ["PROCEDURE_DIAGNOSIS_MISMATCH", "AMOUNT_3X_PROVIDER_AVG"],
  "rule_score": 0.93,
  "ml_score": 0.891,
  "provider_stats": {
    "claim_count_30d": 47,
    "avg_amount_30d": 920.40
  }
}
```

---

### Step 2 — Watch fraud alerts in real time (terminal)

```bash
make logs-alerts
# or
docker compose logs -f stream-processor | grep FRAUD
# LKE: kubectl logs -f -n processing deploy/stream-processor | grep FRAUD
```

Expected output:
```
14:22:18  WARNING  FRAUD [CRITICAL ] CLM-D9E4A102  provider=NPI-005  amount=R$  8,750.00  score=0.9312  rules=['PROCEDURE_DIAGNOSIS_MISMATCH', 'AMOUNT_3X_PROVIDER_AVG']
14:22:24  WARNING  FRAUD [HIGH    ] CLM-F1B3C007  provider=NPI-001  amount=R$  3,140.00  score=0.7821  rules=['AMOUNT_3X_PROVIDER_AVG']
14:22:31  WARNING  FRAUD [CRITICAL ] CLM-88EA5D12  provider=NPI-005  amount=R$ 12,050.00  score=0.9601  rules=['EXTREME_AMOUNT', 'PROCEDURE_DIAGNOSIS_MISMATCH']
```

---

### Step 3 — Open Grafana and observe the dashboard

Open **Grafana** → login with **admin / admin** → **Fraud Detection Platform** dashboard loads automatically.

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Total Claims   │  Fraud Alerts   │  ML Latency p99  │  Active Providers    │
│  [counter ↑]    │  [red if > 10]  │  [< 50ms green]  │  [5 providers]       │
├──────────────────────────────┬───────────────────────────────────────────────┤
│  Claims Throughput           │  Fraud Alerts by Risk Level                  │
│  green: legitimate ~1.8/s    │  CRITICAL (red)   — upcoding, extreme amount │
│  red:   fraud ~0.16/s        │  HIGH (orange)    — amount deviation         │
│                              │  MEDIUM (yellow)  — pattern anomalies        │
├──────────────────────────────┴───────────────────────────────────────────────┤
│  Scoring Latency (p50/p95/p99)       │  ML Inferences by Risk Level          │
│  p50 ~30ms  p99 ~120ms               │  shows model's risk distribution      │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

### Step 4 — Test the Fraud Scorer API manually

Open `/docs` → `POST /score` → **Try it out**

**Scenario A — Legitimate claim:**
```json
{
  "claim_id": "TEST-001",
  "amount": 350.00,
  "claim_count_30d": 12,
  "avg_amount_30d": 380.00,
  "hour_of_day": 10,
  "is_weekend": 0,
  "procedure_risk": 0.15,
  "member_claim_count_90d": 5
}
```
Expected: `fraud_probability` ~ 0.02–0.08, `risk_level`: **LOW**

**Scenario B — Clear fraud:**
```json
{
  "claim_id": "TEST-002",
  "amount": 11500.00,
  "claim_count_30d": 145,
  "avg_amount_30d": 820.00,
  "hour_of_day": 3,
  "is_weekend": 1,
  "procedure_risk": 0.88,
  "member_claim_count_90d": 1
}
```
Expected: `fraud_probability` ~ 0.85–0.97, `risk_level`: **CRITICAL**

---

### Step 5 — Inspect provider state in Redis

```bash
# Local
docker exec -it redis redis-cli

# LKE
kubectl exec -n data deploy/redis-master -- redis-cli
```

```redis
KEYS ps:*
# → "ps:NPI-001"  "ps:NPI-002"  ...

HGETALL ps:NPI-005
# → "count"  "47"
#    "sum"    "432180.50"
# avg = R$9,195  (elevated — fraud inflates the baseline)

HGETALL ps:NPI-003
# → "count"  "22"
#    "sum"    "3960.00"
# avg = R$180  (legitimate lab procedure baseline)
```

---

### Step 6 — Inspect Debezium CDC (optional)

```bash
curl -s http://localhost:8083/connectors/claims-postgres-connector/status \
  | python3 -m json.tool
# Expected: connector.state=RUNNING, task[0].state=RUNNING
```

Debezium publishes full before/after state to `dbz.public.claims`:
```json
{
  "before": null,
  "after": {
    "id": 1024,
    "claim_id": "CLM-A3F2B891",
    "provider_npi": "NPI-001",
    "amount": "362.50",
    "status": "PENDING"
  },
  "__op": "c",
  "__ts_ms": 1748432400000
}
```

---

### Step 7 — Observe MLflow (model tracking)

Open MLflow UI. To simulate a training run:

```bash
# Local
docker exec -it stream-processor bash -c "
pip install mlflow scikit-learn > /dev/null 2>&1
python3 -c \"
import mlflow
from sklearn.ensemble import GradientBoostingClassifier

mlflow.set_tracking_uri('http://mlflow:5000')
mlflow.set_experiment('claim-fraud-demo')

with mlflow.start_run(run_name='demo-run-gbm'):
    mlflow.log_params({'model': 'GBM', 'n_estimators': 150, 'max_depth': 4})
    mlflow.log_metric('auc_roc', 0.943)
    mlflow.log_metric('avg_precision', 0.721)
    print('Run logged!')
\""
```

Refresh MLflow → **claim-fraud-demo** experiment → run appears.

---

### Step 8 — Increase fraud rate (stress test)

```bash
# Local
docker compose stop data-generator
FRAUD_RATE=0.30 CLAIMS_PER_SECOND=5 docker compose up -d data-generator

# LKE
kubectl set env -n processing deploy/data-generator FRAUD_RATE=0.30 CLAIMS_PER_SECOND=5
```

Observe in Grafana: fraud alert rate climbs, ML latency stays stable. Reset:

```bash
# Local
docker compose stop data-generator && docker compose up -d data-generator

# LKE
kubectl set env -n processing deploy/data-generator FRAUD_RATE=0.08 CLAIMS_PER_SECOND=2
```

---

### Step 9 — Medallion data lake (LKE only)

This section requires a running LKE cluster with `deploy.sh` completed.

#### 9.1 Verify bronze consumer is writing to Object Storage

```bash
# Check that the bronze consumer is healthy
kubectl get pods -n processing -l app=bronze-consumer

# Tail its logs — each flush line shows how many records were written and to which key
kubectl logs -f -n processing deploy/bronze-consumer
```

Expected output:
```
2026-05-28T14:30:00Z INFO Subscribed to raw.claims.new | batch_size=200 flush_interval=30s
2026-05-28T14:30:30Z INFO Flushed 60 records → s3://fraud-datalake/bronze/claims/year=2026/month=05/day=28/143000_4820.jsonl
2026-05-28T14:31:00Z INFO Flushed 120 records → s3://fraud-datalake/bronze/claims/year=2026/month=05/day=28/143100_4940.jsonl
```

You can verify the files using the AWS CLI (or any S3-compatible client) pointed at the Linode Object Storage endpoint:

```bash
export AWS_ACCESS_KEY_ID=<access-key-from-terraform-output>
export AWS_SECRET_ACCESS_KEY=<secret-key-from-terraform-output>
export AWS_ENDPOINT_URL_S3=https://br-gru-1.linodeobjects.com

aws s3 ls s3://fraud-datalake/bronze/claims/ --recursive | head -20
```

#### 9.2 Inspect the Nessie catalog

Nessie exposes a REST API at `http://nessie.data.svc.cluster.local:19120/api/v1`. Port-forward to reach it locally:

```bash
kubectl port-forward -n data svc/nessie 19120:19120 &

# List all branches (catalog namespaces are versioned like git)
curl -s http://localhost:19120/api/v1/trees | python3 -m json.tool

# List all tables on the main branch
curl -s "http://localhost:19120/api/v1/trees/tree/main/entries" | python3 -m json.tool
```

After the first `silver-etl` run you will see entries like:
```json
{
  "entries": [
    { "name": { "elements": ["silver", "claims"] }, "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "provider_features"] }, "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "member_features"] }, "type": "ICEBERG_TABLE" },
    { "name": { "elements": ["gold", "claim_features"] }, "type": "ICEBERG_TABLE" }
  ]
}
```

#### 9.3 Monitor Spark ETL jobs

The Spark Operator manages two `ScheduledSparkApplication` resources:

```bash
# See scheduled job definitions
kubectl get scheduledsparkapplication -n processing

# NAME                        SCHEDULE     LAST SCHEDULE   AGE
# silver-etl-scheduled        0 2 * * *    14h             2d
# gold-features-scheduled     0 4 * * 0    2d              2d
```

Trigger a manual run to test without waiting for the cron schedule:

```bash
# silver ETL (bronze → silver Iceberg)
kubectl create -f - <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  generateName: silver-etl-manual-
  namespace: processing
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: ghcr.io/ricardolins/akamai_fraud_detection/spark-jobs:demo
  imagePullPolicy: IfNotPresent
  mainApplicationFile: local:///app/silver_etl.py
  sparkVersion: "3.5.1"
  restartPolicy:
    type: Never
  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.nessie.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog"
    "spark.sql.catalog.nessie.uri": "http://nessie.data.svc.cluster.local:19120/api/v1"
    "spark.sql.catalog.nessie.ref": "main"
    "spark.sql.catalog.nessie.warehouse": "s3a://fraud-datalake/warehouse"
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
  driver:
    cores: 1
    memory: "1024m"
    serviceAccount: spark
    envFrom:
      - secretRef:
          name: object-storage-credentials
  executor:
    cores: 2
    instances: 2
    memory: "2048m"
    envFrom:
      - secretRef:
          name: object-storage-credentials
EOF
```

Watch it run:

```bash
# List applications
kubectl get sparkapplication -n processing

# NAME                    STATUS      AGE
# silver-etl-manual-x4b   RUNNING     42s

# Follow the driver logs
kubectl logs -f -n processing \
  $(kubectl get pod -n processing -l spark-role=driver --sort-by=.metadata.creationTimestamp -o name | tail -1)
```

Expected driver output:
```
[silver-etl] Read 3600 records from s3a://fraud-datalake/bronze/claims/
[silver-etl] Dropped 4 malformed records
[silver-etl] Done. Silver table total rows: 3596
```

#### 9.4 Verify Iceberg table snapshots (time travel)

After the silver ETL run, Iceberg creates a snapshot for every write. You can read from any snapshot using Spark SQL:

```bash
kubectl exec -n processing \
  $(kubectl get pod -n processing -l spark-role=driver -o name | tail -1) \
  -- /opt/spark/bin/spark-sql \
    --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie.data.svc.cluster.local:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse=s3a://fraud-datalake/warehouse
```

Inside spark-sql:

```sql
-- Count rows in the silver table
SELECT COUNT(*) FROM nessie.silver.claims;

-- See all snapshots (time travel audit trail)
SELECT snapshot_id, committed_at, operation
FROM nessie.silver.claims.snapshots
ORDER BY committed_at DESC;

-- View provider feature aggregates (gold zone)
SELECT provider_npi_token, claim_count_30d, avg_amount_30d, max_amount_30d
FROM nessie.gold.provider_features
ORDER BY claim_count_30d DESC
LIMIT 10;

-- Claim feature vectors ready for ML batch scoring
SELECT claim_id, amount_ratio, claim_count_30d, member_claim_count_90d
FROM nessie.gold.claim_features
WHERE amount_ratio > 3.0
ORDER BY amount_ratio DESC
LIMIT 20;
```

---

## 7. Data Flow Trace

Complete trace of a single fraudulent claim from submission to alert:

```
T+0ms    data-generator inserts CLM-D9E4A102 into PostgreSQL
         provider=NPI-005  procedure=27447 (knee replacement)
         diagnosis=I10 (hypertension)  amount=R$8,750

T+5ms    Redpanda receives message on raw.claims.new (partition 1, key=NPI-005)

T+8ms    stream-processor polls Redpanda, receives message

T+9ms    Enrichment:
           Redis HGETALL ps:NPI-005
           → claim_count_30d=47, avg_amount_30d=R$920

T+11ms   Rule scoring:
           R1: 8750 > 920 * 3.0 = 2760 → AMOUNT_3X_PROVIDER_AVG  (score=0.78)
           R3: procedure=27447 (ortho) + diagnosis=I10 (cardio) → MISMATCH (score=0.93)
           rule_score = 0.93

T+38ms   ML scoring:
           POST /score  features=[8750, 47, 920, 9.51, 14, 0, 0.90, 1]
           → fraud_probability=0.891
           ml_score = 0.891

T+39ms   Combine:
           final = max(0.93, 0.891*0.6 + 0.93*0.4) = max(0.93, 0.906) = 0.93

T+40ms   Publish to scored.claims
         Publish to alerts.fraud  (score 0.93 ≥ threshold 0.65)
         Log: FRAUD [CRITICAL] CLM-D9E4A102  score=0.93

T+30s    Bronze consumer flushes batch → s3://fraud-datalake/bronze/claims/…/
         (CLM-D9E4A102 is written as raw JSONL with full PHI)

T+45s    Prometheus scrapes /metrics
         → stream_processor_fraud_alerts_total{risk_level="CRITICAL"} += 1

T+50s    Grafana refreshes (5s interval) — red spike visible on dashboard

02:00    [next day]  Spark silver-etl reads bronze partition:
           - Tokenizes member_id → member_id_token (SHA-256 prefix)
           - Tokenizes provider_npi → provider_npi_token
           - Drops PHI columns; writes to nessie.silver.claims (Iceberg, Parquet/Snappy)

Sunday   [next week]  Spark gold-features reads nessie.silver.claims:
04:00      - Computes 30d provider rolling aggregates
           - Computes 90d member rolling aggregates
           - Joins per-claim → nessie.gold.claim_features (ML feature vectors)
```

---

## 8. Production Architecture Differences

| Aspect | Demo (Docker Compose) | LKE | Full Production |
|---|---|---|---|
| **Streaming state** | Redis hash (simple) | Redis hash (same) | Flink RocksDB (event-time windows, exactly-once) |
| **Stream processing** | Python while-loop | Python while-loop | Apache Flink on LKE (parallel, fault-tolerant, checkpoints) |
| **ML model** | GBM on synthetic data | GBM on synthetic data | XGBoost trained on Iceberg gold features, served via BentoML |
| **Feature freshness** | Updated in-request | Updated in-request | Feast materializes from gold zone to Redis daily |
| **Data lake** | None | Iceberg on Akamai Object Storage (bronze/silver/gold) | Same + Feast offline store, MLflow artifact store |
| **PHI handling** | Plain text | SHA-256 token in silver/gold; raw only in bronze | Same + Vault Transit encryption at rest, field-level masking |
| **Catalog** | None | Nessie (Git-like, JDBC/PostgreSQL backend) | Same + schema evolution governance via Nessie branches |
| **ETL orchestration** | None | Spark Operator ScheduledSparkApplication | Apache Airflow or Argo Workflows |
| **Security** | None | Kubernetes RBAC only | Vault + Keycloak + OPA + Linkerd mTLS + Falco |
| **Redpanda** | Single node, no auth | Single node, no auth | 3-node cluster, TLS, RBAC, tiered storage to Object Storage |
| **Scale** | 2 claims/sec | 2 claims/sec | 50 000+ claims/sec (linear horizontal scaling) |
| **Fault tolerance** | Container restart | Pod restart | Flink savepoints, Redpanda RF=3, Iceberg snapshots |

---

## 9. Troubleshooting

**Services not starting (local):**
```bash
docker compose logs redpanda        # Redpanda startup
docker compose logs postgres        # PostgreSQL init
docker compose logs fraud-scorer    # model training (~20s)
```

**No messages in Redpanda Console:**
```bash
docker compose logs data-generator  # should show "[xxx] CLM-... amount=..."
docker compose logs redpanda-init   # should show "✓ Topics created"
```

**stream-processor not detecting fraud:**
```bash
docker compose logs stream-processor | tail -50
# FRAUD [HIGH|CRITICAL] lines every ~10 seconds
```

**Debezium connector failed:**
```bash
curl http://localhost:8083/connectors/claims-postgres-connector/status
# Restart if FAILED:
curl -X POST http://localhost:8083/connectors/claims-postgres-connector/restart
```

**Grafana dashboard empty:**
- Wait 2–3 min for Prometheus to collect data
- Check targets: http://localhost:9090/targets (all UP)

**LKE — bronze consumer not writing:**
```bash
kubectl logs -n processing deploy/bronze-consumer | tail -30
# Check that object-storage-credentials secret exists:
kubectl get secret object-storage-credentials -n processing
```

**LKE — Spark job fails to start:**
```bash
kubectl describe sparkapplication -n processing
# Common causes: image not found (run build-push.sh), RBAC missing (check 12-spark-rbac.yaml),
# object-storage-credentials secret not present
```

**LKE — Nessie not ready:**
```bash
kubectl logs -n data deploy/nessie | tail -30
# Verify PostgreSQL is reachable: check QUARKUS_DATASOURCE_JDBC_URL in the manifest
```

---

## 10. Teardown

**Local:**
```bash
make stop          # stop containers, preserve volumes
make clean         # full cleanup including volumes and built images
```

**LKE:**
```bash
cd infra && ./destroy.sh
# Deletes the LKE cluster. Object Storage bucket and data are NOT deleted automatically.
# To delete the bucket: terraform destroy -target=linode_object_storage_bucket.datalake
```

---

## 11. File Structure

```
.
├── demo/
│   ├── docker-compose.yml            13 services (local demo)
│   ├── Makefile                      make start/stop/logs/clean
│   │
│   ├── data_generator/
│   │   ├── generator.py              synthetic claims producer (PostgreSQL + Redpanda)
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── stream_processor/
│   │   ├── processor.py              rule + ML scoring consumer (mimics Flink)
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── fraud_scorer/
│   │   ├── main.py                   FastAPI inference endpoint
│   │   ├── model.py                  GBM training on synthetic data
│   │   ├── requirements.txt
│   │   └── Dockerfile
│   │
│   ├── bronze_consumer/              ← medallion layer (runs in LKE)
│   │   ├── consumer.py               Redpanda → Object Storage NDJSON writer
│   │   ├── requirements.txt          confluent-kafka, boto3
│   │   └── Dockerfile
│   │
│   ├── init_db/
│   │   ├── 01_schema.sql             PostgreSQL schema
│   │   └── 02_seed_providers.sql     5 providers (NPI-001..005; NPI-005 fraudulent)
│   │
│   ├── debezium/
│   │   └── connector.json            PostgreSQL CDC connector config
│   │
│   ├── prometheus/
│   │   └── prometheus.yml            scrape config
│   │
│   └── grafana/
│       ├── provisioning/             auto-provision datasource + dashboard
│       └── dashboards/
│           └── fraud_platform.json   8-panel real-time fraud dashboard
│
└── infra/
    ├── build-push.sh                 builds + pushes all 5 images (incl. spark-jobs)
    ├── deploy.sh                     end-to-end LKE provisioning script
    ├── destroy.sh                    tears down LKE cluster
    │
    ├── terraform/
    │   ├── versions.tf               Linode provider ~> 2.23
    │   ├── variables.tf              token, region (br-gru), node type/count
    │   ├── main.tf                   linode_lke_cluster resource
    │   ├── object_storage.tf         fraud-datalake bucket + scoped access key
    │   └── outputs.tf                cluster ID, kubeconfig path, S3 credentials
    │
    ├── helm/
    │   ├── redpanda-values.yaml      single-node, NodePort 30808
    │   ├── postgres-values.yaml      Bitnami PostgreSQL
    │   ├── redis-values.yaml         Bitnami Redis
    │   └── spark-operator-values.yaml  kubeflow/spark-operator, webhook enabled
    │
    ├── k8s/
    │   ├── 00-namespaces.yaml        streaming / data / processing / ml / monitoring
    │   ├── 01-postgres-initdb.yaml   init ConfigMap
    │   ├── 02-redpanda-topics.yaml   topic creation Job
    │   ├── 03-debezium.yaml          Debezium Deployment + Service
    │   ├── 04-fraud-scorer.yaml      FastAPI scorer (NodePort 30800)
    │   ├── 05-stream-processor.yaml  scoring consumer
    │   ├── 06-data-generator.yaml    synthetic claims generator
    │   ├── 07-mlflow.yaml            MLflow (NodePort 30500)
    │   ├── 08-prometheus.yaml        Prometheus (NodePort 30909)
    │   ├── 09-grafana.yaml           Grafana (NodePort 30300)
    │   ├── 10-nessie.yaml            Nessie catalog (ClusterIP 19120)
    │   ├── 11-bronze-consumer.yaml   Bronze consumer Deployment
    │   ├── 12-spark-rbac.yaml        ServiceAccount + ClusterRoleBinding for Spark
    │   └── 13-spark-jobs.yaml        ScheduledSparkApplication — silver-etl + gold-features
    │
    └── spark-jobs/                   ← medallion ETL jobs
        ├── Dockerfile                apache/spark-py:3.5.1 + Iceberg + Nessie + S3A JARs
        ├── silver_etl.py             bronze JSONL → Iceberg silver (PHI tokenized)
        └── gold_features.py          silver → provider/member/claim gold features
```
