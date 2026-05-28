# End-to-End Demo — Healthcare Fraud Detection Platform

This guide walks through a fully functional local demo of the fraud detection architecture.
Every component maps directly to a production counterpart on Akamai Linode / LKE.

---

## 1. What the Demo Shows

A synthetic healthcare claims system generates 2 claims per second. Some claims are legitimate; ~8% are fraudulent. The pipeline detects fraud in real time:

```
Claims system (PostgreSQL)
        │
        │  INSERT  →  Debezium CDC (WAL)
        │  +  direct publish (demo reliability path)
        ▼
   REDPANDA  ─── raw.claims.new
        │
        ▼
  Stream Processor  (mimics Apache Flink)
    ├── Enrichment       → attaches provider/member stats from Redis
    ├── Rule scoring     → 4 deterministic fraud rules
    ├── ML scoring       → calls Fraud Scorer API (GBM model)
    └── Alert generation → score ≥ 0.65 → alert
        │
        ├─── REDPANDA  ─── scored.claims   (all claims with scores)
        └─── REDPANDA  ─── alerts.fraud    (high-risk only)
                │
        Grafana  ←  Prometheus  ←  /metrics
```

### Fraud scenarios injected by the generator

| Scenario | Trigger | Expected rule | Expected score |
|---|---|---|---|
| **Upcoding** | Orthopedic surgery (NPI-001, cardiology) | `PROCEDURE_DIAGNOSIS_MISMATCH` | ≥ 0.90 |
| **Excess amount** | Claim 5–10× provider average | `AMOUNT_3X_PROVIDER_AVG` | ≥ 0.78 |
| **Phantom billing** | Very high provider volume | `HIGH_VOLUME_PROVIDER` | ≥ 0.62 |
| **Suspect Clinic (NPI-005)** | Every claim from this provider is fraudulent | mixed | ≥ 0.70 |

---

## 2. Architecture Mapping

| Demo component | Production equivalent |
|---|---|
| `data-generator` Python script | Real claims adjudication system |
| Single Redpanda node | 3-node Redpanda Enterprise cluster on Linode VMs |
| `stream-processor` Python consumer | Apache Flink on LKE (4 jobs) |
| `fraud-scorer` FastAPI + GBM (synthetic training) | BentoML serving XGBoost from MLflow registry |
| Redis (in-memory provider stats) | Feast online store + Redis on LKE |
| Prometheus + Grafana | Full observability stack (Loki, Tempo, OTel, Falco) |
| PostgreSQL single instance | Managed PostgreSQL cluster on Linode |
| Debezium standalone | Debezium on LKE (Kafka Connect cluster) |
| Local filesystem (MLflow) | Linode Object Storage (MLflow artifact store) |

---

## 3. Prerequisites

| Requirement | Minimum version |
|---|---|
| Docker Desktop or Docker Engine | 24.x |
| Docker Compose plugin | v2.x (`docker compose`) |
| Free RAM | 4 GB |
| Free disk | 3 GB |
| Internet access | Required to pull images on first run |

---

## 4. Quick Start

```bash
# Clone / navigate to the demo directory
cd demo/

# Build images and start everything
make start

# Or without make:
docker compose up --build -d
```

**First run takes ~3–5 minutes** (Docker pulls ~2 GB of images and builds 3 Python services).

Wait until all services are healthy:

```bash
make status
# or
docker compose ps
```

Expected output when ready:
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

---

## 5. Access Points

| Service | URL | Credentials |
|---|---|---|
| **Grafana** (main dashboard) | http://localhost:3000 | admin / admin |
| **Redpanda Console** (topics, messages) | http://localhost:8080 | — |
| **Fraud Scorer API** (Swagger UI) | http://localhost:8000/docs | — |
| **MLflow** (experiment tracking) | http://localhost:5000 | — |
| **Prometheus** (raw metrics) | http://localhost:9090 | — |
| **Debezium REST API** | http://localhost:8083/connectors | — |

---

## 6. Demo Walkthrough

### Step 1 — Verify data is flowing into Redpanda

Open **Redpanda Console** → http://localhost:8080 → **Topics**

You should see:

| Topic | Partitions | Expected activity |
|---|---|---|
| `raw.claims.new` | 4 | ~2 messages/sec |
| `scored.claims` | 4 | ~2 messages/sec |
| `alerts.fraud` | 1 | ~1 message every 6–10 sec |

Click on `raw.claims.new` → **Messages** tab. You should see live claim events:

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

Switch to `alerts.fraud`. Each message here is a claim the system flagged as high-risk:

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
```

Expected output:
```
14:22:18  WARNING  FRAUD [CRITICAL ] CLM-D9E4A102  provider=NPI-005  amount=R$  8,750.00  score=0.9312  rules=['PROCEDURE_DIAGNOSIS_MISMATCH', 'AMOUNT_3X_PROVIDER_AVG']
14:22:24  WARNING  FRAUD [HIGH    ] CLM-F1B3C007  provider=NPI-001  amount=R$  3,140.00  score=0.7821  rules=['AMOUNT_3X_PROVIDER_AVG']
14:22:31  WARNING  FRAUD [CRITICAL ] CLM-88EA5D12  provider=NPI-005  amount=R$ 12,050.00  score=0.9601  rules=['EXTREME_AMOUNT', 'PROCEDURE_DIAGNOSIS_MISMATCH']
```

---

### Step 3 — Open Grafana and observe the dashboard

Open **Grafana** → http://localhost:3000 → login with **admin / admin** → the **Fraud Detection Platform** dashboard loads automatically.

**What to observe:**

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

Open http://localhost:8000/docs → `POST /score` → **Try it out**

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
docker exec -it redis redis-cli

# List all provider state keys
KEYS ps:*
# → "ps:NPI-001"  "ps:NPI-002"  ...

# Inspect provider NPI-005 (the fraudulent one)
HGETALL ps:NPI-005
# → "count"  "47"
#    "sum"    "432180.50"
# avg = 432180.50 / 47 = R$9,195  (elevated due to fraudulent claims)

# Inspect a legitimate provider
HGETALL ps:NPI-003
# → "count"  "22"
#    "sum"    "3960.00"
# avg = R$180  (consistent with lab procedure baseline)
```

---

### Step 6 — Inspect Debezium CDC (optional)

Verify Debezium is capturing PostgreSQL changes and publishing to Redpanda:

```bash
# Check connector status
curl -s http://localhost:8083/connectors/claims-postgres-connector/status | python3 -m json.tool

# Expected: connector.state = RUNNING, task[0].state = RUNNING

# Watch CDC events on the Debezium topic in Redpanda Console:
# Topic: dbz.public.claims
# Each INSERT/UPDATE to the claims table appears here as a CDC event.
```

Debezium publishes the full before/after state:
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

Open http://localhost:5000

In production, every training run is logged here with:
- Parameters (model type, hyperparameters, feature set)
- Metrics (AUC-ROC, precision, recall, average precision)
- Artifacts (model files, feature importance plots)
- Stage transitions (Staging → Production)

The demo MLflow instance is live and ready to receive logged experiments.
To simulate a training run:

```bash
docker exec -it stream-processor bash -c "
pip install mlflow scikit-learn > /dev/null 2>&1
python3 -c \"
import mlflow, numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score

mlflow.set_tracking_uri('http://mlflow:5000')
mlflow.set_experiment('claim-fraud-demo')

with mlflow.start_run(run_name='demo-run-gbm'):
    mlflow.log_params({'model': 'GBM', 'n_estimators': 150, 'max_depth': 4})
    mlflow.log_metric('auc_roc', 0.943)
    mlflow.log_metric('avg_precision', 0.721)
    print('Run logged to MLflow!')
\"
"
```

Refresh http://localhost:5000 → **claim-fraud-demo** experiment → run visible.

---

### Step 8 — Increase fraud rate (stress test)

In another terminal, restart the generator with higher fraud rate:

```bash
docker compose stop data-generator
FRAUD_RATE=0.30 CLAIMS_PER_SECOND=5 docker compose up -d data-generator
```

Observe in Grafana: fraud alert rate climbs, ML latency stays stable (no degradation under load), provider state in Redis updates faster.

Reset:
```bash
docker compose stop data-generator && docker compose up -d data-generator
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
           POST http://fraud-scorer:8000/score
           features: [8750, 47, 920, 9.51, 14, 0, 0.90, 1]
           → fraud_probability = 0.891
           ml_score = 0.891

T+39ms   Combine:
           final = max(0.93, 0.891 * 0.6 + 0.93 * 0.4) = max(0.93, 0.906) = 0.93

T+40ms   Publish to scored.claims (all consumers see score)
         Publish to alerts.fraud  (score 0.93 ≥ threshold 0.65)

T+40ms   Log:  FRAUD [CRITICAL] CLM-D9E4A102  score=0.93

T+45s    Prometheus scrapes stream-processor:8001/metrics
         → stream_processor_fraud_alerts_total{risk_level="CRITICAL"} += 1

T+50s    Grafana refreshes (5s interval) — red spike visible on dashboard
```

---

## 8. Production Architecture Differences

| Aspect | Demo | Production |
|---|---|---|
| **Streaming state** | Redis (simple hash) | Flink RocksDB (event-time windows, watermarks, exactly-once) |
| **Stream processing** | Python while-loop | Apache Flink on LKE (parallel, fault-tolerant, checkpoints to Object Storage) |
| **ML model** | GBM trained on synthetic data at startup | XGBoost trained on Iceberg gold-zone features, managed in MLflow, served via BentoML |
| **Feature freshness** | Updated in-request | Feast materializes features to Redis on schedule (daily batch) |
| **Data lake** | None | Iceberg on Linode Object Storage (bronze/silver/gold zones) |
| **Security** | None | Vault + Keycloak + OPA + Linkerd mTLS + Falco |
| **Redpanda** | Single node, no auth | 3-node cluster, TLS, RBAC, tiered storage to Object Storage |
| **Scale** | 2 claims/sec | 50 000+ claims/sec (linear horizontal scaling) |
| **Fault tolerance** | Container restart | Flink savepoints, Redpanda RF=3, Iceberg snapshots, Redis Cluster |
| **PHI handling** | Plain text (demo) | Vault Transit encryption, tokenization, no PHI in silver/gold |

---

## 9. Troubleshooting

**Services not starting:**
```bash
docker compose logs redpanda        # check Redpanda startup
docker compose logs postgres        # check PostgreSQL
docker compose logs fraud-scorer    # check model training (takes ~20s)
```

**No messages in Redpanda Console:**
```bash
docker compose logs data-generator  # should show "[xxx] CLM-... amount=..."
docker compose logs redpanda-init   # should show "✓ Topics created"
```

**stream-processor not detecting fraud:**
```bash
docker compose logs stream-processor | tail -50
# Should show FRAUD [HIGH|CRITICAL] lines every ~10 seconds
```

**Debezium connector failed:**
```bash
curl http://localhost:8083/connectors/claims-postgres-connector/status
# If FAILED, restart it:
curl -X POST http://localhost:8083/connectors/claims-postgres-connector/restart
```

**Grafana dashboard empty:**
- Wait 2–3 minutes after startup for Prometheus to collect data
- Verify Prometheus targets: http://localhost:9090/targets (all should be UP)

---

## 10. Teardown

```bash
# Stop all containers (preserves volumes)
make stop

# Full cleanup including volumes and built images
make clean
```

---

## 11. File Structure

```
demo/
├── docker-compose.yml            orchestrates all 13 services
├── Makefile                      convenience commands (make start/stop/logs)
│
├── data_generator/
│   ├── generator.py              synthetic claims producer (PostgreSQL + Redpanda)
│   ├── requirements.txt
│   └── Dockerfile
│
├── stream_processor/
│   ├── processor.py              Python consumer simulating 4 Flink jobs
│   ├── requirements.txt
│   └── Dockerfile
│
├── fraud_scorer/
│   ├── main.py                   FastAPI inference endpoint
│   ├── model.py                  GBM model training on synthetic data
│   ├── requirements.txt
│   └── Dockerfile
│
├── init_db/
│   ├── 01_schema.sql             PostgreSQL schema (claims, providers, members)
│   └── 02_seed_providers.sql     5 providers (NPI-001..005; NPI-005 is fraudulent)
│
├── debezium/
│   └── connector.json            PostgreSQL CDC connector configuration
│
├── prometheus/
│   └── prometheus.yml            scrapes fraud-scorer, stream-processor, redpanda
│
└── grafana/
    ├── provisioning/
    │   ├── datasources/          auto-provisions Prometheus data source
    │   └── dashboards/           auto-loads dashboard from /dashboards
    └── dashboards/
        └── fraud_platform.json   8-panel real-time fraud dashboard
```
