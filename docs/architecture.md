# Healthcare Fraud Detection Platform — Architecture

**Cloud:** Akamai Cloud Linode (VMs, Object Storage, LKE)  
**Domain:** Healthcare (HIPAA/LGPD compliance required)  
**Goal:** Ingest data from databases, logs, and files; process with ML to detect fraudulent claims in real-time and batch

---

## 1. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                            DATA SOURCES                                          │
│  ┌────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐  │
│  │ Databases  │  │ Application  │  │ Files        │  │ External Feeds       │  │
│  │ (claims,   │  │ Logs / Audit │  │ (EDI 837,    │  │ (TISS, ANS, CRM,    │  │
│  │ providers, │  │ Logs         │  │ HL7, FHIR,   │  │  partner APIs)       │  │
│  │ members)   │  │              │  │ CSV, JSON)   │  │                      │  │
│  └─────┬──────┘  └──────┬───────┘  └──────┬───────┘  └──────────┬───────────┘  │
└────────┼────────────────┼─────────────────┼─────────────────────┼──────────────┘
         │                │                 │                      │
┌────────▼────────────────▼─────────────────▼─────────────────────▼──────────────┐
│                       INGESTION LAYER                                            │
│                                                                                  │
│  Debezium (CDC)    Vector/Fluentbit    Redpanda Connect    Custom Ingestors      │
│  (database CDC)    (log shipping)      (file connectors)   (HTTP/REST)           │
│                              │                                                   │
│                    ┌─────────▼──────────────────────────────┐                   │
│                    │          REDPANDA CLUSTER               │                   │
│                    │   (Kafka-compatible, Schema Registry)   │                   │
│                    │                                         │                   │
│                    │  Topics: raw.claims, raw.logs,          │                   │
│                    │          raw.files, raw.events          │                   │
│                    └─────────────────────────────────────────┘                   │
└──────────────────────────────────────────────────────────────────────────────────┘
         │                                      │
┌────────▼───────────────────┐   ┌──────────────▼──────────────────────────────────┐
│  STREAM PROCESSING         │   │  BATCH PROCESSING                               │
│  (Apache Flink on LKE)     │   │  (Apache Spark on LKE)                          │
│                            │   │                                                  │
│  • Real-time scoring       │   │  • Historical ETL                               │
│  • Sliding window rules    │   │  • Model training                               │
│  • Pattern detection       │   │  • Feature backfilling                          │
│  • Alert generation        │   │  • Report generation                            │
└────────┬───────────────────┘   └────────────────┬────────────────────────────────┘
         │                                         │
         │                 ┌───────────────────────┘
         │                 │
┌────────▼─────────────────▼──────────────────────────────────────────────────────┐
│                       DATA LAKE (Linode Object Storage + Apache Iceberg)         │
│                                                                                  │
│  bronze/   ──►  silver/  ──►  gold/             catalog: Project Nessie          │
│  (raw)          (clean)        (aggregated,      (Iceberg table versioning)      │
│                                features,                                         │
│                                ML-ready)                                         │
└──────────────────────────────────────────────────────────────────────────────────┘
         │
┌────────▼──────────────────────────────────────────────────────────────────────┐
│                       ML PLATFORM                                               │
│                                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  ┌──────────────────┐  │
│  │   Feast     │  │   MLflow     │  │   Ray / Kube-  │  │  BentoML /       │  │
│  │  (Feature   │  │  (Experiment │  │   flow (dist.  │  │  Seldon Core     │  │
│  │   Store)    │  │   tracking & │  │   training)    │  │  (model serving) │  │
│  │             │  │   registry)  │  │                │  │                  │  │
│  └─────────────┘  └──────────────┘  └────────────────┘  └──────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
         │
┌────────▼──────────────────────────────────────────────────────────────────────┐
│                       SERVING & ALERTING                                        │
│                                                                                 │
│  FastAPI (REST scoring API)    Redis (feature cache)    Redpanda (results bus)  │
│  Grafana (dashboards)          PagerDuty / Alertmanager  Case Management API    │
└───────────────────────────────────────────────────────────────────────────────┘
         │
┌────────▼──────────────────────────────────────────────────────────────────────┐
│                       PLATFORM                                                  │
│  LKE (Kubernetes)  │  ArgoCD (GitOps)  │  Vault  │  Keycloak  │  Prometheus   │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Layer-by-Layer Detail

### 2.1 Data Ingestion — Redpanda + Connectors

**Why Redpanda over Apache Kafka:**

| Factor | Redpanda | Kafka |
|---|---|---|
| Dependency | Single binary, no ZooKeeper, no JVM | ZooKeeper + JVM overhead |
| Latency | ~2–10ms p99 | ~5–20ms p99 |
| Ops complexity | Low — one process per broker | High — Kafka + ZooKeeper + Schema Registry |
| Schema Registry | Built-in | Separate Confluent service |
| Tiered storage | Enterprise: offload to Object Storage | Requires plug-in (Confluent) |
| Kafka compatibility | Full API compatibility | — |
| License | BSL (free for non-SaaS); Enterprise for RBAC, tiered storage | Apache 2.0 / Confluent EE |

**Redpanda Enterprise** is justified here for:
- **Tiered storage** → older topic data is automatically offloaded to Linode Object Storage, cutting broker disk costs 80–90%
- **RBAC + audit logs** → required for HIPAA/LGPD access control and audit trail
- **Remote read replicas** → enables analytics consumers without impacting ingestion throughput

**Connectors (Redpanda Connect — formerly Benthos):**

```
Database CDC:    Debezium → Redpanda (via Kafka Connect)
Log ingestion:   Vector (preferred over Logstash — lower memory, Go binary)
File ingestion:  Redpanda Connect pipelines (S3 trigger → parse → topic)
HTTP/API:        Custom FastAPI ingestors → Redpanda SDK
```

**Topic design:**
```
raw.claims.new          # new claim submissions
raw.claims.status       # claim status updates (CDC)
raw.providers           # provider data changes (CDC)
raw.members             # member data changes (CDC)
raw.logs.app            # application logs
raw.logs.audit          # audit/access logs
raw.files.edi837        # EDI 837 claim files (parsed)
raw.files.hl7           # HL7 messages
enriched.claims         # after stream enrichment
scored.claims           # ML fraud scores
alerts.fraud            # high-confidence fraud events
```

**Partitioning strategy:** partition by `provider_id` for claims topics — this ensures all claims from a provider land on the same partition, enabling stateful fraud pattern detection without shuffle across Flink jobs.

---

### 2.2 Data Lake — Linode Object Storage + Apache Iceberg

**Why Apache Iceberg:**
- ACID transactions on object storage (no "partial write" corruption)
- Time travel and snapshot isolation (audit requirement)
- Schema evolution without rewriting data
- Partition pruning (massive query speedup on billions of rows)
- Works natively with Spark, Flink, Trino/Dremio, DuckDB

**Catalog: Project Nessie**
- Git-like branching for data (create a branch for ML experiments without impacting production)
- ACID multi-table commits
- Lightweight, runs as a pod on LKE

**Three-zone medallion architecture:**

```
bronze/    Raw ingest, immutable, full fidelity
           Format: Parquet + Iceberg
           Retention: 7 years (HIPAA)
           PHI present, encrypted

silver/    Cleaned, deduplicated, normalized
           PHI tokenized (Vault Transit encryption)
           Joined reference tables (members, providers, procedures)
           Schema: canonical claim model

gold/      Aggregated, feature-engineered, ML-ready
           Provider behavior profiles (30/60/90 day windows)
           Member claim history vectors
           No PHI — only derived features and anonymized IDs
```

**Storage layout:**
```
s3://fraud-datalake/
  bronze/claims/year=2026/month=05/day=28/
  silver/claims/year=2026/month=05/
  gold/features/provider_profiles/snapshot_date=2026-05-28/
  gold/features/member_profiles/
  models/trained/
  models/serving/
```

---

### 2.3 Stream Processing — Apache Flink

**Why Flink over Spark Streaming:**
- True streaming (event-by-event), not micro-batch
- Stateful operators with RocksDB backend (handles large state without OOM)
- Event-time processing with watermarks (handles out-of-order EDI/HL7 messages)
- Exactly-once semantics end-to-end with Redpanda
- Lower latency (sub-second vs Spark's seconds)

**Flink jobs for fraud detection:**

```
Job 1: ClaimEnrichmentJob
  Input:  raw.claims.new (Redpanda)
  Logic:  join with provider/member Redis cache, attach features
  Output: enriched.claims (Redpanda)

Job 2: RuleBasedScoringJob
  Input:  enriched.claims
  Logic:  deterministic rules (billing > procedure max, impossible combos,
          upcoding patterns, duplicate claims within 24h)
  Output: scored.claims (partial score)
  State:  RocksDB — per-provider claim counters, sliding 30-day windows

Job 3: MLScoringJob
  Input:  enriched.claims
  Logic:  call BentoML inference endpoint, attach ML fraud score
  Output: scored.claims (merged score)

Job 4: AlertGenerationJob
  Input:  scored.claims
  Logic:  threshold logic, suppression (don't re-alert same claim)
  Output: alerts.fraud (Redpanda) → case management
          Iceberg sink: gold/alerts/
```

**Flink state management on Kubernetes:**
- RocksDB incremental checkpoints every 60s → Linode Object Storage
- Savepoints before deploys (zero-downtime upgrades)
- Flink Session Cluster on LKE with HPA on TaskManagers

---

### 2.4 Batch Processing — Apache Spark on LKE

**Use cases:**
- Daily/weekly ETL: bronze → silver → gold transformations
- ML model training on gold datasets
- Backfilling features when model schema changes
- Historical fraud investigation queries
- Monthly regulatory reports (ANS/TISS compliance)

**Spark on Kubernetes setup:**
- Spark Operator (kubeflow/spark-operator) deployed on LKE
- Dynamic resource allocation: executors spin up/down per job
- Reads/writes Iceberg via `spark-iceberg` catalog
- Nessie catalog for branch-based feature development

**Airflow DAGs orchestrate Spark:**
```
DAG: daily_silver_etl        (runs 02:00, ~45min)
DAG: weekly_gold_features    (runs Sunday 04:00, ~3h)
DAG: monthly_model_train     (runs 1st of month, ~6h)
DAG: on_demand_investigation (triggered by fraud alert)
```

---

### 2.5 Feature Store — Feast

**Why a feature store:**
- Same features used for training must be used for serving (training-serving skew kills ML)
- Enables feature reuse across models (provider risk score, member claim velocity)
- Point-in-time correct joins for training (avoids label leakage)

**Feast setup:**
```
Offline store:  Iceberg on Linode Object Storage (for training datasets)
Online store:   Redis on LKE (for <10ms real-time feature lookup)
Registry:       Feast registry in PostgreSQL (small, on LKE)
```

**Key feature groups:**
```
provider_stats_30d:    claim_count, unique_members, avg_claim_value,
                       procedure_diversity, denial_rate
member_stats_90d:      claim_frequency, total_spend, provider_count,
                       diagnosis_entropy
claim_features:        time_since_last_claim, procedure_rarity_score,
                       icd10_billing_mismatch_flag
network_features:      provider_community_fraud_rate, referral_chain_depth
```

---

### 2.6 ML Platform

**Models and their roles:**

| Model | Type | Trigger | Why |
|---|---|---|---|
| Claim Anomaly Detector | Isolation Forest / Autoencoder | Real-time (Flink) | Catches unknown fraud patterns |
| Claim Classifier | XGBoost / LightGBM | Real-time (Flink) | High-accuracy supervised scoring on known patterns |
| Provider Risk Model | GBM + Graph features | Batch daily | Provider-level risk profiling |
| Member Behavior Model | LSTM / sequence model | Batch daily | Detects abnormal claim sequences over time |
| Graph Fraud Network | GraphSAGE / Node2Vec | Batch weekly | Detects fraud rings (shared members, referrals) |

**MLflow** tracks all experiments, versions models, and serves as the model registry.

**Training pipeline (on Ray/Kubeflow):**
```
1. Pull gold features from Feast (offline store)
2. Split train/validation/test with time-aware split (no future leakage)
3. Hyperparameter tuning (Ray Tune)
4. Register champion model in MLflow
5. Shadow deploy: new model runs alongside current, outputs compared
6. Promote champion → serving if performance delta > threshold
```

**Model serving (BentoML on LKE):**
- Each model exposed as REST endpoint
- HPA on serving pods (target CPU 70%)
- Flink calls inference endpoint with p99 < 50ms SLA

---

### 2.7 Serving Layer

```
┌──────────────────────────────────────────────────────┐
│  External consumers                                   │
│  (claims adjudication, auditor UI, case management)  │
└────────────────┬─────────────────────────────────────┘
                 │ REST / WebSocket
┌────────────────▼──────────┐
│  FastAPI — Fraud Score API │
│  • GET /score/{claim_id}   │
│  • POST /score/batch       │
│  • GET /alerts             │
└────────────────┬──────────┘
                 │
        ┌────────┴────────┐
        │                 │
   ┌────▼────┐    ┌────────▼────────┐
   │  Redis  │    │ Iceberg (gold/) │
   │ (online │    │ (historical     │
   │ features│    │  query via      │
   │ + cache)│    │  Trino/DuckDB)  │
   └─────────┘    └─────────────────┘
```

**Trino** (or DuckDB for smaller scale) provides ad-hoc SQL over Iceberg for investigators and analysts without impacting the operational path.

---

## 3. Scalability

### Redpanda scaling
- Start: 3-node cluster (dedicated Linode VMs, e.g., 16 vCPU / 64GB each)
- Scale out: add brokers, rebalance partitions (Redpanda does this automatically)
- Throughput: ~1M msgs/sec per 3-node cluster on commodity hardware
- **Tiered storage** offloads cold data to Object Storage — brokers stay small

### Flink scaling
- TaskManagers scale via Kubernetes HPA (CPU + custom metric: Redpanda consumer lag)
- Partition key design (`provider_id`) allows linear horizontal scaling
- Stateful jobs use RocksDB — state scales with disk, not heap

### Spark scaling
- Spark on Kubernetes: dynamic executor allocation (0 → N executors per job)
- No idle capacity — executors terminated after each job
- LKE node autoscaler provisions VM nodes on demand

### Data lake scaling
- Linode Object Storage: effectively unlimited, cost-scales linearly
- Iceberg compaction jobs (Spark) merge small files → maintain query performance
- Partition evolution as data grows (e.g., add hour-level partitioning if needed)

---

## 4. Security (HIPAA + LGPD)

### Data classification
```
PHI (Protected Health Information):  member names, CPF, DOB, diagnosis codes
                                      → only in bronze zone, encrypted
De-identified:                        tokenized IDs, aggregated features
                                      → silver and gold zones
```

### Encryption
| Layer | Mechanism |
|---|---|
| Data at rest | AES-256 via HashiCorp Vault Transit + Linode Object Storage SSE |
| Data in transit | TLS 1.3 everywhere; mTLS between services (Linkerd service mesh) |
| PHI tokenization | Vault Transform (format-preserving encryption on CPF, name) |
| Kafka/Redpanda | TLS + SASL/SCRAM; Redpanda Enterprise RBAC |

### Access control
- **Keycloak** as IdP (OIDC/OAuth2): all human and service-to-service auth
- **OPA (Open Policy Agent)** as policy engine: fine-grained access to topics, datasets, API endpoints
- **Redpanda RBAC** (Enterprise): topic-level ACLs, audit logs
- **Kubernetes RBAC + NetworkPolicies**: namespace isolation, no lateral movement
- **Vault**: dynamic short-lived credentials for all DB connections (no static passwords)

### Audit trail
- All data access logged → `raw.logs.audit` topic → Loki
- Redpanda Enterprise audit log captures every topic read/write with user identity
- Flink/Spark job logs include data lineage metadata
- Immutable bronze zone (Iceberg snapshot retention, no delete)

### Network
- LKE private network for all inter-service communication
- Ingestion endpoints on VPC, not public internet
- Bastion host for operational access; no direct SSH to LKE nodes
- Firewall rules: allow-list only (Linode Cloud Firewall)

---

## 5. Operations

### Deployment — GitOps with ArgoCD
```
git push → CI pipeline → Docker build → image push to registry
         → ArgoCD detects Helm chart change → applies to LKE
         → Health checks → rollout or auto-rollback
```

### Observability stack (deployed on LKE)
```
Metrics:  Prometheus → Grafana dashboards
          Alertmanager → PagerDuty/Slack
Logs:     Fluentbit → Loki → Grafana
Traces:   OpenTelemetry SDK → Tempo → Grafana
Kafka:    Redpanda Console (built-in) for topic/consumer lag monitoring
ML:       MLflow UI for model performance, drift detection
```

**Key SLOs to monitor:**
- Redpanda consumer lag on enriched.claims (alert if > 10k messages)
- Flink checkpoint success rate (alert if < 99%)
- ML inference p99 latency (alert if > 100ms)
- False positive rate on fraud alerts (weekly review)
- Model drift: PSI score on feature distributions

### Disaster recovery
- Redpanda: 3 replicas (RF=3), tiered storage backup on Object Storage
- Iceberg: Object Storage is inherently durable (Linode SLA); Nessie catalog in PostgreSQL with daily backup
- Flink savepoints: stored on Object Storage before every deploy
- RTO: ~15min (replay from Redpanda tiered storage + restore Flink savepoint)
- RPO: ~60s (Flink checkpoint interval)

### Operational runbooks to build
1. Redpanda broker replacement (node failure)
2. Flink job restart from savepoint
3. Model rollback (MLflow + BentoML)
4. Fraud alert investigation workflow
5. Data quality incident response

---

## 6. Technology Stack Summary

| Layer | Tool | Deployment | License |
|---|---|---|---|
| Streaming backbone | Redpanda Enterprise | 3+ VMs (dedicated) | Commercial |
| CDC | Debezium | LKE (Kafka Connect) | Apache 2.0 |
| Log collection | Vector | DaemonSet on LKE | MPL 2.0 |
| File/API ingestion | Redpanda Connect | LKE | BSL / Commercial |
| Stream processing | Apache Flink | LKE (Flink Operator) | Apache 2.0 |
| Batch processing | Apache Spark | LKE (Spark Operator) | Apache 2.0 |
| Table format | Apache Iceberg | Library (no server) | Apache 2.0 |
| Data catalog | Project Nessie | LKE | Apache 2.0 |
| Object storage | Linode Object Storage | Managed | Akamai |
| Feature store | Feast | LKE | Apache 2.0 |
| ML experiment tracking | MLflow | LKE | Apache 2.0 |
| Distributed training | Ray | LKE | Apache 2.0 |
| Model serving | BentoML | LKE | Apache 2.0 |
| Query engine | Trino | LKE | Apache 2.0 |
| API | FastAPI | LKE | MIT |
| Cache | Redis | LKE | BSD |
| Secrets | HashiCorp Vault | LKE or VMs | BSL |
| Identity | Keycloak | LKE | Apache 2.0 |
| Policy | OPA | LKE (sidecar) | Apache 2.0 |
| Service mesh | Linkerd | LKE | Apache 2.0 |
| Workflow orchestration | Apache Airflow | LKE | Apache 2.0 |
| GitOps | ArgoCD | LKE | Apache 2.0 |
| Metrics | Prometheus + Grafana | LKE | Apache 2.0 |
| Logs | Loki + Fluentbit | LKE | Apache 2.0 |
| Tracing | Tempo + OpenTelemetry | LKE | Apache 2.0 |
| Runtime security | Falco | DaemonSet on LKE | Apache 2.0 |

---

## 7. Implementation Phases

### Phase 1 — Foundation (weeks 1–6)
- [ ] LKE cluster provisioning (namespaces: ingestion, processing, ml, serving, platform)
- [ ] Redpanda cluster (3 nodes) + schema registry validation
- [ ] Linode Object Storage + Iceberg + Nessie catalog
- [ ] Vault + Keycloak + ArgoCD
- [ ] First Debezium connector (claims database CDC)
- [ ] Bronze zone ingestion pipeline

### Phase 2 — Processing (weeks 7–12)
- [ ] Flink rule-based scoring job (deterministic fraud rules)
- [ ] Spark ETL: bronze → silver (cleansing, tokenization)
- [ ] Spark ETL: silver → gold (feature engineering)
- [ ] Feast feature store + Redis online store
- [ ] Airflow DAGs for orchestration
- [ ] Prometheus + Grafana + Loki observability

### Phase 3 — ML (weeks 13–20)
- [ ] MLflow deployment + first model training (XGBoost claim classifier)
- [ ] BentoML serving + Flink ML scoring job integration
- [ ] Model shadow deployment workflow
- [ ] Anomaly detection model (Isolation Forest)
- [ ] Feature drift monitoring

### Phase 4 — Production hardening (weeks 21–26)
- [ ] Full security audit (Linkerd mTLS, OPA policies, Falco rules)
- [ ] Graph fraud network model (provider ring detection)
- [ ] Trino query layer for investigators
- [ ] SLO dashboards + alerting runbooks
- [ ] Load testing + chaos engineering (failure scenarios)
- [ ] LGPD/HIPAA compliance review

---

## 8. Key Architectural Decisions and Trade-offs

**Redpanda Enterprise vs self-managed Kafka:**
Tiered storage alone justifies the license cost — at 1TB/day ingestion, broker disk would be 30TB/month without tiered storage vs ~2TB with it (15:1 ratio). RBAC audit logs are a compliance requirement, not optional.

**Flink vs Spark Streaming:**
The clinical claim enrichment window requires stateful lookups against provider history with sub-second latency. Spark Structured Streaming's micro-batch model adds 1–30 seconds of inherent latency. Flink handles this with event-time watermarks and RocksDB state without that penalty.

**Iceberg vs Delta Lake vs Hudi:**
Iceberg has the broadest engine support (Flink native, Spark native, Trino, DuckDB, PyArrow) and Nessie catalog is purpose-built for it. Delta Lake is tightly coupled to Databricks/Azure. Hudi adds complexity without benefit at this scale.

**Feast vs custom feature store:**
A custom Redis cache would suffice initially but would create training-serving skew as the team adds models. Feast enforces point-in-time correctness from day one at the cost of ~2 weeks of setup.

**BentoML vs Seldon vs KServe:**
BentoML has the simplest developer experience for the data science team and supports all target frameworks (XGBoost, PyTorch, sklearn). Seldon and KServe add Kubernetes complexity that isn't needed until multi-model serving at scale.
