# Healthcare Fraud Detection Platform — Provider Comparison

**Use case:** Healthcare fraud detection for Amil — ingestion from databases/logs/files, real-time + batch ML scoring  
**Baseline:** Open source stack on Akamai Cloud Linode (detailed in [architecture.md](architecture.md))  
**Compared against:** Databricks (cloud-agnostic), AWS, Microsoft Azure, Google Cloud Platform

---

## 1. Layer-by-Layer Tool Mapping

| Architecture Layer | **Akamai Linode (OSS)** | **Databricks** | **AWS** | **Azure** | **GCP** |
|---|---|---|---|---|---|
| **Streaming backbone** | Redpanda Enterprise | Confluent / MSK (external) | Kinesis Data Streams / MSK | Event Hubs | Pub/Sub |
| **CDC (database capture)** | Debezium | Debezium (self-managed) | DMS + MSK Connect | Azure Data Factory CDC | Datastream |
| **Log ingestion** | Vector → Redpanda | Kinesis / Event Hubs via partner | Kinesis Firehose | Event Hubs | Pub/Sub + Logging |
| **File ingestion** | Redpanda Connect | Delta Live Tables (DLT) | Glue + S3 Events | Data Factory | Dataflow + GCS Events |
| **Stream processing** | Apache Flink (LKE) | Spark Structured Streaming (DLT) | Kinesis Data Analytics (Flink) / Managed Flink | Azure Stream Analytics / HDInsight Flink | Dataflow (Apache Beam) |
| **Batch processing** | Apache Spark (LKE) | Databricks (Spark) | EMR / Glue | HDInsight / Synapse / Azure Databricks | Dataproc / BigQuery |
| **Table format / Lake** | Iceberg + Nessie + Object Storage | Delta Lake (Unity Catalog) | S3 + Glue + Lake Formation + Iceberg | ADLS Gen2 + Purview + Delta Lake | GCS + BigLake + Dataplex |
| **Query engine** | Trino / DuckDB | Databricks SQL | Athena / Redshift Spectrum | Synapse Serverless | BigQuery |
| **Feature store** | Feast + Redis | Databricks Feature Store (Unity Catalog) | SageMaker Feature Store | Azure ML Feature Store | Vertex AI Feature Store |
| **ML experiment tracking** | MLflow (self-hosted) | MLflow (managed, built-in) | SageMaker Experiments | Azure ML (MLflow compatible) | Vertex AI Experiments |
| **Distributed training** | Ray / Kubeflow (LKE) | Databricks ML Runtime (autoscaling) | SageMaker Training | Azure ML Compute | Vertex AI Training |
| **Model serving** | BentoML (LKE) | Databricks Model Serving | SageMaker Endpoints | Azure ML Online Endpoints | Vertex AI Prediction |
| **Workflow orchestration** | Airflow (LKE) | Databricks Workflows | MWAA (Managed Airflow) / Step Functions | Data Factory / Azure Airflow | Cloud Composer (Managed Airflow) |
| **Secrets management** | HashiCorp Vault | Azure Key Vault / AWS Secrets Manager (via cloud) | AWS Secrets Manager + KMS | Azure Key Vault | Secret Manager + KMS |
| **Identity & access** | Keycloak + OPA | Unity Catalog + Databricks IAM | IAM + Lake Formation | Azure AD + Entra ID | Cloud IAM |
| **Service mesh / mTLS** | Linkerd | Not native (use cloud VPC) | App Mesh / VPC | Service Fabric / AKS mesh | Anthos Service Mesh / Traffic Director |
| **Runtime security** | Falco (LKE) | Not native | GuardDuty + Security Hub | Microsoft Defender for Containers | Security Command Center + Container Threat Detection |
| **Metrics** | Prometheus + Grafana | Databricks built-in + cloud native | CloudWatch + Managed Grafana | Azure Monitor + Grafana | Cloud Monitoring + Managed Grafana |
| **Logs** | Loki + Fluentbit | Databricks cluster logs + cloud native | CloudWatch Logs | Azure Monitor Logs | Cloud Logging |
| **Tracing** | Tempo + OpenTelemetry | Not native | X-Ray | Application Insights | Cloud Trace |
| **GitOps / CI-CD** | ArgoCD + Helm | Databricks Asset Bundles + CI | CodePipeline / GitHub Actions | Azure DevOps | Cloud Build / Cloud Deploy |

---

## 2. Scoring by Dimension

Scale: 1 (worst) → 5 (best) for a **healthcare fraud detection** workload in Brazil.

| Dimension | **Linode OSS** | **Databricks** | **AWS** | **Azure** | **GCP** |
|---|:---:|:---:|:---:|:---:|:---:|
| Total cost of ownership | ★★★★★ | ★★ | ★★ | ★★ | ★★★ |
| Time to first production model | ★★ | ★★★★ | ★★★ | ★★★ | ★★★★ |
| Operational complexity | ★★ | ★★★★ | ★★★ | ★★★ | ★★★★ |
| Vendor lock-in risk | ★★★★★ | ★★ | ★ | ★ | ★★ |
| Streaming capability | ★★★★★ | ★★★ | ★★★★ | ★★★ | ★★★★ |
| ML platform maturity | ★★★ | ★★★★★ | ★★★★★ | ★★★★ | ★★★★★ |
| HIPAA / LGPD compliance tools | ★★★ | ★★★★ | ★★★★★ | ★★★★★ | ★★★★ |
| Data residency (Brazil) | ★★★★★ | ★★★ | ★★★★★ | ★★★★★ | ★★★★★ |
| Customizability / flexibility | ★★★★★ | ★★★ | ★★★ | ★★★ | ★★★ |
| Ecosystem integrations | ★★★★ | ★★★★★ | ★★★★★ | ★★★★ | ★★★★ |
| PHI data control | ★★★★★ | ★★★ | ★★★ | ★★★ | ★★★ |
| Streaming latency (p99) | ★★★★★ | ★★★ | ★★★ | ★★★ | ★★★★ |

---

## 3. Deep-Dive by Provider

---

### 3.1 Databricks

**What it is:** Unified data + AI platform running on top of AWS, Azure, or GCP. Built around Delta Lake and Spark. Extremely popular for ML-heavy workloads.

**Equivalent stack for this use case:**
```
Ingestion:       Confluent Cloud (Kafka) or MSK → Delta Live Tables
Stream proc:     Spark Structured Streaming (DLT)
Batch ETL:       Databricks notebooks + Delta Live Tables
Lake:            Delta Lake on S3/ADLS/GCS (Unity Catalog)
Features:        Databricks Feature Store (Unity Catalog)
ML:              MLflow (managed) + Databricks AutoML + Model Serving
Orchestration:   Databricks Workflows
Governance:      Unity Catalog (fine-grained access, lineage, audit)
```

**Strengths for fraud detection:**
- Best-in-class unified experience: data engineering, SQL analytics, and ML in one platform
- MLflow is native and deeply integrated — zero setup
- Delta Live Tables with expectations (data quality rules) are excellent for claim validation
- AutoML can rapidly prototype models without ML expertise on the team
- Unity Catalog provides column-level security on PHI fields out of the box
- Databricks Marketplace has pre-built healthcare datasets and accelerators

**Weaknesses:**
- **Cost:** Databricks DBUs are expensive. At scale (continuous streaming + daily batch + ML training), expect $80k–$200k/year on cloud bills alone before Databricks fees. Total cost easily 5–8x Linode OSS.
- **Streaming is second-class:** Databricks Structured Streaming is micro-batch (minimum 1–5s latency). For real-time claim scoring at millisecond latency, you still need an external Kafka/Redpanda + Flink stack alongside it.
- **No built-in Kafka:** Databricks does not include a streaming broker. You still need Confluent Cloud, MSK, or Redpanda — and pay separately.
- **Lock-in:** Delta Lake format is open source, but Unity Catalog, model serving, and workflows are Databricks-proprietary.
- **Data residency:** depends on the underlying cloud region; Databricks itself adds a data plane in your cloud account, but the control plane is in Databricks' own account (compliance concern for strict LGPD/HIPAA environments).

**When Databricks wins:** When the team has limited platform engineering expertise and needs to move fast. The productivity gain is real — a team of 5 data scientists can build and deploy models in weeks, not months.

---

### 3.2 AWS

**What it is:** The largest cloud provider. Has a managed service for virtually every component of this architecture. Brazilian region: `sa-east-1` (São Paulo).

**Equivalent stack for this use case:**
```
Ingestion:       Kinesis Data Streams + MSK (Managed Kafka) + DMS (CDC)
File ingestion:  S3 + Lambda + EventBridge
Stream proc:     Amazon Managed Service for Apache Flink (MSF)
Batch ETL:       AWS Glue + EMR (Spark)
Lake:            S3 + Glue Catalog + Lake Formation + Iceberg
Query:           Amazon Athena + Redshift Spectrum
Features:        SageMaker Feature Store
ML:              SageMaker (Studio, Training, Pipelines, Endpoints)
Orchestration:   MWAA (Managed Airflow) + Step Functions
Security:        IAM + KMS + Macie + Lake Formation + GuardDuty + Security Hub
Monitoring:      CloudWatch + X-Ray + Managed Grafana
GitOps:          CodePipeline + CodeBuild or GitHub Actions
```

**Strengths for fraud detection:**
- **HIPAA BAA available** — AWS signs a Business Associate Agreement, covering S3, SageMaker, MSK, Glue, and most key services
- **Amazon Fraud Detector** — a purpose-built managed ML service for fraud, pre-trained on Amazon's own transactional data. Can significantly accelerate baseline model quality.
- **SageMaker** is the most mature managed ML platform: built-in bias detection, model monitoring, shadow deployments, A/B testing
- **MSF (Managed Flink)** runs Apache Flink natively with full Kinesis/MSK integration — real streaming, not micro-batch
- **Lake Formation** handles fine-grained column/row-level security on data lake with tag-based access control — strong for PHI governance
- **Macie** auto-discovers PHI in S3 (CPF, names, medical record numbers) and alerts on exposure
- `sa-east-1` is in São Paulo — LGPD data residency satisfied
- Largest partner ecosystem; most healthcare compliance certifications (HIPAA, SOC 2, ISO 27001, HITRUST)

**Weaknesses:**
- **Cost:** Most expensive of all options at scale. MSK + MSF + SageMaker + Glue + MWAA running 24/7 is easily $150k–$400k/year for a production fraud detection platform.
- **Complexity:** Stitching 15+ managed services together requires deep AWS expertise. IAM policies across services become a significant operational burden.
- **Kinesis vs Kafka:** Kinesis Data Streams is not Kafka-compatible — migrating producers/consumers is a rewrite. If you choose MSK instead, you're back to managing Kafka.
- **Vendor lock-in:** SageMaker pipelines, Lake Formation, Amazon Fraud Detector — all proprietary. Migrating away is expensive.
- **Egress costs:** Exporting data out of AWS (for hybrid queries, backups, or migration) is expensive (~$0.09/GB). At healthcare data volumes, this adds up.

**When AWS wins:** When compliance certification breadth is the top requirement, the team already has AWS expertise, and budget is not the primary constraint.

---

### 3.3 Microsoft Azure

**What it is:** Microsoft's cloud platform. Dominant in enterprise/healthcare due to existing Microsoft agreements (Office 365, Teams, Active Directory). Brazilian region: Brazil South (São Paulo).

**Equivalent stack for this use case:**
```
Ingestion:       Azure Event Hubs (Kafka-compatible API) + Azure Data Factory CDC
File ingestion:  Blob Storage + Event Grid + Data Factory
Stream proc:     Azure Stream Analytics / HDInsight Flink / Azure Databricks Streaming
Batch ETL:       Azure Databricks / Synapse Analytics (Spark)
Lake:            ADLS Gen2 + Microsoft Purview + Delta Lake / Iceberg
Query:           Synapse Serverless / Azure Databricks SQL
Features:        Azure ML Feature Store
ML:              Azure ML (experiments, pipelines, endpoints, MLflow-compatible)
Orchestration:   Azure Data Factory + Azure Managed Airflow
Security:        Azure AD + Entra ID + Key Vault + Microsoft Defender + Purview
Monitoring:      Azure Monitor + Application Insights + Managed Grafana
GitOps:          Azure DevOps + GitHub Actions
```

**Strengths for fraud detection:**
- **Event Hubs Kafka compatibility** — producers/consumers using Kafka SDK work without code changes. This is the best Kafka-compatible managed service outside of Confluent.
- **Azure AD / Entra ID** — if Amil already uses Microsoft 365, identity integration is seamless; single SSO for data platform access
- **Microsoft Defender for Healthcare** — purpose-built security for HIPAA workloads, including PHI detection in ADLS and anomaly alerts
- **Microsoft Purview** — data governance with automatic PHI classification, lineage tracking across Synapse/ADF/ADLS, and audit reports — strong LGPD compliance tooling
- **HIPAA BAA available**; Azure is the #1 cloud for healthcare in the US
- **Synapse Analytics** combines Spark (batch), serverless SQL (ad-hoc), and dedicated pools in one workspace — reduces integration complexity
- Brazilian region: Brazil South (São Paulo), with Brazil Southeast (Rio) for DR

**Weaknesses:**
- **Streaming is a weak point:** Azure Stream Analytics is limited in expressiveness compared to Flink. For complex stateful fraud patterns (multi-event, temporal windows), you end up needing Azure Databricks or HDInsight Flink anyway.
- **Cost:** Similar to AWS. Azure Databricks + Synapse + Event Hubs + Azure ML running production workloads: $120k–$350k/year.
- **Fragmentation:** Too many overlapping services (Synapse vs Databricks, ADF vs Managed Airflow, Stream Analytics vs Flink). Choosing the wrong one creates technical debt.
- **Azure ML maturity:** Behind SageMaker and Databricks for MLOps workflows; model serving scalability lags.
- **Vendor lock-in:** ADF pipelines, Synapse SQL, Azure ML pipelines — all proprietary.

**When Azure wins:** When Amil already has an Enterprise Agreement with Microsoft, and the IT team is Azure/Active Directory native. The identity and governance story (Purview + Entra ID) is the strongest of any provider for healthcare compliance.

---

### 3.4 Google Cloud Platform (GCP)

**What it is:** Google's cloud platform. Known for BigQuery (best analytics query engine), Vertex AI (strong ML platform), and Pub/Sub (global, low-latency messaging). Brazilian region: `southamerica-east1` (São Paulo).

**Equivalent stack for this use case:**
```
Ingestion:       Pub/Sub + Datastream (CDC) + Cloud Storage Events
File ingestion:  GCS + Eventarc + Dataflow
Stream proc:     Dataflow (Apache Beam) — unified batch + stream
Batch ETL:       Dataproc (Spark) / BigQuery + dbt
Lake:            GCS + BigLake + Dataplex + Iceberg on GCS
Query:           BigQuery (serverless, columnar, petabyte-scale)
Features:        Vertex AI Feature Store
ML:              Vertex AI (AutoML, Training, Pipelines, Endpoints, MLflow)
Orchestration:   Cloud Composer (Managed Airflow) + Cloud Workflows
Security:        Cloud IAM + KMS + DLP API + Security Command Center + VPC-SC
Monitoring:      Cloud Monitoring + Cloud Logging + Cloud Trace + Managed Grafana
GitOps:          Cloud Build + Cloud Deploy + GitHub Actions
```

**Strengths for fraud detection:**
- **BigQuery** is the most powerful ad-hoc analytics engine available. Fraud investigators can run SQL over petabytes of claims history in seconds with zero infrastructure management. No Trino/Athena needed.
- **BigQuery ML** — train XGBoost, logistic regression, and deep learning models directly in SQL. `CREATE MODEL` on 100M claims rows takes minutes.
- **Vertex AI** is arguably the most complete ML platform today: AutoML, custom training with Ray/PyTorch/TensorFlow, Feature Store, Model Registry, online/batch prediction, built-in model monitoring and drift detection — all under one API.
- **Pub/Sub** global, serverless, auto-scales to millions of messages/sec with no cluster management.
- **Cloud DLP (Data Loss Prevention) API** — built-in PHI detection and de-identification: tokenize CPF, names, diagnoses in a single API call. Best-in-class for LGPD compliance automation.
- **Dataflow (Apache Beam)** unifies batch and stream in one programming model — fewer systems to operate.
- **HIPAA BAA available**; `southamerica-east1` satisfies LGPD data residency.
- **Google Healthcare API** — native FHIR R4 and HL7v2 ingestion and storage, with built-in de-identification. Highly relevant for claims data in the Brazilian healthcare standard (TISS/ANS).

**Weaknesses:**
- **No native Kafka:** Pub/Sub is not Kafka-compatible. If you have existing Kafka producers (Debezium, applications), you need Pub/Sub Lite with Kafka API compatibility or run a Confluent/Redpanda cluster on GKE — adding cost and complexity.
- **Dataflow complexity:** Apache Beam's programming model is more complex than Flink SQL or Spark. The unified model is conceptually elegant but operationally harder for teams not already familiar with it.
- **Cost at streaming scale:** Pub/Sub + Dataflow + Vertex AI + BigQuery at high ingestion rates is expensive. Pub/Sub charges per message (not per partition), which can spike unexpectedly.
- **Vendor lock-in:** BigQuery, Pub/Sub, Vertex AI pipelines, Cloud Composer configuration — all proprietary. BigQuery data export costs are non-trivial.
- **GCP market share in Brazil:** Smaller footprint than AWS/Azure in the Brazilian enterprise market, meaning fewer local partners and certified professionals.

**When GCP wins:** When analyst self-service (BigQuery SQL), ML automation (Vertex AI AutoML), or FHIR/HL7 ingestion (Healthcare API) are top priorities. Also when the team wants to minimize streaming infrastructure management (Pub/Sub vs managing Redpanda).

---

## 4. Total Cost of Ownership Estimate

Assumptions: production fraud detection platform, ~10TB/month new data ingested, 1M claims/day scored, 5-person platform team, 3-year horizon.

| Provider | Infrastructure ($k/yr) | Licenses / SaaS ($k/yr) | Ops headcount | 3-year TCO estimate |
|---|---|---|---|---|
| **Akamai Linode + OSS** | $80–120k | $40–80k (Redpanda Ent.) | 2–3 dedicated platform engineers | **$450–650k** |
| **Databricks (on AWS)** | $150–250k (AWS) | $120–250k (Databricks DBUs) | 1–1.5 (managed services reduce ops) | **$900k–1.5M** |
| **AWS** | $180–350k | $20–50k (support + tooling) | 1.5–2 (still complex to operate) | **$800k–1.2M** |
| **Azure** | $150–300k | $30–80k (support + tooling) | 1.5–2 | **$700k–1.1M** |
| **GCP** | $130–250k | $20–50k | 1.5–2 | **$600k–900k** |

> **Note:** The Linode OSS option has the lowest infrastructure cost but the highest platform engineering cost. The productivity break-even point is approximately 18–24 months — after that it is consistently cheaper. The managed cloud options trade money for time-to-production and reduced ops burden.

---

## 5. Compliance & Security Comparison

| Requirement | **Linode OSS** | **Databricks** | **AWS** | **Azure** | **GCP** |
|---|---|---|---|---|---|
| HIPAA BAA | Must self-certify (Linode does offer BAA) | Via underlying cloud | Yes | Yes | Yes |
| LGPD data residency (Brazil) | Yes — Akamai has Brazil regions | Depends on underlying cloud | Yes (sa-east-1) | Yes (Brazil South) | Yes (southamerica-east1) |
| PHI tokenization | Vault Transit (self-managed) | Unity Catalog + cloud KMS | AWS Macie + KMS | Microsoft Purview + Key Vault | Cloud DLP API (best-in-class) |
| Column-level access on data lake | OPA + Iceberg (manual) | Unity Catalog (native, excellent) | Lake Formation (native) | Purview + Synapse (native) | BigQuery column-level security |
| Audit logs | Redpanda Enterprise + Loki | Unity Catalog audit + cloud native | CloudTrail + CloudWatch | Azure Monitor + Purview | Cloud Audit Logs |
| Encryption at rest | Vault + Object Storage SSE | Cloud-native KMS | AWS KMS | Azure Key Vault | Cloud KMS |
| Network isolation | LKE NetworkPolicies + Linode VPC | VPC peering (cloud-specific) | VPC + PrivateLink | VNET + Private Endpoints | VPC + VPC-SC |
| Runtime threat detection | Falco (self-managed) | Microsoft Defender (Azure) | GuardDuty | Defender for Containers | Security Command Center |

**Key compliance insight:** For LGPD, the critical requirement is that PHI data must not leave Brazil and must be deletable on request. All five options can satisfy this. The difference is in **automation**: GCP's DLP API and Azure's Purview auto-classify and auto-de-identify PHI with minimal configuration; the Linode OSS stack requires you to build this pipeline manually with Vault Transform + custom masking logic.

---

## 6. Streaming Capability Deep-Dive

This is the most important dimension for fraud detection — latency between a fraudulent claim being submitted and an alert being raised.

| Capability | **Redpanda (Linode)** | **Confluent Cloud** | **AWS Kinesis/MSK** | **Azure Event Hubs** | **GCP Pub/Sub** |
|---|---|---|---|---|---|
| Kafka API compatibility | Full | Full | MSK: Full / Kinesis: No | Full (Kafka protocol) | Partial (Pub/Sub Lite) |
| Throughput (per partition) | ~500MB/s | ~250MB/s | ~1MB/s (Kinesis) / ~250MB/s (MSK) | ~20MB/s | Serverless (auto) |
| Write latency p99 | 2–5ms | 5–10ms | 60–200ms (Kinesis) | 5–20ms | 50–150ms |
| Tiered storage | Yes (Enterprise) | Yes | Yes (MSK) | Yes | N/A |
| Schema registry | Built-in | Built-in (Confluent) | Glue Schema Registry | Schema Registry (separate) | Schema Registry (Pub/Sub) |
| RBAC + audit | Enterprise | Yes | IAM | Yes | Yes |
| Self-managed | Yes | No | No | No | No |
| Cost at 1TB/day | ~$4–8k/mo (VMs) | ~$15–30k/mo | ~$10–20k/mo | ~$8–15k/mo | ~$5–10k/mo |

**Conclusion on streaming:** Redpanda on Linode provides the lowest latency and highest throughput per dollar, at the cost of operational management. For a healthcare fraud detection system where sub-100ms detection matters, this is a meaningful advantage over Kinesis or Pub/Sub.

---

## 7. ML Platform Maturity Comparison

| Capability | **Linode OSS (MLflow+Ray+BentoML)** | **Databricks** | **AWS SageMaker** | **Azure ML** | **GCP Vertex AI** |
|---|---|---|---|---|---|
| Experiment tracking | MLflow (excellent) | MLflow native (best UX) | SageMaker Experiments | Azure ML + MLflow | Vertex Experiments |
| Model registry | MLflow | Unity Catalog | SageMaker Registry | Azure ML | Vertex Model Registry |
| Distributed training | Ray (excellent) | Databricks ML Runtime | SageMaker Distributed | Azure ML Compute | Vertex Training |
| AutoML | None (manual) | Databricks AutoML | SageMaker Autopilot | Azure AutoML | Vertex AutoML (best) |
| Feature store | Feast + Redis | Databricks FS | SageMaker FS | Azure ML FS | Vertex FS (best) |
| Online model serving | BentoML (manual scale) | Databricks Serving | SageMaker Endpoints | Azure ML Endpoints | Vertex Endpoints |
| Model monitoring / drift | Custom (Evidently AI) | Lakehouse Monitoring | SageMaker Model Monitor | Azure ML Monitor | Vertex Model Monitoring |
| A/B testing | Manual | Traffic splitting | SageMaker | Azure ML | Vertex |
| Graph ML (fraud rings) | GraphSAGE on Ray | Limited | SageMaker + DGL | Azure ML + PyG | Vertex + BigQuery GraphSAGE |
| BQML (SQL-native ML) | No | No | No | No | **Yes** (unique advantage) |

**Conclusion on ML:** For a team that wants to move fast with minimal infrastructure setup, Databricks or Vertex AI are the most productive platforms. For maximum control over model behavior and cost efficiency at scale, the OSS stack (MLflow + Ray + BentoML) is more powerful but demands platform engineering investment.

---

## 8. Decision Framework

### Choose Linode + OSS when:
- Cost is the primary constraint and the team has platform engineering expertise
- Data sovereignty and PHI control are paramount (no third-party control plane touching your data)
- You need the lowest possible streaming latency (Redpanda sub-5ms)
- You want full customizability and zero vendor lock-in
- Long-term TCO is more important than time-to-production

### Choose Databricks when:
- The team is data scientist-heavy and needs a unified SQL/notebook/ML environment
- Time to production is the top priority (3 months vs 6+ months for OSS)
- You're already on AWS or Azure and can leverage existing committed spend
- The fraud detection models will be complex ensemble models requiring rapid iteration

### Choose AWS when:
- Compliance certification breadth is the top requirement (HITRUST, FedRAMP, HIPAA, SOC2)
- You want **Amazon Fraud Detector** as a head-start baseline model
- The team already has AWS expertise and IAM governance in place
- You need the widest ecosystem of healthcare data partners

### Choose Azure when:
- Amil already has a Microsoft Enterprise Agreement (significant discount opportunity)
- Active Directory / Entra ID is the identity backbone and SSO matters
- Microsoft Purview for automatic PHI discovery and LGPD lineage is valued
- The team is Microsoft-ecosystem native (Azure DevOps, Teams integration)

### Choose GCP when:
- BigQuery self-service analytics for fraud investigators is a priority
- FHIR/HL7 ingestion via Google Healthcare API is needed
- Vertex AI AutoML and built-in Cloud DLP (PHI de-identification) are valued
- The ML team wants the most integrated, modern AI platform

---

## 9. Summary Scorecard

```
                    Cost    Speed   MLOps   Stream  Compliance  Lock-in
Linode OSS          ★★★★★   ★★      ★★★     ★★★★★   ★★★         ★★★★★
Databricks          ★★      ★★★★★   ★★★★★   ★★★     ★★★★        ★★
AWS                 ★★      ★★★     ★★★★★   ★★★★    ★★★★★       ★
Azure               ★★      ★★★     ★★★★    ★★★     ★★★★★       ★
GCP                 ★★★     ★★★★    ★★★★★   ★★★★    ★★★★        ★★
```

**Recommendation for Amil's fraud detection platform:**

The Akamai Linode + OSS architecture is the right long-term foundation because: (1) cost advantage compounds over time, (2) Redpanda Enterprise's streaming performance is unmatched for real-time scoring, and (3) full PHI data control is a genuine compliance advantage over control-plane-sharing managed services.

The realistic risk is **time to production**. To mitigate it: start Phase 1–2 with the OSS stack as designed, but evaluate using **Databricks on Linode/GCP** for the ML experimentation layer only (MLflow + notebooks + AutoML), while keeping the ingestion and data lake layers in the OSS stack. This hybrid approach captures 70% of the cost benefit while compressing the ML development timeline by 3–4 months.
