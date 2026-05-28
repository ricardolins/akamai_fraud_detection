# Linode LKE Deployment Guide

Deploys the complete fraud detection demo on **Akamai Cloud Linode** using a managed Kubernetes cluster (LKE).

---

## Architecture on LKE

```
Akamai Cloud Linode — br-gru (São Paulo)
│
└── LKE Cluster  (3 × g6-standard-4: 12 vCPU / 24GB RAM)
    │
    ├── Namespace: streaming
    │   ├── Redpanda          StatefulSet  (Helm)   — topic broker + schema registry
    │   ├── Redpanda Console  Deployment   (Helm)   — NodePort 30808
    │   └── Debezium          Deployment            — PostgreSQL CDC → Redpanda
    │
    ├── Namespace: data
    │   ├── PostgreSQL        StatefulSet  (Helm)   — claims source database
    │   └── Redis             StatefulSet  (Helm)   — provider state / feature cache
    │
    ├── Namespace: processing
    │   ├── stream-processor  Deployment            — mimics Apache Flink
    │   └── data-generator    Deployment            — synthetic claims producer
    │
    ├── Namespace: ml
    │   ├── fraud-scorer      Deployment + HPA      — FastAPI ML inference NodePort 30800
    │   └── mlflow            Deployment + PVC      — experiment tracking NodePort 30500
    │
    └── Namespace: monitoring
        ├── prometheus        Deployment + PVC      — metrics NodePort 30909
        └── grafana           Deployment + PVC      — dashboards NodePort 30300
```

**Estimated cost:** ~$144/month (3 × g6-standard-4 @ ~$48/node). Destroy when not needed.

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Terraform | ≥ 1.6 | `brew install terraform` |
| kubectl | ≥ 1.28 | `brew install kubectl` |
| Helm | ≥ 3.14 | `brew install helm` |
| Docker | 24.x | Docker Desktop |

You also need:
- A **Linode API token** with Read/Write access → https://cloud.linode.com/profile/tokens
- A **container registry** for the custom images (GitHub Container Registry recommended)

---

## Step-by-Step Deployment

### Step 1 — Build and push custom images

The three custom services (fraud-scorer, stream-processor, data-generator) need to be
built and pushed to a registry that LKE can pull from.

```bash
# GitHub Container Registry (recommended — free for public repos)
echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin

export REGISTRY="ghcr.io/YOUR_GITHUB_USERNAME/akamai_fraud_detection"
export TAG="demo"

cd infra/
./build-push.sh
```

Expected output:
```
Building ghcr.io/.../fraud-scorer:demo
Pushing  ghcr.io/.../fraud-scorer:demo   ✓
Building ghcr.io/.../stream-processor:demo
Pushing  ghcr.io/.../stream-processor:demo  ✓
Building ghcr.io/.../data-generator:demo
Pushing  ghcr.io/.../data-generator:demo  ✓
```

> **Tip:** Make the packages public on GHCR (Settings → Packages → Change visibility)
> so LKE can pull them without an image pull secret.

---

### Step 2 — Set environment variables

```bash
export TF_VAR_linode_token="your-linode-api-token-here"
export REGISTRY="ghcr.io/YOUR_GITHUB_USERNAME/akamai_fraud_detection"
export TAG="demo"
```

---

### Step 3 — Deploy everything

```bash
cd infra/
./deploy.sh
```

The script runs these steps automatically:

```
1. terraform init + apply     → provisions LKE cluster (~5 min)
2. kubectl apply namespaces   → streaming, data, processing, ml, monitoring
3. helm install redpanda      → Redpanda broker + console (~3 min)
4. helm install postgresql    → claims database with schema + seed data
5. helm install redis         → feature cache
6. kubectl apply topics job   → creates raw.claims.new, scored.claims, alerts.fraud
7. kubectl apply services     → debezium, fraud-scorer, stream-processor, data-generator,
                                 mlflow, prometheus, grafana
8. print access URLs          → node IP + NodePort for each service
```

Total time: **~15–20 minutes** on first run.

---

### Step 4 — Access the demo

After `deploy.sh` completes, it prints the access URLs:

```
══════════════════════════════════════════════════════════
  FRAUD DETECTION DEMO — ACCESS URLS
══════════════════════════════════════════════════════════

  Node IP : 143.42.xxx.xxx

  Grafana           →  http://143.42.xxx.xxx:30300   (admin / admin)
  Redpanda Console  →  http://143.42.xxx.xxx:30808
  Fraud Scorer API  →  http://143.42.xxx.xxx:30800/docs
  MLflow            →  http://143.42.xxx.xxx:30500
  Prometheus        →  http://143.42.xxx.xxx:30909
```

---

## What to Show

### Grafana Dashboard
http://NODE_IP:30300 → login admin/admin → **Fraud Detection Platform — LKE**

Panels auto-populate within 2–3 minutes after pods are ready:
- Claims throughput (legitimate vs fraud)
- Fraud alerts by risk level (MEDIUM / HIGH / CRITICAL)
- ML scoring latency p50/p95/p99
- Active providers tracked in Redis

### Redpanda Console
http://NODE_IP:30808 → **Topics**

| Topic | Expected |
|---|---|
| `raw.claims.new` | ~2 messages/sec |
| `scored.claims` | ~2 messages/sec |
| `alerts.fraud` | ~1 message every 6–10 sec |

Click any topic → **Messages** → watch fraud events arrive in real time.

### Fraud Scorer API
http://NODE_IP:30800/docs → `POST /score` → Try with a fraudulent claim:

```json
{
  "claim_id": "DEMO-001",
  "amount": 12500.00,
  "claim_count_30d": 148,
  "avg_amount_30d": 890.00,
  "hour_of_day": 3,
  "is_weekend": 1,
  "procedure_risk": 0.90,
  "member_claim_count_90d": 1
}
```
Expected: `fraud_probability` > 0.90, `risk_level`: **CRITICAL**

### Live fraud alerts stream
```bash
export KUBECONFIG=.kubeconfig-demo
kubectl logs -f -n processing deploy/stream-processor | grep FRAUD
```

```
FRAUD [CRITICAL ] CLM-D9E4A102  provider=NPI-005  amount=R$  8,750.00  score=0.9312
FRAUD [HIGH    ] CLM-F1B3C007  provider=NPI-001  amount=R$  3,140.00  score=0.7821
```

---

## Scaling the Demo

### Increase fraud rate
```bash
kubectl set env -n processing deploy/data-generator FRAUD_RATE=0.30 CLAIMS_PER_SECOND=5
```

### Scale fraud-scorer replicas
```bash
kubectl scale -n ml deploy/fraud-scorer --replicas=4
```

### Check HPA status
```bash
kubectl get hpa -n ml
```

---

## Customizing the Cluster

Edit `infra/terraform/variables.tf` or pass variables:

```bash
# Use São Paulo region (LGPD data residency)
export TF_VAR_region="br-gru"

# Larger nodes if memory pressure
export TF_VAR_node_type="g6-standard-6"   # 6 vCPU / 16GB each

# More nodes
export TF_VAR_node_count=4
```

Then re-run `./deploy.sh`.

---

## Linode Cost Estimate

| Resource | Type | Monthly cost |
|---|---|---|
| 3× LKE Worker Nodes | g6-standard-4 (4 vCPU / 8GB) | ~$144 |
| LKE Control Plane | Managed (free) | $0 |
| Block Storage (PVCs) | ~30GB total (MLflow + Prometheus + Grafana + PG) | ~$3 |
| Linode NodeBalancer | Not used (NodePort instead) | $0 |
| **Total** | | **~$147/month** |

> **Tip:** Destroy the cluster when not actively demoing to avoid charges:
> ```bash
> ./destroy.sh
> ```

---

## Troubleshooting

**Pods in Pending state:**
```bash
kubectl describe pod -n <namespace> <pod-name>
# Usually: insufficient CPU/memory → use larger node type
```

**ImagePullBackOff on custom services:**
```bash
kubectl describe pod -n ml <fraud-scorer-pod>
# Check: image name correct? registry accessible? image is public?
```

**Redpanda topics not created:**
```bash
kubectl logs -n streaming job/redpanda-topic-init
# If failed: delete the job and re-apply 02-redpanda-topics.yaml
```

**Debezium connector not registering:**
```bash
kubectl logs -n streaming job/debezium-connector-init
curl http://NODE_IP:$(kubectl get svc -n streaming debezium -o jsonpath='{.spec.ports[0].nodePort}')/connectors
```

**Check all pod status at once:**
```bash
kubectl get pods -A --sort-by=.metadata.namespace
```

---

## Teardown

```bash
cd infra/
./destroy.sh
```

This runs `terraform destroy`, deletes the LKE cluster, all node pools, and all PVCs. The local `.kubeconfig-demo` file is also removed.

---

## Files Reference

```
infra/
├── terraform/
│   ├── versions.tf       Terraform + Linode provider version constraints
│   ├── variables.tf      token, region, node_type, node_count
│   ├── main.tf           LKE cluster resource + kubeconfig output
│   └── outputs.tf        cluster_id, api_endpoints, next_steps
│
├── helm/
│   ├── redpanda-values.yaml   single-node Redpanda, NodePort console
│   ├── postgres-values.yaml   WAL logical replication + initdb configmap ref
│   └── redis-values.yaml      standalone, no auth (demo)
│
├── k8s/
│   ├── 00-namespaces.yaml       streaming, data, processing, ml, monitoring
│   ├── 01-postgres-initdb.yaml  ConfigMap with schema SQL + seed providers
│   ├── 02-redpanda-topics.yaml  Job: creates raw.claims.new, scored, alerts
│   ├── 03-debezium.yaml         Deployment + Job (connector registration)
│   ├── 04-fraud-scorer.yaml     Deployment + NodePort 30800 + HPA
│   ├── 05-stream-processor.yaml Deployment (mimics Flink)
│   ├── 06-data-generator.yaml   Deployment
│   ├── 07-mlflow.yaml           Deployment + PVC + NodePort 30500
│   ├── 08-prometheus.yaml       Deployment + RBAC + ConfigMap + NodePort 30909
│   └── 09-grafana.yaml          Deployment + ConfigMaps (datasource, dashboards) + NodePort 30300
│
├── build-push.sh    builds + pushes fraud-scorer, stream-processor, data-generator
├── deploy.sh        full deploy: terraform → helm → kubectl → print URLs
└── destroy.sh       terraform destroy + cleanup
```
