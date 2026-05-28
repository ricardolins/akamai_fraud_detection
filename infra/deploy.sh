#!/usr/bin/env bash
# Full end-to-end deployment to Linode LKE.
#
# Prerequisites:
#   1. Terraform installed (>= 1.6)
#   2. kubectl installed
#   3. Helm installed (>= 3.14)
#   4. Docker images already pushed (run build-push.sh first)
#
# Usage:
#   export TF_VAR_linode_token="your-linode-api-token"
#   export REGISTRY="ghcr.io/ricardolins/akamai_fraud_detection"
#   export TAG="demo"
#   ./deploy.sh

set -euo pipefail

# ── Config ─────────────────────────────────────────────────────────────────────
REGISTRY="${REGISTRY:-ghcr.io/ricardolins/akamai_fraud_detection}"
TAG="${TAG:-demo}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K8S_DIR="$SCRIPT_DIR/k8s"
HELM_DIR="$SCRIPT_DIR/helm"
TF_DIR="$SCRIPT_DIR/terraform"
KUBECONFIG_PATH="$SCRIPT_DIR/../.kubeconfig-demo"

# ── Helpers ─────────────────────────────────────────────────────────────────────
info()  { echo -e "\033[0;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[0;32m[ OK ]\033[0m  $*"; }
warn()  { echo -e "\033[0;33m[WARN]\033[0m  $*"; }
die()   { echo -e "\033[0;31m[ERR ]\033[0m  $*" >&2; exit 1; }

wait_for_pods() {
    local ns="$1" label="$2" timeout="${3:-180}"
    info "Waiting for pods in $ns ($label)..."
    kubectl wait pod -n "$ns" -l "$label" \
        --for=condition=Ready --timeout="${timeout}s" \
        2>/dev/null || warn "Some pods in $ns not ready after ${timeout}s — continuing"
}

# ── Preflight ───────────────────────────────────────────────────────────────────
for cmd in terraform kubectl helm; do
    command -v "$cmd" &>/dev/null || die "$cmd not found. Install it first."
done

[[ -z "${TF_VAR_linode_token:-}" ]] && \
    die "TF_VAR_linode_token not set. Export your Linode API token."

info "Registry : $REGISTRY"
info "Tag      : $TAG"
echo ""

# ── STEP 1: Provision LKE with Terraform ────────────────────────────────────────
info "Step 1 — Provisioning LKE cluster (takes ~5 min)..."

cd "$TF_DIR"
terraform init -upgrade -input=false
terraform apply -auto-approve -input=false

export KUBECONFIG="$KUBECONFIG_PATH"
ok "Cluster provisioned. KUBECONFIG=$KUBECONFIG"
kubectl cluster-info
kubectl get nodes
echo ""

# ── STEP 2: Create namespaces ────────────────────────────────────────────────────
info "Step 2 — Creating namespaces..."
kubectl apply -f "$K8S_DIR/00-namespaces.yaml"
ok "Namespaces created"
echo ""

# ── STEP 2.5: Propagate Object Storage credentials to Kubernetes ─────────────────
info "Step 2.5 — Creating object-storage-credentials secret from Terraform outputs..."

cd "$TF_DIR"
S3_ACCESS_KEY=$(terraform output -raw object_storage_access_key)
S3_SECRET_KEY=$(terraform output -raw object_storage_secret_key)
S3_BUCKET=$(terraform output -raw object_storage_bucket)
S3_ENDPOINT=$(terraform output -raw object_storage_endpoint)
cd "$SCRIPT_DIR"

for ns in processing data; do
    kubectl create secret generic object-storage-credentials \
        --namespace "$ns" \
        --from-literal=AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY" \
        --from-literal=AWS_SECRET_ACCESS_KEY="$S3_SECRET_KEY" \
        --from-literal=AWS_ENDPOINT_URL_S3="$S3_ENDPOINT" \
        --from-literal=BUCKET="$S3_BUCKET" \
        --dry-run=client -o yaml | kubectl apply -f -
done
ok "Object Storage secret created in processing + data namespaces"
echo ""

# ── STEP 3: Install infrastructure via Helm ──────────────────────────────────────
info "Step 3 — Installing Helm charts (Redpanda, PostgreSQL, Redis)..."

helm repo add redpanda  https://charts.redpanda.com        --force-update
helm repo add bitnami   https://charts.bitnami.com/bitnami --force-update
helm repo update

info "  Installing Redpanda..."
helm upgrade --install redpanda redpanda/redpanda \
    --namespace streaming \
    --values "$HELM_DIR/redpanda-values.yaml" \
    --timeout 10m \
    --wait

info "  Installing PostgreSQL..."
# Deploy initdb ConfigMap before PostgreSQL chart
kubectl apply -f "$K8S_DIR/01-postgres-initdb.yaml"
helm upgrade --install postgres bitnami/postgresql \
    --namespace data \
    --values "$HELM_DIR/postgres-values.yaml" \
    --timeout 5m \
    --wait

info "  Installing Redis..."
helm upgrade --install redis bitnami/redis \
    --namespace data \
    --values "$HELM_DIR/redis-values.yaml" \
    --timeout 5m \
    --wait

info "  Installing Spark Operator..."
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace processing \
    --values "$HELM_DIR/spark-operator-values.yaml" \
    --timeout 5m \
    --wait

ok "Helm charts installed"
echo ""

# ── STEP 4: Initialize Redpanda topics ───────────────────────────────────────────
info "Step 4 — Creating Redpanda topics..."
kubectl delete job redpanda-topic-init -n streaming --ignore-not-found
kubectl apply  -f "$K8S_DIR/02-redpanda-topics.yaml"
kubectl wait job/redpanda-topic-init -n streaming \
    --for=condition=complete --timeout=60s
ok "Topics created: raw.claims.new, scored.claims, alerts.fraud"
echo ""

# ── STEP 5: Deploy custom services (images with REGISTRY placeholder replaced) ──
info "Step 5 — Deploying custom services..."

for manifest in \
    "$K8S_DIR/03-debezium.yaml" \
    "$K8S_DIR/04-fraud-scorer.yaml" \
    "$K8S_DIR/05-stream-processor.yaml" \
    "$K8S_DIR/06-data-generator.yaml" \
    "$K8S_DIR/07-mlflow.yaml"; do

    # Replace REGISTRY placeholder with actual registry value
    sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g" "$manifest" | kubectl apply -f -
done

ok "Custom service manifests applied"
echo ""

# ── STEP 5.5: Deploy medallion data lake services ───────────────────────────────
info "Step 5.5 — Deploying medallion data lake (Nessie, RBAC, bronze consumer)..."

kubectl apply -f "$K8S_DIR/10-nessie.yaml"
kubectl apply -f "$K8S_DIR/12-spark-rbac.yaml"

# bronze-consumer uses the object-storage-credentials secret; replace REGISTRY placeholder
sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g" "$K8S_DIR/11-bronze-consumer.yaml" | kubectl apply -f -

wait_for_pods data      "app=nessie"           120
wait_for_pods processing "app=bronze-consumer" 120

ok "Medallion services deployed"
echo ""

# ── STEP 5.6: Apply Spark scheduled jobs ────────────────────────────────────────
info "Step 5.6 — Applying Spark ETL scheduled jobs..."

# Replace REGISTRY and S3 endpoint placeholders before applying
sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g; s|S3_ENDPOINT_PLACEHOLDER|$S3_ENDPOINT|g" \
    "$K8S_DIR/13-spark-jobs.yaml" | kubectl apply -f -

ok "Spark ScheduledSparkApplications registered (silver-etl: daily 02:00, gold-features: Sunday 04:00)"
echo ""

# ── STEP 6: Deploy observability ────────────────────────────────────────────────
info "Step 6 — Deploying Prometheus and Grafana..."
kubectl apply -f "$K8S_DIR/08-prometheus.yaml"
kubectl apply -f "$K8S_DIR/09-grafana.yaml"
ok "Observability deployed"
echo ""

# ── STEP 7: Wait for workloads to be ready ────────────────────────────────────
info "Step 7 — Waiting for workloads to become ready (up to 5 min)..."
wait_for_pods ml           "app=fraud-scorer"     240
wait_for_pods processing   "app=stream-processor" 120
wait_for_pods processing   "app=data-generator"   120
wait_for_pods processing   "app=bronze-consumer"  120
wait_for_pods data         "app=nessie"           120
wait_for_pods ml           "app=mlflow"           120
wait_for_pods monitoring   "app=prometheus"       120
wait_for_pods monitoring   "app=grafana"          120
echo ""

# ── STEP 8: Get node IPs and print access URLs ────────────────────────────────
info "Step 8 — Fetching node IPs..."
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null || echo "")

if [[ -z "$NODE_IP" ]]; then
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}')
    warn "Could not find ExternalIP, using first available: $NODE_IP"
fi

ok "Deployment complete!"
echo ""
echo "══════════════════════════════════════════════════════════"
echo "  FRAUD DETECTION DEMO — ACCESS URLS"
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  Node IP : $NODE_IP"
echo ""
echo "  Grafana           →  http://$NODE_IP:30300   (admin / admin)"
echo "  Redpanda Console  →  http://$NODE_IP:30808"
echo "  Fraud Scorer API  →  http://$NODE_IP:30800/docs"
echo "  MLflow            →  http://$NODE_IP:30500"
echo "  Prometheus        →  http://$NODE_IP:30909"
echo ""
echo "══════════════════════════════════════════════════════════"
echo ""
echo "Monitor fraud alerts live:"
echo "  kubectl logs -f -n processing deploy/stream-processor | grep FRAUD"
echo ""
echo "Check all pods:"
echo "  kubectl get pods -A"
echo ""
echo "To destroy the cluster:"
echo "  ./destroy.sh"
