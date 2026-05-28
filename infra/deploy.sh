#!/usr/bin/env bash
# Full end-to-end deployment to Linode LKE.
#
# Prerequisites:
#   1. Terraform installed (>= 1.6)
#   2. kubectl installed
#   3. Helm installed (>= 3.14)
#   4. Docker Desktop running (images are built and pushed to the in-cluster registry)
#
# Usage:
#   export TF_VAR_linode_token="your-linode-api-token"
#   ./deploy.sh

set -euo pipefail

# ── Config ─────────────────────────────────────────────────────────────────────
TAG="${TAG:-demo}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
K8S_DIR="$SCRIPT_DIR/k8s"
HELM_DIR="$SCRIPT_DIR/helm"
TF_DIR="$SCRIPT_DIR/terraform"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
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

wait_for_daemonset() {
    local ns="$1" name="$2" timeout="${3:-120}"
    info "Waiting for DaemonSet $ns/$name to complete init containers..."
    local deadline=$(( $(date +%s) + timeout ))
    while true; do
        local desired ready
        desired=$(kubectl get ds "$name" -n "$ns" -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || echo "0")
        ready=$(kubectl get ds   "$name" -n "$ns" -o jsonpath='{.status.numberReady}'           2>/dev/null || echo "0")
        if [[ "$desired" -gt 0 && "$ready" -eq "$desired" ]]; then
            ok "DaemonSet $name ready ($ready/$desired nodes)"
            return 0
        fi
        if [[ $(date +%s) -ge $deadline ]]; then
            warn "DaemonSet $name not fully ready after ${timeout}s (desired=$desired ready=$ready) — continuing"
            return 0
        fi
        sleep 5
    done
}

# ── Preflight ───────────────────────────────────────────────────────────────────
for cmd in terraform kubectl helm docker; do
    command -v "$cmd" &>/dev/null || die "$cmd not found. Install it first."
done

[[ -z "${TF_VAR_linode_token:-}" ]] && \
    die "TF_VAR_linode_token not set. Export your Linode API token."

info "Tag: $TAG"
echo ""

# ── STEP 1: Provision LKE with Terraform ────────────────────────────────────────
info "Step 1 — Provisioning LKE cluster + Object Storage (takes ~5 min)..."

cd "$TF_DIR"
terraform init -upgrade -input=false
terraform apply -auto-approve -input=false

export KUBECONFIG="$KUBECONFIG_PATH"
ok "Cluster provisioned. KUBECONFIG=$KUBECONFIG"
kubectl cluster-info
kubectl get nodes
echo ""

# ── STEP 2: Get node info ────────────────────────────────────────────────────────
info "Step 2 — Gathering node info..."

NODE_NAME=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="ExternalIP")].address}' 2>/dev/null || echo "")

if [[ -z "$NODE_IP" ]]; then
    NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[0].address}')
    warn "ExternalIP not found, using first available: $NODE_IP"
fi

REGISTRY="$NODE_IP:32500"
ok "Registry will be: $REGISTRY  (NodePort on node $NODE_NAME / $NODE_IP)"
echo ""

# ── STEP 3: Create namespaces ────────────────────────────────────────────────────
info "Step 3 — Creating namespaces..."
kubectl apply -f "$K8S_DIR/00-namespaces.yaml"
ok "Namespaces created"
echo ""

# ── STEP 4: Deploy in-cluster registry ──────────────────────────────────────────
info "Step 4 — Deploying in-cluster registry (registry:2) on node $NODE_NAME..."

# ConfigMap with registry node IP — read by the containerd DaemonSet
kubectl create configmap registry-node-ip -n tools \
    --from-literal=ip="$NODE_IP" \
    --dry-run=client -o yaml | kubectl apply -f -

# Apply registry manifest with the actual node name substituted
sed "s|NODE_NAME_PLACEHOLDER|$NODE_NAME|g" "$K8S_DIR/00b-registry.yaml" | kubectl apply -f -

info "  Waiting for registry pod to be ready..."
kubectl wait pod -n tools -l app=registry \
    --for=condition=Ready --timeout=120s

ok "Registry running at http://$REGISTRY"
echo ""

# ── STEP 5: Configure containerd on all nodes to trust the HTTP registry ─────────
info "Step 5 — Configuring containerd on all LKE nodes to trust $REGISTRY..."
kubectl apply -f "$K8S_DIR/00c-containerd-config.yaml"
wait_for_daemonset tools containerd-registry-config 120

# Give containerd a moment to reload after SIGHUP
sleep 5
ok "Containerd configured on all nodes"
echo ""

# ── STEP 6: Configure Docker Desktop + build and push images ────────────────────
echo "══════════════════════════════════════════════════════════"
echo ""
echo "  ACTION REQUIRED — Configure Docker Desktop"
echo ""
echo "  The in-cluster registry uses HTTP (no TLS)."
echo "  Docker Desktop must be told to trust it as an insecure registry."
echo ""
echo "  Steps:"
echo "    1. Open Docker Desktop → Settings → Docker Engine"
echo "    2. Add the following to the JSON config:"
echo ""
echo "       \"insecure-registries\": [\"$REGISTRY\"]"
echo ""
echo "    3. Click 'Apply & Restart'"
echo "    4. Wait for Docker Desktop to restart (~30 seconds)"
echo "    5. Come back here and press Enter to continue"
echo ""
echo "══════════════════════════════════════════════════════════"
read -r -p "Press Enter after Docker Desktop has restarted... "
echo ""

info "Step 6 — Building and pushing images to $REGISTRY..."

REGISTRY="$REGISTRY" TAG="$TAG" "$SCRIPT_DIR/build-push.sh"

ok "All images pushed to $REGISTRY"
echo ""

# ── STEP 7: Object Storage credentials ──────────────────────────────────────────
info "Step 7 — Creating object-storage-credentials secret from Terraform outputs..."

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

# ── STEP 8: Install infrastructure via Helm ──────────────────────────────────────
info "Step 8 — Installing Helm charts (Redpanda, PostgreSQL, Redis, Spark Operator)..."

helm repo add redpanda      https://charts.redpanda.com        --force-update
helm repo add bitnami       https://charts.bitnami.com/bitnami --force-update
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
helm repo update

info "  Installing Redpanda..."
helm upgrade --install redpanda redpanda/redpanda \
    --namespace streaming \
    --values "$HELM_DIR/redpanda-values.yaml" \
    --timeout 10m \
    --wait

info "  Installing PostgreSQL..."
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
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace processing \
    --values "$HELM_DIR/spark-operator-values.yaml" \
    --timeout 5m \
    --wait

ok "Helm charts installed"
echo ""

# ── STEP 9: Initialize Redpanda topics ───────────────────────────────────────────
info "Step 9 — Creating Redpanda topics..."
kubectl delete job redpanda-topic-init -n streaming --ignore-not-found
kubectl apply  -f "$K8S_DIR/02-redpanda-topics.yaml"
kubectl wait job/redpanda-topic-init -n streaming \
    --for=condition=complete --timeout=60s
ok "Topics created: raw.claims.new, scored.claims, alerts.fraud"
echo ""

# ── STEP 10: Deploy custom services ──────────────────────────────────────────────
info "Step 10 — Deploying custom services..."

for manifest in \
    "$K8S_DIR/03-debezium.yaml" \
    "$K8S_DIR/04-fraud-scorer.yaml" \
    "$K8S_DIR/05-stream-processor.yaml" \
    "$K8S_DIR/06-data-generator.yaml" \
    "$K8S_DIR/07-mlflow.yaml"; do

    sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g" "$manifest" | kubectl apply -f -
done

ok "Custom service manifests applied"
echo ""

# ── STEP 11: Deploy medallion data lake services ─────────────────────────────────
info "Step 11 — Deploying medallion data lake (Nessie, RBAC, bronze consumer)..."

kubectl apply -f "$K8S_DIR/10-nessie.yaml"
kubectl apply -f "$K8S_DIR/12-spark-rbac.yaml"
sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g" "$K8S_DIR/11-bronze-consumer.yaml" | kubectl apply -f -

ok "Medallion services applied"
echo ""

# ── STEP 12: Apply Spark scheduled jobs ──────────────────────────────────────────
info "Step 12 — Applying Spark ETL scheduled jobs..."
sed "s|REGISTRY/|$REGISTRY/|g; s|:demo|:$TAG|g; s|S3_ENDPOINT_PLACEHOLDER|$S3_ENDPOINT|g" \
    "$K8S_DIR/13-spark-jobs.yaml" | kubectl apply -f -
ok "Spark ScheduledSparkApplications registered (silver-etl: daily 02:00, gold-features: Sunday 04:00)"
echo ""

# ── STEP 13: Deploy observability ────────────────────────────────────────────────
info "Step 13 — Deploying Prometheus and Grafana..."
kubectl apply -f "$K8S_DIR/08-prometheus.yaml"
kubectl apply -f "$K8S_DIR/09-grafana.yaml"
ok "Observability deployed"
echo ""

# ── STEP 14: Wait for workloads ──────────────────────────────────────────────────
info "Step 14 — Waiting for workloads to become ready (up to 8 min)..."
wait_for_pods ml           "app=fraud-scorer"     300
wait_for_pods processing   "app=stream-processor" 180
wait_for_pods processing   "app=data-generator"   180
wait_for_pods processing   "app=bronze-consumer"  180
wait_for_pods data         "app=nessie"           180
wait_for_pods ml           "app=mlflow"           180
wait_for_pods monitoring   "app=prometheus"       120
wait_for_pods monitoring   "app=grafana"          120
echo ""

# ── STEP 15: Print access URLs ───────────────────────────────────────────────────
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
echo "  Image Registry    →  http://$NODE_IP:32500/v2/_catalog"
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
