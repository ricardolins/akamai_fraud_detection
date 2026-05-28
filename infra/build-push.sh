#!/usr/bin/env bash
# Build and push custom Docker images to a container registry.
# Run this BEFORE deploy.sh.
#
# Usage:
#   export REGISTRY=ghcr.io/ricardolins/akamai_fraud_detection
#   export TAG=demo
#   ./build-push.sh
#
# For GitHub Container Registry (GHCR):
#   echo $GITHUB_TOKEN | docker login ghcr.io -u ricardolins --password-stdin
#   export REGISTRY=ghcr.io/ricardolins/akamai_fraud_detection
#
# For Docker Hub:
#   docker login
#   export REGISTRY=docker.io/ricardolins

set -euo pipefail

REGISTRY="${REGISTRY:-ghcr.io/ricardolins/akamai_fraud_detection}"
TAG="${TAG:-demo}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Registry : $REGISTRY"
echo "Tag      : $TAG"
echo "Repo root: $REPO_ROOT"
echo ""

SERVICES=(fraud-scorer stream-processor data-generator bronze-consumer)

for svc in "${SERVICES[@]}"; do
    # Map service name to source directory
    case "$svc" in
        stream-processor) context="$REPO_ROOT/demo/stream_processor" ;;
        data-generator)   context="$REPO_ROOT/demo/data_generator" ;;
        fraud-scorer)     context="$REPO_ROOT/demo/fraud_scorer" ;;
        bronze-consumer)  context="$REPO_ROOT/demo/bronze_consumer" ;;
    esac

    image="$REGISTRY/$svc:$TAG"

    echo "──────────────────────────────────────────"
    echo "Building $image"
    echo "  context: $context"
    docker build -t "$image" "$context"
    echo "Pushing  $image"
    docker push "$image"
    echo "✓ $svc done"
    echo ""
done

# Build Spark jobs image (uses infra/spark-jobs, not demo/)
echo "──────────────────────────────────────────"
SPARK_IMAGE="$REGISTRY/spark-jobs:$TAG"
echo "Building $SPARK_IMAGE"
echo "  context: $REPO_ROOT/infra/spark-jobs"
docker build -t "$SPARK_IMAGE" "$REPO_ROOT/infra/spark-jobs"
echo "Pushing  $SPARK_IMAGE"
docker push "$SPARK_IMAGE"
echo "✓ spark-jobs done"
echo ""

echo "══════════════════════════════════════════"
echo "All images pushed."
echo ""
echo "Set in deploy.sh:"
echo "  REGISTRY=$REGISTRY"
echo "  TAG=$TAG"
