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

SERVICES=(fraud-scorer stream-processor data-generator)

for svc in "${SERVICES[@]}"; do
    # Map service name to demo directory name
    case "$svc" in
        stream-processor) demo_dir="stream_processor" ;;
        data-generator)   demo_dir="data_generator" ;;
        fraud-scorer)     demo_dir="fraud_scorer" ;;
    esac

    image="$REGISTRY/$svc:$TAG"
    context="$REPO_ROOT/demo/$demo_dir"

    echo "──────────────────────────────────────────"
    echo "Building $image"
    echo "  context: $context"
    docker build -t "$image" "$context"
    echo "Pushing  $image"
    docker push "$image"
    echo "✓ $svc done"
    echo ""
done

echo "══════════════════════════════════════════"
echo "All images pushed."
echo ""
echo "Set in deploy.sh:"
echo "  REGISTRY=$REGISTRY"
echo "  TAG=$TAG"
