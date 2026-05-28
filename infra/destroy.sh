#!/usr/bin/env bash
# Destroys the entire LKE cluster and all resources.
# This deletes ALL data — use only when done with the demo.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
TF_DIR="$SCRIPT_DIR/terraform"

echo "WARNING: This will DESTROY the LKE cluster and all resources."
read -r -p "Type 'yes' to confirm: " confirm
[[ "$confirm" == "yes" ]] || { echo "Aborted."; exit 0; }

[[ -z "${TF_VAR_linode_token:-}" ]] && \
    { echo "TF_VAR_linode_token not set."; exit 1; }

cd "$TF_DIR"
terraform destroy -auto-approve

rm -f "$SCRIPT_DIR/../.kubeconfig-demo"
echo "Cluster destroyed and kubeconfig removed."
