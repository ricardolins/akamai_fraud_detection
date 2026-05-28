# LKE Cluster — Healthcare Fraud Detection Demo
# Provisions a managed Kubernetes cluster on Akamai Cloud Linode.
# All stateful and stateless workloads run on LKE; no separate VMs required for the demo.
# Production architecture would separate stateful brokers (Redpanda, PostgreSQL) onto
# dedicated high-memory Linode VMs outside of LKE.

resource "linode_lke_cluster" "fraud_demo" {
  label       = var.cluster_label
  k8s_version = var.k8s_version
  region      = var.region
  tags        = var.tags

  pool {
    type  = var.node_type
    count = var.node_count

    autoscaler {
      min = var.node_count
      max = var.node_count + 2   # allow burst during model training
    }
  }
}

# Write kubeconfig to local file so kubectl and helm can pick it up immediately
resource "local_sensitive_file" "kubeconfig" {
  content         = base64decode(linode_lke_cluster.fraud_demo.kubeconfig)
  filename        = "${path.module}/../../.kubeconfig-demo"
  file_permission = "0600"
}
