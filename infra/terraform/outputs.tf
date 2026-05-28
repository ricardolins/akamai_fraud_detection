output "cluster_id" {
  description = "LKE cluster ID"
  value       = linode_lke_cluster.fraud_demo.id
}

output "cluster_label" {
  description = "LKE cluster label"
  value       = linode_lke_cluster.fraud_demo.label
}

output "region" {
  description = "Cluster region"
  value       = linode_lke_cluster.fraud_demo.region
}

output "k8s_version" {
  description = "Kubernetes version running on the cluster"
  value       = linode_lke_cluster.fraud_demo.k8s_version
}

output "kubeconfig_path" {
  description = "Path to the kubeconfig file written by Terraform"
  value       = "${path.module}/../../.kubeconfig-demo"
}

output "api_endpoints" {
  description = "Kubernetes API server endpoints"
  value       = linode_lke_cluster.fraud_demo.api_endpoints
}

output "object_storage_endpoint" {
  description = "S3-compatible endpoint for Linode Object Storage"
  value       = "https://${var.region}-1.linodeobjects.com"
}

output "object_storage_bucket" {
  description = "Data lake bucket name"
  value       = linode_object_storage_bucket.datalake.label
}

output "object_storage_access_key" {
  description = "Object Storage access key (S3-compatible)"
  value       = linode_object_storage_key.datalake.access_key
  sensitive   = true
}

output "object_storage_secret_key" {
  description = "Object Storage secret key (S3-compatible)"
  value       = linode_object_storage_key.datalake.secret_key
  sensitive   = true
}

output "next_steps" {
  description = "Commands to run after terraform apply"
  value       = <<-EOT
    Cluster ready. Run the deploy script:

      export KUBECONFIG=$(pwd)/.kubeconfig-demo
      cd infra && ./deploy.sh

    Or manually:
      kubectl get nodes
      kubectl get pods -A
  EOT
}
