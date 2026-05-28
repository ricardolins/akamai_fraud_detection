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
