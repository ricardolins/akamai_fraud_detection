variable "linode_token" {
  description = "Linode API token with Read/Write access"
  type        = string
  sensitive   = true
}

variable "cluster_label" {
  description = "LKE cluster name"
  type        = string
  default     = "fraud-detection-demo"
}

variable "region" {
  description = "Linode region. Use br-gru for São Paulo (LGPD data residency)."
  type        = string
  default     = "br-gru"
  # Other options: us-east (Newark), eu-central (Frankfurt), ap-southeast (Sydney)
}

variable "k8s_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.32"
}

variable "node_type" {
  description = "Linode instance type per node. g6-standard-4 = 4 vCPU / 8GB RAM."
  type        = string
  default     = "g6-standard-4"
  # g6-standard-2 = 2 vCPU / 4GB  ~$24/node  (minimal, may have memory pressure)
  # g6-standard-4 = 4 vCPU / 8GB  ~$48/node  (recommended for demo)
  # g6-standard-6 = 6 vCPU / 16GB ~$96/node  (comfortable with headroom)
}

variable "node_count" {
  description = "Number of worker nodes"
  type        = number
  default     = 3
}

variable "tags" {
  description = "Tags applied to all resources"
  type        = list(string)
  default     = ["fraud-detection", "demo"]
}
