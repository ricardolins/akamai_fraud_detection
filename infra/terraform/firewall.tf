# Linode Cloud Firewall — restricts all inbound traffic to a single allowed IP.
#
# Default inbound policy: DROP  (blocks everyone not explicitly listed)
# Default outbound policy: ACCEPT
#
# Allowed inbound from var.allowed_ip:
#   - 6443  Kubernetes API server
#   - 30000-32767  NodePort range (Grafana, Redpanda Console, Fraud Scorer,
#                   MLflow, Prometheus, in-cluster registry)
#
# Allowed inbound from RFC-1918 (inter-node and pod-to-pod traffic):
#   - All TCP — required for LKE node communication, pod overlay networking,
#               kube-proxy NodePort forwarding between nodes, and containerd
#               pulling images from the in-cluster registry via internal IPs

resource "linode_firewall" "cluster_fw" {
  label           = "${var.cluster_label}-fw"
  tags            = var.tags
  inbound_policy  = "DROP"
  outbound_policy = "ACCEPT"

  # Kubernetes API — kubectl from developer machine
  inbound {
    label    = "allow-k8s-api"
    action   = "ACCEPT"
    protocol = "TCP"
    ports    = "6443"
    ipv4     = ["${var.allowed_ip}/32"]
  }

  # All NodePorts — Grafana (30300), Redpanda (30808), Scorer (30800),
  # MLflow (30500), Prometheus (30909), registry (32500)
  inbound {
    label    = "allow-nodeports"
    action   = "ACCEPT"
    protocol = "TCP"
    ports    = "30000-32767"
    ipv4     = ["${var.allowed_ip}/32"]
  }

  # Inter-node and pod networking (RFC-1918 ranges used by LKE overlay)
  inbound {
    label    = "allow-internal-cluster"
    action   = "ACCEPT"
    protocol = "TCP"
    ports    = "1-65535"
    ipv4     = ["10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"]
  }

  # UDP overlay (Cilium / flannel VXLAN)
  inbound {
    label    = "allow-internal-udp"
    action   = "ACCEPT"
    protocol = "UDP"
    ports    = "1-65535"
    ipv4     = ["10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"]
  }

  # Calico IPIP tunneling (protocol 4) — LKE uses IP-in-IP for cross-node pod routing.
  # Without this, pods on different nodes cannot communicate.
  inbound {
    label    = "allow-ipip"
    action   = "ACCEPT"
    protocol = "IPENCAP"
    ipv4     = ["10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"]
  }

  # ICMP — kubelet node health checks and network diagnostics
  inbound {
    label    = "allow-icmp"
    action   = "ACCEPT"
    protocol = "ICMP"
    ipv4     = ["10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"]
  }
}

# Attach the firewall to every LKE worker node
resource "linode_firewall_device" "lke_nodes" {
  for_each = toset(flatten([
    for pool in linode_lke_cluster.fraud_demo.pool :
    [for node in pool.nodes : tostring(node.instance_id)]
  ]))

  firewall_id = linode_firewall.cluster_fw.id
  entity_id   = tonumber(each.value)
  entity_type = "linode"
}
