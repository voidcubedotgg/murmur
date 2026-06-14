# Public IPs for SSH / scp / running murmurctl.
output "peers" {
  description = "node -> {public_ip, private_ip}"
  value = {
    for k, s in hcloud_server.peer : k => {
      public_ip  = s.ipv4_address
      private_ip = [for n in s.network : n.ip][0]
    }
  }
}

output "nfs" {
  value = {
    public_ip  = hcloud_server.nfs.ipv4_address
    private_ip = var.nfs_ip
  }
}

# Convenience: the seed peer's public IP (where you submit workloads).
output "seed_public_ip" {
  value = hcloud_server.peer[var.peers[0].node].ipv4_address
}
