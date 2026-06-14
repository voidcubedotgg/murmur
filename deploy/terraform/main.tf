terraform {
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.48"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}

resource "hcloud_ssh_key" "admin" {
  name       = "murmur-admin"
  public_key = file(pathexpand(var.ssh_public_key_path))
}

# Private network: all gossip (8101/8201) and NFS (2049) ride this, unfiltered.
resource "hcloud_network" "murmur" {
  name     = "murmur"
  ip_range = "10.0.0.0/16"
}

resource "hcloud_network_subnet" "murmur" {
  network_id   = hcloud_network.murmur.id
  type         = "cloud"
  network_zone = "eu-central"
  ip_range     = var.subnet
}

# Public-interface firewall: SSH from admin only. Private net is NOT filtered by
# hcloud firewalls, so gossip + NFS between nodes need no rules here.
resource "hcloud_firewall" "ssh" {
  name = "murmur-ssh"
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = [var.admin_cidr]
  }
  # ICMP so you can ping the public IPs while debugging.
  rule {
    direction  = "in"
    protocol   = "icmp"
    source_ips = [var.admin_cidr]
  }
}

# --- NFS server: shared snapshot store, independent of any peer's life --------
resource "hcloud_server" "nfs" {
  name         = "murmur-nfs"
  server_type  = var.nfs_server_type
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.admin.id]
  firewall_ids = [hcloud_firewall.ssh.id]
  user_data = templatefile("${path.module}/cloud-init-nfs.yaml.tftpl", {
    snap_dir = var.snap_dir
    subnet   = var.subnet
  })
  network {
    network_id = hcloud_network.murmur.id
    ip         = var.nfs_ip
  }
  depends_on = [hcloud_network_subnet.murmur]
}

# --- Peers: host-a/b/c -------------------------------------------------------
resource "hcloud_server" "peer" {
  for_each     = { for p in var.peers : p.node => p }
  name         = "murmur-${each.value.node}"
  server_type  = var.server_type
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.admin.id]
  firewall_ids = [hcloud_firewall.ssh.id]
  user_data = templatefile("${path.module}/cloud-init-peer.yaml.tftpl", {
    node     = each.value.node
    priv_ip  = each.value.ip
    seed_ip  = var.peers[0].ip # index 0 is the seed
    nfs_ip   = var.nfs_ip
    snap_dir = var.snap_dir
    size     = length(var.peers)
  })
  network {
    network_id = hcloud_network.murmur.id
    ip         = each.value.ip
  }
  depends_on = [hcloud_server.nfs] # mount target should exist first
}
