variable "hcloud_token" {
  type        = string
  sensitive   = true
  description = "Hetzner Cloud API token. Export TF_VAR_hcloud_token=... (do NOT commit)."
}

variable "ssh_public_key_path" {
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
  description = "Public key uploaded to Hetzner and installed on every node."
}

variable "admin_cidr" {
  type        = string
  description = "Your IP (CIDR, e.g. 1.2.3.4/32) allowed to SSH. Find it: curl ifconfig.me"
}

variable "location" {
  type        = string
  default     = "nbg1"
  description = "Hetzner location (nbg1/fsn1/hel1/ash/...)."
}

variable "server_type" {
  type        = string
  default     = "cx22"
  description = "Peer instance type. cx22 is the cheap shared-vCPU box."
}

variable "nfs_server_type" {
  type        = string
  default     = "cx22"
  description = "NFS box type. Can be the smallest available."
}

variable "image" {
  type        = string
  default     = "ubuntu-24.04"
}

# Fixed private IPs so cloud-init can template seeds/addresses at provision time.
variable "subnet" {
  type    = string
  default = "10.0.0.0/24"
}

variable "nfs_ip" {
  type    = string
  default = "10.0.0.5"
}

# host-a/b/c -> .10/.11/.12. Order matters: index 0 is the seed peer.
variable "peers" {
  type = list(object({ node = string, ip = string }))
  default = [
    { node = "host-a", ip = "10.0.0.10" },
    { node = "host-b", ip = "10.0.0.11" },
    { node = "host-c", ip = "10.0.0.12" },
  ]
}

variable "snap_dir" {
  type    = string
  default = "/srv/murmursnap"
}
