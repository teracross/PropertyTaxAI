terraform {
  required_providers {
     proxmox = {
      source  = "bpg/proxmox"
      version = "~> 0.85" # Use the latest version
    }

    postgresql = {
      source  = "cyrilgdn/postgresql"
      version = "~> 1.22"
    }

    terraform = {
      source = "terraform.io/builtin/terraform"
    }
  }
}

# Configuration for local proxmox cluster
provider "proxmox" {
  endpoint = "https://${var.proxmox_host_addr}/api2/json"
  api_token = "${var.proxmox_user}!${var.proxmox_token_id}=${var.proxmox_api_token}"
  insecure = true
}

# Proxmox LXC for PostgreSQL
resource "proxmox_virtual_environment_container" "postgres" {
  description = "VM for Postgres SQL DB for Property Tax Project"
  node_name = "edlab"
  vm_id     = 110

  initialization {
    hostname = "terraform-provider-proxmox-ubuntu-container"

    ip_config {
      ipv4 {
        address = "dhcp"
      }
    }

  }

  network_interface {
    name = "veth0"
  }

  disk {
    datastore_id = "vm-storage"
    size         = 200
  }

  operating_system {
    # template_file_id = proxmox_virtual_environment_download_file.ubuntu_2504_lxc_img.id
    # Or you can use a volume ID, as obtained from a "pvesm list <storage>"
    template_file_id = "local:vztmpl/debian-12-turnkey-postgresql_18.1-1_amd64.tar.gz"
    type             = "debian"
  }

  mount_point {
    # bind mount, *requires* root@pam authentication
    volume = "/mnt/bindmounts/shared"
    path   = "/mnt/shared"
  }

  mount_point {
    # volume mount, a new volume will be created by PVE
    volume = "vm-storage"
    size   = "10G"
    path   = "/mnt/volume"
  }

  startup {
    order      = "3"
    up_delay   = "60"
    down_delay = "60"
  }

  tags   = ["terraform", "postgres"]
}

resource "proxmox_virtual_environment_download_file" "ubuntu_2504_lxc_img" {
  content_type = "vztmpl"
  datastore_id = "local"
  node_name    = "edlab"
  url          = "https://mirror.turnkeylinux.org/turnkeylinux/images/proxmox/debian-12-turnkey-postgresql_18.1-1_amd64.tar.gz"
}

provider "postgresql" {
  host      = "192.168.1.50"  # Your LXC IP
  port      = var.postgres_port
  username  = var.postgres_user      # Default admin user
  password  = var.postgres_password
  sslmode   = "disable"       # Or "require" if you configure SSL
  superuser = false
}

# Create application database
resource "postgresql_database" "app_db" {
  name  = var.postgres_db
  owner = postgresql_role.app_user.name

  depends_on = [proxmox_virtual_environment_container.postgres]
}

# Create application user
resource "postgresql_role" "app_user" {
  name     = "myappuser"
  login    = true
  password = var.postgres_password
  encrypted_password = true

  depends_on = [proxmox_virtual_environment_container.postgres]
}

# Grant privileges
resource "postgresql_grant" "app_user_db" {
  database    = postgresql_database.app_db.name
  role        = postgresql_role.app_user.name
  object_type = "database"
  privileges  = ["ALL"]
}