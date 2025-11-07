variable "postgres_user" {
  type = string
  default = "db_user"
  sensitive = true
}

variable "postgres_password" {
  type = string
  default = "password"
  sensitive = true
}

variable "postgres_db" {
  type = string
  default = "postgres_db"
  sensitive = true
}

variable "postgres_port" {
  type = number
  default = 5432
  sensitive = true
}

variable "proxmox_host_addr" {
  type = string
  sensitive = true
}

variable "proxmox_user" {
  type = string
  sensitive = true
}

variable "proxmox_api_token" {
  type = string
  sensitive = true
}

variable "proxmox_token_id" {
  type = string
  sensitive = true
}
