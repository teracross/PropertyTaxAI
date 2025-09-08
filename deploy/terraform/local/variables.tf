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