terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.6.2, < 4.0.0"
    }
  }
}

# Configure the Docker provider
provider "docker" {}

# Define a PostgreSQL container
resource "docker_container" "postgres" {
  name  = "postgres-dev"
  image = "postgres:latest" # Or a specific version
  ports {
    internal = var.postgres_port
    external = var.postgres_port
  }
  env = [
    "POSTGRES_USER=${var.postgres_user}",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=${var.postgres_db}"
  ]
}
