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

resource "docker_volume" "postgres_data" {
  name = "postgres_data_volume"
}

resource "local_file" "init_script" {
  content = <<-EOT
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = '${var.postgres_readonly_user}'
      ) THEN
        CREATE USER ${var.postgres_readonly_user} WITH PASSWORD '${var.postgres_readonly_password}';
      END IF;
    END
    $$;

    GRANT CONNECT ON DATABASE ${var.postgres_db} TO ${var.postgres_readonly_user};
    GRANT USAGE ON SCHEMA public TO ${var.postgres_readonly_user};
    GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${var.postgres_readonly_user};
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${var.postgres_readonly_user};
  EOT
  
  filename = "${path.module}/generated-init.sql"
}

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

  volumes {
    volume_name    = docker_volume.postgres_data.name
    container_path = "/var/lib/postgresql/data"
  }

  volumes {
    host_path      = abspath(local_file.init_script.filename)
    container_path = "/docker-entrypoint-initdb.d/init-readonly-user.sql"
    read_only      = true
  }

  depends_on = [local_file.init_script]
}
