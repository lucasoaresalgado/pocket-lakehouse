# Restarts any container whose healthcheck reports `unhealthy` (restart: unless-stopped only covers exits/crashes).
# Needs the Docker socket: that is root-equivalent on the host, so it is opt-in, has no ports, and is pinned.
resource "docker_container" "autoheal" {
  count       = var.autoheal_active
  name        = "${local.prefix}autoheal"
  image       = docker_image.autoheal[0].image_id
  restart     = "unless-stopped"
  memory      = 32
  memory_swap = (32) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean
  env = [
    "AUTOHEAL_CONTAINER_LABEL=all",
    "AUTOHEAL_INTERVAL=10",
    "AUTOHEAL_START_PERIOD=60",
  ]
  volumes {
    host_path      = "/var/run/docker.sock"
    container_path = "/var/run/docker.sock"
  }
  networks_advanced {
    name = docker_network.spark.name
  }
}

# MCP server over the lake (Streamable HTTP). Read-only by default: with mcp_edit_access = 0 it gets the MinIO user that can
# only read the bucket; with 1 the user that can also write and delete. Both users are created by scripts/init-minio.sh.
resource "docker_container" "mcp" {
  count       = var.mcp_active
  name        = "${local.prefix}mcp"
  image       = docker_image.mcp[0].image_id
  restart     = "unless-stopped"
  memory      = var.mcp_memory_mb
  memory_swap = (var.mcp_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean
  read_only   = true
  tmpfs       = { "/tmp" = "size=256m" } # DuckDB spill
  env = sensitive([
    "MCP_EDIT_ACCESS=${var.mcp_edit_access}",
    "MCP_S3_USER=${var.mcp_edit_access == 1 ? var.mcp_editor_user : var.mcp_reader_user}",
    "MCP_S3_PASSWORD=${var.mcp_edit_access == 1 ? random_password.mcp_editor.result : var.mcp_reader_password}",
    "S3_BUCKET=${var.minio_bucket}",
    "S3_ENDPOINT=http://minio:9000",
    "QUOTA_GIB=${var.minio_quota_gib}",
  ])

  ports {
    internal = 8765
    external = local.port.mcp
    ip       = "127.0.0.1"
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["mcp"]
  }
  healthcheck {
    test         = ["CMD", "python", "-c", "import socket; socket.create_connection(('127.0.0.1', 8765), 3)"]
    interval     = "15s"
    timeout      = "5s"
    retries      = 3
    start_period = "30s"
  }
  depends_on = [null_resource.minio_init] # the MCP users must exist
}
