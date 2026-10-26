# S3-compatible lake. The hard disk limit is the bucket quota (applied by scripts/init-minio.sh below).
resource "docker_container" "minio" {
  name         = "${local.prefix}minio"
  image        = docker_image.minio.image_id
  command      = ["server", "/data", "--console-address", ":9001"]
  env          = sensitive([for k, v in local.minio_env : "${k}=${v}"])
  user         = "65532:65532"
  restart      = "unless-stopped"
  memory       = var.minio_memory_mb
  memory_swap  = (var.minio_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean
  wait         = true                      # apply returns when the healthcheck passes (what depends_on: service_healthy did)
  wait_timeout = 120

  ports {
    internal = 9000
    external = local.port.minio_api
    ip       = "127.0.0.1"
  }
  ports {
    internal = 9001
    external = local.port.minio_console
    ip       = "127.0.0.1"
  }
  volumes {
    host_path      = local.minio_data_dir
    container_path = "/data"
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["minio"]
  }
  healthcheck {
    test     = ["CMD", "mc", "ready", "local"]
    interval = "5s"
    timeout  = "3s"
    retries  = 12
  }
}

# MCP users. The reader's password is a variable (default committed on purpose: it can only read); the editor's is random.
resource "random_password" "mcp_editor" {
  length  = 24
  special = false
}

# Bucket, quota and the two MCP users: one-shot `mc` run (the Chainguard MinIO image has no shell). Re-runs when a value
# it applies changes; every step is idempotent.
resource "null_resource" "minio_init" {
  triggers = {
    container = docker_container.minio.id
    quota     = var.minio_quota_gib
    bucket    = var.minio_bucket
    reader    = "${var.mcp_reader_user}:${sha1(var.mcp_reader_password)}"
    editor    = "${var.mcp_editor_user}:${sha1(random_password.mcp_editor.result)}"
    script    = filesha1("${local.repo_dir}/scripts/init-minio.sh")
  }
  provisioner "local-exec" {
    command = "${local.repo_dir}/scripts/init-minio.sh"
    environment = {
      NETWORK             = docker_network.spark.name
      MINIO_HOST          = docker_container.minio.name
      MINIO_ROOT_USER     = local.minio_env["MINIO_ROOT_USER"]
      MINIO_ROOT_PASSWORD = local.minio_env["MINIO_ROOT_PASSWORD"]
      BUCKET              = var.minio_bucket
      QUOTA               = "${var.minio_quota_gib}GiB"
      RO_USER             = var.mcp_reader_user
      RO_PASSWORD         = var.mcp_reader_password
      RW_USER             = var.mcp_editor_user
      RW_PASSWORD         = random_password.mcp_editor.result
    }
  }
}
