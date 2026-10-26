locals {
  repo_dir       = coalesce(var.repo_dir, abspath("${path.module}/.."))
  minio_data_dir = coalesce(var.minio_data_dir, "${local.repo_dir}/minio-data")

  # KEY=value files (comments and blank lines ignored)
  minio_env = sensitive({
    for l in split("\n", file(coalesce(var.minio_env_file, "${local.repo_dir}/minio.env"))) :
    trimspace(split("=", l)[0]) => trimspace(substr(l, length(split("=", l)[0]) + 1, -1))
    if strcontains(l, "=") && !startswith(trimspace(l), "#")
  })
  s3_env = sensitive({
    for l in split("\n", file(coalesce(var.s3_env_file, "${local.repo_dir}/s3.env"))) :
    trimspace(split("=", l)[0]) => trimspace(substr(l, length(split("=", l)[0]) + 1, -1))
    if strcontains(l, "=") && !startswith(trimspace(l), "#")
  })

  # "spark" keeps the historical names (minio, spark-master, ...); anything else is prefixed
  prefix  = var.project == "spark" ? "" : "${var.project}-"
  network = "${var.project}_spark"

  # host ports (+ offset)
  port = { for k, v in {
    minio_api = 9000, minio_console = 9001, master_ui = 8090, master = 7077, worker_ui = 8091,
    history   = 18080, client_ui = 4040, jupyter = 8888, jupyter_ui = 4041, mcp = 8765
  } : k => v + var.host_port_offset }

  # same probe as before: healthy when GET <path> on the port answers 2xx/3xx (bash only; the Spark image may lack curl)
  probe = { for k, v in {
    master  = { port = 8080, path = "/" }
    history = { port = 18080, path = "/" }
    jupyter = { port = 8888, path = "/api" }
  } : k => ["CMD", "bash", "-c", format("exec 3<>/dev/tcp/127.0.0.1/%d && printf 'GET %s HTTP/1.0\\r\\n\\r\\n' >&3 && head -1 <&3 | grep -qE ' (2|3)[0-9][0-9] '", v.port, v.path)] }

  spark_conf = templatefile("${path.module}/templates/spark-defaults.conf.tftpl", {
    executor_memory_mb = var.executor_memory_mb
    worker_cores       = var.worker_cores
    cores_max          = var.worker_count * var.worker_cores # all workers' cores: adding a worker adds capacity
    driver_memory_mb   = var.driver_memory_mb
    shuffle_partitions = var.shuffle_partitions
  })

  s3_env_list = [for k, v in local.s3_env : "${k}=${v}"]
}

resource "docker_network" "spark" {
  name = local.network
}

# ---- images ---------------------------------------------------------------------------------------------------------
resource "docker_image" "spark" {
  name         = "spark-study:3.5.9"
  keep_locally = true
  build {
    context    = local.repo_dir
    dockerfile = "Dockerfile"
    tag        = ["spark-study:3.5.9"]
  }
  triggers = { dockerfile = filesha1("${local.repo_dir}/Dockerfile") }
}

resource "docker_image" "jupyter" {
  name         = "spark-study-jupyter:3.5.9"
  keep_locally = true
  build {
    context    = local.repo_dir
    dockerfile = "Dockerfile.jupyter"
    tag        = ["spark-study-jupyter:3.5.9"]
  }
  triggers   = { dockerfile = filesha1("${local.repo_dir}/Dockerfile.jupyter") }
  depends_on = [docker_image.spark] # FROM spark-study:3.5.9
}

resource "docker_image" "minio" {
  name         = "cgr.dev/chainguard/minio:latest"
  keep_locally = true
}

resource "docker_image" "autoheal" {
  count        = var.autoheal_active
  name         = "willfarrell/autoheal:1.2.0"
  keep_locally = true
}

resource "docker_image" "mcp" {
  count        = var.mcp_active
  name         = "spark-study-mcp:1"
  keep_locally = true
  build {
    context    = "${local.repo_dir}/mcp"
    dockerfile = "Dockerfile"
    tag        = ["spark-study-mcp:1"]
  }
  triggers = {
    dockerfile = filesha1("${local.repo_dir}/mcp/Dockerfile")
    server     = filesha1("${local.repo_dir}/mcp/server.py")
  }
}
