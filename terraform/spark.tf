locals {
  spark_common_env = local.s3_env_list
  spark_mounts = {
    "${local.repo_dir}/data"       = "/data"
    "${local.repo_dir}/apps"       = "/apps"
    "${local.repo_dir}/event-logs" = "/spark-events"
  }
}

resource "docker_container" "master" {
  name        = "${local.prefix}spark-master"
  image       = docker_image.spark.image_id
  command     = ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
  env         = concat(sensitive(local.spark_common_env), ["SPARK_MASTER_HOST=spark-master", "SPARK_DAEMON_MEMORY=256m"])
  restart     = "unless-stopped"
  memory      = var.master_memory_mb
  memory_swap = (var.master_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean

  ports {
    internal = 8080
    external = local.port.master_ui
  }
  ports {
    internal = 7077
    external = local.port.master
  }
  dynamic "volumes" {
    for_each = local.spark_mounts
    content {
      host_path      = volumes.key
      container_path = volumes.value
    }
  }
  upload {
    file    = "/opt/spark/conf/spark-defaults.conf"
    content = local.spark_conf
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["spark-master"]
  }
  healthcheck {
    test         = local.probe.master
    interval     = "15s"
    timeout      = "5s"
    retries      = 3
    start_period = "30s"
  }
}

# worker_count workers, each one executor with all of its cores
resource "docker_container" "worker" {
  count   = var.worker_count
  name    = "${local.prefix}spark-worker-${count.index + 1}"
  image   = docker_image.spark.image_id
  command = ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
  env = concat(sensitive(local.spark_common_env), [
    "SPARK_WORKER_CORES=${var.worker_cores}",
    "SPARK_WORKER_MEMORY=${var.worker_memory_mb}m",
    "SPARK_DAEMON_MEMORY=256m",
    "SPARK_WORKER_WEBUI_PORT=${8081 + count.index}",
  ])
  restart     = "unless-stopped"
  memory      = var.worker_memory_mb + var.worker_overhead_mb
  memory_swap = (var.worker_memory_mb + var.worker_overhead_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean

  ports {
    internal = 8081 + count.index
    external = local.port.worker_ui + count.index
  }
  dynamic "volumes" {
    for_each = local.spark_mounts
    content {
      host_path      = volumes.key
      container_path = volumes.value
    }
  }
  upload {
    file    = "/opt/spark/conf/spark-defaults.conf"
    content = local.spark_conf
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["spark-worker-${count.index + 1}"]
  }
  healthcheck {
    test         = ["CMD", "bash", "-c", format("exec 3<>/dev/tcp/127.0.0.1/%d && printf 'GET / HTTP/1.0\\r\\n\\r\\n' >&3 && head -1 <&3 | grep -qE ' (2|3)[0-9][0-9] '", 8081 + count.index)]
    interval     = "15s"
    timeout      = "5s"
    retries      = 3
    start_period = "30s"
  }
  depends_on = [docker_container.master]
}

resource "docker_container" "history" {
  name        = "${local.prefix}spark-history"
  image       = docker_image.spark.image_id
  command     = ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.history.HistoryServer"]
  env         = concat(sensitive(local.spark_common_env), ["SPARK_DAEMON_MEMORY=384m"])
  restart     = "unless-stopped"
  memory      = var.history_memory_mb
  memory_swap = (var.history_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean

  ports {
    internal = 18080
    external = local.port.history
  }
  dynamic "volumes" {
    for_each = local.spark_mounts
    content {
      host_path      = volumes.key
      container_path = volumes.value
    }
  }
  upload {
    file    = "/opt/spark/conf/spark-defaults.conf"
    content = local.spark_conf
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["spark-history"]
  }
  healthcheck {
    test         = local.probe.history
    interval     = "15s"
    timeout      = "5s"
    retries      = 3
    start_period = "30s"
  }
}

# Driver / submit container: run your extraction jobs from here (client mode).
resource "docker_container" "client" {
  name        = "${local.prefix}spark-client"
  image       = docker_image.spark.image_id
  command     = ["sleep", "infinity"]
  env         = concat(sensitive(local.spark_common_env), ["SPARK_LOCAL_HOSTNAME=spark-client"])
  restart     = "unless-stopped"
  memory      = var.client_memory_mb
  memory_swap = (var.client_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean

  ports {
    internal = 4040
    external = local.port.client_ui
  }
  dynamic "volumes" {
    for_each = local.spark_mounts
    content {
      host_path      = volumes.key
      container_path = volumes.value
    }
  }
  upload {
    file    = "/opt/spark/conf/spark-defaults.conf"
    content = local.spark_conf
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["spark-client"]
  }
  depends_on = [docker_container.master]
}

# JupyterLab with PySpark (client mode). Limited to 2 cores by default so it can coexist with spark-client jobs.
resource "docker_container" "jupyter" {
  name    = "${local.prefix}spark-jupyter"
  image   = docker_image.jupyter.image_id
  command = ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.token=", "--ServerApp.password="]
  env = concat(sensitive(local.s3_env_list), [
    "SPARK_LOCAL_HOSTNAME=spark-jupyter",
    "STORAGE=s3", # pipeline tables go to MinIO; local-disk storage is disabled
    "S3_BUCKET=${var.minio_bucket}",
    "S3_ENDPOINT=http://minio:9000",
    "QUOTA_GIB=${var.minio_quota_gib}",
  ])
  restart     = "unless-stopped"
  memory      = var.jupyter_memory_mb
  memory_swap = (var.jupyter_memory_mb) * 2 # what Docker applies by default (compose's mem_limit did too); explicit so plans stay clean

  ports {
    internal = 8888
    external = local.port.jupyter
    ip       = "127.0.0.1"
  }
  ports {
    internal = 4040
    external = local.port.jupyter_ui
    ip       = "127.0.0.1"
  }
  dynamic "volumes" {
    for_each = merge(local.spark_mounts, { "${local.repo_dir}/notebooks" = "/notebooks" })
    content {
      host_path      = volumes.key
      container_path = volumes.value
    }
  }
  upload {
    file    = "/opt/spark/conf/spark-defaults.conf"
    content = local.spark_conf
  }
  networks_advanced {
    name    = docker_network.spark.name
    aliases = ["spark-jupyter"]
  }
  healthcheck {
    test         = local.probe.jupyter
    interval     = "15s"
    timeout      = "5s"
    retries      = 3
    start_period = "30s"
  }
  # the bucket must exist (and the master be up) before notebooks start
  depends_on = [docker_container.master, null_resource.minio_init]
}
