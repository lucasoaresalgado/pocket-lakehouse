output "urls" {
  description = "Where to find things (host side)."
  value = {
    jupyter       = "http://localhost:${local.port.jupyter}"
    minio_console = "http://localhost:${local.port.minio_console}"
    spark_master  = "http://localhost:${local.port.master_ui}"
    spark_history = "http://localhost:${local.port.history}"
    mcp           = var.mcp_active == 1 ? "http://127.0.0.1:${local.port.mcp}/mcp" : "(off: mcp_active = 0)"
  }
}

output "cluster" {
  description = "Capacity the tunables add up to."
  value = {
    workers     = var.worker_count
    cores_total = var.worker_count * var.worker_cores
    executor_mb = var.executor_memory_mb
    quota_gib   = var.minio_quota_gib
    mcp         = var.mcp_active == 1 ? (var.mcp_edit_access == 1 ? "edit" : "read-only") : "off"
    autoheal    = var.autoheal_active == 1 ? "on" : "off"
  }
}
