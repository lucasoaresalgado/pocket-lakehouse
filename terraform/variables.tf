# Every tunable of the stack. Override in terraform.tfvars (copy terraform.tfvars.example) or with TF_VAR_<name>.

# ---- optional services (0 / 1) ------------------------------------------------------------------------------------
variable "autoheal_active" {
  description = "1 deploys autoheal (restarts unhealthy containers). It mounts the Docker socket, which is root-equivalent on the host, so it is opt-in."
  type        = number
  default     = 0
  validation {
    condition     = contains([0, 1], var.autoheal_active)
    error_message = "autoheal_active must be 0 or 1."
  }
}

variable "mcp_active" {
  description = "1 deploys the MCP server (Streamable HTTP at http://127.0.0.1:<mcp port>/mcp)."
  type        = number
  default     = 0
  validation {
    condition     = contains([0, 1], var.mcp_active)
    error_message = "mcp_active must be 0 or 1."
  }
}

variable "mcp_edit_access" {
  description = "0 = read-only (SELECT only, MinIO user mcp-reader); 1 = may also write and delete in the bucket (MinIO user mcp-editor). Only matters when mcp_active = 1."
  type        = number
  default     = 0
  validation {
    condition     = contains([0, 1], var.mcp_edit_access)
    error_message = "mcp_edit_access must be 0 or 1."
  }
}

# ---- storage (MinIO) ----------------------------------------------------------------------------------------------
variable "minio_quota_gib" {
  description = "Hard quota of the bucket, in GiB. Also given to the pipelines' size guards (QUOTA_GIB), so they always agree."
  type        = number
  default     = 5
  validation {
    condition     = var.minio_quota_gib > 0
    error_message = "minio_quota_gib must be positive."
  }
}

variable "minio_bucket" {
  type    = string
  default = "lake"
}

variable "minio_memory_mb" {
  type    = number
  default = 512
}

# ---- Spark cluster ------------------------------------------------------------------------------------------------
variable "worker_count" {
  description = "Number of Spark workers. Each runs one executor that uses all of the worker's cores."
  type        = number
  default     = 2
  validation {
    condition     = var.worker_count >= 1
    error_message = "worker_count must be at least 1."
  }
}

variable "worker_cores" {
  description = "Cores per worker (and per executor)."
  type        = number
  default     = 2
  validation {
    condition     = var.worker_cores >= 1
    error_message = "worker_cores must be at least 1."
  }
}

variable "worker_memory_mb" {
  description = "Memory a worker offers to executors (SPARK_WORKER_MEMORY). The container limit is this plus worker_overhead_mb."
  type        = number
  default     = 2048
}

variable "worker_overhead_mb" {
  description = "Extra container memory above worker_memory_mb (worker daemon, JVM overhead)."
  type        = number
  default     = 512
}

variable "executor_memory_mb" {
  description = "Memory of each executor. Must fit in worker_memory_mb."
  type        = number
  default     = 1536
  validation {
    condition     = var.executor_memory_mb <= var.worker_memory_mb
    error_message = "executor_memory_mb must not exceed worker_memory_mb (the executor would never be scheduled)."
  }
}

variable "driver_memory_mb" {
  type    = number
  default = 1024
}

variable "shuffle_partitions" {
  type    = number
  default = 8
}

variable "master_memory_mb" {
  type    = number
  default = 512
}

variable "history_memory_mb" {
  type    = number
  default = 640
}

variable "client_memory_mb" {
  description = "spark-client: the container for spark-submit (driver in client mode)."
  type        = number
  default     = 1536
}

variable "jupyter_memory_mb" {
  type    = number
  default = 2048
}

variable "mcp_memory_mb" {
  type    = number
  default = 768
}

# ---- credentials of the MCP users ----------------------------------------------------------------------------------
variable "mcp_reader_user" {
  type    = string
  default = "mcp-reader"
}

variable "mcp_reader_password" {
  description = "Default password of the read-only MCP user (it can only list and read the bucket). Change it if the host is shared."
  type        = string
  default     = "mcp-readonly-default"
  sensitive   = true
}

variable "mcp_editor_user" {
  type    = string
  default = "mcp-editor"
}
# the editor's password is generated (random_password) and kept in the state

# ---- placement ----------------------------------------------------------------------------------------------------
variable "project" {
  description = "Name of the Docker network (<project>_spark). With the default, containers keep their usual names (minio, spark-master, ...); any other value prefixes them with '<project>-' so a second copy can run beside the first."
  type        = string
  default     = "spark"
}

variable "host_port_offset" {
  description = "Added to every published host port, to run a second copy next to the first."
  type        = number
  default     = 0
}

variable "repo_dir" {
  description = "Repository root (Dockerfiles, notebooks/, data/, apps/ ...)."
  type        = string
  default     = null
}

variable "minio_data_dir" {
  description = "Host folder where MinIO keeps its objects. Default: <repo>/minio-data."
  type        = string
  default     = null
}

variable "minio_env_file" {
  description = "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (git-ignored; copy minio.env.example)."
  type        = string
  default     = null
}

variable "s3_env_file" {
  description = "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (+ optional CONTACT_EMAIL) given to Spark and Jupyter (git-ignored; copy s3.env.example)."
  type        = string
  default     = null
}

variable "docker_host" {
  type    = string
  default = "unix:///var/run/docker.sock"
}
