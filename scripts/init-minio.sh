#!/usr/bin/env bash
# Creates the bucket and applies the hard quota (idempotent). Usage: ./scripts/init-minio.sh   (BUCKET=lake QUOTA=5GiB by default)
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; . ./minio.env; set +a
BUCKET="${BUCKET:-lake}"; QUOTA="${QUOTA:-5GiB}"
mc() { docker run --rm --network spark_spark -e "MC_HOST_local=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000" cgr.dev/chainguard/minio-client:latest "$@"; }
mc mb --ignore-existing "local/${BUCKET}"
mc quota set "local/${BUCKET}" --size "${QUOTA}"
mc quota info "local/${BUCKET}"
