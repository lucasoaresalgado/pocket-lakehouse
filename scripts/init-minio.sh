#!/usr/bin/env bash
# Creates the bucket, applies the hard quota and creates the MCP users (idempotent). Run by Terraform (null_resource.minio_init),
# which passes everything below as environment variables; it can also be run by hand with the same variables.
#   NETWORK MINIO_HOST MINIO_ROOT_USER MINIO_ROOT_PASSWORD   where MinIO is and how to log in
#   BUCKET QUOTA                                             e.g. lake, 5GiB
#   RO_USER RO_PASSWORD                                      read-only user (policy <bucket>-ro)
#   RW_USER RW_PASSWORD                                      read/write user (policy <bucket>-rw)
set -euo pipefail
: "${NETWORK:?}" "${MINIO_HOST:?}" "${MINIO_ROOT_USER:?}" "${MINIO_ROOT_PASSWORD:?}" "${BUCKET:?}" "${QUOTA:?}"
: "${RO_USER:?}" "${RO_PASSWORD:?}" "${RW_USER:?}" "${RW_PASSWORD:?}"
POLICIES="$(mktemp -d)"; trap 'rm -rf "$POLICIES"' EXIT; chmod a+rx "$POLICIES"
mc() { docker run --rm --network "$NETWORK" -v "${POLICIES}:/policies:ro" -e "MC_HOST_local=http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@${MINIO_HOST}:9000" cgr.dev/chainguard/minio-client:latest "$@"; }
mc mb --ignore-existing "local/${BUCKET}"
mc quota set "local/${BUCKET}" --size "${QUOTA}"
mc quota info "local/${BUCKET}"

# policies scoped to the one bucket (not the built-in readonly/readwrite, which cover every bucket)
policy() {  # name, actions
  printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":[%s],"Resource":["arn:aws:s3:::%s","arn:aws:s3:::%s/*"]}]}\n' "$2" "$BUCKET" "$BUCKET" > "${POLICIES}/$1.json"
  chmod a+r "${POLICIES}/$1.json"
  mc admin policy create local "$1" "/policies/$1.json"
}
mcp_user() {  # user, password, policy
  mc admin user add local "$1" "$2"
  mc admin policy attach local "$3" --user "$1" 2>&1 | grep -v "already" || true
  echo "MCP user ${1}: policy ${3}"
}
policy "${BUCKET}-ro" '"s3:GetObject","s3:ListBucket","s3:GetBucketLocation"'
policy "${BUCKET}-rw" '"s3:GetObject","s3:ListBucket","s3:GetBucketLocation","s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts","s3:ListBucketMultipartUploads"'
mcp_user "$RO_USER" "$RO_PASSWORD" "${BUCKET}-ro"
mcp_user "$RW_USER" "$RW_PASSWORD" "${BUCKET}-rw"
