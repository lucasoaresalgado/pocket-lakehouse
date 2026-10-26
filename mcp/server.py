"""MCP server over the lake (Streamable HTTP). Read-only unless MCP_EDIT_ACCESS=1.

Read-only is enforced twice: here (SELECT statements only) and in MinIO (the credentials in mcp-access-0.env can only
read the bucket). DuckDB is also locked to s3://<bucket>/, so a query cannot read the container's own files.
"""
import os
import re
import threading

import boto3
import duckdb
from duckdb import StatementType
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

BUCKET = os.environ.get("S3_BUCKET", "lake")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
USER, PASSWORD = os.environ["MCP_S3_USER"], os.environ["MCP_S3_PASSWORD"]
EDIT = os.environ.get("MCP_EDIT_ACCESS", "0") == "1"
QUOTA_GIB = float(os.environ.get("QUOTA_GIB", "5"))
ROW_LIMIT = int(os.environ.get("MCP_ROW_LIMIT", "1000"))
TIMEOUT_S = int(os.environ.get("MCP_TIMEOUT_S", "60"))
DEFAULT_DB = "estudo"   # the folder (prefix) the pipelines write to
NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")

mcp = FastMCP(
    "pocket-lakehouse" + (" (edit)" if EDIT else " (read-only)"),
    host="0.0.0.0", port=int(os.environ.get("MCP_PORT", "8765")),
    # only the localhost-published port is meant to be used; blocks DNS-rebinding from a browser
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=True,
                                                 allowed_hosts=["localhost:*", "127.0.0.1:*"],
                                                 allowed_origins=["http://localhost:*", "http://127.0.0.1:*"]),
)

s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=USER, aws_secret_access_key=PASSWORD,
                  region_name="us-east-1")

_db = duckdb.connect(":memory:")
_db.execute("SET memory_limit='512MB'; SET threads=2; SET autoinstall_known_extensions=false")
_db.execute("CREATE SECRET lake (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', URL_STYLE 'path', USE_SSL false)"
            % (USER, PASSWORD, ENDPOINT.split("://", 1)[1]))
_db.execute("SET allowed_directories=['s3://%s/']" % BUCKET)
_db.execute("SET enable_external_access=false")
_db.execute("SET lock_configuration=true")


def _name(v: str, what: str) -> str:
    if not NAME.match(v) or ".." in v:
        raise ValueError("invalid %s: %r" % (what, v))
    return v


def _jsonable(v):
    return v if v is None or isinstance(v, (bool, int, float, str)) else str(v)


def _run(sql: str, limit: int) -> dict:
    stmts = duckdb.extract_statements(sql)
    if not stmts:
        raise ValueError("empty statement")
    if not EDIT and any(s.type != StatementType.SELECT for s in stmts):
        raise PermissionError("read-only mode: only SELECT / DESCRIBE queries are allowed (MCP_EDIT_ACCESS=0)")
    cur = _db.cursor()
    timer = threading.Timer(TIMEOUT_S, cur.interrupt)
    timer.start()
    try:
        cur.execute(sql)
        if cur.description is None:
            return {"ok": True}
        limit = max(1, min(limit, ROW_LIMIT))
        rows = cur.fetchmany(limit + 1)
        return {"columns": [d[0] for d in cur.description],
                "rows": [[_jsonable(x) for x in r] for r in rows[:limit]],
                "truncated": len(rows) > limit}
    except duckdb.InterruptException:
        raise TimeoutError("query stopped after %d s" % TIMEOUT_S)
    finally:
        timer.cancel()
        cur.close()


def _parquet(db: str, table: str) -> str:
    return "read_parquet('s3://%s/%s/%s/**/*.parquet', hive_partitioning=true)" % (BUCKET, _name(db, "db"), _name(table, "table"))


@mcp.tool()
def list_tables(db: str = DEFAULT_DB) -> list:
    """Tables (folders of Parquet files) under a lake prefix. 'estudo' is where the pipelines write."""
    out, token = [], None
    while True:
        kw = {"ContinuationToken": token} if token else {}
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix=_name(db, "db") + "/", Delimiter="/", **kw)
        out += [p["Prefix"].split("/")[-2] for p in r.get("CommonPrefixes", [])]
        token = r.get("NextContinuationToken")
        if not token:
            break
    return sorted(t for t in out if t[0] not in "._" and "__" not in t)


@mcp.tool()
def describe_table(table: str, db: str = DEFAULT_DB) -> dict:
    """Column names and types of a table."""
    return _run("DESCRIBE SELECT * FROM " + _parquet(db, table), 1000)


@mcp.tool()
def query(sql: str, limit: int = 100) -> dict:
    """Run DuckDB SQL. Read a table with: SELECT ... FROM read_parquet('s3://lake/estudo/<table>/**/*.parquet', hive_partitioning=true).
    Rows are capped (limit, at most MCP_ROW_LIMIT) and the query is stopped after MCP_TIMEOUT_S seconds.""" + (
        " Edit mode: any statement is allowed (e.g. COPY ... TO 's3://lake/...')." if EDIT else " Read-only: SELECT / DESCRIBE only.")
    return _run(sql, limit)


@mcp.tool()
def lake_usage() -> dict:
    """Bytes stored in the bucket, per top-level prefix, against the bucket quota (QUOTA_GIB)."""
    per, token = {}, None
    while True:
        kw = {"ContinuationToken": token} if token else {}
        r = s3.list_objects_v2(Bucket=BUCKET, **kw)
        for o in r.get("Contents", []):
            per[o["Key"].split("/", 1)[0]] = per.get(o["Key"].split("/", 1)[0], 0) + o["Size"]
        token = r.get("NextContinuationToken")
        if not token:
            break
    gib = 1 << 30
    return {"quota_gib": QUOTA_GIB, "used_gib": round(sum(per.values()) / gib, 3),
            "free_gib": round(QUOTA_GIB - sum(per.values()) / gib, 3),
            "per_prefix_gib": {k: round(v / gib, 3) for k, v in sorted(per.items())}}


if EDIT:
    @mcp.tool()
    def delete_table(table: str, db: str = DEFAULT_DB) -> dict:
        """Edit mode only: delete every object of a table. Irreversible; the pipeline's _ctl bookkeeping is not touched."""
        prefix = "%s/%s/" % (_name(db, "db"), _name(table, "table"))
        n, token = 0, None
        while True:
            kw = {"ContinuationToken": token} if token else {}
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, **kw)
            keys = [{"Key": o["Key"]} for o in r.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys})
                n += len(keys)
            token = r.get("NextContinuationToken")
            if not token:
                return {"deleted_objects": n}


if __name__ == "__main__":
    print("MCP %s on :%s" % ("EDIT" if EDIT else "read-only", mcp.settings.port), flush=True)
    mcp.run(transport="streamable-http")
