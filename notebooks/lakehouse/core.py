# Generic building blocks shared by the clima and empresas categories.
# Same conventions as mineracao/pipelines/common.py (which predates this module and keeps its own copy):
# tables are Parquet in MinIO (s3a://lake/<target_db>/<table>), two-phase writes (stage → commit), run bookkeeping as
# JSON under <target_db>/_ctl/, polite HTTP (one request at a time, 403/429 = stop).

import hashlib
import itertools
import json
import os
import re
import shutil
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
import zlib
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

os.umask(0) # this and fs.permissions.umask-mode=000 -> Everything recorded gets open access permission, so one container doesn't generate files another can't erase.

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
STORAGE = os.environ.get("STORAGE", "s3")        # always MinIO ("s3"); "file" exists only for the offline tests
if STORAGE != "s3" and os.environ.get("ALLOW_LOCAL_STORAGE") != "1":
    raise RuntimeError("Tables are stored in MinIO only: STORAGE must be 's3' (local-disk mode is reserved for the offline tests).")
S3_BUCKET = os.environ.get("S3_BUCKET", "lake")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
QUOTA_GIB = float(os.environ.get("QUOTA_GIB", "5"))  # hard quota of the bucket (scripts/init-minio.sh); used by size guards
RAW_DIR = os.path.join(DATA_ROOT, "raw")
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "")
USER_AGENT = "pocket-lakehouse/1.0" + (" (%s)" % CONTACT_EMAIL if CONTACT_EMAIL else "")
HTTP_TIMEOUT_S = 900
PAUSA_S = float(os.environ.get("PAUSA_S", "3"))  # pause between requests to the same publisher
KEEP_RAW = os.environ.get("KEEP_RAW") == "1"


def get_spark(app_name: str, cores_max: int = 2, master: str = "spark://spark-master:7077") -> SparkSession:
    """Session on the study cluster. 2 cores by default so spark-client jobs can still run next to a notebook."""
    return (SparkSession.builder.appName(app_name).master(master)
            .config("spark.cores.max", cores_max)
            .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
            .getOrCreate())


# ---------------------------------------------------------------------------
# Datas
# ---------------------------------------------------------------------------

def today_local() -> str:
    """AAAA-MM-DD in America/Sao_Paulo (UTC-3, no DST since 2019)."""
    return datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def is_iso_date(s: str) -> bool:
    try:
        return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", s)) and date.fromisoformat(s).isoformat() == s
    except ValueError:
        return False


def check_date(name: str, v: str) -> None:
    if v and not is_iso_date(v):
        raise ValueError('%s inválido: "%s" (use AAAA-MM-DD)' % (name, v))


def add_days(d: str, n: int) -> str:
    return (date.fromisoformat(d) + timedelta(days=n)).isoformat()


def days_between(start: str, end: str) -> List[str]:
    a, b = date.fromisoformat(start), date.fromisoformat(end)
    return [(a + timedelta(days=i)).isoformat() for i in range((b - a).days + 1)]


def is_ym(s: str) -> bool:
    return bool(re.match(r"^\d{4}-(0[1-9]|1[0-2])$", s))


def ym_range(start: str, end: str) -> List[str]:
    """['2026-01', ..., '2026-03'] (inclusive)"""
    i, j = int(start[:4]) * 12 + int(start[5:7]) - 1, int(end[:4]) * 12 + int(end[5:7]) - 1
    return ["%d-%02d" % (k // 12, k % 12 + 1) for k in range(i, j + 1)]


# ---------------------------------------------------------------------------
# Texto (driver side and Spark side — only native Spark functions, no Python UDFs)
# ---------------------------------------------------------------------------

def strip_accents(s) -> str:
    s = unicodedata.normalize("NFD", "" if s is None else str(s))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn")


def norm(s) -> str:
    """maiúsculas, sem acento, só letras/números separados por um espaço"""
    return re.sub(r"[^A-Z0-9]+", " ", strip_accents(s).upper()).strip()


def snake(name: str) -> str:
    """header → ASCII snake_case column name ('Competência Mov' → 'competencia_mov')"""
    return re.sub(r"[^\w]+", "_", strip_accents(name).strip().lower()).strip("_")


_ACC_FROM = "áàâãäåéèêëíìîïóòôõöúùûüçñýÿÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑÝ"
_ACC_TO = "aaaaaaeeeeiiiiooooouuuucnyyAAAAAAEEEEIIIIOOOOOUUUUCNY"


def s_(c) -> Column:
    """string column, trimmed, null → ''"""
    c = F.col(c) if isinstance(c, str) else c
    return F.trim(F.coalesce(c.cast("string"), F.lit("")))


def nz(c) -> Column:
    """trimmed string column, '' → NULL"""
    t = s_(c)
    return F.when(t != "", t)


def norm_col(c) -> Column:
    """Spark twin of norm()"""
    c = F.col(c) if isinstance(c, str) else c
    x = F.upper(F.translate(F.coalesce(c.cast("string"), F.lit("")), _ACC_FROM, _ACC_TO))
    return F.trim(F.regexp_replace(x, r"[^A-Z0-9]+", " "))


def mask_cpf_col(c) -> Column:
    """LGPD: an open CPF (11 digits, bare or as 000.000.000-00) becomes the Receita mask; anything else passes through.
    '07935099120' → '***350991**'; '079.350.991-20' → '***.350.991-**' (the format CEAF and the Receita publish)."""
    c = F.col(c) if isinstance(c, str) else c
    t = F.trim(c)
    return (F.when(t.rlike(r"^\d{11}$"), F.concat(F.lit("***"), F.substring(t, 4, 6), F.lit("**")))
            .when(t.rlike(r"^\d{3}\.\d{3}\.\d{3}-\d{2}$"), F.concat(F.lit("***"), F.substring(t, 4, 8), F.lit("-**")))
            .otherwise(c))


def conform(df: DataFrame, spec: Sequence[Tuple[str, str]]) -> DataFrame:
    """select/cast the columns of the target table; '' → NULL; adds loaded_at when the spec has it"""
    out = df
    if any(n == "loaded_at" for n, _ in spec) and "loaded_at" not in df.columns:
        out = out.withColumn("loaded_at", F.current_timestamp())
    cols = []
    for name, typ in spec:
        c = F.col(name)
        if typ == "string":
            c = F.when(c.cast("string") == "", F.lit(None)).otherwise(c.cast("string"))
        else:
            c = c.cast(typ)
        cols.append(c.alias(name))
    return out.select(*cols)


def persist_disk(df: DataFrame) -> DataFrame:
    return df.persist(StorageLevel.DISK_ONLY)


# ---------------------------------------------------------------------------
# CSV / ZIP: header on the driver, distributed read in Spark
# ---------------------------------------------------------------------------

def detect_encoding(path: str, max_bytes: int = 64 << 20) -> str:
    """UTF-8 if the first max_bytes decode as UTF-8, else windows-1252 (a superset of latin-1)"""
    import codecs
    dec = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as f:
        try:
            n = 0
            while n < max_bytes:
                chunk = f.read(8 << 20)
                if not chunk:
                    dec.decode(b"", final=True)
                    break
                dec.decode(chunk)
                n += len(chunk)
            return "UTF-8"
        except UnicodeDecodeError:
            return "windows-1252"


def first_line(path: str, enc: str) -> str:
    with open(path, "rb") as f:
        raw = f.readline()
    s = raw.decode("utf-8" if enc.upper() == "UTF-8" else enc, "replace").rstrip("\r\n")
    return s[1:] if s.startswith("\ufeff") else s


def split_csv(line: str, d: str) -> List[str]:
    """one CSV line → fields (handles "quoted;fields" and doubled quotes)"""
    if '"' not in line:
        return [x.strip() for x in line.split(d)]
    out, cur, inq, i = [], "", False, 0
    while i < len(line):
        ch = line[i]
        if inq:
            if ch == '"':
                if i + 1 < len(line) and line[i + 1] == '"':
                    cur += '"'
                    i += 1
                else:
                    inq = False
            else:
                cur += ch
        elif ch == '"':
            inq = True
        elif ch == d:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
        i += 1
    out.append(cur.strip())
    return out


def read_csv(spark: SparkSession, paths, sep: str, enc: str, header: bool = True, multiline: bool = False) -> DataFrame:
    """all-string DataFrame with positional columns c0..cN (duplicated/odd header names cannot clash)"""
    paths = [paths] if isinstance(paths, str) else list(paths)
    df = (spark.read.option("header", header).option("sep", sep).option("encoding", enc)
          .option("quote", '"').option("escape", '"').option("multiLine", multiline)
          .option("mode", "PERMISSIVE")
          .csv(["file://" + p for p in paths]))
    return df.toDF(*["c%d" % i for i in range(len(df.columns))])


def unzip(zip_path: str, dest_dir: str, members: Optional[Iterable[str]] = None) -> List[str]:
    """extracts (all or some) members flat into dest_dir; returns the extracted paths"""
    os.makedirs(dest_dir, exist_ok=True)
    out = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in (list(members) if members is not None else zf.namelist()):
            if name.endswith("/"):
                continue
            dest = os.path.join(dest_dir, os.path.basename(name))
            with zf.open(name) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst, 8 << 20)
            out.append(dest)
    return out


# ---------------------------------------------------------------------------
# HTTP com boa conduta: 1 requisição por vez, UA com contato, 403/429 = parar
# ---------------------------------------------------------------------------

class FatalHttp(Exception):
    def __init__(self, msg: str, code: int = 0):
        super().__init__(msg)
        self.code = code


class _GzipResponse:
    """urllib never decompresses; some servers send gzip even when it was not asked for. Streams the plain bytes."""

    def __init__(self, resp):
        self._r = resp
        self._d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        self._buf = b""
        self._eof = False

    def __getattr__(self, name):
        return getattr(self._r, name)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self._r.close()

    def read(self, n: int = -1) -> bytes:
        while (n < 0 or len(self._buf) < n) and not self._eof:
            chunk = self._r.read(1 << 20)
            if not chunk:
                self._eof = True
                self._buf += self._d.flush()
                break
            self._buf += self._d.decompress(chunk)
        out, self._buf = (self._buf, b"") if n < 0 else (self._buf[:n], self._buf[n:])
        return out


def http_open(url: str, method: str = "GET", headers: Optional[dict] = None, data: Optional[bytes] = None,
              timeout: float = HTTP_TIMEOUT_S, retries: int = 3):
    """403/429 → FatalHttp at once (never insist against the server); other 4xx → FatalHttp; 5xx/network → retry"""
    hdrs = {"User-Agent": USER_AGENT, "Accept": "*/*", "Accept-Encoding": "identity"}
    hdrs.update(headers or {})
    for attempt in itertools.count(1):
        try:
            resp = urllib.request.urlopen(urllib.request.Request(url, method=method, headers=hdrs, data=data), timeout=timeout)
            return _GzipResponse(resp) if method != "HEAD" and (resp.headers.get("Content-Encoding") or "").lower() == "gzip" else resp
        except urllib.error.HTTPError as e:
            body = ""
            if method != "HEAD":
                try:
                    body = re.sub(r"\s+", " ", e.read(2000).decode("utf-8", "replace"))[:400]
                except Exception:
                    pass
            if e.code in (403, 429):
                raise FatalHttp("HTTP %d em %s — servidor recusou/limitou; job encerrado sem novas tentativas. %s" % (e.code, url, body), e.code)
            if e.code < 500:
                raise FatalHttp("HTTP %d em %s. %s" % (e.code, url, body), e.code)
            if attempt >= retries:
                raise RuntimeError("HTTP %d em %s após %d tentativas. %s" % (e.code, url, retries, body))
            time.sleep(15 * attempt)
        except (urllib.error.URLError, OSError) as e:
            if url.startswith("file://") and isinstance(getattr(e, "reason", e), FileNotFoundError):
                raise FatalHttp("HTTP 404 em %s (arquivo local inexistente)" % url, 404)  # local mirrors/fixtures behave like HTTP
            if attempt >= retries:
                raise RuntimeError("Falha de rede em %s: %s" % (url, e))
            print("[HTTP] rede falhou (%s); nova tentativa em %ds" % (e, 10 * attempt))
            time.sleep(10 * attempt)


def http_bytes(url: str, **kw) -> bytes:
    with http_open(url, **kw) as r:
        return r.read()


def http_text(url: str, enc: str = "utf-8", **kw) -> str:
    return http_bytes(url, **kw).decode(enc, "replace")


def http_json(url: str, params: Optional[dict] = None, **kw):
    if params:
        url += ("&" if "?" in url else "?") + urllib.parse.urlencode(params)
    return json.loads(http_text(url, **kw))


def head_info(url: str) -> dict:
    """ETag / Last-Modified / size. Servers that refuse HEAD (403) but serve GET get a 1-byte Range GET instead."""
    try:
        with http_open(url, "HEAD") as r:
            ln = r.headers.get("content-length")
            return {"url": url, "etag": r.headers.get("etag") or "", "last_modified": r.headers.get("last-modified") or "",
                    "tamanho": int(ln) if ln else None}
    except FatalHttp as e:
        if e.code != 403:
            raise
    time.sleep(PAUSA_S)
    with http_open(url, "GET", {"Range": "bytes=0-0"}) as r:
        m = re.search(r"/(\d+)$", r.headers.get("content-range") or "")
        ln = r.headers.get("content-length")
        return {"url": url, "etag": r.headers.get("etag") or "", "last_modified": r.headers.get("last-modified") or "",
                "tamanho": int(m.group(1)) if m else (int(ln) if ln and r.status != 206 else None)}


def download(url: str, fonte: str, name: Optional[str] = None, headers: Optional[dict] = None) -> Tuple[str, int]:
    """streams the file to RAW_DIR/<fonte>/ and returns (path, bytes)"""
    name = name or os.path.basename(urllib.parse.unquote(urllib.parse.urlparse(url).path)) or "download"
    dest = os.path.join(RAW_DIR, fonte, "%s_%s" % (hashlib.sha1(url.encode()).hexdigest()[:8], name))
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    n = 0
    with http_open(url, headers=headers) as r, open(dest + ".part", "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            n += len(chunk)
    os.replace(dest + ".part", dest)
    return dest, n


def cleanup(*paths: str) -> None:
    if KEEP_RAW:
        return
    for p in paths:
        if p and os.path.isdir(p):
            shutil.rmtree(p, ignore_errors=True)
        elif p and os.path.isfile(p):
            try:
                os.remove(p)
            except OSError:
                pass


def raw_dir(fonte: str, tag: str) -> str:
    return os.path.join(RAW_DIR, fonte, "%s_%d" % (tag, int(time.time() * 1000)))


# ---------------------------------------------------------------------------
# Destino (Parquet) — STORAGE=s3 → s3a://S3_BUCKET/<db>/<tabela>; STORAGE=file → DATA_ROOT/<db>/<tabela> (tests only).
# table_path() values are fsspec paths (no scheme); uri() converts to what Spark understands.
# ---------------------------------------------------------------------------

def _root() -> str:
    return S3_BUCKET if STORAGE == "s3" else DATA_ROOT


_FS = None


def fs():
    """fsspec filesystem of the active storage (listing cache off: Spark writes behind its back)"""
    global _FS
    if _FS is None:
        if STORAGE == "s3":
            import s3fs
            _FS = s3fs.S3FileSystem(client_kwargs={"endpoint_url": S3_ENDPOINT}, use_listings_cache=False, skip_instance_cache=True)
        else:
            import fsspec
            _FS = fsspec.filesystem("file")
    return _FS


def uri(path: str) -> str:
    return ("s3a://" if STORAGE == "s3" else "file://") + path


def table_path(db: str, table: str) -> str:
    return "%s/%s/%s" % (_root(), db, table)


def _rm(path: str) -> None:
    try:
        fs().rm(path, recursive=True)
    except FileNotFoundError:
        pass


def _parquet_files(path: str) -> List[str]:
    try:
        return [f for f in fs().find(path) if f.endswith(".parquet")]
    except FileNotFoundError:
        return []


def table_exists(db: str, table: str) -> bool:
    """also true for partitioned tables (Parquet files only inside col=value/ folders)"""
    return bool(_parquet_files(table_path(db, table)))


def partitions(db: str, table: str, col: str) -> List[str]:
    """values of the col=value/ folders of a partitioned table, sorted"""
    try:
        names = [p.rstrip("/").rsplit("/", 1)[-1] for p in fs().ls(table_path(db, table), detail=False)]
    except FileNotFoundError:
        return []
    return sorted(n.split("=", 1)[1] for n in names if n.startswith(col + "="))


def read_table_pd(db: str, table: str, columns: Optional[List[str]] = None, filters=None) -> pd.DataFrame:
    """Parquet table → pandas through our own fs() (pandas' cached s3fs instance goes stale after an upsert)"""
    import pyarrow.parquet as pq
    p = table_path(db, table)
    return pq.ParquetDataset(p, filesystem=fs(), filters=filters).read(columns=columns).to_pandas()


def stage(df: DataFrame, spark: SparkSession, db: str, table: str, append: bool = False) -> int:
    """writes the new rows next to the table (path.__stage) and returns how many rows the stage holds"""
    path = table_path(db, table) + ".__stage"
    if not append:
        _rm(path)
    try:
        df.write.mode("append" if append else "overwrite").parquet(uri(path))
    except Exception as e:
        _rm(path)  # a write stopped by the bucket quota must not leave a half stage eating the quota
        raise _quota_error(e, "stage de %s.%s" % (db, table))
    return spark.read.option("mergeSchema", "true").parquet(uri(path)).count()


def drop_stage(db: str, table: str) -> None:
    _rm(table_path(db, table) + ".__stage")


def commit(spark: SparkSession, db: str, table: str, pk: Optional[Sequence[str]] = None, snapshot: bool = False) -> None:
    """upsert by pk, or replace the whole table (snapshot); the live table is only touched after the stage is complete"""
    path = table_path(db, table)
    staged = path + ".__stage"
    if snapshot or not table_exists(db, table):
        merged = staged
    else:
        new = spark.read.option("mergeSchema", "true").parquet(uri(staged))
        old = spark.read.parquet(uri(path))
        merged = path + ".__merge"
        _rm(merged)
        # the upsert writes a full new copy next to the live table: both must fit under the quota for a moment
        need = table_bytes(db, table) + int(fs().du(staged))
        if not quota_check(need)["cabe"]:
            _rm(staged)
            raise RuntimeError("Upsert de %s.%s precisa de ~%.2f GiB temporários e não cabe na cota do bucket (%g GiB): "
                               "stage descartado, tabela intacta" % (db, table, need / (1 << 30), QUOTA_GIB))
        try:
            (old.join(new.select(*pk).distinct(), list(pk), "left_anti").unionByName(new, allowMissingColumns=True)
             .write.mode("overwrite").parquet(uri(merged)))
        except Exception as e:
            _rm(merged)
            _rm(staged)
            raise _quota_error(e, "upsert de %s.%s" % (db, table))
    _rm(path)
    fs().mv(merged, path, recursive=True)
    _rm(staged)


def load(spark: SparkSession, df: DataFrame, db: str, table: str, pk: Optional[Sequence[str]] = None, snapshot: bool = False) -> int:
    n = stage(df, spark, db, table)
    commit(spark, db, table, pk, snapshot)
    return n


def commit_partitions(spark: SparkSession, db: str, table: str, part_col: str, pk: Optional[Sequence[str]] = None) -> List[str]:
    """Partitioned table (folders part_col=value/). Only the partitions present in the stage are rewritten:
    pk=None replaces them whole; with pk the staged rows are upserted into them. Returns the partitions touched.
    Each partition swap is atomic on its own (one folder move); the table as a whole is not."""
    path = table_path(db, table)
    staged = path + ".__stage"
    new = spark.read.option("mergeSchema", "true").parquet(uri(staged))
    vals = sorted(str(r[0]) for r in new.select(part_col).distinct().collect())
    merged = path + ".__merge"
    _rm(merged)
    for v in vals:
        new_v = new.filter(F.col(part_col).cast("string") == v).drop(part_col)
        old_p = "%s/%s=%s" % (path, part_col, v)
        if pk and _parquet_files(old_p):
            old_v = spark.read.parquet(uri(old_p))
            new_v = old_v.join(new_v.select(*pk).distinct(), list(pk), "left_anti").unionByName(new_v, allowMissingColumns=True)
        try:
            new_v.write.mode("overwrite").parquet(uri("%s/%s=%s" % (merged, part_col, v)))
        except Exception as e:
            _rm(merged)
            _rm(staged)
            raise _quota_error(e, "partição %s=%s de %s.%s" % (part_col, v, db, table))
    for v in vals:
        _rm("%s/%s=%s" % (path, part_col, v))
        fs().makedirs(path, exist_ok=True)
        fs().mv("%s/%s=%s" % (merged, part_col, v), "%s/%s=%s" % (path, part_col, v), recursive=True)
    _rm(merged)
    _rm(staged)
    return vals


def bucket_usage_bytes() -> int:
    """bytes stored in the bucket (what the MinIO quota counts)"""
    try:
        return int(sum(fs().du(_root(), total=False).values())) if STORAGE == "s3" else 0
    except FileNotFoundError:
        return 0


def table_bytes(db: str, table: str) -> int:
    try:
        return int(fs().du(table_path(db, table)))
    except FileNotFoundError:
        return 0


def quota_check(estimate_bytes: int, replaces_bytes: int = 0) -> dict:
    """will a write of ~estimate_bytes fit under the bucket quota? (the old copy stays until the commit)"""
    used = bucket_usage_bytes()
    quota = int(QUOTA_GIB * (1 << 30))
    free = quota - used
    return {"quota_gib": QUOTA_GIB, "usado_gib": round(used / (1 << 30), 2), "livre_gib": round(free / (1 << 30), 2),
            "estimativa_gib": round(estimate_bytes / (1 << 30), 2), "cabe": STORAGE != "s3" or estimate_bytes < free,
            "liberado_depois_gib": round(replaces_bytes / (1 << 30), 2)}


def require_space(estimate_bytes: int, what: str, ignorar_cota: bool = False, hint: str = "") -> dict:
    """refuses (before anything is downloaded or written) a load estimated not to fit under the bucket quota.
    ignorar_cota=True skips the guard (MinIO still enforces the hard quota; the stage is then dropped and the table kept)."""
    q = quota_check(estimate_bytes)
    if not q["cabe"] and not ignorar_cota:
        raise RuntimeError("%s: estimativa de %.2f GiB, mas só há %.2f GiB livres na cota do bucket (%g GiB). %s"
                           "(ignorar_cota=True ignora a estimativa; QUOTA_GIB/init-minio.sh mudam a cota)"
                           % (what, q["estimativa_gib"], q["livre_gib"], QUOTA_GIB, (hint + " ") if hint else ""))
    return q


def _quota_error(e: Exception, what: str) -> Exception:
    msg = str(e)
    if re.search(r"quota", msg, re.I):
        return RuntimeError("Cota do bucket excedida durante %s — gravação desfeita (stage removido), tabela intacta. "
                            "Reduza a carga (filtros/janela menor) ou aumente a cota." % what)
    return e


# ---------------------------------------------------------------------------
# Controle (JSON) — run state and loaded-file fingerprints
# ---------------------------------------------------------------------------

class Ctl:
    """run state / cursor per source+mode (state file) and fingerprint of the files already loaded (file file)"""

    def __init__(self, db: str, state_table: str = "ctl_publico_extracao", file_table: str = "ctl_publico_arquivo"):
        self.dir = "%s/%s/_ctl" % (_root(), db)
        self.f_state = "%s/%s.json" % (self.dir, state_table)
        self.f_file = "%s/%s.json" % (self.dir, file_table)

    @staticmethod
    def _load(f: str) -> dict:
        try:
            with fs().open(f, "r") as fh:
                return json.load(fh)
        except FileNotFoundError:
            return {}

    @staticmethod
    def _save(f: str, d: dict) -> None:
        tmp = f + ".tmp" if STORAGE != "s3" else f  # an S3 PUT is already atomic; locally write-then-rename
        fs().makedirs(f.rsplit("/", 1)[0], exist_ok=True)
        with fs().open(tmp, "w") as fh:
            json.dump(d, fh, ensure_ascii=False, indent=1)
        if tmp != f:
            fs().mv(tmp, f)

    def read_state(self, fonte: str, modo: str) -> Optional[dict]:
        return self._load(self.f_state).get("%s|%s" % (fonte, modo))

    def write_state(self, fonte: str, modo: str, status: str, resumo: dict, **extra) -> None:
        d = self._load(self.f_state)
        row = {"fonte": fonte, "modo": modo, "ultimo_status": status,
               "ultimo_resultado": json.dumps(resumo, ensure_ascii=False, default=str)[:3900], "atualizado_em": utcnow_iso()}
        row.update(extra)
        d["%s|%s" % (fonte, modo)] = row
        self._save(self.f_state, d)

    def write_error(self, fonte: str, modo: str, e) -> None:
        """records the error without touching the cursor/window (a backfill resumes from the same point)"""
        try:
            d = self._load(self.f_state)
            k = "%s|%s" % (fonte, modo)
            row = d.get(k) or {"fonte": fonte, "modo": modo}
            row.update(ultimo_status="erro", ultimo_resultado=json.dumps({"erro": str(e)}, ensure_ascii=False)[:3900],
                       atualizado_em=utcnow_iso())
            d[k] = row
            self._save(self.f_state, d)
        except Exception:
            pass

    def read_file_info(self, fonte: str, url: str) -> Optional[dict]:
        return self._load(self.f_file).get("%s|%s" % (fonte, url))

    def write_file_info(self, fonte: str, fi: dict, **extra) -> None:
        d = self._load(self.f_file)
        row = {"fonte": fonte, "url": fi["url"], "etag": fi.get("etag", ""), "last_modified": fi.get("last_modified", ""),
               "tamanho": fi.get("tamanho"), "atualizado_em": utcnow_iso()}
        row.update(extra)
        d["%s|%s" % (fonte, fi["url"])] = row
        self._save(self.f_file, d)


def unchanged(prev: Optional[dict], fi: dict) -> bool:
    """true if the remote file is the one already loaded (same ETag, or same Last-Modified when there is no ETag)"""
    if not prev:
        return False
    if fi.get("etag") and prev.get("etag") and fi["etag"] == prev["etag"]:
        return True
    return bool(not fi.get("etag") and fi.get("last_modified") and prev.get("last_modified") and fi["last_modified"] == prev["last_modified"])
