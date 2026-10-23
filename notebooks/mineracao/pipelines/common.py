# Shared building blocks of the mining & oil public-data pipelines.

import hashlib
import itertools
import json
import os
import re
import shutil
import time
import unicodedata
import zlib
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

os.umask(0)  # files written from Python stay deletable from the WSL host (Spark side: umask-mode in spark-defaults)

DATA_ROOT = os.environ.get("DATA_ROOT", "/data")
STORAGE = os.environ.get("STORAGE", "s3")        # where the tables go: always MinIO ("s3"); "file" exists only for the offline tests
if STORAGE != "s3" and os.environ.get("ALLOW_LOCAL_STORAGE") != "1":
    raise RuntimeError("Tables are stored in MinIO only: STORAGE must be 's3' (local-disk mode is reserved for the offline tests).")
S3_BUCKET = os.environ.get("S3_BUCKET", "lake")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
RAW_DIR = os.path.join(DATA_ROOT, "raw")
CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "")  # optional: lets the publishers (ANM, ANP, IBGE) reach you if the downloads bother them
USER_AGENT = "pocket-lakehouse/1.0" + (" (%s)" % CONTACT_EMAIL if CONTACT_EMAIL else "")
HTTP_TIMEOUT_S = 900
PAUSA_S = float(os.environ.get("PAUSA_S", "3"))  # pause between requests to the same publisher
KEEP_RAW = os.environ.get("KEEP_RAW") == "1"
STATE_TABLE = "ctl_publico_extracao"
FILE_TABLE = "ctl_publico_arquivo"


def get_spark(app_name: str = "mineracao", cores_max: int = 2, master: str = "spark://spark-master:7077") -> SparkSession:
    """Session on the study cluster. 2 cores by default so spark-client jobs can still run next to a notebook."""
    return (SparkSession.builder.appName(app_name).master(master)
            .config("spark.cores.max", cores_max)
            .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
            .getOrCreate())


# ---------------------------------------------------------------------------
# Months (YYYY-MM)
# ---------------------------------------------------------------------------

def is_ym(s: str) -> bool:
    return bool(re.match(r"^\d{4}-(0[1-9]|1[0-2])$", s))


def ym_to_idx(ym: str) -> int:
    return int(ym[:4]) * 12 + int(ym[5:7]) - 1


def idx_to_ym(i: int) -> str:
    return "%d-%02d" % (i // 12, i % 12 + 1)


def add_months(ym: str, n: int) -> str:
    return idx_to_ym(ym_to_idx(ym) + n)


def today_local() -> str:
    """YYYY-MM-DD in America/Sao_Paulo (UTC-3, no DST since 2019)."""
    return datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")


def to_ym(name: str, v: str) -> str:
    """accepts YYYY-MM or YYYY-MM-DD and returns YYYY-MM ('' stays '')"""
    if not v:
        return ""
    s = str(v).strip()[:7]
    if not is_ym(s):
        raise ValueError('invalid %s: "%s" (use YYYY-MM or YYYY-MM-DD)' % (name, v))
    return s


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# Text (driver side)
# ---------------------------------------------------------------------------

def norm(s) -> str:
    """uppercase, no accents, only letters/digits separated by single spaces"""
    s = unicodedata.normalize("NFD", "" if s is None else str(s))
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return re.sub(r"[^A-Z0-9]+", " ", s.upper()).strip()


def compact_norm(s) -> str:
    return norm(s).replace(" ", "")


def bump(m: Dict[str, int], k: str, n: int = 1) -> None:
    m[k] = m.get(k, 0) + n


def top_n(m: Dict[str, int], n: int) -> List[dict]:
    return [{"valor": k, "linhas": v} for k, v in sorted(m.items(), key=lambda kv: -kv[1])[:n]]


# ---------------------------------------------------------------------------
# Text and numbers (Spark side) — only native functions, no Python UDFs
# ---------------------------------------------------------------------------

_ACC_FROM = "áàâãäåéèêëíìîïóòôõöúùûüçñýÿÁÀÂÃÄÅÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑÝ"
_ACC_TO = "aaaaaaeeeeiiiiooooouuuucnyyAAAAAAEEEEIIIIOOOOOUUUUCNY"


def s_(c) -> Column:
    """string column, trimmed, null → ''"""
    c = F.col(c) if isinstance(c, str) else c
    return F.trim(F.coalesce(c.cast("string"), F.lit("")))


def norm_col(c) -> Column:
    """Spark twin of norm(): upper, no accents, non-alphanumerics collapsed to one space"""
    c = F.col(c) if isinstance(c, str) else c
    x = F.upper(F.translate(F.coalesce(c.cast("string"), F.lit("")), _ACC_FROM, _ACC_TO))
    return F.trim(F.regexp_replace(x, r"[^A-Z0-9]+", " "))


def parse_num_col(c) -> Column:
    """number in Brazilian ("1.234,56") or international ("1234.56") format; blank/text → null (double)"""
    s = F.regexp_replace(F.regexp_replace(s_(c), r"\s", ""), r"^R\$", "")
    conv = (F.when(s.contains(","), F.regexp_replace(F.regexp_replace(s, r"\.", ""), ",", "."))
            .when(s.rlike(r"^-?\d{1,3}(\.\d{3}){2,}$"), F.regexp_replace(s, r"\.", ""))
            .otherwise(s))
    return F.when(~s.isin("", "-", "--") & conv.rlike(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$"), conv.cast("double"))


_MES = {"JAN": 1, "FEV": 2, "FEB": 2, "MAR": 3, "ABR": 4, "APR": 4, "MAI": 5, "MAY": 5, "JUN": 6, "JUL": 7, "AGO": 8,
        "AUG": 8, "SET": 9, "SEP": 9, "OUT": 10, "OCT": 10, "NOV": 11, "DEZ": 12, "DEC": 12}


def parse_periodo_col(c) -> Column:
    """period → YYYY-MM. Accepts YYYY/MM, YYYY-MM, MM/YYYY, YYYYMM, DD/MM/YYYY, jan/2024, 2024-01-01.
    The first pattern that matches decides (an invalid month does not fall through to the next pattern)."""
    s = s_(c)
    r1 = r"^(\d{4})[-/.](\d{1,2})([-/.]\d{1,2})?(\D.*)?$"
    r2 = r"^(\d{1,2})[-/.](\d{1,2})[-/.](\d{4})"
    r3 = r"^(\d{1,2})[-/.](\d{4})$"
    r4 = r"^(\d{4})(\d{2})$"
    r5 = r"^([A-Z]{3})[A-Z]* (\d{2,4})$"
    nm = norm_col(s)

    def g(rx, i, src=s):
        return F.regexp_extract(src, rx, i).cast("int")

    def ok(y, m):
        return F.when((y >= 1900) & (y <= 2100) & (m >= 1) & (m <= 12),
                      F.concat(y.cast("string"), F.lit("-"), F.lpad(m.cast("string"), 2, "0")))

    mes_map = F.create_map(*[x for k, v in _MES.items() for x in (F.lit(k), F.lit(v))])
    y5_raw = g(r5, 2, nm)
    y5 = F.when(F.length(F.regexp_extract(nm, r5, 2)) == 2, F.lit(2000) + y5_raw).otherwise(y5_raw)
    return (F.when(s == "", F.lit(None).cast("string"))
            .when(s.rlike(r1), ok(g(r1, 1), g(r1, 2)))
            .when(s.rlike(r2), ok(g(r2, 3), g(r2, 2)))
            .when(s.rlike(r3), ok(g(r3, 2), g(r3, 1)))
            .when(s.rlike(r4), ok(g(r4, 1), g(r4, 2)))
            .when(nm.rlike(r5) & mes_map[F.regexp_extract(nm, r5, 1)].isNotNull(), ok(y5, mes_map[F.regexp_extract(nm, r5, 1)])))


def date_col(c) -> Column:
    """yyyy-mm-dd[...] → date, anything else → null"""
    s = s_(c)
    return F.when(s.rlike(r"^\d{4}-\d{2}-\d{2}"), F.to_date(F.substring(s, 1, 10)))


def pad14(c) -> Column:
    """CNPJ: padStart(14, '0').slice(-14)"""
    return F.when(F.length(c) >= 14, F.substring(c, -14, 14)).otherwise(F.lpad(c, 14, "0"))


def h32(*cols) -> Column:
    """sha256 over the \\x01-joined values (null → ''), first 32 hex chars (stable surrogate keys)"""
    parts = [F.coalesce((F.col(c) if isinstance(c, str) else c).cast("string"), F.lit("")) for c in cols]
    return F.substring(F.sha2(F.concat_ws("\x01", *parts), 256), 1, 32)


def conform(df: DataFrame, spec: Sequence[Tuple[str, str]]) -> DataFrame:
    """select/cast the columns of the target table schema; '' → NULL"""
    out = df
    if any(n == "loaded_at" for n, _ in spec) and "loaded_at" not in df.columns:
        out = out.withColumn("loaded_at", F.current_timestamp())
    cols = []
    for name, typ in spec:
        c = F.col(name)
        if typ == "string":
            c = F.when(c.cast("string") == "", F.lit(None)).otherwise(c.cast("string"))
        elif typ == "date":
            c = c.cast("date")
        else:
            c = c.cast(typ)
        cols.append(c.alias(name))
    return out.select(*cols)


def persist_disk(df: DataFrame) -> DataFrame:
    return df.persist(StorageLevel.DISK_ONLY)


# ---------------------------------------------------------------------------
# CSV: header/delimiter on the driver, distributed read in Spark
# ---------------------------------------------------------------------------

def detect_encoding(path: str) -> str:
    """utf-8 if the whole file decodes as UTF-8, else windows-1252"""
    import codecs
    dec = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as f:
        try:
            while True:
                chunk = f.read(8 << 20)
                if not chunk:
                    dec.decode(b"", final=True)
                    return "UTF-8"
                dec.decode(chunk)
        except UnicodeDecodeError:
            return "windows-1252"


def decode_line(b: bytes) -> str:
    try:
        s = b.decode("utf-8")
    except UnicodeDecodeError:
        s = b.decode("windows-1252")
    if s.startswith("﻿"):
        s = s[1:]
    return s[:-1] if s.endswith("\r") else s


def head_lines(path: str, n: int = 200) -> List[str]:
    out = []
    with open(path, "rb") as f:
        for raw in f:
            out.append(decode_line(raw.rstrip(b"\n")))
            if len(out) >= n:
                break
    return out


def detect_delim(header: str) -> str:
    best, n = ";", -1
    for c in [";", ",", "\t", "|"]:
        k = len(header.split(c))
        if k > n:
            n, best = k, c
    return best


def split_csv(line: str, d: str) -> List[str]:
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


def map_header(headers: Sequence[str], spec: Sequence[Tuple[str, str]]) -> Tuple[Dict[str, int], List[str]]:
    """maps source headers → internal columns by regex over the compacted name (no accent/space).
    Spec order matters: the first pattern that matches takes the header (each header is used once)."""
    comp = [compact_norm(h) for h in headers]
    used, idx = set(), {}
    for col, rx in spec:
        for i, h in enumerate(comp):
            if i not in used and re.search(rx, h):
                idx[col] = i
                used.add(i)
                break
    return idx, [h for i, h in enumerate(headers) if i not in used]


def read_csv(spark: SparkSession, path: str, sep: str, enc: str) -> DataFrame:
    """all-string DataFrame with columns c0..cN (positional, so duplicated/odd header names cannot clash)"""
    df = (spark.read.option("header", True).option("sep", sep).option("encoding", enc)
          .option("quote", '"').option("escape", '"')
          .option("ignoreLeadingWhiteSpace", True).option("ignoreTrailingWhiteSpace", True)
          .csv("file://" + path))
    return df.toDF(*["c%d" % i for i in range(len(df.columns))]).na.drop("all")


def csv_source(spark: SparkSession, path: str, spec: Sequence[Tuple[str, str]], default_delim: str = ";"):
    """(df, header, idx, livres, delim) of a CSV file whose first line is the header"""
    lines = [ln for ln in head_lines(path, 5) if ln.strip()]
    if not lines:
        raise ValueError("Empty file: %s" % path)
    delim = detect_delim(lines[0]) if lines else default_delim
    header = split_csv(lines[0], delim)
    idx, livres = map_header(header, spec)
    df = read_csv(spark, path, delim, detect_encoding(path))
    return df, header, idx, livres, delim


def col_getter(idx: Dict[str, int]) -> Callable[[str], Column]:
    """g(k): trimmed string column of the mapped source column, '' when the column does not exist"""
    return lambda k: s_("c%d" % idx[k]) if k in idx else F.lit("")


# ---------------------------------------------------------------------------
# Polite HTTP: one request at a time, a User-Agent with contact, 403/429 = stop
# ---------------------------------------------------------------------------

class FatalHttp(Exception):
    pass


class _GzipResponse:
    """urllib never decompresses; some servers (IBGE) send gzip even when it was not asked for. Streams the plain bytes."""

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


def http_open(url: str, method: str = "GET", headers: Optional[dict] = None):
    hdrs = {"User-Agent": USER_AGENT, "Accept": "*/*", "Accept-Encoding": "identity"}
    hdrs.update(headers or {})
    for attempt in itertools.count(1):
        try:
            resp = urllib.request.urlopen(urllib.request.Request(url, method=method, headers=hdrs), timeout=HTTP_TIMEOUT_S)
            return _GzipResponse(resp) if method != "HEAD" and (resp.headers.get("Content-Encoding") or "").lower() == "gzip" else resp
        except urllib.error.HTTPError as e:
            body = ""
            if method == "GET":
                try:
                    body = re.sub(r"\s+", " ", e.read(2000).decode("utf-8", "replace"))[:400]
                except Exception:
                    pass
            if e.code in (403, 429):
                raise FatalHttp("HTTP %d on %s — the server refused/rate-limited; job stopped without retrying. %s" % (e.code, url, body))
            if e.code < 500:
                raise FatalHttp("HTTP %d on %s (did the URL change?). %s" % (e.code, url, body))
            if attempt >= 3:
                raise RuntimeError("HTTP %d on %s after 3 attempts. %s" % (e.code, url, body))
            time.sleep(30 * attempt)
        except (urllib.error.URLError, OSError) as e:
            if attempt >= 3:
                raise RuntimeError("Network failure on %s: %s" % (url, e))
            print("[HTTP] network failed (%s); retrying in %ds" % (e, 20 * attempt))
            time.sleep(20 * attempt)


def http_text(url: str) -> str:
    with http_open(url) as r:
        return r.read().decode("utf-8", "replace")


def _info(url: str, r, tamanho: Optional[int]) -> dict:
    return {"url": url, "etag": r.headers.get("etag") or "", "last_modified": r.headers.get("last-modified") or "", "tamanho": tamanho}


def head_info(url: str) -> dict:
    """ETag / Last-Modified / size. Some publishers (ANP) answer 403 to HEAD but serve GET: then a 1-byte Range GET is
    tried. A 403/429 on that GET is still fatal (the 'stop when the server refuses' rule is kept)."""
    try:
        with http_open(url, "HEAD") as r:
            ln = r.headers.get("content-length")
            return _info(url, r, int(ln) if ln else None)
    except FatalHttp as e:
        if not re.search(r"HTTP (403|429)\b", str(e)):
            return {"url": url, "etag": "", "last_modified": "", "tamanho": None}  # server without HEAD: carry on without change detection
        if "HTTP 403" not in str(e):
            raise
    time.sleep(PAUSA_S)
    with http_open(url, "GET", {"Range": "bytes=0-0"}) as r:
        m = re.search(r"/(\d+)$", r.headers.get("content-range") or "")
        ln = r.headers.get("content-length")
        return _info(url, r, int(m.group(1)) if m else (int(ln) if ln and r.status != 206 else None))


def download(url: str, fonte: str) -> Tuple[str, int]:
    """streams the file to RAW_DIR/<fonte>/ and returns (path, bytes)"""
    name = os.path.basename(urllib.parse.unquote(urllib.parse.urlparse(url).path)) or "download"
    dest = os.path.join(RAW_DIR, fonte, "%s_%s" % (hashlib.sha1(url.encode()).hexdigest()[:8], name))
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    n = 0
    with http_open(url) as r, open(dest + ".part", "wb") as f:
        while True:
            chunk = r.read(1 << 20)
            if not chunk:
                break
            f.write(chunk)
            n += len(chunk)
    os.replace(dest + ".part", dest)
    return dest, n


def peek_header(url: str, nbytes: int = 65536) -> str:
    """first non-empty line of a remote CSV (Range request, so dry runs never download the whole file)"""
    with http_open(url, "GET", {"Range": "bytes=0-%d" % (nbytes - 1)}) as r:
        data = r.read(nbytes)
    for raw in data.split(b"\n"):
        ln = decode_line(raw)
        if ln.strip():
            return ln
    raise ValueError("empty file")


def check_source(url: str, spec, faltam_fn: Callable[[Dict[str, int]], List[str]], default_delim: str = ";") -> dict:
    """dry-run verification of a CSV source: reachable? headers still map to the expected columns?"""
    out = {"url": url}
    try:
        fi = head_info(url)
        out.update(etag=fi["etag"], last_modified=fi["last_modified"], tamanho=fi["tamanho"])
        time.sleep(PAUSA_S)
        line = peek_header(url)
        delim = detect_delim(line) if line else default_delim
        header = split_csv(line, delim)
        idx, livres = map_header(header, spec)
        faltam = faltam_fn(idx)
        out.update(delimitador=delim, n_colunas=len(header), mapeamento={k: header[v] for k, v in idx.items()},
                   nao_usadas=livres[:15], faltam=faltam, layout_ok=not faltam)
    except Exception as e:  # a dry run reports, it does not fail
        out["erro"] = str(e)[:300]
    return out


# ---------------------------------------------------------------------------
# Target (Parquet) and run bookkeeping (JSON)
#   STORAGE=file → DATA_ROOT/<db>/<table> (local disk; offline tests only, requires ALLOW_LOCAL_STORAGE=1)
#   STORAGE=s3   → s3a://S3_BUCKET/<db>/<table> (MinIO). table_path() values are fsspec paths (no scheme);
#                  uri() converts them to the scheme Spark understands.
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
    """Spark-readable URI for a table_path() value"""
    return ("s3a://" if STORAGE == "s3" else "file://") + path


def table_path(db: str, table: str) -> str:
    return "%s/%s/%s" % (_root(), db, table)


def _rm(path: str) -> None:
    try:
        fs().rm(path, recursive=True)
    except FileNotFoundError:
        pass


def _mv_dir(src: str, dst: str) -> None:
    """moves a table folder file by file (recent s3fs fails on mv(recursive=True): it tries to copy the folder itself as a key)"""
    for f in fs().find(src):
        target = dst + f[len(src):]
        fs().makedirs(target.rsplit("/", 1)[0], exist_ok=True)
        fs().copy(f, target)
    _rm(src)


def table_exists(db: str, table: str) -> bool:
    try:
        return any(f.endswith(".parquet") for f in fs().ls(table_path(db, table), detail=False))
    except FileNotFoundError:
        return False


def read_table_pd(db: str, table: str, columns: Optional[List[str]] = None) -> pd.DataFrame:
    p = table_path(db, table)
    if STORAGE == "s3":  # through our own fs(): pandas would use a cached s3fs instance whose listings go stale after an upsert
        import pyarrow.parquet as pq
        return pq.ParquetDataset(p, filesystem=fs()).read(columns=columns).to_pandas()
    return pd.read_parquet(p, columns=columns)


def watermark_ym(db: str, table: str, col: str = "ano_mes") -> Optional[str]:
    """MAX(col) as YYYY-MM, no Spark needed"""
    if not table_exists(db, table):
        return None
    s = read_table_pd(db, table, [col])[col].dropna()
    return pd.Timestamp(s.max()).strftime("%Y-%m") if len(s) else None


def stage(df: DataFrame, spark: SparkSession, db: str, table: str) -> int:
    """writes the new rows next to the table (path.__stage) and returns how many there are"""
    path = table_path(db, table) + ".__stage"
    _rm(path)
    df.write.mode("overwrite").parquet(uri(path))
    return spark.read.parquet(uri(path)).count()


def commit(spark: SparkSession, db: str, table: str, pk: Optional[Sequence[str]] = None, snapshot: bool = False) -> None:
    """upsert by PK (pk) or replace the whole table (snapshot); the live table is only touched after the stage is complete"""
    path = table_path(db, table)
    staged = path + ".__stage"
    if snapshot or not table_exists(db, table):
        merged = staged
    else:
        new = spark.read.parquet(uri(staged))
        old = spark.read.parquet(uri(path))
        merged = path + ".__merge"
        _rm(merged)
        (old.join(new.select(*pk).distinct(), list(pk), "left_anti").unionByName(new, allowMissingColumns=True)
         .write.mode("overwrite").parquet(uri(merged)))
    _rm(path)
    _mv_dir(merged, path)
    _rm(staged)


def load(spark: SparkSession, df: DataFrame, db: str, table: str, pk: Optional[Sequence[str]] = None, snapshot: bool = False) -> int:
    n = stage(df, spark, db, table)
    commit(spark, db, table, pk, snapshot)
    return n


class Ctl:
    """state/cursor (ctl_publico_extracao) and files already loaded (ctl_publico_arquivo), stored as JSON in the lake"""

    def __init__(self, db: str):
        self.dir = "%s/%s/_ctl" % (_root(), db)
        self.f_state = "%s/%s.json" % (self.dir, STATE_TABLE)
        self.f_file = "%s/%s.json" % (self.dir, FILE_TABLE)

    def exists(self) -> bool:
        return fs().exists(self.f_state) and fs().exists(self.f_file)

    def ensure(self) -> None:
        fs().makedirs(self.dir, exist_ok=True)
        for f in (self.f_state, self.f_file):
            if not fs().exists(f):
                self._save(f, {})

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

    def write_state(self, fonte, modo, cursor, ini, fim, status, resumo) -> None:
        d = self._load(self.f_state)
        d["%s|%s" % (fonte, modo)] = {"fonte": fonte, "modo": modo, "cursor_periodo": cursor, "janela_inicio": ini,
                                      "janela_fim": fim, "ultimo_status": status,
                                      "ultimo_resultado": json.dumps(resumo, ensure_ascii=False, default=str)[:3900],
                                      "atualizado_em": utcnow_iso()}
        self._save(self.f_state, d)

    def write_error(self, fonte, modo, e) -> None:
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

    def write_file_info(self, fonte, fi, nbytes, lidas, gravadas) -> None:
        d = self._load(self.f_file)
        d["%s|%s" % (fonte, fi["url"])] = {"fonte": fonte, "url": fi["url"], "etag": fi["etag"], "last_modified": fi["last_modified"],
                                           "tamanho": nbytes, "linhas_lidas": lidas, "linhas_gravadas": gravadas,
                                           "atualizado_em": utcnow_iso()}
        self._save(self.f_file, d)


def unchanged(prev: Optional[dict], fi: dict) -> bool:
    """true when the remote file is the same as in the last load (same ETag or Last-Modified)"""
    if not prev:
        return False
    if fi["etag"] and prev.get("etag") and fi["etag"] == prev["etag"]:
        return True
    return bool(not fi["etag"] and fi["last_modified"] and prev.get("last_modified") and fi["last_modified"] == prev["last_modified"])


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


# ---------------------------------------------------------------------------
# Extraction focus: substances, states and basins (optional local dim_* tables, else the defaults below)
# ---------------------------------------------------------------------------

# default rules: normalized ANM name → standard substance (the first match wins)
SUBST_REGRAS: List[Tuple[str, str]] = [
    ("MANGANES", r"MANGANES"),
    ("FERRO", r"\bFERRO\b|FERRIFERO|HEMATITA|ITABIRITO|MAGNETITA|CANGA|\bPELOTA|LATERITA FERRUGINOSA"),
    ("BAUXITA", r"BAUXITA|BAUXITIC|ALUMINIO|GIBBSITA|ALUMINA\b"),
    ("FOSFATO", r"FOSFAT|FOSFORITA|APATITA|\bFOSFORO"),
    ("NIQUEL", r"NIQUEL|GARNIERITA"),
    ("COBRE", r"COBRE|CALCOPIRITA|BORNITA"),
    ("NIOBIO", r"NIOBIO|PIROCLORO|COLUMBITA|TANTALITA|\bTANTALO\b"),
    ("OURO", r"\bOURO\b|AURIFER"),
]
PRIORIDADE_PADRAO = {"FERRO": "P1", "BAUXITA": "P2", "FOSFATO": "P2", "NIQUEL": "P2", "COBRE": "P3", "MANGANES": "P3", "OURO": "P3", "NIOBIO": "P3"}
UFS_PADRAO = ["MG", "PA", "ES", "RJ", "MA", "GO", "AL", "SE", "BA", "SP"]
BACIAS_PADRAO = ["CAMPOS", "SANTOS", "SERGIPE", "ALAGOAS", "ESPIRITO SANTO"]


class Foco:
    def __init__(self):
        self.origem = "padrao_script"
        self.depara: Dict[str, str] = {}            # normalized name → standard substance (dim_substanciaMineral)
        self.prioridade: Dict[str, str] = dict(PRIORIDADE_PADRAO)
        self.substancias: set = set()               # substances to extract
        self.ufs: set = set(UFS_PADRAO)             # empty = all
        self.bacias: List[str] = list(BACIAS_PADRAO)  # empty = all


def pick_cfg_db(cfg_db: str, target_db: str, tables: Sequence[str] = ("dim_alvoExtracao", "dim_substanciaMineral")) -> str:
    for db in [d for d in (cfg_db, target_db) if d]:
        if any(table_exists(db, t) for t in tables):
            return db
    return ""


def load_foco(cfg_db: str, fonte: str, incluir_p3: bool) -> Foco:
    foco = Foco()
    if cfg_db and table_exists(cfg_db, "dim_substanciaMineral"):
        for r in read_table_pd(cfg_db, "dim_substanciaMineral").to_dict("records"):
            if r.get("nome_origem"):
                foco.depara[norm(r["nome_origem"])] = str(r["substancia"]).upper()
            if r.get("substancia") and r.get("prioridade"):
                foco.prioridade[str(r["substancia"]).upper()] = str(r["prioridade"]).upper()
        foco.origem = "%s.dim_substanciaMineral" % cfg_db
    alvo: List[dict] = []
    if cfg_db and table_exists(cfg_db, "dim_alvoExtracao"):
        alvo = [r for r in read_table_pd(cfg_db, "dim_alvoExtracao").to_dict("records")
                if r.get("ativo") in (1, True) and r.get("fonte") in ("*", fonte)]
        foco.origem += " + %s.dim_alvoExtracao" % cfg_db
    subs = [norm(r["valor"]) for r in alvo if str(r["tipo"]).upper() == "SUBSTANCIA"]
    ufs = [norm(r["valor"]) for r in alvo if str(r["tipo"]).upper() == "UF"]
    bac = [norm(r["valor"]) for r in alvo if str(r["tipo"]).upper() == "BACIA"]
    if subs:
        foco.substancias = set(subs)
    else:
        foco.substancias = {s for s, p in foco.prioridade.items() if p in ("P1", "P2") or (incluir_p3 and p == "P3")}
    if ufs:
        foco.ufs = set() if "*" in ufs else set(ufs)
    if bac:
        foco.bacias = [] if "*" in bac else bac
    return foco


def classifica(foco: Foco, nome_origem: str) -> str:
    n = norm(nome_origem)
    if n in foco.depara:
        return foco.depara[n]
    for s, rx in SUBST_REGRAS:
        if re.search(rx, n):
            return s
    return "OUTROS"


def classifica_col(foco: Foco, normc: Column) -> Column:
    """Spark twin of classifica(); `normc` must already be norm_col(...)"""
    expr = F.lit("OUTROS")
    for s, rx in reversed(SUBST_REGRAS):
        expr = F.when(normc.rlike(rx), F.lit(s)).otherwise(expr)
    if foco.depara:
        m = F.create_map(*[x for k, v in foco.depara.items() for x in (F.lit(k), F.lit(v))])
        expr = F.coalesce(m[normc], expr)
    return expr


def prioridade_col(foco: Foco, sub: Column) -> Column:
    m = F.create_map(*[x for k, v in foco.prioridade.items() for x in (F.lit(k), F.lit(v))])
    return m[sub]


def plano_foco(foco: Foco, bacias: bool = False) -> dict:
    d = {"origem": foco.origem, "substancias": sorted(foco.substancias), "ufs": sorted(foco.ufs)}
    if bacias:
        d["bacias"] = foco.bacias
    return d


# ---------------------------------------------------------------------------
# Municipalities: (state, normalized name) → 7-digit IBGE code (public IBGE API, cached locally)
# ---------------------------------------------------------------------------

IBGE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


class Municipios:
    def __init__(self, rows: Sequence[Tuple[str, str, str]]):
        self.by_name: Dict[str, Tuple[str, str]] = {}
        for cod, uf, nome in rows:
            self.by_name["%s|%s" % (uf, norm(nome))] = (str(cod), str(nome))
        self.by_cod: Dict[str, str] = {cod: nome for cod, nome in self.by_name.values()}

    def frames(self, spark: SparkSession):
        by_name = spark.createDataFrame([(k.split("|", 1)[0], k.split("|", 1)[1], v[0], v[1]) for k, v in self.by_name.items()],
                                        "b_uf string, b_mun string, n_cod string, n_nome string")
        by_cod = spark.createDataFrame(list(self.by_cod.items()), "cod7 string, c_nome string")
        return F.broadcast(by_name), F.broadcast(by_cod)


def load_municipios(min_rows: int = 5000, refresh: bool = False) -> Municipios:
    cache = os.path.join(DATA_ROOT, "ref", "dim_municipiosIbge.parquet")
    if refresh or not os.path.isfile(cache):
        rows = []
        for m in json.loads(http_text(IBGE_URL)):
            uf = (((m.get("microrregiao") or {}).get("mesorregiao") or {}).get("UF") or {}).get("sigla") \
                or ((((m.get("regiao-imediata") or {}).get("regiao-intermediaria") or {}).get("UF")) or {}).get("sigla")
            rows.append((str(m["id"]), uf, m["nome"]))
        os.makedirs(os.path.dirname(cache), exist_ok=True)
        pd.DataFrame(rows, columns=["cod_ibge", "uf", "municipio"]).to_parquet(cache, index=False)
    pdf = pd.read_parquet(cache)
    if len(pdf) < min_rows:
        raise RuntimeError("dim_municipiosIbge looks wrong: only %d municipalities" % len(pdf))
    return Municipios(list(zip(pdf["cod_ibge"], pdf["uf"], pdf["municipio"])))


def resolve_municipio(df: DataFrame, mun_frames, uf: str, mun_norm: str, cod_fonte: Optional[str]) -> DataFrame:
    """adds _cod and _mun_nome. IBGE code from the source (7 digits) wins; the (UF, name) match is the fallback.
    Code = hit?.cod ?? (source code if 7 digits); name = hit?.nome ?? normalized source name."""
    by_name, by_cod = mun_frames
    d = df.withColumn("_cod7", F.when(F.length(F.col(cod_fonte)) == 7, F.col(cod_fonte)) if cod_fonte else F.lit(None).cast("string"))
    d = d.join(by_cod, d["_cod7"] == by_cod["cod7"], "left").drop("cod7")
    d = d.join(by_name, (d[uf] == by_name["b_uf"]) & (d[mun_norm] == by_name["b_mun"]), "left").drop("b_uf", "b_mun")
    hit_cod = F.col("c_nome").isNotNull()
    return (d.withColumn("_cod", F.coalesce(F.when(hit_cod, F.col("_cod7")), F.col("n_cod"), F.col("_cod7")))
            .withColumn("_mun_nome", F.coalesce(F.col("c_nome"), F.col("n_nome"), F.col(mun_norm)))
            .drop("_cod7", "c_nome", "n_cod", "n_nome"))
