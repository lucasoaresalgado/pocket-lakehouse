# Read-only (by default) MCP server over the lake: DuckDB on the Parquet tables in MinIO.
FROM python:3.12-slim
RUN pip install --no-cache-dir "mcp>=1.12,<2" duckdb boto3
RUN useradd --uid 10001 --create-home mcp
USER mcp
ENV HOME=/home/mcp PYTHONUNBUFFERED=1
# the httpfs extension is fetched now: at run time the container has a read-only filesystem and no need for the internet
RUN python -c "import duckdb; duckdb.install_extension('httpfs')"
COPY server.py /app/server.py
EXPOSE 8765
CMD ["python", "/app/server.py"]
