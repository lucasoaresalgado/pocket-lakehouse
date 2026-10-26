# Horizontal scaling: is it viable with the current code?

**Question:** the goal is to stay monolithic (one repo, one `docker compose` stack, one lake), but also to scale horizontally
by turning containers off and running them on other machines. Is that 100% viable today?

**Answer: no.** Some pieces move to another machine easily, but the Spark cluster is built on single-host assumptions.

*Basis:* a read of `docker-compose.yml`, `conf/spark-defaults.conf`, `notebooks/lakehouse/core.py` and the pipeline entry
points. Nothing was tried on two machines.

## What already works

- **Storage.** Tables live only in MinIO, and the Python code takes the S3 address from `S3_ENDPOINT`. Moving the lake to
  another machine is mostly changing one variable.
- **The MCP server.** It is stateless and driven by environment variables (S3 endpoint and credentials), so it can run on
  any machine that can reach MinIO.
- **The optional services.** The MCP server and autoheal are opt-in switches (`MCP_ACTIVE`, `AUTOHEAL_ACTIVE`). Autoheal is
  per host anyway, because it uses the local Docker socket.

## What blocks it

1. **Raw files are read from the host's local disk.**
   - `core.read_csv` passes `file://` paths under `/data/raw` to Spark.
   - Today every Spark container mounts the same `./data` folder, so the executors see the files the driver downloaded.
   - A worker on another machine would not have those files, and the load would fail with file-not-found.
   - This is the biggest problem. Fixing it means staging the downloads in MinIO, or putting a shared filesystem under
     `/data`.
2. **Container-name networking.** Everything finds everything else through Docker DNS on one bridge network:
   - `spark://spark-master:7077` is the default in `core.py`, the mineracao `common.py`, and both `run.py` files
     (clima, empresas).
   - `http://minio:9000` is hard-coded in `conf/spark-defaults.conf`.
   - `scripts/init-minio.sh` runs `docker run --network spark_spark`.
   - Across machines these names do not resolve, so it needs real DNS, a VPN such as Tailscale, or a Docker Swarm overlay
     network.
3. **Spark client mode needs a route back to the driver.**
   - Executors connect back to the driver (Jupyter or `spark-client`) on ports Spark picks at random.
   - Nothing in the config fixes `spark.driver.host`, `spark.driver.port` or `spark.blockManager.port`.
   - A remote worker cannot reach the driver unless those are pinned and opened.
4. **Adding workers would not add capacity.** `spark.cores.max` is 4, and the two local workers already supply those 4
   cores. The executor memory of 1536m is sized for the 2.5 GB worker containers.
5. **The compose file is a single deployment.**
   - It has fixed `container_name` values and `depends_on` chains. A worker depends on the master, so starting only a
     worker elsewhere pulls in or waits on a master.
   - There are no per-role profiles such as "worker only" or "storage only".
   - `./conf` is bind-mounted, so each machine needs its own copy.
6. **Ports and security.**
   - MinIO and Jupyter are published on `127.0.0.1` only.
   - The Spark master, Jupyter and the MCP endpoint have no authentication, and MinIO traffic is plain HTTP.
   - Opening any of this to a LAN needs at least a firewall or VPN, and Jupyter needs a token.

## Rough effort

| Goal | Effort |
|---|---|
| MCP or MinIO on another machine | Small: set `S3_ENDPOINT` and the bind address |
| Spark workers on another machine | Medium: fixed driver ports, a resolvable master address, and a MinIO-staged `/data/raw` or a shared mount |
| Clean split of one compose file into roles | Medium: add profiles and parameterize the hostnames |

## Suggested order

1. Stop reading raw downloads from local disk (stage them in MinIO, or share `/data`). This is the only blocker that needs
   a code change; the rest is configuration.
2. Parameterize the hostnames (`SPARK_MASTER_URL`, `S3_ENDPOINT` in `spark-defaults.conf`) instead of hard-coding
   `spark-master` and `minio`.
3. Pin the driver ports and advertise the driver's address.
4. Add compose profiles per role (storage, master, worker, driver/Jupyter, MCP), and raise `spark.cores.max` to match the
   workers you add.
5. Add authentication and network restrictions before exposing anything beyond one machine.
