"""Template: extract a table over JDBC in parallel and write Parquet.

Usage (inside spark-client):
  spark-submit --master spark://spark-master:7077 /apps/extract_jdbc.py \
    --url "jdbc:sqlserver://HOST:1433;databaseName=DB;encrypt=true;trustServerCertificate=true" \
    --table dbo.SA1010 --user U --password-env DB_PASSWORD \
    --partition-col R_E_C_N_O_ --lower 1 --upper 1000000 --partitions 4 --out /data/out/SA1010
"""
import argparse, os
from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--url", required=True)
p.add_argument("--table", required=True)
p.add_argument("--user", required=True)
p.add_argument("--password-env", default="DB_PASSWORD")
p.add_argument("--partition-col")
p.add_argument("--lower", type=int)
p.add_argument("--upper", type=int)
p.add_argument("--partitions", type=int, default=4)
p.add_argument("--fetchsize", type=int, default=5000)
p.add_argument("--out", required=True)
a = p.parse_args()

spark = SparkSession.builder.appName(f"extract-{a.table}").getOrCreate()
r = (spark.read.format("jdbc")
     .option("url", a.url).option("dbtable", a.table)
     .option("user", a.user).option("password", os.environ[a.password_env])
     .option("fetchsize", a.fetchsize))
if a.partition_col:
    r = (r.option("partitionColumn", a.partition_col).option("lowerBound", a.lower)
          .option("upperBound", a.upper).option("numPartitions", a.partitions))
df = r.load()
df.write.mode("overwrite").parquet(a.out)
print("rows written:", spark.read.parquet(a.out).count())
spark.stop()
