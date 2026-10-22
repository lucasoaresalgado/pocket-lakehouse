from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("s3-smoke").getOrCreate()
df = spark.range(0, 200000).selectExpr("id", "id % 7 as k")
df.write.mode("overwrite").parquet("s3a://lake/_smoke/t")
back = spark.read.parquet("s3a://lake/_smoke/t")
print("ROWS", back.count(), "GROUPS", back.groupBy("k").count().count())
spark.stop()
