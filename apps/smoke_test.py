from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("smoke").getOrCreate()
df = spark.range(0, 5_000_000).selectExpr("id", "id % 10 as k")
print(df.groupBy("k").count().orderBy("k").collect())
spark.stop()
