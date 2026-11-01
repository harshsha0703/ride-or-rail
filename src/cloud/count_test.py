import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("cloud-count-test").getOrCreate()
df = spark.read.parquet(sys.argv[1])
print("ROWS:", df.count())
df.groupBy("year", "month").count().show()