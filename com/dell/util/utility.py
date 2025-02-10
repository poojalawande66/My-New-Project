from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Read File").getOrCreate()

print("Spark Version :- ", spark.version)