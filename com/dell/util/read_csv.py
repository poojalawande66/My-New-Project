from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").appName("Read File").getOrCreate()

csv_Read = spark.read.format("csv").option("header","true").options("inferschema","true")\
            .load("")