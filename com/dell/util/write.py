from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("read file")\
    .config("spark.jars.packages", "org.apache.spark:spark-avro_2.11:2.4.6") \
    .getOrCreate()

print(spark.version)
df= spark.read.format("csv")\
    .option("Header","true")\
    .option("inferSchema","true")\
    .option("delimiter","|")\
    .load("D:/Input/salary.csv")
#df.show()
value=df.collect()[0][1]
#print(value)

#df.write.format("parquet").mode("append").save("D:/Input/output1/")

#df.write.format("orc").mode("append").save("D:/Input/output2/")

df.write.format("avro").mode("append").save("file:///D:/Input/output_avro/")



