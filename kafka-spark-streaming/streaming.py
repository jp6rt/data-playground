from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, avg, window, to_json
from pyspark.sql.types import StructType
import datetime

spark = SparkSession \
    .builder \
    .appName("MyApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

mySchema = StructType() \
    .add("ts", "timestamp") \
    .add("device", "string") \
    .add("co", "string") \
    .add("humidity", "string") \
    .add("light", "string") \
    .add("lpg", "string") \
    .add("motion", "string") \
    .add("smoke", "string") \
    .add("temp", "string")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "source-events") \
    .load()

castedDf = df \
    .selectExpr("CAST(value AS STRING)")

jsonDf = castedDf.select(from_json(castedDf.value, mySchema).alias("values"))
flattenedJsonDf = jsonDf \
    .select("values.*") \
    .withWatermark("ts", "30 seconds") \
    .groupBy(
        window('ts', '30 seconds'),
        "device"
    ) \
    .agg(avg("temp").alias("avg_temp")) \
    # .select("device", "avg_temp")


flattenedJsonDf.writeStream \
    .format("console") \
    .trigger(processingTime='2 seconds') \
    .option("truncate","false") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start() \
    .awaitTermination()
"""
# resultDf = flattenedJsonDf \
#    .select(to_json(flattenedJsonDf.select(["window", "device", "avg_temp"])).alias("value")) \
#    .selectExpr("CAST(key AS STRING), CAST(value AS STRING)")

ds = resultDf \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output-events") \
    .option("checkpointLocation", "/tmp/spark-checkpoint") \
    .start()

ds.awaitTermination()
"""

