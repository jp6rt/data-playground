from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, split, sum, avg
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("MyApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

df = spark \
    .read \
    .option("header", True) \
    .csv("/home/joeyresuento/Projects/data_training/engineering/kafka-2.13/myfolder/iot_telemetry_data.csv")

df.printSchema()

df.groupBy(["device", "light"]).count().show()
df.groupBy("device").agg(avg("temp")).show()

