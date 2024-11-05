from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, FloatType

spark = SparkSession.builder \
    .appName("KafkaTemperatureConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("suhu", FloatType(), True)
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu") \
    .option("startingOffsets", "latest") \
    .load()

df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

alert_df = df.filter(col("suhu") > 80)

query = alert_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
