WU Big Data Kafka


5027221046 - Imam Nurhadi
5027221045 - Gilang Raya Kurniawan

Download kafka dari apache kafka versi binary
https://downloads.apache.org/kafka/3.8.1/kafka_2.13-3.8.1.tgz

Atur Variable lingkungan dahulu jika belum contoh

Dan


Nomor 1
masuk ke terminal lalu start
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties

masuk ke terminal baru, jangan close terminal yang lama
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties

Masukkan ke terminal yang baru, jangan close yang lama
 C:\kafka\bin\windows\kafka-topics.bat --create --topic sensor-suhu --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1



Nomor 2
Masukkan kode di bawah di IDE misal dengan nama temperature_producer.py.py


import time
import random
from kafka import KafkaProducer
import json


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


sensors = ['S1', 'S2', 'S3']


try:
    while True:
        for sensor_id in sensors:
            suhu = random.uniform(60, 100)  
            data = {
                'sensor_id': sensor_id,
                'suhu': round(suhu, 2)  
            }
            producer.send('sensor-suhu', data)
            print(f"Data dikirim: {data}")
        time.sleep(1)  
except KeyboardInterrupt:
    print("Producer dihentikan.")
finally:
    producer.close()

Maka akan keluar hasil seperti di bawah secara real time, hasil tersebut disimpan di server kafka dengan nama sensor-suhu

jika library kafka python error gunakan versi dibawah
pip install git+https://github.com/dpkp/kafka-python.git


Nomor 3 dan Nomer 4
Buat file kode baru bernama
temperature_consumer.py

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

Kode diatas menggunakan sensor-suhu hasil produksi kafka dan membacanya menggunakan pyspark dimana kode diatas memfilter suhu yang berada diatas 80% dan print hasil ke terminal secara real time



jika pyspark tidak bisa membaca server kafka gunakan 
https://github.com/steveloughran/winutils/releases/tag/tag_2017-08-29-hadoop-2.8.1-native

Ekstrak masukkan ke Hadoop/bin
