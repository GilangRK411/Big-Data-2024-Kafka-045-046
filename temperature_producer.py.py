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
