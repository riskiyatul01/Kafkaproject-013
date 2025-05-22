from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

gudangs = ['G1', 'G2', 'G3']

while True:
    for gudang in gudangs:
        data = {"gudang_id": gudang, "kelembaban": random.randint(65, 80)}
        producer.send('sensor-kelembaban-gudang', value=data)
        print(f"{data}")
    time.sleep(1)
