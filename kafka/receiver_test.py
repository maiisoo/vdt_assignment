import json

from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'vdt2024',
    bootstrap_servers='localhost:9092')

print("starting the consumer")
for msg in consumer:
    print("Received = {}".format(json.loads(msg.value)))