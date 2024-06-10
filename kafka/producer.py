import csv
import json
import time

from kafka import KafkaProducer
from config.config import *


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def push_to_topic(file_path, topic_name):
    with open(file_path, mode='r') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            data = {
                'student_code': int(row[0]),
                'activity': row[1],
                'numberOfFile': int(row[2]),
                'timestamp': row[3]
            }
            producer.send(topic_name, value=data)
            print(f"Sent to Kafka topic {topic_name}: {data}")
            time.sleep(1)


producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],
                         value_serializer=json_serializer)

csv_file_path = '../data/log_action.csv'
topic_name = "vdt2024"
push_to_topic(csv_file_path, topic_name)

producer.close()
