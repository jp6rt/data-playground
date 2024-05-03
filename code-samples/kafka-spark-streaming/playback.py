import threading
import time
import uuid
import csv
import json
from datetime import datetime
from pykafka import KafkaClient

KAFKA_HOST = "localhost:9092" # Or the address you want

client = KafkaClient(hosts = KAFKA_HOST)
topic = client.topics["source-events"]

# update this path to point to your local data file
output_path = "/home/joeyresuento/Projects/data_training/engineering/big-data-git/data/iot_telemetry_data.csv"
telemetry_data = []

with open(output_path, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    telemetry_data = [row for row in reader]

with topic.get_sync_producer() as producer:
    counter = 0

    while True:
        message = telemetry_data[ counter%len(telemetry_data) ]
        # message['ts'] = int(datetime.now().timestamp() * 1e3)
        message['ts'] = datetime.now().isoformat()
        producer.produce(json.dumps(message).encode())
        counter += 1
        print(f"Producing message: {json.dumps(message)}")
        time.sleep(0.01)
