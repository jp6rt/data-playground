import json
from confluent_kafka import Consumer
from datetime import datetime

c = Consumer({
    # 'bootstrap.servers': 'localhost:9092',
    'bootstrap.servers': '192.168.253.163:9092',
    'group.id': 'test_group',
    'auto.offset.reset': 'latest'
})

c.subscribe(['my-sink-topic'])

while True:
    msg = c.poll(0.01)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    # print('Received message: {}'.format(msg.value().decode('utf-8')))
    decoded_msg = json.loads(msg.value().decode('utf-8'))
    
    # print(decoded_msg)
    ts = decoded_msg['ts']
    curr_time = int(datetime.now().timestamp() * 1e3)
    latency = curr_time - ts
    print(f"message: device={decoded_msg['device']} latency={latency} device_count={decoded_msg['device_count']}")

c.close()
