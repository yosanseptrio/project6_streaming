import time
import json
import datetime
from kafka import KafkaProducer, KafkaClient

producer = KafkaProducer(bootstrap_servers=['kafka:9092'])

data = open('beach-water-quality-automated-sensors-1.csv','r').read().split("\n")
for msg in data:
    producer.send("raw-beach-sensor", msg.encode('utf-8'))
    time.sleep(1)