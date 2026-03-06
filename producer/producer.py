import json
import time
import random
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers="kafka:29092",
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka", flush=True)
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready yet, retrying in 5 seconds...", flush=True)
            time.sleep(5)


producer = create_producer()

countries = ["FR", "US", "DE", "UK", "ES"]
event_types = ["purchase", "click", "view"]

while True:
    event = {
        "event_time": datetime.utcnow().isoformat(),
        "country": random.choice(countries),
        "amount": round(random.uniform(5, 200), 2),
        "event_type": random.choice(event_types)
    }

    producer.send("events", event)
    producer.flush()

    print(f"sent event: {event}", flush=True)

    time.sleep(1)