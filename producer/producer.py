import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "ecommerce_events"

def generate_event():
    return {
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 50),
        "event_type": random.choice(["view","add_to_cart","purchase"]),
        "price": round(random.uniform(10.0, 500.0), 2),
        "event_timestamp": datetime.utcnow().isoformat()
    }

try:
    while True:
        event = generate_event()
        producer.send(TOPIC, event)
        producer.flush()
        print(f"Sent event: {event}")
        time.sleep(1)
except KeyboardInterrupt:
    print("Producer stopped")
finally:
    producer.close()