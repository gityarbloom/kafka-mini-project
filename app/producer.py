from confluent_kafka import Producer
from dotenv import load_dotenv
from models import *
import json
import uuid
import os

load_dotenv()

_producer = None

def get_producer():
    global _producer
    if _producer is None:
        host = os.getenv("KAFKA_HOST")
        port = os.getenv("KAFKA_PORT")
        producer_config = {"bootstrap.servers": f"{host}:{port}"}
        _producer = Producer(producer_config)
    return _producer

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode('utf-8')}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

def send_to_kafka(user: User):
    user.user_id = str(uuid.uuid4())

    user.create_at = datetime.now().isoformat()
    value = json.dumps(user.model_dump()).encode('utf-8')
    producer = get_producer()
    producer.produce(topic="users.registered", value=value, callback=delivery_report)
    producer.poll(0)
    producer.flush()