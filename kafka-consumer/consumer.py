from confluent_kafka import Consumer
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import time
import os

load_dotenv()


def get_mongo_client(uri: str):
    for i in range(10):
        try:
            mongo_client = MongoClient(uri)
            return mongo_client
        except Exception as e:
            print("Mongo retry...", e)
            time.sleep(2)
            raise Exception("Mongo failed")


def get_consumer(consumer_config):
    for i in range(10):
        try:
            consumer = Consumer(consumer_config)
            return consumer
        except Exception as e:
            print("Kafka retry...", e)
            time.sleep(2)
            raise Exception("Kafka failed")





def play_the_consumer():
    host = os.getenv("KAFKA_HOST", "localhost")
    port = os.getenv("KAFKA_PORT", "9092")
    consumer_config = {
        "bootstrap.servers": f"{host}:{port}",
        "group.id": "users-storege",
        "auto.offset.reset": "earliest"
    }
    consumer = get_consumer(consumer_config)
    consumer.subscribe(["users.registered"])
    print("🟢 Consumer is running and subscribed to users topic")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue
            value = msg.value().decode('utf-8')
            user = json.loads(value)
            send_to_mongodb(user)
            print(f"📦 Received user: {user}")
    except KeyboardInterrupt:
        print("\n🔴 Stopping consumer")
    finally:
        consumer.close()


def send_to_mongodb(user):
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongo-database:mongo-users-stor-password@mongodb:27017/")
    mongo_database_name = os.getenv("MONGO_DATABASE_NAME", "users-stor")
    mongo_collection_name = os.getenv("MONGO_COLLECTION_NAME", "users")
    client = get_mongo_client(mongo_uri)
    db = client[mongo_database_name]
    collection = db[mongo_collection_name]
    try:
        collection.insert_one(user)
    except Exception as e:
        raise Exception(f"Mongo Error: {e}")