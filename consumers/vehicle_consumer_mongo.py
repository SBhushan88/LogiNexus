import json
from datetime import datetime, timezone

from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv
import os


# ==============================
# LOAD ENVIRONMENT VARIABLES
# ==============================

load_dotenv()

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")


# ==============================
# KAFKA CONFIGURATION
# ==============================

KAFKA_TOPIC = "vehicle-location"
KAFKA_GROUP = "logistics-vehicle-mongo"


# ==============================
# MONGODB CONNECTION
# ==============================

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DB]

collection = db["logistics_vehicle_events"]


# ==============================
# KAFKA CONSUMER
# ==============================

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id=KAFKA_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


# ==============================
# START CONSUMER
# ==============================

print("=" * 60)
print("LOGISTICS VEHICLE → MONGODB CONSUMER")
print("=" * 60)
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"MongoDB DB  : {MONGO_DB}")
print("Collection  : logistics_vehicle_events")
print("Status      : LISTENING")
print("=" * 60)


try:

    for message in consumer:

        event = message.value

        # Add the time at which MongoDB received the event
        event["saved_at"] = datetime.now(timezone.utc).isoformat()

        # Save event to MongoDB
        collection.insert_one(event)

        print(
            f"[MONGO] "
            f"{event['vehicle_id']} | "
            f"Speed: {event['speed_kmph']} km/h | "
            f"Status: {event['status']} | "
            f"Route: {event['route_id']}"
        )


except KeyboardInterrupt:

    print("\nStopping MongoDB consumer...")


finally:

    consumer.close()
    mongo_client.close()

    print("MongoDB consumer stopped.")