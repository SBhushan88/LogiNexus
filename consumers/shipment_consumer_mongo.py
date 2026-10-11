import json
import os
from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

# ---------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://localhost:27017/"
)

MONGO_DB = os.getenv(
    "MONGO_DB",
    "sda_course"
)

# ---------------------------------------------------------
# KAFKA CONSUMER
# ---------------------------------------------------------

consumer = KafkaConsumer(
    "shipment-events",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="logistics-shipment-mongo",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)

# ---------------------------------------------------------
# MONGODB CONNECTION
# ---------------------------------------------------------

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DB]

collection = db["logistics_shipments"]

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LOGISTICS SHIPMENT → MONGODB CONSUMER")
print("=" * 60)
print("Kafka Topic : shipment-events")
print(f"MongoDB DB  : {MONGO_DB}")
print("Collection  : logistics_shipments")
print("Status      : LISTENING")
print("=" * 60)

# ---------------------------------------------------------
# CONSUME EVENTS
# ---------------------------------------------------------

try:

    for message in consumer:

        event = message.value

        event["saved_at"] = datetime.now().isoformat()

        collection.insert_one(event)

        print(
            f"[MONGO] {event['shipment_id']} | "
            f"{event['origin']} → {event['destination']} | "
            f"Status: {event['status']} | "
            f"Delay: {event['delay_minutes']} min"
        )

except KeyboardInterrupt:

    print("\nShipment MongoDB consumer stopped.")

finally:

    consumer.close()
    mongo_client.close()