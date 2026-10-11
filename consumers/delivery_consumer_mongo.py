import json
import os
from datetime import datetime

from kafka import KafkaConsumer
from pymongo import MongoClient
from dotenv import load_dotenv

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
    "delivery-events",
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    group_id="logistics-delivery-mongo",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)

# ---------------------------------------------------------
# MONGODB
# ---------------------------------------------------------

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DB]

collection = db["logistics_delivery_events"]

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LOGISTICS DELIVERY → MONGODB CONSUMER")
print("=" * 60)
print("Kafka Topic : delivery-events")
print(f"MongoDB DB  : {MONGO_DB}")
print("Collection  : logistics_delivery_events")
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
            f"[MONGO] "
            f"{event['shipment_id']} | "
            f"{event['origin']} → {event['destination']} | "
            f"Outcome: {event['outcome']} | "
            f"Delay: {event['delay_minutes']} min"
        )

except KeyboardInterrupt:

    print("\nDelivery MongoDB consumer stopped.")

finally:

    consumer.close()
    mongo_client.close()