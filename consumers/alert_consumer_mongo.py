import json
import os
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaConsumer
from pymongo import MongoClient


# ============================================================
# CONFIGURATION
# ============================================================

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

KAFKA_TOPIC = "logistics-alerts"


# ============================================================
# MONGODB CONNECTION
# ============================================================

mongo_client = MongoClient(MONGO_URI)

db = mongo_client[MONGO_DB]

collection = db["logistics_alerts"]


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(

    KAFKA_TOPIC,

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    group_id="logistics-alert-mongo",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


# ============================================================
# HEADER
# ============================================================

print("=" * 75)
print("LOGISTICS ALERT → MONGODB CONSUMER")
print("=" * 75)

print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"MongoDB DB  : {MONGO_DB}")
print("Collection  : logistics_alerts")

print("=" * 75)

print("Consumer started...")
print("=" * 75)


# ============================================================
# CONSUME ALERTS
# ============================================================

try:

    for message in consumer:

        alert = message.value

        # Add MongoDB storage timestamp

        alert["saved_at"] = datetime.now().isoformat()

        # Prevent duplicate storage based on alert_id

        alert_id = alert.get("alert_id")

        existing = collection.find_one(
            {"alert_id": alert_id}
        )

        if existing:

            print(
                f"[SKIPPED] Alert already exists: "
                f"{alert_id}"
            )

            continue


        # Insert alert

        collection.insert_one(alert)


        # Display confirmation

        print("\n" + "-" * 75)

        print("ALERT SAVED TO MONGODB")

        print("-" * 75)

        print(
            f"Alert ID   : "
            f"{alert.get('alert_id')}"
        )

        print(
            f"City       : "
            f"{alert.get('city')}"
        )

        print(
            f"Vehicle    : "
            f"{alert.get('vehicle_id')}"
        )

        print(
            f"Shipment   : "
            f"{alert.get('shipment_id')}"
        )

        print(
            f"Risk Score : "
            f"{alert.get('risk_score')}"
        )

        print(
            f"Risk Level : "
            f"{alert.get('risk_level')}"
        )

        print(
            f"Reasons    : "
            f"{', '.join(alert.get('reasons', []))}"
        )

        print("-" * 75)


except KeyboardInterrupt:

    print(
        "\nAlert MongoDB consumer stopped."
    )


finally:

    consumer.close()

    mongo_client.close()