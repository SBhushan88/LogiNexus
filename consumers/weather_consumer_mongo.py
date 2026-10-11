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
    "weather-events",

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    group_id="logistics-weather-mongo",

    auto_offset_reset="earliest",

    enable_auto_commit=True,

    value_deserializer=lambda x:
        json.loads(
            x.decode("utf-8")
        )
)


# ---------------------------------------------------------
# MONGODB
# ---------------------------------------------------------

mongo_client = MongoClient(
    MONGO_URI
)

db = mongo_client[
    MONGO_DB
]

collection = db[
    "logistics_weather"
]


# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LOGISTICS WEATHER → MONGODB CONSUMER")
print("=" * 60)

print("Kafka Topic : weather-events")

print(
    f"MongoDB DB  : {MONGO_DB}"
)

print(
    "Collection  : logistics_weather"
)

print("Status      : LISTENING")

print("=" * 60)


# ---------------------------------------------------------
# CONSUME EVENTS
# ---------------------------------------------------------

try:

    for message in consumer:

        event = message.value

        event["saved_at"] = (
            datetime.now().isoformat()
        )

        collection.insert_one(
            event
        )

        print(
            f"[MONGO] "
            f"{event['city']} | "
            f"{event['weather_condition']} | "
            f"Rain: "
            f"{event['precipitation_mm']} mm | "
            f"Wind: "
            f"{event['wind_speed_kmph']} km/h | "
            f"Impact: "
            f"{event['impact_level']}"
        )


except KeyboardInterrupt:

    print(
        "\nWeather MongoDB consumer stopped."
    )


finally:

    consumer.close()

    mongo_client.close()