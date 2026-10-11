import json
import os
import random
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = "vehicle-location"


# ============================================================
# VEHICLE MASTER DATA
# ============================================================

VEHICLES = [
    {
        "vehicle_id": "VH001",
        "city": "Delhi",
        "route_id": "RT_DEL_JAI_01",
        "start_lat": 28.6139,
        "start_lon": 77.2090
    },
    {
        "vehicle_id": "VH002",
        "city": "Delhi",
        "route_id": "RT_DEL_CHD_01",
        "start_lat": 28.6139,
        "start_lon": 77.2090
    },
    {
        "vehicle_id": "VH003",
        "city": "Gurgaon",
        "route_id": "RT_GUR_JAI_01",
        "start_lat": 28.4595,
        "start_lon": 77.0266
    },
    {
        "vehicle_id": "VH004",
        "city": "Delhi",
        "route_id": "RT_DEL_AGR_01",
        "start_lat": 28.6139,
        "start_lon": 77.2090
    },
    {
        "vehicle_id": "VH005",
        "city": "Delhi",
        "route_id": "RT_DEL_LKO_01",
        "start_lat": 28.6139,
        "start_lon": 77.2090
    }
]


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    key_serializer=lambda key: key.encode("utf-8")
)


# ============================================================
# START MESSAGE
# ============================================================

print("=" * 60)
print("LOGISTICS VEHICLE STREAMING PRODUCER")
print("=" * 60)
print(f"Kafka Server : {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Kafka Topic  : {KAFKA_TOPIC}")
print(f"Vehicles     : {len(VEHICLES)}")
print("Status       : STREAMING")
print("=" * 60)


# ============================================================
# EVENT COUNTER
# ============================================================

event_counter = 1


# ============================================================
# CONTINUOUS STREAM
# ============================================================

try:

    while True:

        for vehicle in VEHICLES:

            # ------------------------------------------------
            # EVENT ID
            # ------------------------------------------------

            event_id = f"VEH{event_counter:06d}"

            event_counter += 1

            # ------------------------------------------------
            # TIMESTAMP
            # ------------------------------------------------

            timestamp = datetime.now().isoformat()

            # ------------------------------------------------
            # SIMULATED GPS MOVEMENT
            # ------------------------------------------------

            latitude = (
                vehicle["start_lat"]
                + random.uniform(-0.05, 0.05)
            )

            longitude = (
                vehicle["start_lon"]
                + random.uniform(-0.05, 0.05)
            )

            # ------------------------------------------------
            # VEHICLE STATUS
            # ------------------------------------------------

            status = random.choices(
                ["IN_TRANSIT", "IDLE", "LOADING"],
                weights=[80, 10, 10],
                k=1
            )[0]

            # ------------------------------------------------
            # SPEED
            # ------------------------------------------------

            if status == "IDLE":

                speed = 0

            elif status == "LOADING":

                speed = round(
                    random.uniform(0, 35),
                    2
                )

            else:

                speed = round(
                    random.uniform(20, 80),
                    2
                )

            # ------------------------------------------------
            # EVENT
            # ------------------------------------------------

            event = {
                "event_id": event_id,
                "vehicle_id": vehicle["vehicle_id"],
                "city": vehicle["city"],
                "timestamp": timestamp,
                "latitude": round(latitude, 6),
                "longitude": round(longitude, 6),
                "speed_kmph": speed,
                "status": status,
                "route_id": vehicle["route_id"]
            }

            # ------------------------------------------------
            # SEND TO KAFKA
            # ------------------------------------------------

            producer.send(
                KAFKA_TOPIC,
                key=vehicle["vehicle_id"],
                value=event
            )

            producer.flush()

            # ------------------------------------------------
            # TERMINAL OUTPUT
            # ------------------------------------------------

            print(
                f"{event_id} | "
                f"{vehicle['vehicle_id']} | "
                f"{vehicle['city']} | "
                f"{status} | "
                f"{speed} km/h | "
                f"{timestamp}"
            )

        # ----------------------------------------------------
        # WAIT 2 SECONDS BEFORE NEXT ROUND
        # ----------------------------------------------------

        time.sleep(2)


except KeyboardInterrupt:

    print("\nVehicle producer stopped.")

finally:

    producer.close()