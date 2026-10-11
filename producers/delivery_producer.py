import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

# ---------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = "delivery-events"

# ---------------------------------------------------------
# KAFKA PRODUCER
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------------------------------------
# DELIVERY MASTER DATA
# ---------------------------------------------------------

deliveries = [
    {
        "shipment_id": "SH10001",
        "vehicle_id": "VH001",
        "route_id": "RT_DEL_JAI_01",
        "origin": "Delhi",
        "destination": "Jaipur"
    },
    {
        "shipment_id": "SH10002",
        "vehicle_id": "VH002",
        "route_id": "RT_DEL_CHD_01",
        "origin": "Delhi",
        "destination": "Chandigarh"
    },
    {
        "shipment_id": "SH10003",
        "vehicle_id": "VH003",
        "route_id": "RT_GUR_JAI_01",
        "origin": "Gurgaon",
        "destination": "Jaipur"
    },
    {
        "shipment_id": "SH10004",
        "vehicle_id": "VH004",
        "route_id": "RT_DEL_AGR_01",
        "origin": "Delhi",
        "destination": "Agra"
    },
    {
        "shipment_id": "SH10005",
        "vehicle_id": "VH005",
        "route_id": "RT_DEL_LKO_01",
        "origin": "Delhi",
        "destination": "Lucknow"
    },
    {
        "shipment_id": "SH10006",
        "vehicle_id": "VH001",
        "route_id": "RT_DEL_JAI_01",
        "origin": "Delhi",
        "destination": "Jaipur"
    },
    {
        "shipment_id": "SH10007",
        "vehicle_id": "VH002",
        "route_id": "RT_DEL_CHD_01",
        "origin": "Delhi",
        "destination": "Chandigarh"
    },
    {
        "shipment_id": "SH10008",
        "vehicle_id": "VH003",
        "route_id": "RT_GUR_JAI_01",
        "origin": "Gurgaon",
        "destination": "Jaipur"
    },
    {
        "shipment_id": "SH10009",
        "vehicle_id": "VH004",
        "route_id": "RT_DEL_AGR_01",
        "origin": "Delhi",
        "destination": "Agra"
    },
    {
        "shipment_id": "SH10010",
        "vehicle_id": "VH005",
        "route_id": "RT_DEL_LKO_01",
        "origin": "Delhi",
        "destination": "Lucknow"
    }
]

# ---------------------------------------------------------
# DELIVERY OUTCOMES
# ---------------------------------------------------------

delivery_outcomes = [
    "DELIVERED",
    "DELIVERED",
    "DELIVERED",
    "DELIVERED",
    "DELAYED",
    "FAILED"
]

# ---------------------------------------------------------
# EVENT COUNTER
# ---------------------------------------------------------

event_counter = 1

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LOGISTICS DELIVERY STREAMING PRODUCER")
print("=" * 60)
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"Deliveries  : {len(deliveries)}")
print("Frequency   : Continuous")
print("Status      : STREAMING")
print("=" * 60)

# ---------------------------------------------------------
# STREAMING LOOP
# ---------------------------------------------------------

try:

    while True:

        delivery = random.choice(deliveries)

        outcome = random.choice(delivery_outcomes)

        # -------------------------------------------------
        # DELIVERY TIME
        # -------------------------------------------------

        delivery_time = random.randint(30, 360)

        # -------------------------------------------------
        # DELAY LOGIC
        # -------------------------------------------------

        if outcome == "DELAYED":

            delay_minutes = random.randint(15, 180)

        elif outcome == "FAILED":

            delay_minutes = random.randint(30, 240)

        else:

            delay_minutes = 0

        # -------------------------------------------------
        # DELIVERY ATTEMPT
        # -------------------------------------------------

        if outcome == "FAILED":

            attempt_number = random.randint(1, 3)

        else:

            attempt_number = 1

        # -------------------------------------------------
        # FAILURE REASON
        # -------------------------------------------------

        if outcome == "FAILED":

            failure_reason = random.choice([
                "CUSTOMER_UNAVAILABLE",
                "ADDRESS_ISSUE",
                "VEHICLE_ISSUE",
                "DOCUMENTATION_ISSUE"
            ])

        else:

            failure_reason = None

        # -------------------------------------------------
        # CREATE EVENT
        # -------------------------------------------------

        event = {
            "event_id": f"DEL{event_counter:06d}",
            "shipment_id": delivery["shipment_id"],
            "vehicle_id": delivery["vehicle_id"],
            "route_id": delivery["route_id"],
            "timestamp": datetime.now().isoformat(),
            "origin": delivery["origin"],
            "destination": delivery["destination"],
            "outcome": outcome,
            "delivery_time_minutes": delivery_time,
            "delay_minutes": delay_minutes,
            "attempt_number": attempt_number,
            "failure_reason": failure_reason
        }

        # -------------------------------------------------
        # SEND TO KAFKA
        # -------------------------------------------------

        producer.send(
            KAFKA_TOPIC,
            key=delivery["shipment_id"].encode("utf-8"),
            value=event
        )

        producer.flush()

        # -------------------------------------------------
        # TERMINAL OUTPUT
        # -------------------------------------------------

        print(
            f"[DELIVERY] "
            f"{delivery['shipment_id']} | "
            f"{delivery['origin']} → {delivery['destination']} | "
            f"Outcome: {outcome} | "
            f"Delay: {delay_minutes} min | "
            f"Time: {delivery_time} min"
        )

        event_counter += 1

        time.sleep(1)

except KeyboardInterrupt:

    print("\nDelivery producer stopped.")

finally:

    producer.close()