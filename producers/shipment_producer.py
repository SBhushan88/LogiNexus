import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer


# -----------------------------------------
# KAFKA CONFIGURATION
# -----------------------------------------

KAFKA_TOPIC = "shipment-events"
KAFKA_SERVER = "localhost:9092"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)


# -----------------------------------------
# INITIAL SHIPMENTS
# -----------------------------------------

shipments = [
    {
        "shipment_id": "SH10001",
        "vehicle_id": "VH001",
        "origin": "Delhi",
        "destination": "Jaipur",
        "route_id": "RT_DEL_JAI_01",
        "priority": "STANDARD"
    },
    {
        "shipment_id": "SH10002",
        "vehicle_id": "VH002",
        "origin": "Delhi",
        "destination": "Chandigarh",
        "route_id": "RT_DEL_CHD_01",
        "priority": "HIGH"
    },
    {
        "shipment_id": "SH10003",
        "vehicle_id": "VH003",
        "origin": "Gurgaon",
        "destination": "Jaipur",
        "route_id": "RT_GUR_JAI_01",
        "priority": "STANDARD"
    },
    {
        "shipment_id": "SH10004",
        "vehicle_id": "VH004",
        "origin": "Delhi",
        "destination": "Agra",
        "route_id": "RT_DEL_AGR_01",
        "priority": "EXPRESS"
    },
    {
        "shipment_id": "SH10005",
        "vehicle_id": "VH005",
        "origin": "Delhi",
        "destination": "Lucknow",
        "route_id": "RT_DEL_LKO_01",
        "priority": "HIGH"
    },
    {
        "shipment_id": "SH10006",
        "vehicle_id": "VH001",
        "origin": "Delhi",
        "destination": "Jaipur",
        "route_id": "RT_DEL_JAI_01",
        "priority": "HIGH"
    },
    {
        "shipment_id": "SH10007",
        "vehicle_id": "VH002",
        "origin": "Delhi",
        "destination": "Chandigarh",
        "route_id": "RT_DEL_CHD_01",
        "priority": "STANDARD"
    },
    {
        "shipment_id": "SH10008",
        "vehicle_id": "VH003",
        "origin": "Gurgaon",
        "destination": "Jaipur",
        "route_id": "RT_GUR_JAI_01",
        "priority": "EXPRESS"
    },
    {
        "shipment_id": "SH10009",
        "vehicle_id": "VH004",
        "origin": "Delhi",
        "destination": "Agra",
        "route_id": "RT_DEL_AGR_01",
        "priority": "STANDARD"
    },
    {
        "shipment_id": "SH10010",
        "vehicle_id": "VH005",
        "origin": "Delhi",
        "destination": "Lucknow",
        "route_id": "RT_DEL_LKO_01",
        "priority": "EXPRESS"
    }
]


# -----------------------------------------
# SHIPMENT STATE
# -----------------------------------------

shipment_states = {}

next_shipment_number = 10011
event_counter = 1


# -----------------------------------------
# STATUS TRANSITIONS
# -----------------------------------------

def get_next_status(current_status):

    if current_status == "PICKED_UP":
        return random.choices(
            ["IN_TRANSIT", "AT_WAREHOUSE"],
            weights=[85, 15]
        )[0]

    elif current_status == "IN_TRANSIT":
        return random.choices(
            ["IN_TRANSIT", "OUT_FOR_DELIVERY", "DELAYED"],
            weights=[70, 20, 10]
        )[0]

    elif current_status == "AT_WAREHOUSE":
        return random.choices(
            ["IN_TRANSIT", "DELAYED"],
            weights=[85, 15]
        )[0]

    elif current_status == "OUT_FOR_DELIVERY":
        return random.choices(
            ["OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"],
            weights=[45, 45, 10]
        )[0]

    elif current_status == "DELAYED":
        return random.choices(
            ["DELAYED", "IN_TRANSIT", "OUT_FOR_DELIVERY"],
            weights=[45, 35, 20]
        )[0]

    return current_status


# -----------------------------------------
# CREATE NEW SHIPMENT
# -----------------------------------------

def create_new_shipment():

    global next_shipment_number

    template = random.choice(shipments)

    new_shipment = {
        "shipment_id": f"SH{next_shipment_number}",
        "vehicle_id": template["vehicle_id"],
        "origin": template["origin"],
        "destination": template["destination"],
        "route_id": template["route_id"],
        "priority": random.choice(
            ["STANDARD", "HIGH", "EXPRESS"]
        )
    }

    next_shipment_number += 1

    return new_shipment


# -----------------------------------------
# INITIALIZE 10 ACTIVE SHIPMENTS
# -----------------------------------------

for shipment in shipments:

    shipment_states[shipment["shipment_id"]] = {
        **shipment,
        "status": "PICKED_UP"
    }


# -----------------------------------------
# MAIN STREAM
# -----------------------------------------

while True:

    active_ids = list(shipment_states.keys())

    for shipment_id in active_ids:

        shipment = shipment_states[shipment_id]

        current_status = shipment["status"]

        # ---------------------------------
        # DELIVERED SHIPMENT
        # ---------------------------------

        if current_status == "DELIVERED":

            print(
                f"[DELIVERED] {shipment_id} completed. "
                f"Creating a new shipment..."
            )

            new_shipment = create_new_shipment()

            shipment_states[new_shipment["shipment_id"]] = {
                **new_shipment,
                "status": "PICKED_UP"
            }

            del shipment_states[shipment_id]

            continue

        # ---------------------------------
        # GENERATE NEXT STATUS
        # ---------------------------------

        new_status = get_next_status(current_status)

        shipment["status"] = new_status

        # ---------------------------------
        # DELAY
        # ---------------------------------

        if new_status == "DELAYED":
            delay_minutes = random.randint(15, 120)
        else:
            delay_minutes = 0

        # ---------------------------------
        # EXPECTED DELIVERY
        # ---------------------------------

        expected_delivery = (
            datetime.now()
            + timedelta(hours=random.randint(2, 8))
        )

        # ---------------------------------
        # EVENT
        # ---------------------------------

        event = {
            "event_id": f"SHP{event_counter:06d}",
            "shipment_id": shipment["shipment_id"],
            "vehicle_id": shipment["vehicle_id"],
            "origin": shipment["origin"],
            "destination": shipment["destination"],
            "route_id": shipment["route_id"],
            "priority": shipment["priority"],
            "status": new_status,
            "delay_minutes": delay_minutes,
            "expected_delivery": expected_delivery.isoformat(),
            "timestamp": datetime.now().isoformat()
        }

        event_counter += 1

        # ---------------------------------
        # SEND TO KAFKA
        # ---------------------------------

        producer.send(
            KAFKA_TOPIC,
            key=shipment["shipment_id"],
            value=event
        )

        producer.flush()

        print(
            f"[SHIPMENT] "
            f"{shipment['shipment_id']} | "
            f"{shipment['origin']} → {shipment['destination']} | "
            f"Status: {new_status} | "
            f"Delay: {delay_minutes} min"
        )

    time.sleep(0.5)