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

KAFKA_TOPIC = "warehouse-events"

# ---------------------------------------------------------
# KAFKA PRODUCER
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------------------------------------
# WAREHOUSE MASTER DATA
# ---------------------------------------------------------

warehouses = [
    {
        "warehouse_id": "WH_DEL_01",
        "city": "Delhi",
        "capacity": 1000
    },
    {
        "warehouse_id": "WH_GUR_01",
        "city": "Gurgaon",
        "capacity": 800
    },
    {
        "warehouse_id": "WH_JAI_01",
        "city": "Jaipur",
        "capacity": 700
    },
    {
        "warehouse_id": "WH_CHD_01",
        "city": "Chandigarh",
        "capacity": 600
    },
    {
        "warehouse_id": "WH_AGR_01",
        "city": "Agra",
        "capacity": 500
    },
    {
        "warehouse_id": "WH_LKO_01",
        "city": "Lucknow",
        "capacity": 750
    }
]

# ---------------------------------------------------------
# EVENT TYPES
# ---------------------------------------------------------

event_types = [
    "LOADING",
    "UNLOADING",
    "SORTING",
    "PACKING",
    "DISPATCH",
    "RECEIVING"
]

# ---------------------------------------------------------
# EVENT COUNTER
# ---------------------------------------------------------

event_counter = 1

# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LOGISTICS WAREHOUSE STREAMING PRODUCER")
print("=" * 60)
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"Warehouses  : {len(warehouses)}")
print("Frequency   : Continuous")
print("Status      : STREAMING")
print("=" * 60)

# ---------------------------------------------------------
# STREAMING LOOP
# ---------------------------------------------------------

try:

    while True:

        warehouse = random.choice(warehouses)

        event_type = random.choice(event_types)

        # Current warehouse utilization
        utilization = random.randint(30, 98)

        # Number of active workers
        active_workers = random.randint(10, 60)

        # Number of packages processed
        packages_processed = random.randint(5, 80)

        # Processing time
        processing_time = random.randint(5, 45)

        # -------------------------------------------------
        # CONGESTION LOGIC
        # -------------------------------------------------

        if utilization >= 90:

            congestion_level = "HIGH"

        elif utilization >= 70:

            congestion_level = "MEDIUM"

        else:

            congestion_level = "LOW"

        # -------------------------------------------------
        # CREATE EVENT
        # -------------------------------------------------

        event = {
            "event_id": f"WH{event_counter:06d}",
            "warehouse_id": warehouse["warehouse_id"],
            "city": warehouse["city"],
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "utilization_percent": utilization,
            "capacity": warehouse["capacity"],
            "active_workers": active_workers,
            "packages_processed": packages_processed,
            "processing_time_minutes": processing_time,
            "congestion_level": congestion_level
        }

        # -------------------------------------------------
        # SEND TO KAFKA
        # -------------------------------------------------

        producer.send(
            KAFKA_TOPIC,
            key=warehouse["warehouse_id"].encode("utf-8"),
            value=event
        )

        producer.flush()

        # -------------------------------------------------
        # TERMINAL OUTPUT
        # -------------------------------------------------

        print(
            f"[WAREHOUSE] "
            f"{warehouse['warehouse_id']} | "
            f"{warehouse['city']} | "
            f"{event_type} | "
            f"Utilization: {utilization}% | "
            f"Congestion: {congestion_level}"
        )

        event_counter += 1

        # One event every second
        time.sleep(1)

except KeyboardInterrupt:

    print("\nWarehouse producer stopped.")

finally:

    producer.close()