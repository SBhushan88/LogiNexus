import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

VEHICLE_TOPIC = "vehicle-location"
SHIPMENT_TOPIC = "shipment-events"
WAREHOUSE_TOPIC = "warehouse-events"
DELIVERY_TOPIC = "delivery-events"
WEATHER_TOPIC = "weather-events"
ALERT_TOPIC = "logistics-alerts"


# ============================================================
# UTC TIMESTAMP
# ============================================================

def current_utc_timestamp():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# RISK ENGINE
# ============================================================

def calculate_risk_score(
    shipment_delayed=False,
    heavy_weather=False,
    vehicle_idle=False,
    route_deviation=False,
    low_vehicle_speed=False,
    warehouse_congested=False
):

    score = 0
    reasons = []

    if shipment_delayed:
        score += 30
        reasons.append("Shipment delayed")

    if heavy_weather:
        score += 25
        reasons.append("Severe weather")

    if vehicle_idle:
        score += 20
        reasons.append("Vehicle idle")

    if route_deviation:
        score += 20
        reasons.append("Route deviation")

    if low_vehicle_speed:
        score += 15
        reasons.append("Low vehicle speed")

    if warehouse_congested:
        score += 15
        reasons.append("Warehouse congestion")

    score = min(score, 100)

    if score <= 30:
        risk_level = "LOW"
    elif score <= 60:
        risk_level = "MEDIUM"
    else:
        risk_level = "HIGH"

    return {
        "risk_score": score,
        "risk_level": risk_level,
        "reasons": reasons
    }


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    VEHICLE_TOPIC,
    SHIPMENT_TOPIC,
    WAREHOUSE_TOPIC,
    DELIVERY_TOPIC,
    WEATHER_TOPIC,

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    # LIVE CONSUMER GROUP
    group_id="logistics-unified-processor-live-v3",

    # ONLY NEW EVENTS
    auto_offset_reset="latest",

    enable_auto_commit=True,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


# ============================================================
# KAFKA PRODUCER
# ============================================================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# ============================================================
# STATE
# ============================================================

vehicles = {}
shipments = {}
warehouses = {}
deliveries = {}
weather = {}

alert_counter = 1

alert_cooldown = {}

COOLDOWN_SECONDS = 60


# ============================================================
# CITY RESOLUTION
# ============================================================

def resolve_city(event):

    # 1. Direct city
    if event.get("city"):
        return event.get("city")

    # 2. Vehicle → city
    vehicle_id = event.get("vehicle_id")

    if vehicle_id and vehicle_id in vehicles:

        vehicle = vehicles[vehicle_id]

        if vehicle.get("city"):
            return vehicle.get("city")

    # 3. Shipment → vehicle → city
    shipment_id = event.get("shipment_id")

    if shipment_id and shipment_id in shipments:

        shipment = shipments[shipment_id]

        shipment_vehicle = shipment.get("vehicle_id")

        if shipment_vehicle and shipment_vehicle in vehicles:

            vehicle = vehicles[shipment_vehicle]

            if vehicle.get("city"):
                return vehicle.get("city")

    # 4. Route → vehicle → city
    route_id = event.get("route_id")

    if route_id:

        for vehicle in vehicles.values():

            if vehicle.get("route_id") == route_id:

                if vehicle.get("city"):
                    return vehicle.get("city")

    # 5. Warehouse → city
    warehouse_id = event.get("warehouse_id")

    if warehouse_id and warehouse_id in warehouses:

        warehouse = warehouses[warehouse_id]

        if warehouse.get("city"):
            return warehouse.get("city")

    # 6. Fallback
    return "UNKNOWN"


# ============================================================
# FIND SHIPMENT
# ============================================================

def find_shipment(event):

    shipment_id = event.get("shipment_id")

    if shipment_id:

        shipment_event = shipments.get(shipment_id)

        if shipment_event:

            return shipment_id, shipment_event

    vehicle_id = event.get("vehicle_id")

    if vehicle_id:

        for sid, shipment_event in reversed(
            list(shipments.items())
        ):

            if shipment_event.get("vehicle_id") == vehicle_id:

                return sid, shipment_event

    return None, None


# ============================================================
# ALERT GENERATION
# ============================================================

def generate_alert(
    city,
    vehicle_id,
    shipment_id,
    risk_result
):

    global alert_counter

    risk_score = risk_result["risk_score"]
    risk_level = risk_result["risk_level"]
    reasons = risk_result["reasons"]

    # Only MEDIUM and HIGH alerts
    if risk_level == "LOW":
        return

    alert_key = (
        f"{city}|"
        f"{vehicle_id}|"
        f"{shipment_id}|"
        f"{risk_level}"
    )

    current_time = time.time()

    if alert_key in alert_cooldown:

        last_time = alert_cooldown[alert_key]

        if current_time - last_time < COOLDOWN_SECONDS:
            return

    alert_cooldown[alert_key] = current_time

    alert_id = f"ALT{alert_counter:06d}"

    alert_counter += 1

    alert = {
        "alert_id": alert_id,

        # IMPORTANT:
        # Store alert creation time in UTC
        "timestamp": current_utc_timestamp(),

        "city": city,
        "vehicle_id": vehicle_id,
        "shipment_id": shipment_id,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "alert_type": "HIGH_LOGISTICS_RISK",
        "reasons": reasons
    }

    producer.send(
        ALERT_TOPIC,
        key=alert_id.encode("utf-8"),
        value=alert
    )

    producer.flush()

    print()
    print("=" * 70)
    print("LOGISTICS ALERT GENERATED")
    print("=" * 70)

    print(f"Alert ID     : {alert_id}")
    print(f"Timestamp UTC: {alert['timestamp']}")
    print(f"City         : {city}")
    print(f"Vehicle      : {vehicle_id}")
    print(f"Shipment     : {shipment_id}")
    print(f"Risk Score   : {risk_score}")
    print(f"Risk Level   : {risk_level}")
    print(f"Reasons      : {', '.join(reasons)}")

    print("=" * 70)
    print()


# ============================================================
# PROCESS VEHICLE
# ============================================================

def process_vehicle(event):

    vehicle_id = event.get("vehicle_id")

    if vehicle_id:
        vehicles[vehicle_id] = event

    city = resolve_city(event)

    speed = event.get("speed_kmph", 0)

    status = event.get("status", "")

    vehicle_idle = status == "IDLE"

    low_vehicle_speed = (
        speed is not None
        and speed < 15
    )

    shipment_id, shipment = find_shipment(event)

    shipment_delayed = False

    if shipment:

        shipment_delayed = (
            shipment.get("status") == "DELAYED"
            or shipment.get("delay_minutes", 0) > 30
        )

    weather_data = weather.get(city, {})

    heavy_weather = (
        weather_data.get("impact_level") == "HIGH"
        or weather_data.get("weather_condition")
        in ["THUNDERSTORM", "HEAVY_RAIN"]
    )

    warehouse_congested = False

    for warehouse in warehouses.values():

        if warehouse.get("city") == city:

            if warehouse.get("congestion") == "HIGH":

                warehouse_congested = True
                break

    risk_result = calculate_risk_score(
        shipment_delayed=shipment_delayed,
        heavy_weather=heavy_weather,
        vehicle_idle=vehicle_idle,
        route_deviation=False,
        low_vehicle_speed=low_vehicle_speed,
        warehouse_congested=warehouse_congested
    )

    print(
        f"[VEHICLE] "
        f"Vehicle: {vehicle_id} | "
        f"City: {city} | "
        f"Speed: {speed} km/h | "
        f"Status: {status} | "
        f"Risk: {risk_result['risk_score']} "
        f"({risk_result['risk_level']})"
    )

    generate_alert(
        city,
        vehicle_id,
        shipment_id,
        risk_result
    )


# ============================================================
# PROCESS SHIPMENT
# ============================================================

def process_shipment(event):

    shipment_id = event.get("shipment_id")

    if shipment_id:
        shipments[shipment_id] = event

    city = resolve_city(event)

    vehicle_id = event.get("vehicle_id")

    shipment_delayed = (
        event.get("status") == "DELAYED"
        or event.get("delay_minutes", 0) > 30
    )

    vehicle = vehicles.get(vehicle_id, {})

    vehicle_idle = (
        vehicle.get("status") == "IDLE"
    )

    speed = vehicle.get("speed_kmph", 0)

    low_vehicle_speed = (
        speed is not None
        and speed < 15
    )

    weather_data = weather.get(city, {})

    heavy_weather = (
        weather_data.get("impact_level") == "HIGH"
        or weather_data.get("weather_condition")
        in ["THUNDERSTORM", "HEAVY_RAIN"]
    )

    warehouse_congested = False

    for warehouse in warehouses.values():

        if warehouse.get("city") == city:

            if warehouse.get("congestion") == "HIGH":

                warehouse_congested = True
                break

    risk_result = calculate_risk_score(
        shipment_delayed=shipment_delayed,
        heavy_weather=heavy_weather,
        vehicle_idle=vehicle_idle,
        route_deviation=False,
        low_vehicle_speed=low_vehicle_speed,
        warehouse_congested=warehouse_congested
    )

    print(
        f"[SHIPMENT] "
        f"City: {city} | "
        f"Vehicle: {vehicle_id} | "
        f"Shipment: {shipment_id} | "
        f"Risk: {risk_result['risk_score']} "
        f"({risk_result['risk_level']})"
    )

    generate_alert(
        city,
        vehicle_id,
        shipment_id,
        risk_result
    )


# ============================================================
# PROCESS WAREHOUSE
# ============================================================

def process_warehouse(event):

    warehouse_id = event.get("warehouse_id")

    if warehouse_id:
        warehouses[warehouse_id] = event

    city = resolve_city(event)

    warehouse_congested = (
        event.get("congestion") == "HIGH"
    )

    risk_result = calculate_risk_score(
        warehouse_congested=warehouse_congested
    )

    print(
        f"[WAREHOUSE] "
        f"City: {city} | "
        f"Warehouse: {warehouse_id} | "
        f"Risk: {risk_result['risk_score']} "
        f"({risk_result['risk_level']})"
    )

    generate_alert(
        city,
        event.get("vehicle_id"),
        event.get("shipment_id"),
        risk_result
    )


# ============================================================
# PROCESS DELIVERY
# ============================================================

def process_delivery(event):

    shipment_id = event.get("shipment_id")

    if shipment_id:
        deliveries[shipment_id] = event

    city = resolve_city(event)

    vehicle_id = event.get("vehicle_id")

    shipment = shipments.get(shipment_id, {})

    shipment_delayed = (
        event.get("outcome") == "DELAYED"
        or event.get("delay_minutes", 0) > 30
        or shipment.get("status") == "DELAYED"
    )

    vehicle = vehicles.get(vehicle_id, {})

    vehicle_idle = (
        vehicle.get("status") == "IDLE"
    )

    speed = vehicle.get("speed_kmph", 0)

    low_vehicle_speed = (
        speed is not None
        and speed < 15
    )

    weather_data = weather.get(city, {})

    heavy_weather = (
        weather_data.get("impact_level") == "HIGH"
        or weather_data.get("weather_condition")
        in ["THUNDERSTORM", "HEAVY_RAIN"]
    )

    risk_result = calculate_risk_score(
        shipment_delayed=shipment_delayed,
        heavy_weather=heavy_weather,
        vehicle_idle=vehicle_idle,
        route_deviation=False,
        low_vehicle_speed=low_vehicle_speed,
        warehouse_congested=False
    )

    print(
        f"[DELIVERY] "
        f"City: {city} | "
        f"Vehicle: {vehicle_id} | "
        f"Shipment: {shipment_id} | "
        f"Risk: {risk_result['risk_score']} "
        f"({risk_result['risk_level']})"
    )

    generate_alert(
        city,
        vehicle_id,
        shipment_id,
        risk_result
    )


# ============================================================
# PROCESS WEATHER
# ============================================================

def process_weather(event):

    city = event.get("city")

    if city:
        weather[city] = event

    city = resolve_city(event)

    heavy_weather = (
        event.get("impact_level") == "HIGH"
        or event.get("weather_condition")
        in ["THUNDERSTORM", "HEAVY_RAIN"]
    )

    risk_result = calculate_risk_score(
        heavy_weather=heavy_weather
    )

    print(
        f"[WEATHER] "
        f"City: {city} | "
        f"Condition: {event.get('weather_condition')} | "
        f"Impact: {event.get('impact_level')} | "
        f"Risk: {risk_result['risk_score']} "
        f"({risk_result['risk_level']})"
    )

    generate_alert(
        city,
        None,
        None,
        risk_result
    )


# ============================================================
# STARTUP
# ============================================================

print("=" * 80)
print("REAL-TIME LOGISTICS UNIFIED STREAM PROCESSOR")
print("=" * 80)
print(f"Vehicle   : {VEHICLE_TOPIC}")
print(f"Shipment  : {SHIPMENT_TOPIC}")
print(f"Warehouse : {WAREHOUSE_TOPIC}")
print(f"Delivery  : {DELIVERY_TOPIC}")
print(f"Weather   : {WEATHER_TOPIC}")
print(f"Alerts    : {ALERT_TOPIC}")
print("=" * 80)
print("Consumer Group : logistics-unified-processor-live-v3")
print("Offset Mode    : latest")
print("Timestamp      : UTC")
print("=" * 80)
print("Processor started successfully...")
print("=" * 80)


# ============================================================
# MAIN LOOP
# ============================================================

try:

    for message in consumer:

        topic = message.topic
        event = message.value

        print(
            f"[RECEIVED] "
            f"Topic: {topic} | "
            f"Partition: {message.partition} | "
            f"Offset: {message.offset}"
        )

        try:

            if topic == VEHICLE_TOPIC:
                process_vehicle(event)

            elif topic == SHIPMENT_TOPIC:
                process_shipment(event)

            elif topic == WAREHOUSE_TOPIC:
                process_warehouse(event)

            elif topic == DELIVERY_TOPIC:
                process_delivery(event)

            elif topic == WEATHER_TOPIC:
                process_weather(event)

        except Exception as e:

            print()
            print("[PROCESSING ERROR]")
            print(f"Topic : {topic}")
            print(f"Error : {e}")
            print(f"Event : {event}")
            print()

except KeyboardInterrupt:

    print()
    print("Processor stopped by user.")

finally:

    consumer.close()
    producer.close()

    print("Kafka consumer and producer closed.")