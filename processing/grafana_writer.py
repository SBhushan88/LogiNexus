import json
import os
from datetime import datetime, timezone

import mysql.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

MYSQL_HOST = os.getenv(
    "MYSQL_HOST",
    "localhost"
)

MYSQL_PORT = int(
    os.getenv(
        "MYSQL_PORT",
        "3306"
    )
)

MYSQL_USER = os.getenv(
    "MYSQL_USER",
    "root"
)

MYSQL_PASSWORD = os.getenv(
    "MYSQL_PASSWORD",
    "admin"
)

MYSQL_DB = "logistics_grafana"


# ============================================================
# TOPICS
# ============================================================

TOPICS = [
    "vehicle-location",
    "shipment-events",
    "warehouse-events",
    "delivery-events",
    "weather-events",
    "logistics-alerts"
]


# ============================================================
# TIMESTAMP HANDLING
# ============================================================

def parse_timestamp(timestamp):

    if not timestamp:

        return datetime.now(
            timezone.utc
        ).replace(
            tzinfo=None
        )

    try:

        dt = datetime.fromisoformat(timestamp)

        # If timestamp has no timezone,
        # treat it as UTC for compatibility.
        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        # Convert to UTC
        dt = dt.astimezone(
            timezone.utc
        )

        # MySQL DATETIME has no timezone.
        # Store UTC as a naive datetime.
        return dt.replace(
            tzinfo=None
        )

    except Exception as e:

        print(
            f"[TIMESTAMP ERROR] "
            f"{timestamp} | {e}"
        )

        return datetime.now(
            timezone.utc
        ).replace(
            tzinfo=None
        )


# ============================================================
# MYSQL CONNECTION
# ============================================================

def get_mysql_connection():

    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB
    )


# ============================================================
# VEHICLE
# ============================================================

def save_vehicle(cursor, event):

    sql = """
    INSERT INTO vehicle_status
    (
        vehicle_id,
        city,
        route_id,
        status,
        speed_kmph,
        latitude,
        longitude,
        event_time,
        updated_at
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        city = VALUES(city),
        route_id = VALUES(route_id),
        status = VALUES(status),
        speed_kmph = VALUES(speed_kmph),
        latitude = VALUES(latitude),
        longitude = VALUES(longitude),
        event_time = VALUES(event_time),
        updated_at = NOW()
    """

    values = (
        event.get("vehicle_id"),
        event.get("city"),
        event.get("route_id"),
        event.get("status"),
        event.get("speed_kmph"),
        event.get("latitude"),
        event.get("longitude"),
        parse_timestamp(
            event.get("timestamp")
        )
    )

    cursor.execute(
        sql,
        values
    )

    print(
        f"[MYSQL] Vehicle updated: "
        f"{event.get('vehicle_id')} | "
        f"{event.get('status')}"
    )


# ============================================================
# SHIPMENT
# ============================================================

def save_shipment(cursor, event):

    sql = """
    INSERT INTO shipment_status
    (
        shipment_id,
        vehicle_id,
        status,
        priority,
        delay_minutes,
        expected_delivery,
        event_time,
        updated_at
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        vehicle_id = VALUES(vehicle_id),
        status = VALUES(status),
        priority = VALUES(priority),
        delay_minutes = VALUES(delay_minutes),
        expected_delivery = VALUES(expected_delivery),
        event_time = VALUES(event_time),
        updated_at = NOW()
    """

    expected_delivery = event.get(
        "expected_delivery"
    )

    if expected_delivery:

        expected_delivery = parse_timestamp(
            expected_delivery
        )

    values = (
        event.get("shipment_id"),
        event.get("vehicle_id"),
        event.get("status"),
        event.get("priority"),
        event.get("delay_minutes", 0),
        expected_delivery,
        parse_timestamp(
            event.get("timestamp")
        )
    )

    cursor.execute(
        sql,
        values
    )

    print(
        f"[MYSQL] Shipment updated: "
        f"{event.get('shipment_id')} | "
        f"{event.get('status')}"
    )


# ============================================================
# WAREHOUSE
# ============================================================

def save_warehouse(cursor, event):

    sql = """
    INSERT INTO warehouse_status
    (
        warehouse_id,
        city,
        utilization_percent,
        active_workers,
        packages_processed,
        processing_time_minutes,
        congestion,
        event_time,
        updated_at
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        city = VALUES(city),
        utilization_percent = VALUES(utilization_percent),
        active_workers = VALUES(active_workers),
        packages_processed = VALUES(packages_processed),
        processing_time_minutes = VALUES(processing_time_minutes),
        congestion = VALUES(congestion),
        event_time = VALUES(event_time),
        updated_at = NOW()
    """

    values = (
        event.get("warehouse_id"),
        event.get("city"),
        event.get("utilization_percent"),
        event.get("active_workers"),
        event.get("packages_processed"),
        event.get("processing_time_minutes"),
        event.get("congestion"),
        parse_timestamp(
            event.get("timestamp")
        )
    )

    cursor.execute(
        sql,
        values
    )

    print(
        f"[MYSQL] Warehouse updated: "
        f"{event.get('warehouse_id')} | "
        f"{event.get('congestion')}"
    )


# ============================================================
# DELIVERY
# ============================================================

def save_delivery(cursor, event):

    sql = """
    INSERT INTO delivery_status
    (
        shipment_id,
        vehicle_id,
        route_id,
        outcome,
        delivery_time_minutes,
        delay_minutes,
        attempt_number,
        failure_reason,
        event_time,
        updated_at
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,%s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        vehicle_id = VALUES(vehicle_id),
        route_id = VALUES(route_id),
        outcome = VALUES(outcome),
        delivery_time_minutes = VALUES(delivery_time_minutes),
        delay_minutes = VALUES(delay_minutes),
        attempt_number = VALUES(attempt_number),
        failure_reason = VALUES(failure_reason),
        event_time = VALUES(event_time),
        updated_at = NOW()
    """

    values = (
        event.get("shipment_id"),
        event.get("vehicle_id"),
        event.get("route_id"),
        event.get("outcome"),
        event.get("delivery_time_minutes"),
        event.get("delay_minutes"),
        event.get("attempt_number"),
        event.get("failure_reason"),
        parse_timestamp(
            event.get("timestamp")
        )
    )

    cursor.execute(
        sql,
        values
    )

    print(
        f"[MYSQL] Delivery updated: "
        f"{event.get('shipment_id')} | "
        f"{event.get('outcome')}"
    )


# ============================================================
# WEATHER
# ============================================================

def save_weather(cursor, event):

    sql = """
    INSERT INTO weather_status
    (
        city,
        temperature_c,
        humidity_percent,
        precipitation_mm,
        wind_speed_kmph,
        weather_condition,
        impact_level,
        event_time,
        updated_at
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,%s,NOW()
    )
    ON DUPLICATE KEY UPDATE
        temperature_c = VALUES(temperature_c),
        humidity_percent = VALUES(humidity_percent),
        precipitation_mm = VALUES(precipitation_mm),
        wind_speed_kmph = VALUES(wind_speed_kmph),
        weather_condition = VALUES(weather_condition),
        impact_level = VALUES(impact_level),
        event_time = VALUES(event_time),
        updated_at = NOW()
    """

    values = (
        event.get("city"),
        event.get("temperature_c"),
        event.get("humidity_percent"),
        event.get("precipitation_mm"),
        event.get("wind_speed_kmph"),
        event.get("weather_condition"),
        event.get("impact_level"),
        parse_timestamp(
            event.get("timestamp")
        )
    )

    cursor.execute(
        sql,
        values
    )

    print(
        f"[MYSQL] Weather updated: "
        f"{event.get('city')} | "
        f"{event.get('impact_level')}"
    )


# ============================================================
# ALERT
# ============================================================

def save_alert(cursor, event):

    sql_alert = """
    INSERT INTO logistics_alerts
    (
        alert_id,
        timestamp,
        city,
        vehicle_id,
        shipment_id,
        risk_score,
        risk_level,
        alert_type,
        reasons
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s,%s,%s
    )
    ON DUPLICATE KEY UPDATE
        timestamp = VALUES(timestamp),
        city = VALUES(city),
        vehicle_id = VALUES(vehicle_id),
        shipment_id = VALUES(shipment_id),
        risk_score = VALUES(risk_score),
        risk_level = VALUES(risk_level),
        alert_type = VALUES(alert_type),
        reasons = VALUES(reasons)
    """

    alert_timestamp = parse_timestamp(
        event.get("timestamp")
    )

    reasons = event.get(
        "reasons",
        []
    )

    if isinstance(reasons, list):

        reasons = ", ".join(
            reasons
        )

    values = (
        event.get("alert_id"),
        alert_timestamp,
        event.get("city"),
        event.get("vehicle_id"),
        event.get("shipment_id"),
        event.get("risk_score"),
        event.get("risk_level"),
        event.get(
            "alert_type",
            "HIGH_LOGISTICS_RISK"
        ),
        reasons
    )

    cursor.execute(
        sql_alert,
        values
    )

    # Also save to risk_summary
    sql_risk = """
    INSERT INTO risk_summary
    (
        timestamp,
        city,
        vehicle_id,
        shipment_id,
        risk_score,
        risk_level,
        reasons
    )
    VALUES
    (
        %s,%s,%s,%s,%s,%s,%s
    )
    """

    cursor.execute(
        sql_risk,
        (
            alert_timestamp,
            event.get("city"),
            event.get("vehicle_id"),
            event.get("shipment_id"),
            event.get("risk_score"),
            event.get("risk_level"),
            reasons
        )
    )

    print(
        f"[MYSQL] Alert saved: "
        f"{event.get('alert_id')} | "
        f"{event.get('risk_level')} | "
        f"{event.get('risk_score')}"
    )


# ============================================================
# STARTUP
# ============================================================

print("=" * 80)
print("LOGISTICS CONTROL TOWER → GRAFANA MYSQL WRITER")
print("=" * 80)

print(
    f"MySQL Database : {MYSQL_DB}"
)

print(
    f"MySQL Host     : {MYSQL_HOST}:{MYSQL_PORT}"
)

print(
    "Timestamp Mode : UTC stored in MySQL"
)

print("=" * 80)

print("Kafka Topics:")

for topic in TOPICS:

    print(
        f"  - {topic}"
    )

print("=" * 80)


# ============================================================
# MYSQL CONNECTION
# ============================================================

try:

    connection = get_mysql_connection()

    cursor = connection.cursor()

    print(
        "MySQL connection successful."
    )

except Exception as e:

    print(
        "MySQL connection failed:"
    )

    print(e)

    raise


# ============================================================
# KAFKA CONSUMER
# ============================================================

consumer = KafkaConsumer(
    *TOPICS,

    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

    group_id="grafana-mysql-writer-live-v3",

    auto_offset_reset="latest",

    enable_auto_commit=True,

    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


print(
    "Grafana MySQL writer started..."
)

print("=" * 80)


# ============================================================
# MAIN LOOP
# ============================================================

try:

    for message in consumer:

        topic = message.topic

        event = message.value

        try:

            if topic == "vehicle-location":

                save_vehicle(
                    cursor,
                    event
                )

            elif topic == "shipment-events":

                save_shipment(
                    cursor,
                    event
                )

            elif topic == "warehouse-events":

                save_warehouse(
                    cursor,
                    event
                )

            elif topic == "delivery-events":

                save_delivery(
                    cursor,
                    event
                )

            elif topic == "weather-events":

                save_weather(
                    cursor,
                    event
                )

            elif topic == "logistics-alerts":

                save_alert(
                    cursor,
                    event
                )

            connection.commit()

        except Exception as e:

            connection.rollback()

            print()
            print(
                "[MYSQL ERROR]"
            )

            print(
                f"Topic : {topic}"
            )

            print(
                f"Error : {e}"
            )

            print(
                f"Event : {event}"
            )

            print()

except KeyboardInterrupt:

    print()
    print(
        "Grafana MySQL writer stopped by user."
    )

finally:

    consumer.close()

    cursor.close()

    connection.close()

    print(
        "MySQL connection closed."
    )