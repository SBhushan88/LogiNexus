import json
import time
import requests
import os

from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv


# ---------------------------------------------------------
# LOAD ENVIRONMENT VARIABLES
# ---------------------------------------------------------

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

KAFKA_TOPIC = "weather-events"

# ---------------------------------------------------------
# KAFKA PRODUCER
# ---------------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ---------------------------------------------------------
# CITY / LOCATION MASTER DATA
# ---------------------------------------------------------

locations = [
    {
        "city": "Delhi",
        "latitude": 28.6139,
        "longitude": 77.2090
    },
    {
        "city": "Gurgaon",
        "latitude": 28.4595,
        "longitude": 77.0266
    },
    {
        "city": "Jaipur",
        "latitude": 26.9124,
        "longitude": 75.7873
    },
    {
        "city": "Chandigarh",
        "latitude": 30.7333,
        "longitude": 76.7794
    },
    {
        "city": "Agra",
        "latitude": 27.1767,
        "longitude": 78.0081
    },
    {
        "city": "Lucknow",
        "latitude": 26.8467,
        "longitude": 80.9462
    }
]

# ---------------------------------------------------------
# OPEN-METEO API
# ---------------------------------------------------------

API_URL = "https://api.open-meteo.com/v1/forecast"


# ---------------------------------------------------------
# WEATHER CODE INTERPRETATION
# ---------------------------------------------------------

def get_weather_condition(weather_code):

    if weather_code == 0:
        return "CLEAR"

    elif weather_code in [1, 2, 3]:
        return "CLOUDY"

    elif weather_code in [45, 48]:
        return "FOG"

    elif weather_code in [51, 53, 55]:
        return "DRIZZLE"

    elif weather_code in [56, 57]:
        return "FREEZING_DRIZZLE"

    elif weather_code in [61, 63, 65]:
        return "RAIN"

    elif weather_code in [66, 67]:
        return "FREEZING_RAIN"

    elif weather_code in [71, 73, 75, 77]:
        return "SNOW"

    elif weather_code in [80, 81, 82]:
        return "RAIN_SHOWERS"

    elif weather_code in [85, 86]:
        return "SNOW_SHOWERS"

    elif weather_code in [95, 96, 99]:
        return "THUNDERSTORM"

    else:
        return "UNKNOWN"


# ---------------------------------------------------------
# EVENT COUNTER
# ---------------------------------------------------------

event_counter = 1


# ---------------------------------------------------------
# HEADER
# ---------------------------------------------------------

print("=" * 60)
print("LIVE LOGISTICS WEATHER STREAMING PRODUCER")
print("=" * 60)
print(f"Kafka Topic : {KAFKA_TOPIC}")
print(f"Locations   : {len(locations)}")
print("Data Source : Open-Meteo")
print("Frequency   : Every 60 seconds")
print("Status      : STREAMING")
print("=" * 60)


# ---------------------------------------------------------
# STREAMING LOOP
# ---------------------------------------------------------

try:

    while True:

        # -------------------------------------------------
        # BUILD MULTI-LOCATION REQUEST
        # -------------------------------------------------

        latitudes = ",".join(
            str(location["latitude"])
            for location in locations
        )

        longitudes = ",".join(
            str(location["longitude"])
            for location in locations
        )

        params = {
            "latitude": latitudes,
            "longitude": longitudes,
            "current": (
                "temperature_2m,"
                "relative_humidity_2m,"
                "precipitation,"
                "rain,"
                "showers,"
                "weather_code,"
                "wind_speed_10m"
            ),
            "timezone": "Asia/Kolkata"
        }

        # -------------------------------------------------
        # API REQUEST
        # -------------------------------------------------

        response = requests.get(
            API_URL,
            params=params,
            timeout=15
        )

        response.raise_for_status()

        weather_data = response.json()

        # -------------------------------------------------
        # MULTI-LOCATION RESPONSE
        # -------------------------------------------------

        for index, location in enumerate(locations):

            current = weather_data[index]["current"]

            weather_code = current["weather_code"]

            condition = get_weather_condition(
                weather_code
            )

            # -------------------------------------------------
            # OPERATIONAL WEATHER IMPACT
            # -------------------------------------------------

            precipitation = current["precipitation"]
            wind_speed = current["wind_speed_10m"]

            if (
                condition in ["THUNDERSTORM", "HEAVY_RAIN"]
                or precipitation >= 5
                or wind_speed >= 50
            ):

                impact_level = "HIGH"

            elif (
                condition in [
                    "RAIN",
                    "RAIN_SHOWERS",
                    "DRIZZLE"
                ]
                or precipitation > 0
                or wind_speed >= 30
            ):

                impact_level = "MEDIUM"

            else:

                impact_level = "LOW"

            # -------------------------------------------------
            # CREATE EVENT
            # -------------------------------------------------

            event = {
                "event_id": f"WTH{event_counter:06d}",
                "city": location["city"],
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "timestamp": datetime.now().isoformat(),

                "temperature_c": current["temperature_2m"],
                "humidity_percent": current[
                    "relative_humidity_2m"
                ],

                "precipitation_mm": precipitation,
                "rain_mm": current["rain"],
                "showers_mm": current["showers"],

                "weather_code": weather_code,
                "weather_condition": condition,

                "wind_speed_kmph": wind_speed,

                "impact_level": impact_level
            }

            # -------------------------------------------------
            # SEND TO KAFKA
            # -------------------------------------------------

            producer.send(
                KAFKA_TOPIC,
                key=location["city"].encode("utf-8"),
                value=event
            )

            print(
                f"[WEATHER] "
                f"{location['city']} | "
                f"{condition} | "
                f"Temp: {current['temperature_2m']}°C | "
                f"Rain: {precipitation} mm | "
                f"Wind: {wind_speed} km/h | "
                f"Impact: {impact_level}"
            )

            event_counter += 1

        producer.flush()

        print("-" * 60)
        print("Next weather update in 60 seconds...")

        time.sleep(60)


except KeyboardInterrupt:

    print("\nWeather producer stopped.")


except Exception as e:

    print(f"\nWeather producer error: {e}")


finally:

    producer.close()