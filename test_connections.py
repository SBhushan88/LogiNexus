import os
from dotenv import load_dotenv
from kafka import KafkaProducer
from pymongo import MongoClient
import mysql.connector

# Load .env
load_dotenv()

print("\n==============================")
print("LOGISTICS PROJECT CONNECTION TEST")
print("==============================\n")

# -----------------------------
# 1. TEST KAFKA
# -----------------------------
print("1. Testing Kafka...")

try:
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    )

    print("   ✓ Kafka connection successful")
    producer.close()

except Exception as e:
    print("   ✗ Kafka connection failed")
    print("   Error:", e)


# -----------------------------
# 2. TEST MONGODB
# -----------------------------
print("\n2. Testing MongoDB...")

try:
    mongo_client = MongoClient(
        os.getenv("MONGO_URI"),
        serverSelectionTimeoutMS=5000
    )

    mongo_client.admin.command("ping")

    db = mongo_client[os.getenv("MONGO_DB")]

    print("   ✓ MongoDB connection successful")
    print("   Database:", db.name)

    mongo_client.close()

except Exception as e:
    print("   ✗ MongoDB connection failed")
    print("   Error:", e)


# -----------------------------
# 3. TEST MYSQL
# -----------------------------
print("\n3. Testing MySQL...")

try:
    mysql_connection = mysql.connector.connect(
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DB")
    )

    print("   ✓ MySQL connection successful")
    print("   Database:", os.getenv("MYSQL_DB"))

    mysql_connection.close()

except Exception as e:
    print("   ✗ MySQL connection failed")
    print("   Error:", e)


print("\n==============================")
print("CONNECTION TEST COMPLETE")
print("==============================")