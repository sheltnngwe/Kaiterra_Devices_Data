import json

import paho.mqtt.client as mqtt
from pymongo import MongoClient

# OBB connection details
broker_address = "10.32.228.110"
broker_port = 30883
username = "obb_mqtt"
password = "OpenBlueBridge123"
topic = "rawactivity/kaiterra/device/history/+"

# MongoDB connection details
mongodb_host = "localhost"
mongodb_port = 27017
mongodb_database = "indoor_air_quality_db"
mongodb_collection = "air_quality_data"


def create_mongodb_collection():
    client = MongoClient(mongodb_host, mongodb_port)
    db = client[mongodb_database]
    collection_names = db.list_collection_names()
    if mongodb_collection not in collection_names:
        db.create_collection(mongodb_collection)
        print(f"Collection '{mongodb_collection}' created in database '{mongodb_database}'")
    else:
        print(f"Collection '{mongodb_collection}' already exists in database '{mongodb_database}'")
    client.close()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker")
        client.subscribe(topic)
    else:
        print("Failed to connect, error code: " + str(rc))


def on_message(client, userdata, msg):
    # Handle received MQTT message
    print("Received message on topic: " + msg.topic)
    print("Message payload: " + msg.payload.decode())

    # Parse the received JSON data
    data = json.loads(msg.payload.decode())

    # Connect to MongoDB and create the collection if it doesn't exist
    # create_mongodb_collection()

    # Insert the data into MongoDB
    # client = MongoClient(mongodb_host, mongodb_port)
    # db = client[mongodb_database]
    # collection = db[mongodb_collection]
    # collection.insert_one(data)
    # client.close()


client = mqtt.Client()
client.username_pw_set(username, password)
client.on_connect = on_connect
client.on_message = on_message

client.connect(broker_address, broker_port)

client.loop_forever()
