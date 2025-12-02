#!/usr/bin/env python3
# Shelf simulator – PARTE 1
# Estilo baseado em publisher01.py
from influxdb_client_3 import flight_client_options
import certifi
from influxdb_client_3 import InfluxDBClient3, Point
import paho.mqtt.client as mqtt
import json
import time
import sys
from datetime import datetime, timezone

# MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883
USERNAME = "2023269477" #inventei, n sei se é arbitrário ou não...?
PASSWORD = "srsa" #eu acho?


fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()


#INFLUXDB Configuration

token = "tCpqdhmLKj25M0W1Xt9F0_ok-nlk4hHPCPlDG6bjORsUdf23yWrpJgO9AidA6PZZfxn5G1JQ7i6u-b97s89sqQ=="
org = "SRSA"
host = "https://us-east-1-1.aws.cloud2.influxdata.com/"
database = "SRSA_PROJECT"
write_client = InfluxDBClient3(host=host, token=token, database=database, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[SHELF] Connected to MQTT broker")
    else:
        print("[SHELF] Connection failed:", rc)


def shelf_loop(GROUPID, zone_id, asset_id, update_time):

    while True:
        payload = {
            "asset_id": asset_id,
            "type": "SHELF",
            "item_id": "item_A", #CORRIGIR
            "stock": "stock",
            "unit": "units", #corrigir?
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        }
        #===================MQTT====================
        topic = f"warehouse/{GROUPID}/locations/{zone_id}/{asset_id}/status"
        client.publish(topic, json.dumps(payload))
        print("[SHELF STATUS]", payload)
        #===================INFLUXDB====================
        p = Point("Data").tag("Shelves", f"{asset_id}").field("item_id",payload["item_id"] ).field("stock", payload["stock"]).field("units",payload['units']).time(payload["timestamp"])
        write_client.write(p)
        print(f"Values inserted")

        time.sleep(1)


# ========= MAIN =========
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python3 shelves.py <GroupID> <zone_id> <asset_id> <update_time>") #Example: python3 shelves.py MyGroup storage-a S1 10
        sys.exit(1)

    GROUPID = sys.argv[1]
    zone_id = sys.argv[2]
    asset_id = sys.argv[3]
    update_time = int(sys.argv[4])

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect

    client.connect(BROKER, PORT, 60)
    client.loop_start()

    shelf_loop(GROUPID, zone_id, asset_id, update_time)
