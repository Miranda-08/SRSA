#!/usr/bin/env python3
# Shelf simulator – PARTE 1
# Estilo baseado em publisher01.py

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

        topic = f"warehouse/{GROUPID}/locations/{zone_id}/{asset_id}/status"
        client.publish(topic, json.dumps(payload))
        print("[SHELF STATUS]", payload)

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
