'''
6.6. MQTT Debugger (mqtt_debugger.py)
Writes in the console every message that is transferred through the relevant MQTT topics. With
the new structure, it should subscribe to warehouse/{GroupID}/# and
{GroupID}/internal/#. The information should be displayed as: [time]:
[topic]:[message].
'''

#!/usr/bin/env python3
# MQTT Debugger – Parte 1
# Estilo baseado em subscriber01.py das práticas

import paho.mqtt.client as mqtt
import json

# MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883
USERNAME = "2023269477"       # igual aos outros scripts
PASSWORD = "srsa"
GROUPID = "2023269477"
QOS_LEVEL = 1

# Topic: ouve TUDO do grupo
TOPIC = f"warehouse/{GROUPID}/#"


def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[DEBUGGER] Connected to MQTT broker")
        client.subscribe(TOPIC, QOS_LEVEL)
        print(f"[DEBUGGER] Subscribed to: {TOPIC}")
    else:
        print(f"[DEBUGGER] Connection failed with code {rc}")


def on_message(client, userdata, msg):
    print("\n============== NEW MQTT MESSAGE ==============")
    print(f"Topic: {msg.topic}")

    try:
        payload = json.loads(msg.payload.decode())
        print("Payload:", json.dumps(payload, indent=4))
    except json.JSONDecodeError:
        print("Raw payload:", msg.payload.decode())

    print("==============================================\n")


def run_debugger():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USERNAME, PASSWORD)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT, 60)

    try:
        client.loop_forever()
    except KeyboardInterrupt:
        client.disconnect()
        print("[DEBUGGER] Exiting.")


if __name__ == "__main__":
    run_debugger()
