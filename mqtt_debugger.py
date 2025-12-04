'''
6.6. MQTT Debugger (mqtt_debugger.py)
Writes in the console every message that is transferred through the relevant MQTT topics. With
the new structure, it should subscribe to warehouse/{GroupID}/# and
{GroupID}/internal/#. The information should be displayed as: [time]:
[topic]:[message].
'''

import paho.mqtt.client as mqtt
import json
from datetime import datetime

# MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883
GROUPID = "2023269477"
QOS_LEVEL = 1

TOPICS = [f"warehouse/{GROUPID}/#","{GROUPID}/internal/#"]

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[DEBUGGER] Connected to MQTT broker")
        for TOPIC in TOPICS:
            client.subscribe(TOPIC, QOS_LEVEL)
            print(f"[DEBUGGER] Subscribed to: {TOPIC}")
    else:
        print(f"[DEBUGGER] Connection failed with code {rc}")


def on_message(client, userdata, msg):
    print("=================NEW MESSAGE====================")
    time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload_text = msg.payload.decode(errors="replace")
    # if payload is JSON, print compact single-line JSON, otherwise raw string
    try:
        parsed = json.loads(payload_text)
        message_text = json.dumps(parsed, separators=(",", ":"))
    except json.JSONDecodeError:
        message_text = payload_text

    print(f"[time]: {time_str}")
    print(f"[{msg.topic}]:{message_text}")


def run_debugger():
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

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
