#!/usr/bin/env python3
# AMR robot simulator – PARTE 1
# Estilo baseado em publisher01.py | MQTT paho-mqtt 2.1.0




# CORRIGIR SHELF-SX

'''
Command Processing & Logic: The script must use the struct library to decode
the 3-byte binary payload received on the command topic (see "Warehouse
Gateway" section). It must extract two target IDs to manage its deterministic
cycle:
■ Pick Phase: Upon receiving the command, the robot switches to
6
MOVING_TO_PICK. When the move time (3s) expires, it updates its
location_id to SHELF-S{Byte2} (e.g., if Byte 2 is 0x0A, the location
becomes "SHELF-S10").
■ Drop Phase: After the pick wait time (1s) and the move-to-drop wait time
(2s), the robot updates its location_id to STATION-P{Byte3} (e.g., if Byte
3 is 0x01, the location becomes "STATION-P1").
'''

import paho.mqtt.client as mqtt
import json
import time
import random
import sys
from datetime import datetime, timezone

# MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883
USERNAME = "2023269477" #inventei, n sei se é arbitrário ou não...?
PASSWORD = "srsa" #eu acho?

# Not used in Part 1, but kept to match your structure
received_task_via_MQTT = True  

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[AMR] Connected to MQTT broker")
        topic_cmd = f"warehouse/{GROUPID}/amr/{robot_id}/command"
        client.subscribe(topic_cmd, 1)  # obrigatório PAG. 5
        print(f"[AMR] Subscribed to {topic_cmd} (Part 1 - ignored)")
    else:
        print("[AMR] Connection failed:", rc)

# Parte 1: comandos são ignorados (Parte 2 terá FORCE_CHARGE)
def on_message(client, userdata, msg):
    print(f"[AMR] (ignored) received command topic: {msg.topic}")
    # Parte 1: não fazemos nada aqui.

def get_location(state):
    if state == "IDLE":                 return "DOCK"
    elif state.startswith("MOVING"):    return "TRANSIT"
    elif state == "PICKING":            return "SHELF-Sx"          # EX: real shelves depend on order; part1 doesn't require real IDs
    elif state == "DROPPING":           return "PACKING_ZONE"
    elif state == "CHARGING":           return "CHARGING_STATION"
    elif state == "STALLED":            return "TRANSIT"           # enunciado: fail only occurs while moving

    else:                               return "UNKNOWN"



def amr_state_machine(robot_id, GROUPID):
    battery = 100
    state = "IDLE"

    STATES = [
        ("MOVING_TO_PICK", 3),
        ("PICKING", 1),
        ("MOVING_TO_DROP", 2),
        ("DROPPING", 1)
    ]

    while True:
        # ================= STALLED ==============================
        if state.startswith("MOVING") and random.random() < 0.05:
            state = "STALLED"

        # ================= CARREGAMENTO =========================
        if battery <= 0 and state != "CHARGING":
            state = "CHARGING"

        # ================= JSON =================================
        payload = {
            "robot_id": robot_id,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "location_id": get_location(state),
            "battery": battery,
            "status": state
        }

        # ================= TOPIC PUBLISH =========================
        topic = f"warehouse/{GROUPID}/amr/{robot_id}/status"
        client.publish(topic, json.dumps(payload))
        print("[AMR STATUS]", payload)


        # ================= ESTADOS ================================
        if state == "CHARGING":
            print(f"[AMR] {robot_id} charging...")
            time.sleep(10)
            battery = 100
            state = "IDLE"
            continue    

        elif state == "STALLED":
            print(f"[AMR] {robot_id} STALLED. Waiting for 10s (TEMPORARY: Part 1) [/ Waiting for a high-priority override command (SUPPOSED TO BE LIKE THAT: Part 2)]")
            # POR ENQUANTO, PARTE 1:
            time.sleep(10)
            print(f"[AMR] (DEBUG) {robot_id} not STALLED anymore by God's mercy")
            continue

            # PARTE 2: [n completo]
            #while True:
                #time.sleep(1)
                #client.publish(topic, json.dumps(payload))
            #continue?

        elif state == "IDLE":
            if received_task_via_MQTT: #por enquanto simulamos recebimento de uma task acho eu
                print(f"[AMR] {robot_id} received a FAKE (part 1) task")
                state = "MOVING_TO_PICK"
                continue

        elif state != "IDLE":
            battery -= 1

        else:
            for st, duration in STATES:
                if state == st:
                    time.sleep(duration)
                    current_index = STATES.index((st, duration))
                    if current_index < len(STATES) - 1:
                        state = STATES[current_index + 1][0]
                    else:
                        state = "IDLE"
                    break


# ========= MAIN =========
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 amr_robot.py <GroupID> <RobotID>")
        sys.exit(1)

    GROUPID = sys.argv[1]
    robot_id = sys.argv[2]

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect

    client.connect(BROKER, PORT, 60)
    client.loop_start()

    amr_state_machine(robot_id, GROUPID)
