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

def battery_decay_charging(state,battery):
    if state=="CHARGING":
          battery==100
          time.sleep(10)
          return battery
    else:
         battery-=1
         time.sleep(1)
         return battery
          

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
    battery=battery_decay_charging(state,battery)
    STATES = [
        ("MOVING_TO_PICK", 3),
        ("PICKING", 1),
        ("MOVING_TO_DROP", 2),
        ("DROPPING", 1)
    ]

    print(f"FIRST TURN")
    while True:
        time.sleep(4)
        print(f"\nNEW TURN")
        # ================= STALLED ==============================
        if state.startswith("MOVING") and random.random(0,1) < 0.05:
            print("[AMR STATUS] Robot {robot_id} was moving but got STALLED for 10s... (coloquei stalled como status temporário APENAS para debug da parte 1; em teoria, stalled continua até que haja um high-priority override command)")
            state = "STALLED"
            time.sleep(10) #temporario

        if state == "STALLED":
            #TEMPORÁRIO, PARA A PARTE 1
            count -= 1
            if count == 0:
                print(f"[AMR STATUS] Robot {robot_id} FULLY CHARGED. Returned to state IDLE.")
                state = "IDLE"
            continue

        # ================= CARREGAMENTO =========================
        if battery <= 0 and state != "CHARGING":
            print(f"[AMR STATUS] Robot {robot_id} is out of battery and will recharge for 10s...")
            state = "CHARGING"

        if state == "CHARGING":
            count -= 1
            if count == 0:
                print(f"[AMR STATUS] Robot {robot_id} FULLY CHARGED. Returned to state IDLE.")
                battery = 100
                state = "IDLE"
            continue

        # ================= TASK =========================
        if state == "IDLE":
            if received_task_via_MQTT: #por enquanto simulamos recebimento de uma task acho eu
                print(f"[AMR STATUS] Robot {robot_id} received a FAKE (part 1) task.")
                print(f"[AMR STATUS] Robot {robot_id} will start MOVING TO PICK for 3s...")
                state = "MOVING_TO_PICK"
                time.sleep(3)

        if state == "MOVING_TO_PICK":
                print(f"[AMR STATUS] Robot {robot_id} arrived at picking spot.")
                print(f"[AMR STATUS] Robot {robot_id} will start PICKING the item for 1s...")
                state = "PICKING"
                time.sleep(1)
                continue

        if state == "PICKING":
                print(f"[AMR STATUS] Robot {robot_id} picked the item.")
                print(f"[AMR STATUS] Robot {robot_id} will start MOVING TO DROP for 1s...")
                state = "MOVING_TO_DROP"
                time.sleep(2)
                continue

        if state == "MOVING_TO_DROP":
                print(f"[AMR STATUS] Robot {robot_id} arrived at dropping spot.")
                print(f"[AMR STATUS] Robot {robot_id} will start DROPPING for 1s...")
                state = "DROPPING"
                time.sleep(1)
                continue
        

        if state == "DROPPING":
                print(f"[AMR STATUS] Robot {robot_id} dropped the item.")
                print(f"[AMR STATUS] Robot {robot_id} enter IDLE state again")
                state = "IDLE"
                continue


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
        print("[AMR PAYLOAD] PAYLOAD PUBLISHED:", payload)


        # ================= ESTADOS ================================   


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

    client.connect(BROKER, PORT)
    client.loop_start()

    while True:
        amr_state_machine(robot_id, GROUPID)
