import paho.mqtt.client as mqtt, json, time, socket
from collections import defaultdict
from datetime import datetime, timezone

# ********************************************* CONFIG *********************************************
BROKER = "10.6.1.9"
PORT = 1883
GROUPID = "2023269477"

GATEWAY_UDP_HOST = "0.0.0.0"
GATEWAY_UDP_PORT = 9090

STALLED_TURN_THRESHOLD = 20
LOW_BATTERY_THRESHOLD = 15


# ********************************************* STATE *********************************************
# Contadores por robot
transit_counter = defaultdict(int)
robot_last_alert_time = {}
robot_health_status = defaultdict(lambda: "NORMAL")


# ********************************************* ALERT *********************************************
def send_force_charge(robot_id, reason):
    now = time.time()

    # Anti-spam: máx. 1 alerta a cada 10s por robot
    if robot_id in robot_last_alert_time:
        if now - robot_last_alert_time[robot_id] < 10:
            return

    alert = {
        "robot_id": robot_id,
        "level": "CRITICAL",
        "override_task": "FORCE_CHARGE",
        "reason": reason,
        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(json.dumps(alert).encode(), (GATEWAY_UDP_HOST, GATEWAY_UDP_PORT))
        sock.close  ()

        robot_last_alert_time[robot_id] = now
        print(f"[MONITOR] FORCE_CHARGE sent → {robot_id} | Reason: {reason}")

    except Exception as e:
        print(f"[MONITOR] Error sending alert: {e}")


# ********************************************* HEALTH LOGIC (TURN-BASED) *********************************************
def process_robot_status(data):
    robot_id = data["robot_id"]
    location = data.get("location_id", "")
    status = data.get("status", "")
    battery = data.get("battery", 100)

    previous_health = robot_health_status[robot_id]
    current_health = "NORMAL"

    # ================= STALLED =================
    if location == "TRANSIT" and status == "MOVING":
        transit_counter[robot_id] += 1
        print(f"[MONITOR] {robot_id} TRANSIT turns = {transit_counter[robot_id]}")

        if transit_counter[robot_id] >= STALLED_TURN_THRESHOLD:
            send_force_charge(robot_id, "STALLED")
            transit_counter[robot_id] = 0
    else:
        transit_counter[robot_id] = 0

    # ================= LOW BATTERY =================
    if battery < LOW_BATTERY_THRESHOLD:
        if status not in ("CHARGING", "MOVING_TO_CHARGE", "MOVING_TO_CHARGE_FORCED"):
            send_force_charge(robot_id, "LOW_BATTERY")

    # ================= HEALTH PRINTS =================
    robot_health_status[robot_id] = current_health

    if previous_health != current_health:
        if current_health == "NORMAL":
            print(f"[MONITOR] ✅ {robot_id} recovered → HEALTHY")
        else:
            print(f"[MONITOR] ❌ {robot_id} health → CRITICAL")


# ********************************************* MQTT CALLBACKS *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        topic = f"{GROUPID}/internal/amr/+/status"
        client.subscribe(topic, 1)
        print(f"[MONITOR] Connected and subscribed to {topic}")
    else:
        print("[MONITOR] Connection failed:", rc)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        process_robot_status(data)
    except Exception as e:
        print("[MONITOR] Invalid message:", e)


# ********************************************* MAIN *********************************************
if __name__ == "__main__":
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_start()

    print("[MONITOR] System Monitor running (turn-based logic)")
    print(f"[MONITOR] Alerts → UDP {GATEWAY_UDP_HOST}:{GATEWAY_UDP_PORT}")

    try:
        while True:
            time.sleep(0.1)
    except KeyboardInterrupt:
        print("\n[MONITOR] Shutting down...")
        client.loop_stop()
        client.disconnect()
