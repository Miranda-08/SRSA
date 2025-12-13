import paho.mqtt.client as mqtt
import json
import time
import socket
from collections import defaultdict
from datetime import datetime

# ********************************************* CONFIG *********************************************
BROKER = "10.6.1.9"
PORT = 1883
GROUPID = "2023269477"  

GATEWAY_UDP_HOST = "0.0.0.0"  # Change to Gateway IP if remote
GATEWAY_UDP_PORT = 9090

# Health thresholds
STALLED_TIME_THRESHOLD = 30  
LOW_BATTERY_THRESHOLD = 15  

# ********************************************* ROBOT HEALTH TRACKING *********************************************
# Track robot state history over time
robot_history = defaultdict(list)  # {robot_id: [(timestamp, location, status, battery), ...]}
robot_health_status = {}  # {robot_id: "NORMAL" or "CRITICAL"}
robot_last_alert_time = {}  # {robot_id: timestamp} - to avoid spam


# ********************************************* HEALTH ASSESSMENT *********************************************
def assess_robot_health(robot_id, current_data):
    current_time = time.time()
    current_location = current_data.get("location_id", "")
    current_status = current_data.get("status", "")
    current_battery = current_data.get("battery", 100)
    
    # Get robot history
    history = robot_history[robot_id]
    
    # Add current state to history
    history.append((current_time, current_location, current_status, current_battery))
    
    cutoff_time = current_time - 60
    robot_history[robot_id] = [(t, loc, st, bat) for t, loc, st, bat in history if t > cutoff_time]
    
    # Check for STALLED condition
    if current_status == "MOVING":
        # Check if location has been the same for STALLED_TIME_THRESHOLD seconds
        recent_history = [(t, loc, st, bat) for t, loc, st, bat in history 
                         if t > current_time - STALLED_TIME_THRESHOLD]
        
        if len(recent_history) >= 2:
            # Check if all recent locations are the same
            locations = [loc for _, loc, _, _ in recent_history]
            if len(set(locations)) == 1 and locations[0] == current_location:
                print(f"[MONITOR]   STALLED detected: {robot_id} has been MOVING at {current_location} for {STALLED_TIME_THRESHOLD}s")
                return "CRITICAL", "STALLED"
    
    # Check for LOW_BATTERY condition
    if current_battery < LOW_BATTERY_THRESHOLD:
        if current_status not in ("CHARGING", "MOVING_TO_CHARGE", "MOVING_TO_CHARGE_FORCED"):
            print(f"[MONITOR]   LOW_BATTERY detected: {robot_id} has {current_battery}% battery and status is {current_status}")
            return "CRITICAL", "LOW_BATTERY"
    
    # Robot is healthy
    return "NORMAL", None


# ********************************************* ALERT HANDLING *********************************************
def send_alert(robot_id, level, reason):
    """
    Send health alert to Warehouse Gateway via UDP
    Alert format: {"robot_id": "AMR-2", "level": "CRITICAL", "override_task": "FORCE_CHARGE", "reason": "LOW_BATTERY"}
    """
    current_time = time.time()
    
    # Avoid sending alerts too frequently (max once per 10 seconds per robot)
    if robot_id in robot_last_alert_time:
        time_since_last = current_time - robot_last_alert_time[robot_id]
        if time_since_last < 10:
            return  # Too soon since last alert
    
    # Determine override task based on reason
    override_task = None
    if reason == "LOW_BATTERY":
        override_task = "FORCE_CHARGE"
    elif reason == "STALLED":
        # For stalled robots, we might want to force charge or reset
        # For now, we'll use FORCE_CHARGE as a recovery mechanism
        override_task = "FORCE_CHARGE"
    
    alert = {
        "robot_id": robot_id,
        "level": level,
        "override_task": override_task,
        "reason": reason,
        "timestamp": datetime.now().isoformat()
    }
    
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        message = json.dumps(alert)
        sock.sendto(message.encode(), (GATEWAY_UDP_HOST, GATEWAY_UDP_PORT))
        sock.close()
        
        robot_last_alert_time[robot_id] = current_time
        print(f"[MONITOR] ALERT SENT → Gateway: {alert}")
    except Exception as e:
        print(f"[MONITOR] Error sending alert: {e}")


# ********************************************* MQTT CALLBACKS *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[MONITOR] Connected to MQTT broker")
        # Subscribe to all internal clean robot data topics
        topic = f"{GROUPID}/internal/amr/+/status"
        client.subscribe(topic, 1)
        print(f"[MONITOR] Subscribed to: {topic}")
    else:
        print(f"[MONITOR] Connection failed with code {rc}")


def on_message(client, userdata, msg):
    """Process incoming robot status messages and assess health"""
    topic = msg.topic
    payload = msg.payload.decode(errors="replace")
    
    # Only process robot status messages
    if "/internal/amr/" not in topic:
        return
    
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print(f"[MONITOR] Invalid JSON in topic {topic}")
        return
    
    robot_id = data.get("robot_id")
    if not robot_id:
        return
    
    # Assess robot health
    health_level, reason = assess_robot_health(robot_id, data)
    
    # Check if health status changed
    previous_health = robot_health_status.get(robot_id, "NORMAL")
    robot_health_status[robot_id] = health_level
    
    # Send alert if CRITICAL and status changed or if still CRITICAL (periodic alerts)
    if health_level == "CRITICAL":
        if previous_health != "CRITICAL" or reason:  # New CRITICAL or reason provided
            send_alert(robot_id, health_level, reason)
            print(f"[MONITOR]  {robot_id} health: {health_level} ({reason})")
    else:
        if previous_health == "CRITICAL":
            print(f"[MONITOR]  {robot_id} health recovered: {health_level}")
        print(f"[MONITOR] {robot_id} health: {health_level}")


# ********************************************* MAIN *********************************************
if __name__ == "__main__":

    
    # Initialize MQTT client
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    
    client.connect(BROKER, PORT)
    client.loop_start()
    
    print("[MONITOR] System Monitor running...")
    print(f"[MONITOR] - Sending alerts to Gateway UDP: {GATEWAY_UDP_HOST}:{GATEWAY_UDP_PORT}")
    
    # Main loop
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MONITOR] Shutting down...")
        client.loop_stop()
        client.disconnect()

