import paho.mqtt.client as mqtt, json, time, socket, struct, threading, certifi
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options


#explicação da lógica do código by chat gpt:
#   https://chatgpt.com/s/t_693458bc80e881919d550a91e7002ce1

# ********************************************* CONFIG CONEXÕES *********************************************
# ^ MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883
GROUPID = "2023269477"

# ^ INFLUXDB Configuration
fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()
token = "tCpqdhmLKj25M0W1Xt9F0_ok-nlk4hHPCPlDG6bjORsUdf23yWrpJgO9AidA6PZZfxn5G1JQ7i6u-b97s89sqQ=="
org = "SRSA"
host = "https://us-east-1-1.aws.cloud2.influxdata.com/"
database = "SRSA_PROJECT"
write_client = InfluxDBClient3(host=host, token=token, database=database, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))

# ^ TOPICS Configuration
TOPIC_DISPATCH = f"{GROUPID}/internal/tasks/dispatch" #subscribe
TOPIC_INTERNAL_AMR = f"{GROUPID}/internal/amr"        #
TOPIC_INTERNAL_STATIC = f"{GROUPID}/internal/static"  #

# ^ UPD Ports
UDP_PORT_MONITOR = 9090   # alerts from System Monitor


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[GATEWAY] Connected to MQTT broker")
        topic = f"warehouse/{GROUPID}/#"
        client.subscribe(topic, 1)
        client.subscribe(TOPIC_DISPATCH, 1)
        print(f"[GATEWAY] Subscribed to topics: {topic} // {TOPIC_DISPATCH}")
    else:
        print(f"[GATEWAY] Connection failed with code {rc}")


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode(errors="replace")
    
    # if payload is JSON, print compact single-line JSON, otherwise raw string

    # -----------------------------------------------------------
    # 1. PROCESS JSON SENSOR DATA FROM RAW TOPICS
    # -----------------------------------------------------------
    if topic.startswith(f"warehouse/{GROUPID}/"):
        try:
            data = json.loads(payload)
            asset_type = data.get("type", "")
            # Normalize
            if asset_type == "SHELF":
                # convert kg → units if needed (example only)
                if data["unit"] == "kg":
                    data["stock"] = data["stock"] // 23
                    data["unit"] = "units"
                # internal topic
                new_topic = f"{TOPIC_INTERNAL_STATIC}/{data['asset_id']}/status"
            elif asset_type == "PACK_STATION":
                new_topic = f"{TOPIC_INTERNAL_STATIC}/{data['asset_id']}/status"
            else:  # ROBOT
                new_topic = f"{TOPIC_INTERNAL_AMR}/{data['robot_id']}/status"
            # republish
            client.publish(new_topic, json.dumps(data))
            # store in InfluxDB
            p = (
                Point("GatewayCleanData")
                .field("raw", json.dumps(data))
            )
            write_client.write(p)
            print(f"[GATEWAY] Forwarded clean → {new_topic}: {data}")
        except Exception as e:
            print("[GATEWAY ERROR] JSON:", e)

        return
    
    # -----------------------------------------------------------
    # 2. DISPATCH COMMANDS FROM COORDINATOR (JSON → 3 bytes)
    # -----------------------------------------------------------
    if topic == TOPIC_DISPATCH:
        try:
            data = json.loads(payload)
            robot_id = data["robot_id"]
            cmd = data["command"]
            shelf = int(data["target_shelf_id"][1:])  # "S5" → 5
            station = int(data["target_station_id"][1:])

            if cmd == "EXECUTE_TASK":
                byte1 = 0x01
            elif cmd == "FORCE_CHARGE":
                byte1 = 0x03
            else:
                print("[GATEWAY] Unknown cmd:", cmd)
                return

            # 3 byte payload
            cmd_bytes = struct.pack("BBB", byte1, shelf, station)

            dest_topic = f"warehouse/{GROUPID}/amr/{robot_id}/command"
            client.publish(dest_topic, cmd_bytes)

            print(f"[GATEWAY] SENT BYTES to {dest_topic}: {cmd_bytes}")

        except Exception as e:
            print("[GATEWAY] Dispatch error:", e)


# ********************************************* CÓDIGO PRINCIPAL *********************************************
def udp_server():
    print(f"[GATEWAY] UDP Server listening on port {UDP_PORT_MONITOR}")
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT_MONITOR))

    while True:
        data, addr = sock.recvfrom(1024)
        try:
            msg = json.loads(data.decode())

            robot_id = msg.get("robot_id")
            override = msg.get("override_task")

            print(f"[GATEWAY] UDP ALERT: {msg}")

            if override == "FORCE_CHARGE":
                # encode override to robot
                cmd_bytes = struct.pack("BBB", 0x03, 0x00, 0x00)

                topic = f"warehouse/{GROUPID}/amr/{robot_id}/command"
                client.publish(topic, cmd_bytes)

                print(f"[GATEWAY] FORCE_CHARGE sent to {robot_id}")

        except:
            print("[GATEWAY] Invalid UDP message:", data)



# ========= MAIN =========
if __name__ == "__main__":
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_start()

    # start UDP thread
    t = threading.Thread(target=udp_server, daemon=True)
    t.start()

    print("[GATEWAY] Running forever...")
    while True:
        time.sleep(1)