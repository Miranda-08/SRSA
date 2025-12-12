import paho.mqtt.client as mqtt, json, time, socket, struct, threading, certifi
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options
import struct

#A) Recebe dados de robots e sensores
#B) Armazena e republica dados normalizados
#C) Converte comandos JSON para binário e envia para robots

#A) O Gateway subscreve:
#   - Tudo o que vem dos simuladores: warehouse/{GroupID}/# (inclui robots, shelves, packing stations)
#   - Comandos de tarefas: {GroupID}/internal/tasks/dispatch => fleet coordinator publica json "robot_id", "command", "target_shelf_id", "target_station_id" | publica comando em bytes em warehouse/{GroupID}/amr/{robot_id}/command
#   - Alertas por UDP na porta: 9090

#B1) NORMALIZAÇÃO DE DADOS RECEBIDOS
#   - Cada msg pode ser: robots (ausência de type ou "AMR"), shelves ("SHELF") ou packing stations ("PACKING_STATION")    (campo type em parêntesis)
#   - O Gateway tem de padronizar unidades: SHELF: Recebe kg → converte para units = kg / 23 | PACKING_STATION: state AVAILABLE or BUSY | ROBOTS: location, battery, state
#   => Depois do processamento, o Gateway tem um JSON limpo e coerente.

#B2) RE-ENVIO DE DADOS NORMALIZADOS
#   - ROBOTS: republica o JSON normalizado em {GroupID}/internal/amr/{robot_id}/status
#   - Shelves e packing stations: republica o JSON normalizado em {GroupID}/internal/static/{asset_id}/status

#C) ARMAZENAMENTO NA BASE DE DADOS
#   - Escreve no InfluxDB os valores normalizados, timestamp, Identificadores (robot_id, asset_id, zone_id), Estado (bateria, stock, status, etc.)


# ********************************************* INICIALIZAÇÕES *********************************************
UNIT_KG_FACTOR = 23 # 1 unit = 23 kg

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

# ^ UDP Port
UDP_PORT = 9090

# ^ TOPICS Configuration
TOPIC_ALL_RAW = f"warehouse/{GROUPID}/#"
TOPIC_DISPATCH = f"{GROUPID}/internal/tasks/dispatch"



# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[GATEWAY] Connected to MQTT broker")
        client.subscribe(TOPIC_ALL_RAW, 1)
        client.subscribe(TOPIC_DISPATCH, 1)
        print(f"[GATEWAY] Subscribed to topics: all raw: {TOPIC_ALL_RAW} // dispatch: {TOPIC_DISPATCH}")
    else:
        print(f"[GATEWAY] Connection failed with code {rc}")


def on_message(client, userdata, msg):
    topic = msg.topic
    payload_raw = msg.payload.decode(errors="replace")

    # ======================================================
    # HANDLING RAW SENSOR DATA
    # ======================================================
    if topic.startswith(f"warehouse/{GROUPID}/") and not topic.endswith("/command"):
        try:
            data = json.loads(payload_raw)
        except Exception as e:
            print("[GATEWAY] Invalid JSON from raw:", e)
            return
        # Determine type
        asset_type = data.get("type", "")

        # ==========================================
        # NORMALIZATION
        # ==========================================
       
        # SHELF
        if asset_type == "SHELF":
            stock = data.get("stock")
            unit = data.get("unit")
            # all SHELVES to "units"; 1 unit = 23 kg
            if unit == "kg":
                if isinstance(stock, (int, float)):
                    stock_units = int(stock / UNIT_KG_FACTOR)
                    data["stock"] = stock_units
                    data["unit"] = "units"
            asset_id = data["asset_id"]
            internal_topic = f"{GROUPID}/internal/static/{asset_id}/status"

        # PACK_STATION
        elif asset_type == "PACK_STATION":
            asset_id = data["asset_id"]
            internal_topic = f"{GROUPID}/internal/static/{asset_id}/status"

        # ROBOT
        else:
            robot_id = data.get("robot_id")
            internal_topic = f"{GROUPID}/internal/amr/{robot_id}/status"

        # ==================================================
        # REPUBLISH NORMALIZED DATA
        # ==================================================

        normalized_json = json.dumps(data)
        client.publish(internal_topic, normalized_json)
        print(f"[GATEWAY] REPUBLISHED CLEAN → {internal_topic}: {data}")

        # ==================================================
        # STORE IN INFLUXDB
        # ==================================================

        p = (
            Point("gateway_clean_data")
            .field("json", normalized_json)
        )
        write_client.write(p)

        return

    # ======================================================
    # HANDLING DISPATCH COMMANDS (JSON → 3 BYTES)
    # ======================================================
    if topic == f"{GROUPID}/internal/tasks/dispatch":

        try:
            data = json.loads(payload_raw)
        except:
            print("[GATEWAY] Invalid JSON in dispatch")
            return

        robot_id = data["robot_id"]
        cmd = data["command"]
        shelf = int(data["target_shelf_id"][1:])   # "S10" → 10
        station = int(data["target_station_id"][1:])  # "P1" → 1

        # Byte1
        if cmd == "EXECUTE_TASK":
            byte1 = 0x01
        elif cmd == "FORCE_CHARGE":
            byte1 = 0x03
        else:
            print("[GATEWAY] Invalid command type")
            return

        # Build 3-byte binary
        cmd_bytes = struct.pack("BBB", byte1, shelf, station)

        dest = f"warehouse/{GROUPID}/amr/{robot_id}/command"
        client.publish(dest, cmd_bytes)

        print(f"[GATEWAY] SENT COMMAND BYTES → {dest}: {cmd_bytes}")

        # Log command
        p = (
            Point("gateway_commands")
            .field("command", cmd)
            .field("robot_id", robot_id)
            .field("shelf", shelf)
            .field("station", station)
        )
        write_client.write(p)

        return


# ********************************************* CÓDIGO PRINCIPAL *********************************************
def udp_server():
    print(f"[GATEWAY] UDP Server listening on port {UDP_PORT}")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("0.0.0.0", UDP_PORT))

    while True:
        data, addr = sock.recvfrom(1024)

        try:
            msg = json.loads(data.decode())
        except:
            print("[GATEWAY] Invalid UDP message:", data)

        robot_id = msg.get("robot_id")
        override = msg.get("override_task")

        print(f"[GATEWAY] UDP ALERT RECEIVED: {msg}")

        if override == "FORCE_CHARGE":
            # encode override to robot
            cmd_bytes = struct.pack("BBB", 0x03, 0x00, 0x00)
            topic = f"warehouse/{GROUPID}/amr/{robot_id}/command"
            client.publish(topic, cmd_bytes)
            print(f"[GATEWAY] FORCE_CHARGE sent to {robot_id}")
        
            # Log override
            p = (
                Point("gateway_alerts")
                .field("robot_id", robot_id)
                .field("override", "FORCE_CHARGE")
            )
            write_client.write(p)



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