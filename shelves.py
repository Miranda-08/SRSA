import json, time, sys, certifi, paho.mqtt.client as mqtt
from random import randint
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options


# ********************************************* INICIALIZAÇÕES *********************************************
refill_timer = 0
refilling = False
stock = 0
GROUPID = 2023269477
status = "AVAILABLE"  # For packing stations
asset_id = None  # Will be set in main
zone_id = None  # Will be set in main


# ********************************************* CONFIG CONEXÕES *********************************************
# ^ MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883

# ^ INFLUXDB Configuration
fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()
token = "tCpqdhmLKj25M0W1Xt9F0_ok-nlk4hHPCPlDG6bjORsUdf23yWrpJgO9AidA6PZZfxn5G1JQ7i6u-b97s89sqQ=="
org = "SRSA"
host = "https://us-east-1-1.aws.cloud2.influxdata.com/"
database = "SRSA_PROJECT"
write_client = InfluxDBClient3(host=host, token=token, database=database, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))

TOPIC_STOCK = f"{GROUPID}/internal/stock/update"
ROBOT_STATUS = f"{GROUPID}/internal/amr/+/status"


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    global asset_id, zone_id, GROUPID
    if rc == 0:
        if zone_id == "packing_zone":
            print("[PACK STATION] Connected to MQTT broker")
            client.subscribe(ROBOT_STATUS, 1)
            print(f"[PACK STATION {asset_id}] Subscribed to robot status: {ROBOT_STATUS}")
        else:
            print("[SHELF] Connected to MQTT broker")
            client.subscribe(TOPIC_STOCK, 1)
            print(f"[SHELF] Subscribed to topic: stock {TOPIC_STOCK}")
    else:
        print("[SHELF] Connection failed:", rc)

def on_message(client, userdata, msg):
    global stock, refilling, refill_timer, asset_id, zone_id, status
    
    if msg.topic.endswith("/stock/update"):
        data = json.loads(msg.payload)
        if data["asset_id"] == asset_id:
            quantity = int(data.get("quantity", 0))
            if stock >= quantity:
                stock -= quantity
                print(f"[SHELF {asset_id}] Stock updated → {stock}")
            else:
                refilling = True
                refill_timer = 2
                print(f"[SHELF {asset_id}] Not enough stock! Starting refill for 2 seconds...")
    
    # Handle robot status updates for packing stations
    elif zone_id == "packing_zone" and "/internal/amr/" in msg.topic:
        try:
            data = json.loads(msg.payload)
            robot_status = data.get("status", "")
            robot_location = data.get("location_id", "")

            if robot_location.startswith("STATION-"):
                station_id = robot_location.split("-")[1]  # Extract "P1", "P2", etc.
                if station_id == asset_id:
                    # Robot is at this station
                    if robot_status == "DROPPING" :
                        # Robot is dropping - mark as BUSY
                        if status != "BUSY":
                            status = "BUSY"
                            print(f"[PACKING STATION {asset_id}] Status changed to BUSY (robot dropping)")
            elif status == "BUSY":
                        status = "AVAILABLE"
                        print(f"[PACKING STATION {asset_id}] Status changed to AVAILABLE (robot finished dropping, status: {robot_status})")

        except Exception as e:
            print(f"[PACKING STATION {asset_id}] Error processing robot status: {e}")

            
# ********************************************* FUNÇÕES AUXILIARES *********************************************
def tick_sleep():
    """
    Mantém o loop com um período extremamente estável, usando relógio monotónico.
    Explicado com + detalhe em amr_robot.py.
    """
    global next_tick, interval
    next_tick += interval
    sleep_time = next_tick - time.monotonic()
    if sleep_time > 0:
        time.sleep(sleep_time)

def publish_info(asset_id, GROUPID, zone_id, item_id, stock, units):
        # ================= JSON =================================
        payload = {
            "asset_id": asset_id,
            "type": "SHELF",
            "item_id": item_id,
            "stock": stock,
            "unit": units
        }
        #=================== TOPIC PUBLISH =======================
        topic = f"warehouse/{GROUPID}/locations/{zone_id}/{asset_id}/status"
        client.publish(topic, json.dumps(payload))
        print("[SHELF STATUS]", payload)



# ********************************************* CÓDIGO PRINCIPAL *********************************************
def shelf_loop(GROUPID, zone_id, asset_id, update_time):

    global refill_timer, refilling, stock, interval, next_tick
    time.sleep(1) # Tempo para deixar MQTT conectar antes do loop
    interval = update_time

    # ===================== PACK STATION =====================
    if zone_id == "packing_zone":
        global status
        client.on_message = on_message

        print(f"\n\n[PACK STATION INIT]\n")
        status = "AVAILABLE"
        next_tick = time.monotonic()

        while True:
            print(f"\n\n[PACK STATION] NEW TURN\n")

            # Status is updated automatically when robots drop items (via MQTT callback)
            
            # ================= JSON ===============================
            payload = {
                "asset_id": asset_id,
                "type": "PACK_STATION",
                "status": status
            }

            # ================= MQTT PUBLISH =======================
            topic = f"warehouse/{GROUPID}/locations/{zone_id}/{asset_id}/status"
            client.publish(topic, json.dumps(payload))
            print("[PACKING STATION STATUS]", payload)

            tick_sleep()
    
    # ===================== SHELF =====================
    # storage-a → S1–S5 → units ||||| storage-b → S6–S10 → kg

    shelf_num = int(asset_id[1:])  # Sx → x; shelf_num = x 
    if 1 <= shelf_num <= 5:
        item_id = f"item_{chr(64 + shelf_num)}"
        units = "units"
        stock_init = randint(3, 10)
    else:
        item_id = f"item_{chr(64 + shelf_num)}"
        units = "kg"
        stock_init = randint(5, 40)

    stock = stock_init

    print(f"\n\n[SHELF INIT]\n")
    publish_info(asset_id, GROUPID, zone_id, item_id, stock, units)

    next_tick = time.monotonic()

    while True:
        print(f"\n\n[SHELVES] NEW TURN\n")

        if stock == 0 and refilling == False:
            print(f"[SHELF {asset_id}] Stock vazio! Vai iniciar refill automático, durante 2 segundos...")
            refill_timer = 2
            refilling = True

        if refilling == True:
            if refill_timer == 0:
                print(f"[SHELF {asset_id}] Refill automático completo!")
                refilling = False
                stock = stock_init
            else:
                print(f"[SHELF {asset_id}] Refilling...")
                refill_timer -= 1
            publish_info(asset_id, GROUPID, zone_id, item_id, stock, units)
            tick_sleep()
            continue

        publish_info(asset_id, GROUPID, zone_id, item_id, stock, units)
        tick_sleep()


# ========= MAIN =========
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python3 shelves.py <GroupID> <zone_id> <asset_id> <update_time>")
        print("zone_id = A ou B ou packing_zone")
        print("Se <zone_id> = A: <asset_id> = S1, S2, S3, S4 ou S5")
        print("Se <zone_id> = B: <asset_id> = S6, S7, S8, S9 ou S10")
        print("Se <zone_id> = packing_zone: <asset_id> = P1, P2 ou P3")
        sys.exit(1)


    GROUPID = sys.argv[1]
    zone_id = sys.argv[2]
    asset_id = sys.argv[3]
    update_time = int(sys.argv[4])

    if zone_id == "A" and asset_id not in ('S1','S2','S3','S4','S5'):
        print("ERRO: Para zone_id = A só é possível ter asset_id = S1, S2, S3, S4 ou S5")
        sys.exit(1)
    elif zone_id == "B" and asset_id not in ('S6','S7','S8','S9','S10'):
        print("ERRO: Para zone_id = B só é possível ter asset_id = S6, S7, S8, S9 ou S10")
        sys.exit(1)
    elif zone_id == "packing_zone" and asset_id not in ('P1', 'P2', 'P3'):
        print("ERRO: Para zone_id = packing_zone só é possível ter asset_id = P1, P2 ou P3")
        sys.exit(1)
    elif zone_id not in ('A', 'B', "packing_zone"):
        print("ERRO: Só é possível colocar zone_id = A ou zone_id = B ou zone_id = packing_zone")
        sys.exit(1)


    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    shelf_loop(GROUPID, zone_id, asset_id, update_time)
