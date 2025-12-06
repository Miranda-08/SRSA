import json, time, sys, certifi, paho.mqtt.client as mqtt
from random import randint
from influxdb_client_3 import InfluxDBClient3, Point
from influxdb_client_3 import flight_client_options


# ********************************************* INICIALIZAÇÕES *********************************************
refill_timer = 0
refilling = False


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


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[SHELF] Connected to MQTT broker")
    else:
        print("[SHELF] Connection failed:", rc)


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
        #=================== INFLUXDB ============================
        p = (
            Point("Data")
            .tag("Shelves", f"{asset_id}")
            .field("item_id",payload["item_id"] )
            .field("stock", payload["stock"])
            .field("units",payload['unit'])
        )
        write_client.write(p)
        print(f"Values inserted into InfluxDB: {p}")


# ********************************************* CÓDIGO PRINCIPAL *********************************************
def shelf_loop(GROUPID, zone_id, asset_id, update_time):
    # COISAS POR FAZER:
    #   - decremento REAL do shelf quando o AMR faz PICK

    global refill_timer, refilling, stock, interval, next_tick
    time.sleep(1) # Tempo para deixar MQTT conectar antes do loop

    # ===================== CONFIGURAÇÃO POR ZONA =====================
    # storage-a → S1–S5 → units ||||| storage-b → S6–S10 → kg
    # Identificar shelf number a partir de do asset_id e inicializar
    shelf_num = int(asset_id[1:])  # Sx → x; shelf_num = x 
    if 1 <= shelf_num <= 5:
        item_id = f"item_{chr(64 + shelf_num)}" # Se shelf_num = 1: 64 + 1 = 65; char(65) = 'A' || Se shelf_num = 2: 64 + 2 = 66; char(66) = 'B' || ... até num = 5 ou seja 'E'
        units = "units" # 1 'units' = 23kg
        stock_init = randint(3, 10) #arbitrario
    else:
        item_id = f"item_{chr(64 + shelf_num)}" # igual, para shelf_num > 5
        units = "kg"
        stock_init = randint(5, 40) #arbitrario
    stock = stock_init
    

    print(f"\n\n[SHELF INIT]\n")
    publish_info(asset_id, GROUPID, zone_id, item_id, stock, units)

    interval = update_time
    next_tick = time.monotonic()

    turn_counter = 0 # por enquanto, by Chat GPT
    while True:
        print(f"\n\n[SHELVES] NEW TURN\n")

        # POR ENQUANTO, PARA DEBUG: A CADA 3 TURNOS DECREMENTA 1 UNIDADE AO STOCK, by Chat GPT
        # turn_counter += 1
        # if turn_counter >= 3:
        #     turn_counter = 0
        #     if stock > 0:
        #         stock -= 1
        #         print(f"[SHELF {asset_id}] Stock decrementado automaticamente → {stock}")
        #

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
        print("Se <zone_id> = A: <asset_id> = S1, S2, S3, S4 ou S5")
        print("Se <zone_id> = B: <asset_id> = S6, S7, S8, S9 ou S10")
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
    elif zone_id not in ('A', 'B'):
        print("ERRO: Só é possível colocar zone_id = A ou zone_id = B")
        sys.exit(1)


    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect

    client.connect(BROKER, PORT, 60)
    client.loop_start()

    shelf_loop(GROUPID, zone_id, asset_id, update_time)
