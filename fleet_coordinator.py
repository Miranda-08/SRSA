import paho.mqtt.client as mqtt, json, sys, time, certifi
from random import choice

# ********************************************* INICIALIZAÇÕES *********************************************
robots = {}             # { "AMR-1": {"status": "...", "location": "..."} }
packing_stations = {}   # { "P1": "AVAILABLE" or "BUSY" }
shelves = {}            # { "S3": stock (int) }

pending_orders = []     # filas de pedidos (para simulação)


# ********************************************* CONFIG CONEXÕES *********************************************
BROKER = "10.6.1.9"
PORT = 1883

GROUPID = 2023269477

TOPIC_AMR_STATUS = f"{GROUPID}/internal/amr/+/status"
TOPIC_STATIC_STATUS = f"{GROUPID}/internal/static/+/status"
TOPIC_DISPATCH = f"{GROUPID}/internal/tasks/dispatch"
TOPIC_STOCK = f"{GROUPID}/internal/stock/update"
TOPIC_ORDERS = f"{GROUPID}/orders"


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[COORDINATOR] Connected")
        # Subscribe
        client.subscribe(f"{GROUPID}/internal/amr/+/status", 1)
        client.subscribe(f"{GROUPID}/internal/static/+/status", 1)
        client.subscribe(f"{GROUPID}/orders", 1)
        print("[COORDINATOR] Subscribed to internal status and orders")
    else:
        print("[COORDINATOR] Connection failed", rc)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode(errors="replace")
    # ===================================================
    # ORDERS (for simulation)
    # ===================================================
    if topic.endswith("/orders"):
        try:
            order = json.loads(payload)
            pending_orders.append(order)
            print("[COORDINATOR] New order received:", order)
        except:
            print("[COORDINATOR] Invalid order JSON")
        return
    # ===================================================
    # STATIC STATUS (shelves + packing stations)
    # ===================================================
    if "/internal/static/" in topic:
        data = json.loads(payload)

        if data["type"] == "PACK_STATION":
            packing_stations[data["asset_id"]] = data["status"]

        elif data["type"] == "SHELF":
            shelves[data["asset_id"]] = data["stock"]
        return

    # ===================================================
    # AMR STATUS
    # ===================================================
    if "/internal/amr/" in topic:
        data = json.loads(payload)
        rid = data["robot_id"]
        robots[rid] = data
        # When robot is PICKING → decrement stock
        if data["status"] == "MOVING_TO_DROP":
# isto está errado pq existe um caso especial: ele entra em moving to drop SEMPRE que acaba o PICKING MASSSSSS
# ele tbm entra em moving to drop se tiver sido interrompido durante DROPPING ou mesmo durante MOVING TO DROP; ao ser interrompido, no final da interrupção volta ao estado anterior, que é MOVING TO DROP. Isto pode causar múltiplos falsos decrementos
            handle_pick(rid)
        assign_tasks()
        return


# ********************************************* PICK HANDLER *********************************************
def handle_pick(robot_id):
    """ Robot is PICKING, decrement stock for that shelf """

    r = robots[robot_id]
    loc = r["location_id"]  # e.g. SHELF-S3

    if not loc.startswith("SHELF-S"):
        return

    shelf_id = loc.split("-")[1]  # S3

    # Example: remove 1 item per PICK
    quantity = 1

    payload = {
        "asset_id": shelf_id,
        "quantity": quantity
    }

    client.publish(f"{GROUPID}/internal/stock/update", json.dumps(payload))
    print(f"[COORDINATOR] Decremented stock {quantity} from {shelf_id}")


# ********************************************* ASSIGN TASKS *********************************************
def assign_tasks():

    if not pending_orders:
        return

    order = pending_orders[0]  # check first pending

    # Order format:
    # { "item": "item_B", "quantity": 3 }

    # STEP 1: find shelves with stock
    target_shelf = None

    for s, stk in shelves.items():
        if stk >= order["quantity"]:
            target_shelf = s
            break

    if target_shelf is None:
        print("[COORDINATOR] No shelf has enough stock yet.")
        return

    # STEP 2: find available robot
    target_robot = None
    for rid, data in robots.items():
        if data["status"] == "IDLE":
            target_robot = rid
            break

    if target_robot is None:
        print("[COORDINATOR] No idle robot.")
        return

    # STEP 3: find AVAILABLE packing station
    available_ps = [p for p, st in packing_stations.items() if st == "AVAILABLE"]
    if not available_ps:
        print("[COORDINATOR] No packing station available.")
        return

    station = choice(available_ps)  # pick any free station

    # STEP 4: send dispatch task
    payload = {
        "robot_id": target_robot,
        "command": "EXECUTE_TASK",
        "target_shelf_id": target_shelf,
        "target_station_id": station
    }

    client.publish(f"{GROUPID}/internal/tasks/dispatch", json.dumps(payload))
    print(f"[COORDINATOR] DISPATCHED TASK → {payload}")

    # STEP 5: PACK STATION becomes BUSY
    packing_stations[station] = "BUSY"

    # STEP 6: remove order
    pending_orders.pop(0)


# ********************************************* MAIN *********************************************
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 fleet_coordinator.py <GroupID>")
        sys.exit(1)

    GROUPID = sys.argv[1]

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_start()

    print("[COORDINATOR] Running...")

    while True:
        time.sleep(1)
