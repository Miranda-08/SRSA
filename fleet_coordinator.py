import paho.mqtt.client as mqtt, json, sys, time, threading, socket


# ********************************************* INICIALIZAÇÕES *********************************************
# Real-time world state maintained by the Fleet Coordinator
world_state = {
    "robots": {},           # { "AMR-1": {"robot_id": "...", "status": "...", "location": "...", ...} }
    "packing_stations": {}, # { "P1": "AVAILABLE" or "BUSY" }
    "shelves": {}           # { "S3": {"asset_id": "S3", "item_id": "item_A", "stock": 10, ...} }
}

pending_orders = []     # filas de pedidos (para simulação)


# ********************************************* CONFIG CONEXÕES *********************************************
BROKER = "10.6.1.9"
PORT = 1883
UDP_PORT = 9091

GROUPID = "2023269477"  # String format to match warehouse_gateway.py

TOPIC_INTERNAL_STATUS = f"{GROUPID}/internal/+/+/status"  # Subscribes to all internal status updates
TOPIC_DISPATCH = f"{GROUPID}/internal/tasks/dispatch"
TOPIC_STOCK = f"{GROUPID}/internal/stock/update"


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[COORDINATOR] Connected to MQTT broker")
        # Subscribe to all internal clean data topics from Gateway
        # Use GROUPID directly to ensure it's the current value
        topic = f"{GROUPID}/internal/+/+/status"
        client.subscribe(topic, 1)
        print(f"[COORDINATOR] Subscribed to: {topic}")
    else:
        print(f"[COORDINATOR] Connection failed with code {rc}")

def udp_server():
    """UDP Server that listens for new order connections on port 9091"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(("0.0.0.0", UDP_PORT))
        print(f"[COORDINATOR] UDP Server listening on port {UDP_PORT}")
        print(f"[COORDINATOR] UDP Server bound to 0.0.0.0:{UDP_PORT}")
    except Exception as e:
        print(f"[COORDINATOR] ERROR: Failed to bind UDP server: {e}")
        return
    
    while True:
        try:
            data, addr = sock.recvfrom(1024)
            print(f"[COORDINATOR] UDP data received from {addr}: {data}")
            
            try:
                order = json.loads(data.decode())
                # Order format: {"item": "item_A", "quantity": 10, "pack_station": "P1"}
                pending_orders.append(order)
                print(f"[COORDINATOR]  UDP ALERT: New order received from {addr}: {order}")
                print(f"[COORDINATOR] Pending orders queue: {len(pending_orders)} order(s)")
                
                # Try to assign tasks when new order arrives
                assign_tasks()
            except json.JSONDecodeError as e:
                print(f"[COORDINATOR]  Invalid JSON in UDP message: {data}")
                print(f"[COORDINATOR] Error: {e}")
            except Exception as e:
                print(f"[COORDINATOR]  Error processing UDP order: {e}")

        except Exception as e:
            print(f"[COORDINATOR]  Error in UDP server loop: {e}")

# ********************************************* PICK HANDLER *********************************************
def handle_pick(robot_id):
    """Robot is PICKING, decrement stock for that shelf"""
    if robot_id not in world_state["robots"]:
        return
    
    r = world_state["robots"][robot_id]
    loc = r.get("location_id", "")  # e.g. SHELF-S3

    if not loc.startswith("SHELF-S"):
        return

    shelf_id = loc.split("-")[1]  # S3

    # Example: remove 1 item per PICK
    quantity = 1

    payload = {
        "asset_id": shelf_id,
        "quantity": quantity
    }

    topic_stock = f"{GROUPID}/internal/stock/update"
    client.publish(topic_stock, json.dumps(payload))
    print(f"[COORDINATOR] Decremented stock {quantity} from {shelf_id}")


# ********************************************* ASSIGN TASKS *********************************************
def assign_tasks():
    """Analyze world_state and pending orders to dispatch tasks when conditions are met"""
    
    if not pending_orders:
        return

    # Process orders in queue order
    for i, order in enumerate(pending_orders):
        # Order format: {"item": "item_A", "quantity": 10, "pack_station": "P1"}
        required_item = order.get("item")
        required_quantity = order.get("quantity")
        pack_station = order.get("pack_station")
        
        if not required_item or not pack_station:
            print(f"[COORDINATOR] Invalid order format: {order}")
            continue

        # STEP 1: Find shelf with matching item and sufficient stock
        target_shelf = None
        for shelf_id, shelf_data in world_state["shelves"].items():
            shelf_item = shelf_data.get("item_id")
            shelf_stock = shelf_data.get("stock", 0)
            
            if shelf_item == required_item and shelf_stock >= required_quantity:
                target_shelf = shelf_id
                break

        if target_shelf is None:
            print(f"[COORDINATOR] No shelf found with item '{required_item}' and stock >= {required_quantity}")
            continue

        # STEP 2: Find available (IDLE) robot - try in order (AMR-1, AMR-2, AMR-3, ...)
        target_robot = None
        
        # Get all robots and sort by robot ID to ensure consistent ordering
        available_robots = []
        for robot_id, robot_data in world_state["robots"].items():
            if robot_data.get("status") == "IDLE":
                available_robots.append(robot_id)
        
        # Sort robots by ID (AMR-1, AMR-2, AMR-3, etc.) - extract number for sorting
        # Create list of tuples (robot_number, robot_id) for sorting
        robots_with_numbers = []
        for robot_id in available_robots:
            if "-" in robot_id:
                try:
                    robot_number = int(robot_id.split("-")[1])
                except:
                    robot_number = 999
            else:
                robot_number = 999
            robots_with_numbers.append((robot_number, robot_id))
        
        # Sort by robot number
        robots_with_numbers.sort()
        
        # Extract sorted robot IDs
        sorted_robots = [robot_id for _, robot_id in robots_with_numbers]
        
        if sorted_robots:
            target_robot = sorted_robots[0]  # Use first available robot in order
            print(f"[COORDINATOR] Selected robot {target_robot} (available robots: {sorted_robots})")
        else:
            print("[COORDINATOR] No IDLE robot available")
            continue

        # STEP 3: Find available packing station - try specified one first, then in order (P1, P2, P3, ...)
        target_station = None
        
        # Get all available packing stations
        available_stations = []
        for station_id, station_status in world_state["packing_stations"].items():
            if station_status == "AVAILABLE":
                available_stations.append(station_id)
        
        # If the specified packing station is available, use it
        if pack_station in available_stations:
            target_station = pack_station
        elif available_stations:
            # Sort packing stations by ID (P1, P2, P3, etc.) - extract number for sorting
            stations_with_numbers = []
            for station_id in available_stations:
                # Extract number from station ID (e.g., "P1" -> 1)
                try:
                    station_number = int(station_id[1:])  # Skip first character (P) and get number
                except:
                    station_number = 999
                stations_with_numbers.append((station_number, station_id))
            
            # Sort by station number
            stations_with_numbers.sort()
            
            # Extract sorted station IDs
            sorted_stations = [station_id for _, station_id in stations_with_numbers]
            target_station = sorted_stations[0]  # Use first available station in order
            print(f"[COORDINATOR] Specified station {pack_station} not available, using {target_station} (available: {sorted_stations})")
        else:
            print(f"[COORDINATOR] No packing station available (specified: {pack_station}) waiting for one to become available")
            continue

        # STEP 4: All conditions met - send dispatch task
        payload = {
            "robot_id": target_robot,
            "command": "EXECUTE_TASK",
            "target_shelf_id": target_shelf,
            "target_station_id": target_station
        }

        topic_dispatch = f"{GROUPID}/internal/tasks/dispatch"
        client.publish(topic_dispatch, json.dumps(payload))


        # STEP 5: Mark packing station as BUSY
        world_state["packing_stations"][target_station] = "BUSY"

        # STEP 6: Remove order from pending list
        pending_orders.pop(i)
        print(f"[COORDINATOR] Order fulfilled and removed from queue. Remaining: {len(pending_orders)}")
        
        # Only process one order per call to avoid race conditions
        return


def on_message(client, userdata, msg):
    """
    Process incoming MQTT messages and update world_state.
    All world state information comes from MQTT messages published by the Gateway:
    - Robot status from {GROUPID}/internal/amr/{robot_id}/status
    - Shelf and packing station status from {GROUPID}/internal/static/{asset_id}/status
    """
    topic = msg.topic
    payload = msg.payload.decode(errors="replace")
    
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        print(f"[COORDINATOR] Invalid JSON in topic {topic}")
        return
    
    # ===================================================
    # STATIC STATUS (shelves + packing stations)
    # ===================================================
    if "/internal/static/" in topic:
        asset_type = data.get("type", "")
        
        if asset_type == "PACK_STATION":
            asset_id = data["asset_id"]
            world_state["packing_stations"][asset_id] = data["status"]
            print(f"[COORDINATOR] Updated packing station {asset_id}: {data['status']}")
        
        elif asset_type == "SHELF":
            asset_id = data["asset_id"]
            # Store full shelf data including item_id for matching
            world_state["shelves"][asset_id] = {
                "asset_id": asset_id,
                "item_id": data.get("item_id"),
                "stock": data.get("stock", 0),
                "unit": data.get("unit", "units")
            }
            print(f"[COORDINATOR] Updated shelf {asset_id}: item={data.get('item_id')}, stock={data.get('stock')}")
        
        return

    # ===================================================
    # AMR STATUS (robot updates)
    # ===================================================
    if "/internal/amr/" in topic:
        robot_id = data.get("robot_id")
        if robot_id:
            
            # 1. Retrieve the previous status *before* updating the world_state
            prev_status = world_state["robots"].get(robot_id, {}).get('status')
            
            # Update robot state in world_state
            world_state["robots"][robot_id] = data
            robot_status = data.get('status')
            print(f"[COORDINATOR] Updated robot {robot_id}: status={robot_status}")
            
            # 2. Add a check: only call handle_pick if status *just changed* to PICKING
            if robot_status == "PICKING" and prev_status != "PICKING":
                # If the robot is picking, decrement stock for the shelf it is at
                handle_pick(robot_id)
            # ========================
            
            # When robot status changes, try to assign pending tasks
            assign_tasks()
        return




# ********************************************* MAIN *********************************************
if __name__ == "__main__":
    # Allow GROUPID to be passed as argument or use default
    if len(sys.argv) >= 2:
        # Update module-level GROUPID by modifying the module's global namespace
        globals()["GROUPID"] = sys.argv[1]
    else:
        print(f"[COORDINATOR] Using default GROUPID: {GROUPID}")
        print("[COORDINATOR] Usage: python3 fleet_coordinator.py [GroupID]")

    # Initialize MQTT client
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_start()

    # Start UDP server in a separate thread (blocking operation)
    udp_thread = threading.Thread(target=udp_server, daemon=True)
    udp_thread.start()
    
    # Give UDP server a moment to start
    time.sleep(0.5)

    print("[COORDINATOR] Fleet Coordinator running...")
    print("[COORDINATOR] - Listening to MQTT for world state updates")
    print("[COORDINATOR] - Listening to UDP port 9091 for new orders")
    print("[COORDINATOR] - Processing pending orders and dispatching tasks")
    while True:
        time.sleep(1)
