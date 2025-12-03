import paho.mqtt.client as mqtt, json, time, random, sys, struct, certifi
from influxdb_client_3 import flight_client_options
from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime, timezone


# ********************************************* INICIALIZAÇÕES *********************************************
battery = 100
state = "IDLE"
commands = [] #comandos por executar
Sx = None #current shelf
Px = None #current packing station
remaining_time = 0 #segundos que faltam no estado atual

fh = open(certifi.where(), "r")
cert = fh.read()
fh.close()

# ********************************************* CONFIG CONEXÕES *********************************************
# ^ MQTT Broker Configuration
BROKER = "10.6.1.9"
PORT = 1883

# ^ INFLUXDB configuration
token = "tCpqdhmLKj25M0W1Xt9F0_ok-nlk4hHPCPlDG6bjORsUdf23yWrpJgO9AidA6PZZfxn5G1JQ7i6u-b97s89sqQ=="
org = "SRSA"
host = "https://us-east-1-1.aws.cloud2.influxdata.com/"
database = "SRSA_PROJECT"
write_client = InfluxDBClient3(host=host, token=token, database=database, org=org, flight_client_options=flight_client_options(tls_root_certs=cert))


# ********************************************* FUNÇÕES CALLBACK *********************************************
def on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        print("[AMR] Connected to MQTT broker")
        topic_cmd = f"warehouse/{GROUPID}/amr/{robot_id}/command"
        client.subscribe(topic_cmd, 1)  # QoS 1: garante entrega pelo menos uma vez
        print(f"[AMR] Subscribed to {topic_cmd}")
    else:
        print("[AMR] Connection failed:", rc)

def on_message(client, userdata, msg):
    global commands
    if msg.topic.endswith("/command"):
        cmd_type, shelf_id, station_id = struct.unpack("BBB", msg.payload) # => unpack já os torna inteiros
        commands.append((cmd_type, shelf_id, station_id))
        print("[AMR] Received command:", cmd_type, shelf_id, station_id)


# ********************************************* FUNÇÕES AUXILIARES *********************************************
def tick_sleep():
    """
    Garante que cada iteração do loop principal dura O MAIS PRÓXIMO POSSÍVEL de 1 segundo.

    Para isso usamos um relógio monotónico, um contador que só avança e nunca é
    ajustado pelo sistema (mudanças de hora, NTP, etc.), para medição de tempo + exata.

    - CASO 1: A iteração foi RÁPIDA (<1s)
    Cada vez que o loop acaba, esta função calcula quanto tempo FALTA
    para completar 1 segundo desde a última iteração e dorme exatamente esse tempo
    OU
    - CASO 2: A iteração foi LENTA (>1s)
    Cada vez que o loop acaba, a função calcula quanto tempo demorou em EXCESSO
    em relação ao suposto 1s; não dorme neste caso e faz a próxima iteração começar
    imediatamente para reduzir o atraso.

    Isto impede que os atrasos se acumulem ao longo do tempo (sem “deriva”), e mantém
    o loop a trabalhar num ritmo muito estável de 1 segundo por ciclo, mesmo que
    a carga de trabalho varie entre iterações.
    """
    global next_tick, interval
    next_tick += interval
    sleep_time = next_tick - time.monotonic()
    if sleep_time > 0:
        time.sleep(sleep_time)

def get_location(state, shelf_id, station_id):
    if state == "IDLE":                 return "DOCK"
    elif state.startswith("MOVING"):    return "TRANSIT"
    elif state == "PICKING":            return f"SHELF-S{shelf_id}" if shelf_id is not None else "SHELF"
    elif state == "DROPPING":           return f"STATION-P{station_id}" if station_id is not None else "STATION"
    elif state == "CHARGING":           return "CHARGING_STATION"
    elif state == "STALLED":            return "TRANSIT"

    else:                               return "UNKNOWN"

def publish_info(robot_id, GROUPID, state, battery, Sx, Px):
        # ================= JSON =================================
        payload = {
            "robot_id": robot_id,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "location_id": get_location(state, Sx, Px),
            "battery": battery,
            "status": state
        }
        # ================= TOPIC PUBLISH =========================
        topic = f"warehouse/{GROUPID}/amr/{robot_id}/status"
        client.publish(topic, json.dumps(payload))
        print("\n[AMR PAYLOAD] PAYLOAD PUBLISHED:", payload)
        #================= INFLUXDB ================================
        p = (
            Point("Data")
            .tag("Robots", f"amr_robot{robot_id}")
            .field("status",payload["status"] )
            .field("location", payload["location_id"])
            .field("battery",payload['battery'])
            .time(payload["timestamp"])
        )
        write_client.write(p)
        print(f"[INFLUX DB] Values inserted to InfluxDB: {p}")


# ********************************************* CÓDIGO PRINCIPAL *********************************************
DEBUG = True # <----------------------------------------------------------------------------------------------------------- DEBUG

def amr_state_machine(robot_id, GROUPID):
    # COISAS POR FAZER:
    #   - task só é considerado completada (removida de commands) SE E SÓ SE o robô de facto a completar com sucesso (pode acontecer stalled, no battery, forced command, etc)
    #   - se for interrompido por alguma razão... o que acontece ao que estava a fazer? se, por exemplo, estava a carregar um item, esse item fica perdido? continua a ser
    #segurado pelo robo?
    #   - tasks FORCED (cmd_type=3) terem prioridade sobre as outras: o robô larga "repentinamente" tudo o que está a fazer?
    #   - robô deve mover-se até à estação de carga? quanto tempo demoraria? existe limitação no nº de robôs a serem charged?
    #   - bateria neste momento, enquanto charging, é constante. é suposto ir variando até aos 100% em 10 turnos (segundos)?

    global state, battery, commands, Sx, Px, remaining_time, next_tick, interval

    time.sleep(1) # Tempo para deixar MQTT conectar antes do loop
    # ========= DEBUG MODE ==========
    if DEBUG:
        print("*"*10 + "[DEBUG] Modo de debug ativo — inicializando comando fake" + "*"*10)
        commands.append((1, 2, 5)) # cmd_type=1, shelf 2, station 5
        commands.append((1, 1, 4))
        commands.append((1, 3, 3))
        commands.append((1, 2, 2))
        commands.append((1, 1, 1))
        commands.append((1, 2, 5))
    # ===============================

    print(f"\n\nSTART\n")
    publish_info(robot_id, GROUPID, state, battery, Sx, Px)

    interval = 1.0
    next_tick = time.monotonic()
    while True:
        print(f"\n\nNEW TURN\n")

        # ================= STALLED ==============================
        if state.startswith("MOVING") and random.random() < 0.05:
            print(f"[AMR STATUS] Robot {robot_id} was moving but got STALLED for 10s...")
            #(stalled como status temporário APENAS para debug da parte 1; em teoria, stalled continua até que haja um high-priority override command)
            state = "STALLED"
            remaining_time = 10
        if state == "STALLED":
            #TEMPORÁRIO, PARA A PARTE 1: sai sozinho ao fim de 10s
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} saiu do estado STALLED por um milagre de Deus (TEMP - Part 1). Retorna para estado IDLE")
                state = "IDLE"
            else:
                remaining_time -= 1
            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue

        # ================= CARREGAMENTO =========================
        if battery <= 15 and state != "CHARGING":
            print(f"[AMR STATUS] Robot {robot_id} is almost out of battery and will recharge for 10s...")
            state = "CHARGING"
            remaining_time = 10
        if state == "CHARGING":
            remaining_time -= 1
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} FULLY CHARGED. Returned to state IDLE.")
                battery = 100
                state = "IDLE"
            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue

        # ================= TASK =========================
        if state == "IDLE":
            if commands:
                cmd_type, Sx, Px = commands.pop(0)
                    #cmd_type 1 = EXECUTE_TASK, 3 = FORCE_CHARGE
                print(f"[AMR STATUS] Robot {robot_id} received a [FAKE - part 1] task: TYPE {cmd_type}, SHELF {Sx}, STATION {Px}")
                print(f"[AMR STATUS] Robot {robot_id} will start MOVING TO PICK for 3s... => shelf {Sx}")
                state = "MOVING_TO_PICK"
                remaining_time = 3
            else:
                publish_info(robot_id, GROUPID, state, battery, Sx, Px)
                tick_sleep()
                continue

        if state == "MOVING_TO_PICK":
            battery = max(0, battery-1)
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} arrived at at picking shelf S{Sx}.")
                print(f"[AMR STATUS] Robot {robot_id} will start PICKING item for 1s...")
                state = "PICKING"
                remaining_time = 1
            else:
                remaining_time = max(0, remaining_time-1)

            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue

        if state == "PICKING":
            battery = max(0, battery-1)
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} picked the item.")
                print(f"[AMR STATUS] Robot {robot_id} will start MOVING TO DROP for 2s... => packing station {Px}")
                state = "MOVING_TO_DROP"
                remaining_time = 2
            else:
                remaining_time = max(0, remaining_time-1)

            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue

        if state == "MOVING_TO_DROP":
            battery = max(0, battery-1)
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} arrived at packing station P{Px}.")
                print(f"[AMR STATUS] Robot {robot_id} will start DROPPING for 1s...")
                state = "DROPPING"
                remaining_time = 1
            else:
                remaining_time = max(0, remaining_time-1)

            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue
        
        if state == "DROPPING":
            battery = max(0, battery-1)
            if remaining_time == 0:
                print(f"[AMR STATUS] Robot {robot_id} dropped the item at station P{Px}.")
                print(f"[AMR STATUS] Robot {robot_id} entering IDLE state again")
                state = "IDLE"
            else:
                remaining_time = max(0, remaining_time-1)

            publish_info(robot_id, GROUPID, state, battery, Sx, Px)
            tick_sleep()
            continue
        
        tick_sleep()
        

# ********************************************* MAIN *********************************************
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 amr_robot.py <GroupID> <RobotID>")
        sys.exit(1)

    GROUPID = sys.argv[1]
    robot_id = sys.argv[2]

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER, PORT)
    client.loop_start()

    amr_state_machine(robot_id, GROUPID)
