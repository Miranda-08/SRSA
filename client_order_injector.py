import sys
import socket
import json

# ********************************************* CONFIG *********************************************
UDP_HOST = "0.0.0.0" 
UDP_PORT = 9091

# ********************************************* USAGE *********************************************
def send_order(item, quantity, pack_station):
    """Send multiple orders - one for each unit requested"""
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    
    try:
        for i in range(quantity):
            order = {
                "item": item,
                "quantity": 1,  # Each order is for 1 unit
                "pack_station": pack_station
            }
            
            message = json.dumps(order)
            sock.sendto(message.encode(), (UDP_HOST, UDP_PORT))
            print(f"[CLIENT] Order {i+1}/{quantity} sent to {UDP_HOST}:{UDP_PORT}: {order}")
        
        sock.close()
        print(f"[CLIENT] Successfully sent {quantity} order(s)")
    except Exception as e:
        print(f"[CLIENT] Error sending order: {e}")
        sock.close()


# ********************************************* MAIN *********************************************
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python3 client_order_injector.py <item> <quantity> <pack_station>")
        print("Example: python3 client_order_injector.py item_A 10 P1")
        print("\nNote: The order will be sent 'quantity' times, each with quantity=1")
        print("Order format (sent multiple times):")
        print('  {"item": "item_A", "quantity": 1, "pack_station": "P1"}')
        sys.exit(1)
    
    item = sys.argv[1]
    quantity = int(sys.argv[2])
    pack_station = sys.argv[3]
    
    send_order(item, quantity, pack_station)
