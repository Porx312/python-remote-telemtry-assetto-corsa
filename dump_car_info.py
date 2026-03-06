import socket
import struct

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(2.0)

# Request CAR_INFO for CarID 17 (The Japanese player)
# Or just scan 0-20
for car_id in range(20):
    packet = struct.pack('BB', 201, car_id)
    sock.sendto(packet, ('127.0.0.1', 14001)) # Server cmd port or whatever
    # Also send to 13001, 12001 (all 3 servers)
    sock.sendto(packet, ('127.0.0.1', 13001))
    sock.sendto(packet, ('127.0.0.1', 12001))

print("Listening for CAR_INFO packets...")
try:
    while True:
        data, addr = sock.recvfrom(4096)
        if data and data[0] == 54: # CAR_INFO
            print(f"CAR_INFO for Car {data[1]}:")
            print(data.hex())
            # try to see the ascii representation
            print(data)
except socket.timeout:
    print("Done listening.")
