import socket, struct, select
def test():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('0.0.0.0', 12001)) # Different port to test
    print("Listening for packet 53 on 12001 (Make sure to start a test server or client sender)")
    s.settimeout(5.0)
    try:
        data, addr = s.recvfrom(2048)
        print("Received", len(data), "bytes:", data.hex())
    except socket.timeout:
        print("Timeout.")
test()
