
import struct
import time
from ac_packet import ACSP, PacketParser
from main import DriverInfo, active_drivers, save_lap, save_driver, current_track, current_server

# Mock database.save_lap to avoid actual DB writes and just verify logic
import database
def mock_save_lap(steam_id, car_model, track, server_name, lap_time, valid, timestamp=None):
    print(f"[MOCK DB] Saving Lap: {steam_id}, {car_model}, {track}, {server_name}, {lap_time}, {valid}, {timestamp}")

database.save_lap = mock_save_lap

def test_lap_completion():
    print("--- Testing Lap Completion Logic ---")
    
    # 1. Setup Driver
    car_id = 4
    driver = DriverInfo("Porx", "76561199230780195", "ks_toyota_ae86_tuned")
    active_drivers[car_id] = driver
    
    # Global state
    global current_track, current_server, last_server_addr
    import main
    main.current_track = "imola"
    main.current_server = "Test Server"
    
    # 2. Mock LAP_COMPLETED Packet (ACSP.LAP_COMPLETED = 73)
    # Structure: [PacketID=73][CarID=4][LapTime=1:30.000 (90000ms)][Laps=1][Extra=0]
    # LapTime uint32, Laps uint8, Extra uint8
    
    packet_id = 73
    lap_time = 90000 # 1:30.000
    laps = 1
    extra = 0
    
    payload = struct.pack('B I B B', car_id, lap_time, laps, extra)
    # Note: main.py loop reads packet_type first from parser?
    # No, main.py reads packet_type from parser.read_uint8(). So we must include it if we simulate the stream.
    # But main.py logic inside the loop:
    # parser = PacketParser(data)
    # packet_type = parser.read_uint8()
    
    data = struct.pack('B', packet_id) + payload
    
    # execute logic extracted from main.py
    parser = PacketParser(data)
    p_type = parser.read_uint8()
    
    if p_type == ACSP.LAP_COMPLETED:
        print("Packet Type: LAP_COMPLETED")
        while (len(data) - parser.offset) >= 7:
            cid = parser.read_uint8()
            ltime = parser.read_uint32()
            lc = parser.read_uint8()
            ex = parser.read_uint8()
            
            print(f"Parsed: CarID={cid}, Time={ltime}, Laps={lc}")
            
            drv = active_drivers.get(cid)
            if drv:
                if lc > drv.lap_count:
                    print("Lap count increased! Logic triggers.")
                    # Simulate logic
                    drv.lap_count = lc
                    database.save_lap(drv.guid, drv.model, main.current_track, main.current_server, ltime, True, int(time.time()*1000))
                else:
                    print(f"Lap count not increased ({lc} <= {drv.lap_count})")
            else:
                print("Driver not found")

    # 3. Test Manual Timing (CLIENT_EVENT = 130)
    # Logic: event_type 9 or 5.
    print("\n--- Testing Manual Timing Logic ---")
    
    # 3a. Start Outlap (Event 9)
    # We need to set up the driver state.
    driver.current_lap_start = None
    
    packet_id = 130
    car_type = 4
    event_type = 9 # Lap Completed / Line Crossing
    
    payload = struct.pack('B B', car_type, event_type)
    data = struct.pack('B', packet_id) + payload
    
    parser = PacketParser(data)
    p_type = parser.read_uint8()
    
    if p_type == ACSP.CLIENT_EVENT:
        ct = parser.read_uint8()
        et = parser.read_uint8()
        cid = ct
        
        print(f"Parsed ClientEvent: Car={cid}, Event={et}")
        
        drv = active_drivers.get(cid)
        if drv:
            now = int(time.time() * 1000)
            if drv.current_lap_start is not None:
                print("Timer running. Finishing lap...")
            else:
                print("Timer not running. Starting timer (Outlap end).")
                drv.current_lap_start = now - 90000 # Simulate 90s ago
                print(f"Set start time to {drv.current_lap_start}")

    # 3b. Finish Lap (Event 9 again)
    print("\n--- Sending Second Event (Lap Finish) ---")
    
    parser = PacketParser(data) # Same packet
    p_type = parser.read_uint8()
    
    if p_type == ACSP.CLIENT_EVENT:
        ct = parser.read_uint8()
        et = parser.read_uint8()
        cid = ct
        
        drv = active_drivers.get(cid)
        if drv:
            now = int(time.time() * 1000)
            if drv.current_lap_start is not None:
                l_time = now - drv.current_lap_start
                print(f"Lap Time Logic: {l_time}ms")
                if l_time > 10000:
                    database.save_lap(drv.guid, drv.model, main.current_track, main.current_server, l_time, True, now)
                else:
                    print("Short lap ignored")

if __name__ == "__main__":
    test_lap_completion()
