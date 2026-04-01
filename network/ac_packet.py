import struct

class ACSP:
    # Standard AC Dedicated Server Protocol
    # Outgoing (Backend -> Server)
    HANDSHAKE = 0
    REALTIMEPOS_UPDATE = 200  # Subscribe to updates
    GET_CAR_INFO = 201        # Request car info
    SEND_CHAT = 202
    BROADCAST_CHAT = 203
    GET_SESSION_INFO = 59     # Request session info

    # Incoming (Server -> Backend)
    NEW_SESSION = 50
    NEW_CONNECTION = 51
    CONNECTION_CLOSED = 52
    CAR_UPDATE = 53           # Realtime car position/speed (subscribe with packet 200)
    CAR_INFO = 54             # Car info response
    END_SESSION = 55
    VERSION = 56
    CHAT = 57
    CLIENT_LOADED = 58
    LAP_COMPLETED = 73        # Lap completed (carId + lapTime + cuts + leaderboard)
    CLIENT_EVENT = 130

    # Collision sub-types
    CE_COLLISION_WITH_CAR = 10
    CE_COLLISION_WITH_ENV = 11

class PacketParser:
    def __init__(self, data):
        self.data = data
        self.offset = 0

    def read_uint8(self):
        if self.offset + 1 > len(self.data): return None
        val = self.data[self.offset]
        self.offset += 1
        return val

    def read_uint16(self):
        if self.offset + 2 > len(self.data): return None
        val = struct.unpack_from('<H', self.data, self.offset)[0]
        self.offset += 2
        return val

    def read_uint32(self):
        if self.offset + 4 > len(self.data): return None
        val = struct.unpack_from('<I', self.data, self.offset)[0]
        self.offset += 4
        return val

    def read_float(self):
        if self.offset + 4 > len(self.data): return None
        val = struct.unpack_from('<f', self.data, self.offset)[0]
        self.offset += 4
        return val

    def remaining(self):
        return len(self.data) - self.offset

    def read_string(self):
        """Reads a std::string from AC server (1 byte length + ASCII/UTF-8 bytes)"""
        if self.offset >= len(self.data): return ""
        length = self.read_uint8()
        if length is None or length == 0: return ""

        if self.offset + length <= len(self.data):
            chunk = self.data[self.offset : self.offset + length]
            self.offset += length
            try:
                return chunk.decode('utf-8', errors='replace').split('\x00')[0]
            except: pass
        return ""

    def read_wstring(self):
        """Reads a std::wstring from AC server (1 byte length + UTF-32 bytes)"""
        if self.offset >= len(self.data): return ""
        length = self.read_uint8()
        if length is None or length == 0: return ""

        res = ""
        for _ in range(length):
            if self.offset + 4 <= len(self.data):
                char_bytes = self.data[self.offset : self.offset + 4]
                self.offset += 4
                try:
                    char = char_bytes.decode('utf-32le', errors='replace').replace('\x00', '')
                    res += char
                except: pass
            else:
                break
        return res.strip()
