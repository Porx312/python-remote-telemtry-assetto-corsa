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
    CAR_UPDATE = 53           # Realtime position
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
        if self.offset >= len(self.data): return ""
        
        start_offset = self.offset
        length = self.read_uint8()
        if length is None: return ""

        # --- DETECCIÓN DE UTF-32 (Padded 4-bytes) ---
        if self.offset + 3 <= len(self.data):
            try:
                val_bytes = self.data[self.offset-1 : self.offset+3]
                val = struct.unpack('<I', val_bytes)[0]
                
                if (val & 0xFFFFFF00) == 0 and val != 0:
                     self.offset -= 1
                     res = ""
                     while self.offset + 4 <= len(self.data):
                         char_code = struct.unpack_from('<I', self.data, self.offset)[0]
                         if char_code == 0:
                             self.offset += 4
                             break
                         if (char_code & 0xFFFFFF00) != 0:
                             break
                        
                         res += chr(char_code)
                         self.offset += 4
                     
                     if len(res) > 0: return res.strip()
                     else: self.offset = start_offset + 1
            except:
                self.offset = start_offset + 1

        # Strategy B: Length prefix followed by UTF-32
        if length > 0 and self.offset + (length * 4) <= len(self.data):
             if self.data[self.offset + 1] == 0 and self.data[self.offset + 2] == 0 and self.data[self.offset + 3] == 0:
                 chunk = self.data[self.offset : self.offset + (length * 4)]
                 self.offset += length * 4
                 try:
                     return chunk.decode('utf-32le').split('\x00')[0]
                 except: pass

        if length == 0: return ""
        
        # --- DETECCIÓN DE UTF-16LE ---
        if self.offset + (length * 2) <= len(self.data):
            if self.data[self.offset + 1] == 0:
                chunk = self.data[self.offset : self.offset + (length * 2)]
                self.offset += length * 2
                try:
                     return chunk.decode('utf-16le').split('\x00')[0]
                except: pass

        # --- CASO POR DEFECTO: ASCII / UTF-8 ---
        if self.offset + length <= len(self.data):
            chunk = self.data[self.offset : self.offset + length]
            self.offset += length
            try:
                return chunk.decode('utf-8', errors='ignore').split('\x00')[0]
            except: pass
            
        return ""
