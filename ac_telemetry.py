"""
ac_telemetry.py - AC Remote Telemetry UDP Client (puerto 9996)
Implementa el protocolo de Assetto Corsa Remote Telemetry:
https://docs.google.com/document/d/1KfkZiIluXZ6mMhLWfDX1qAGbvhGRC3ZUzjVIt5FQpp4/pub

DIFERENTE al plugin ACSP (puerto 13000). Este protocolo da datos en tiempo real
de UNO SOLO del coches: el que está en la PC que corre el juego.

Para batallas multijugador en servidor dedicado, hay que ejecutar este script
en CADA PC de jugador y centralizar los resultados.
"""
import socket
import struct
import threading
import time

#  ------- Constantes del protocolo AC Remote Telemetry --------
AC_TELEMETRY_PORT = 9996

OPERATION_HANDSHAKE = 0
OPERATION_SUBSCRIBE_UPDATE = 1
OPERATION_SUBSCRIBE_SPOT = 2
OPERATION_DISMISS = 3

IDENTIFIER = 0       # eIPhoneDevice
VERSION = 1

# Tamaños de respuesta del servidor
SIZE_HANDSHAKER_RESPONSE = 408
SIZE_RT_CAR_INFO = 328
SIZE_RT_LAP = 212


def _make_handshaker(operation):
    """Crea un paquete handshaker de 12 bytes."""
    return struct.pack('<iii', IDENTIFIER, VERSION, operation)


def parse_rt_car_info(data):
    """
    Desempaqueta el paquete RTCarInfo (328 bytes) del servidor AC.
    Retorna un dict con los campos más relevantes para la batalla:
      - speedKmh: velocidad en km/h
      - carPositionNormalized: posición en el spline [0.0, 1.0]
      - carCoordinatesX/Y/Z: posición 3D en el mundo
    """
    if len(data) < SIZE_RT_CAR_INFO:
        return None
    try:
        speed_kmh    = struct.unpack_from('<f', data, 8)[0]
        spline       = struct.unpack_from('<f', data, 308)[0]
        coord_x      = struct.unpack_from('<f', data, 316)[0]
        coord_y      = struct.unpack_from('<f', data, 320)[0]
        coord_z      = struct.unpack_from('<f', data, 324)[0]
        return {
            'speedKmh': speed_kmh,
            'spline': spline,
            'pos': (coord_x, coord_y, coord_z)
        }
    except struct.error:
        return None


class ACTelemetryClient:
    """
    Cliente UDP de telemetría en tiempo real de Assetto Corsa (puerto 9996).
    Usa el protocolo de AC Remote Telemetry (NO el plugin ACSP).
    
    Uso:
        client = ACTelemetryClient('127.0.0.1', guid='76561198...')
        client.on_update = lambda data: battle_manager.update(guid, data['spline'], data['speedKmh'], data['pos'])
        client.start()
    """

    def __init__(self, server_ip='127.0.0.1', server_port=AC_TELEMETRY_PORT, guid='unknown'):
        self.server_ip = server_ip
        self.server_port = server_port
        self.guid = guid
        self.sock = None
        self.running = False
        self.thread = None
        self.on_update = None   # Callback: fn(data_dict)
        self.on_lap = None      # Callback: fn(data_dict) 

    def start(self):
        """Inicia el cliente en un hilo aparte."""
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2.0)
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        print(f"[TELEMETRY {self.guid}] Cliente iniciado -> {self.server_ip}:{self.server_port}")

    def stop(self):
        """Detiene el cliente."""
        self.running = False
        self._dismiss()
        if self.sock:
            self.sock.close()
            self.sock = None

    def _send(self, operation):
        try:
            pkt = _make_handshaker(operation)
            self.sock.sendto(pkt, (self.server_ip, self.server_port))
        except Exception as e:
            print(f"[TELEMETRY {self.guid}] Error enviando paquete: {e}")

    def _dismiss(self):
        self._send(OPERATION_DISMISS)

    def _run(self):
        # 1. Handshake inicial
        self._send(OPERATION_HANDSHAKE)
        handshaked = False
        
        while self.running:
            try:
                data, _ = self.sock.recvfrom(1024)
                size = len(data)

                if size == SIZE_HANDSHAKER_RESPONSE and not handshaked:
                    # Respuesta al handshake: suscribir updates
                    print(f"[TELEMETRY {self.guid}] Handshake OK. Suscribiendo a updates en tiempo real...")
                    self._send(OPERATION_SUBSCRIBE_UPDATE)
                    handshaked = True

                elif size == SIZE_RT_CAR_INFO:
                    info = parse_rt_car_info(data)
                    if info and self.on_update:
                        self.on_update(info)

                elif size == SIZE_RT_LAP:
                    if self.on_lap:
                        self.on_lap(data)

            except socket.timeout:
                # Reintento del handshake si no hemos recibido nada
                if not handshaked:
                    self._send(OPERATION_HANDSHAKE)
            except Exception as e:
                if self.running:
                    print(f"[TELEMETRY {self.guid}] Error: {e}")
                break
