import time
from battle_engine import BattleManager
from database import start_touge_battle, update_touge_score

STEAM_ID_2 = "76561199230780195"
STEAM_ID_1 = "76561199831589225"

def main():
    print("🚗 Iniciando simulación de Touge Battle...")
    
    # Init battle manager and wire callbacks just like main.py
    bm = BattleManager()
    bm.on_battle_start = lambda p1, p2: start_touge_battle(
        "Simulated Server", "pk_usui_pass", "", p1, p2
    )
    bm.on_score_update = update_touge_score

    print("\n--- ETAPA 1: IDLE ---")
    print("Simulando coches quietos lado a lado...")
    for _ in range(40):  # 4 seconds at 0 km/h
        bm.update(STEAM_ID_1, spline=0.100, speed=0.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.100, speed=0.0, world_position=(0,0,2))
        time.sleep(0.1)

    print("\n--- ETAPA 2: ARMED -> LAUNCHING ---")
    print("Acelerando a fondo (> 40 km/h)")
    for _ in range(30):
        bm.update(STEAM_ID_1, spline=0.101, speed=60.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.101, speed=60.0, world_position=(0,0,2))
        time.sleep(0.1)

    print("\n--- ETAPA 3: ACTIVE (Punto 1 para P1) ---")
    print("P1 toma la delantera y llega a la meta...")
    # Simulate racing to the end, P1 faster, taking much longer (approx 20 seconds)
    for i in range(200):
        bm.update(STEAM_ID_1, spline=0.150 + (i*0.0035), speed=100.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.149 + (i*0.0030), speed=90.0,  world_position=(0,0,2))
        time.sleep(0.1)

    print("\n--- ETAPA 4: Esperando Cooldown para Run 2 ---")
    for _ in range(150): # wait >10 seconds
        bm.update(STEAM_ID_1, spline=0.100, speed=0.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.100, speed=0.0, world_position=(0,0,2))
        time.sleep(0.1)

    print("\n--- ETAPA 5: Run 2 (Penalización por choque para P2) ---")
    for _ in range(30): # Launch again
        bm.update(STEAM_ID_1, spline=0.101, speed=60.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.101, speed=60.0, world_position=(0,0,2))
        time.sleep(0.1)
        
    print("Corre el Run 2 por un tiempo...")
    for i in range(120):
        # P2 is Chase, P1 is Lead. But wait, roles alternate!
        # In Run 1, P1 was Lead. So in Run 2, P2 is Lead.
        # P2 pulls ahead.
        bm.update(STEAM_ID_1, spline=0.150 + (i*0.0030), speed=90.0,  world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.150 + (i*0.0035), speed=100.0, world_position=(0,0,2))
        time.sleep(0.1)
        
    print("P2 gana el Run 2 (Punto para P2)")
    # Force finish run 2
    for i in range(10):
        bm.update(STEAM_ID_1, spline=0.90, speed=90.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.95, speed=100.0, world_position=(0,0,2))
        
    print("\n--- ETAPA 6: Esperando Cooldown para Run 3 ---")
    for _ in range(150): # wait >10 seconds
        bm.update(STEAM_ID_1, spline=0.100, speed=0.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.100, speed=0.0, world_position=(0,0,2))
        time.sleep(0.1)

    print("\n--- ETAPA 7: Run 3 (Batalla Final, P1 Lead, P2 Chase) ---")
    for _ in range(30): # Launch again
        bm.update(STEAM_ID_1, spline=0.101, speed=60.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.101, speed=60.0, world_position=(0,0,2))
        time.sleep(0.1)
        
    for i in range(60):
        bm.update(STEAM_ID_1, spline=0.150 + (i*0.003), speed=100.0, world_position=(0,0,0))
        bm.update(STEAM_ID_2, spline=0.150 + (i*0.003), speed=120.0, world_position=(0,0,2))
        time.sleep(0.1)

    print("P2 golpea a P1! (Falta, punto para P1)")
    # Simulate P2 hitting P1
    bm.handle_collision(STEAM_ID_1, STEAM_ID_2, impact_speed=25.0)


    print("\n✅ Simulación completada. Revisa tu DB / API web para ver los resultados.")

if __name__ == "__main__":
    main()
