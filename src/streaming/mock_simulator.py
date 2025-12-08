import time
import math
import random
from src.data.redis_client import get_redis_connection, set_match_state

def generate_frame(frame_idx):
    """
    Genera posiciones falsas para 22 jugadores + 1 balón.
    """
    players = []
    
    # Equipo Local (Rojo) - Se mueven en círculos
    for i in range(11):
        angle = frame_idx * 0.1 + (i * (2 * math.pi / 11))
        x = 50 + 20 * math.cos(angle)
        y = 50 + 20 * math.sin(angle)
        players.append({
            "id": f"home_{i}",
            "team": "home",
            "x": x,
            "y": y,
            "jersey_number": i + 1
        })

    # Equipo Visitante (Azul) - Se mueven de lado a lado
    for i in range(11):
        x = (frame_idx * 2 + i * 10) % 100
        y = 30 + i * 4
        players.append({
            "id": f"away_{i}",
            "team": "away",
            "x": x,
            "y": y,
            "jersey_number": i + 1
        })

    # Balón
    ball = {
        "id": "ball",
        "team": "ball",
        "x": 50 + 10 * math.cos(frame_idx * 0.2),
        "y": 50 + 10 * math.sin(frame_idx * 0.2)
    }

    return {
        "match_id": "test_match",
        "frame": frame_idx,
        "timestamp": time.time(),
        "objects": players + [ball]
    }

def run_simulation():
    r = get_redis_connection()
    if not r:
        return

    print("🚀 Iniciando Simulador de Mock Data hacia Redis...")
    frame = 0
    try:
        while True:
            # 1. Generar datos
            state = generate_frame(frame)
            
            # 2. Guardar en Redis (Simula el Ingestor)
            set_match_state(r, "test_match", state)
            
            # 3. Log cada 10 frames para no ensuciar consola
            if frame % 10 == 0:
                print(f"📡 Frame {frame} enviado. Objetos: {len(state['objects'])}")
            
            frame += 1
            # 4. Esperar 40ms (aprox 25 FPS)
            time.sleep(0.04) 

    except KeyboardInterrupt:
        print("\n🛑 Simulación detenida.")

if __name__ == "__main__":
    run_simulation()