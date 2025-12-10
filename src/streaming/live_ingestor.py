import json
import time
import os
import sys
from src.data.redis_client import get_redis_connection, set_match_state

# --- CONFIGURACIÓN FÍSICA ---
PITCH_LENGTH = 105.0  
PITCH_WIDTH = 68.0    

# --- CORRECCIÓN DE EJES ---
# Ajuste fino:
# Si los equipos salen en el lado equivocado -> Cambia INVERT_X
# Si el lateral derecho sale arriba (donde el izquierdo) -> Cambia INVERT_Y
INVERT_X = False  
INVERT_Y = True  # Cambiado a False para probar la corrección del Lateral

# --- SUAVIZADO ---
INTERPOLATION_STEPS = 2 

def meters_to_percentage(x, y):
    if x is None or y is None:
        return None, None
    
    # Aplicar Inversión manual si se requiere
    if INVERT_X: x = -x
    if INVERT_Y: y = -y

    # Eje X: -52.5 a 52.5 -> 0 a 100
    x_pct = ((x + (PITCH_LENGTH / 2)) / PITCH_LENGTH) * 100
    
    # Eje Y: -34 a 34 -> 0 a 100
    # En Web/SVG, el 0 está ARRIBA. En fútbol, Y positivo suele ser IZQUIERDA (Arriba).
    # Si invertimos Y, cambiamos Arriba por Abajo.
    y_pct = ((y + (PITCH_WIDTH / 2)) / PITCH_WIDTH) * 100
    
    return max(-2, min(102, x_pct)), max(-2, min(102, y_pct))

def load_ids_map():
    ids_map = {}
    teams_found = [] 
    try:
        path = os.path.join("data", "ids_tracking.json")
        if not os.path.exists(path): return ids_map

        with open(path, 'r', encoding='utf-8') as f:
            match_data = json.load(f)
            
            # Intentar sacar IDs oficiales
            home_id = match_data.get('home_team', {}).get('id')
            away_id = match_data.get('away_team', {}).get('id')
            
            # Obtener lista de jugadores
            raw_players = match_data.get('players')
            if not raw_players and isinstance(match_data, list): raw_players = match_data
            elif not raw_players and isinstance(match_data, dict): raw_players = list(match_data.values())
            
            if not raw_players: raw_players = []

            print(f"📊 Metadata: {len(raw_players)} jugadores cargados.")

            for p in raw_players:
                if isinstance(p, list): 
                    for sub in p: process_player(sub, ids_map, teams_found, home_id, away_id)
                elif isinstance(p, dict):
                    process_player(p, ids_map, teams_found, home_id, away_id)
            
    except Exception as e:
        print(f"❌ Error IDs: {e}")
    return ids_map

def process_player(p, ids_map, teams_found, home_id=None, away_id=None):
    pid = str(p.get('id'))
    tid = p.get('team_id')
    if not pid or not tid: return

    if tid not in teams_found: teams_found.append(tid)
    
    if home_id and away_id:
        team_side = 'home' if tid == home_id else 'away'
    else:
        team_side = 'home' if tid == teams_found[0] else 'away'

    ids_map[pid] = {
        'team': team_side,
        'number': p.get('number', '?'),
        'name': p.get('short_name') or p.get('last_name', 'Unknown'),
        'role': p.get('player_role', {}).get('acronym', '')
    }

def transform_frame(raw_frame, ids_map):
    # Ya NO ignoramos frames vacíos. Si no hay balón, mandamos lista vacía.
    
    frontend_objects = []

    # Balón
    ball = raw_frame.get('ball_data', {})
    if ball and ball.get('x') is not None:
        bx, by = meters_to_percentage(ball['x'], ball['y'])
        frontend_objects.append({"id": "ball", "team": "ball", "x": bx, "y": by})

    # Jugadores
    players = raw_frame.get('player_data', [])
    if players:
        for p in players:
            pid = str(p.get('player_id'))
            if p.get('x') is None: continue

            px, py = meters_to_percentage(p['x'], p['y'])
            info = ids_map.get(pid)
            
            if not info:
                frontend_objects.append({"id": f"p_{pid}", "team": "unknown", "jersey_number": "", "x": px, "y": py})
            else:
                frontend_objects.append({
                    "id": f"p_{pid}",
                    "team": info['team'],
                    "jersey_number": info['number'],
                    "name": info['name'],
                    "x": px, "y": py
                })

    return {
        "match_id": "real_match",
        "frame": raw_frame.get('frame'),
        "period": raw_frame.get('period'),
        "timestamp": raw_frame.get('timestamp'), # ¡Aquí viaja el tiempo!
        "objects": frontend_objects
    }

def interpolate_frames(start_frame, end_frame, steps):
    interpolated = []
    start_objs = {obj['id']: obj for obj in start_frame['objects']}
    
    for i in range(1, steps + 1):
        fraction = i / (steps + 1)
        new_frame = start_frame.copy()
        new_frame['objects'] = []
        # Importante: Mantener el timestamp del inicio durante la interpolación
        
        for end_obj in end_frame['objects']:
            oid = end_obj['id']
            if oid in start_objs:
                start_obj = start_objs[oid]
                new_x = start_obj['x'] + (end_obj['x'] - start_obj['x']) * fraction
                new_y = start_obj['y'] + (end_obj['y'] - start_obj['y']) * fraction
                new_obj = end_obj.copy()
                new_obj['x'] = new_x
                new_obj['y'] = new_y
                new_frame['objects'].append(new_obj)
            else:
                new_frame['objects'].append(end_obj)
        interpolated.append(new_frame)
    return interpolated

def run_ingestion():
    r = get_redis_connection()
    if not r: return

    print("📚 Metadata cargada.")
    ids_map = load_ids_map()
    
    tracking_path = os.path.join("data", "tracking_file.jsonl")
    if not os.path.exists(tracking_path): return

    print(f"🚀 Streaming desde el inicio (Frame 0)...")
    prev_processed = None

    with open(tracking_path, 'r') as f:
        for line in f:
            try:
                raw = json.loads(line)
                
                # --- SIN FILTROS: PROCESAMOS TODO ---
                current = transform_frame(raw, ids_map)
                
                # Interpolación (solo si hay jugadores para mover)
                if prev_processed and len(current['objects']) > 0:
                    fakes = interpolate_frames(prev_processed, current, INTERPOLATION_STEPS)
                    for fake in fakes:
                        set_match_state(r, "test_match", fake)
                        time.sleep(0.04 / (INTERPOLATION_STEPS + 1))

                set_match_state(r, "test_match", current)
                time.sleep(0.04 / (INTERPOLATION_STEPS + 1))
                
                if current['frame'] % 100 == 0:
                    ts = current.get('timestamp') or "PRE-MATCH"
                    print(f"📡 Frame {current['frame']} | Time: {ts}")

                prev_processed = current

            except json.JSONDecodeError: continue
            except KeyboardInterrupt: break
            except Exception as e: print(f"⚠️ {e}")

if __name__ == "__main__":
    run_ingestion()