# publisher.py
# ⚠️ IMPORTANTE: Este script DEBE estar en la raíz de tu proyecto.

import pandas as pd
import json
import time
from google.cloud import pubsub_v1
import os 
from datetime import datetime
import sys

# =========================================================================
# ⚠️ FIX DE PATH Y CONFIGURACIÓN
# =========================================================================
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from TACTIX_LIVE.utils.config_loader import load_config
except ImportError as e:
    print(f"❌ ERROR CRÍTICO: {e}")
    print(f"Verifica que TACTIX_LIVE/utils/config_loader.py exista.")
    sys.exit(1)

# =========================================================================
# 1. FUNCIONES DE UTILIDAD (Conversión Robusta)
# =========================================================================

def time_to_seconds(time_val) -> float | None:
    """
    Convierte cualquier formato de tiempo (HH:MM:SS.ss, MM:SS.ss o float) a segundos.
    """
    if pd.isna(time_val) or time_val == "" or time_val is None:
        return None
    
    # Si ya es número, retornarlo
    if isinstance(time_val, (int, float)):
        return float(time_val)

    try:
        time_str = str(time_val).strip()
        parts = time_str.split(':')
        
        if len(parts) == 3:   # HH:MM:SS.ss
            h, m, s = map(float, parts)
            return h * 3600 + m * 60 + s
        elif len(parts) == 2: # MM:SS.ss
            m, s = map(float, parts)
            return m * 60 + s
        else:
            return float(time_str) # Intento final si es un string numérico
    except Exception:
        return None 

def load_ids_map(file_path: str) -> dict:
    """Carga ids_tracking.json con codificación UTF-8."""
    print(f"   -> Cargando mapa de IDs: {file_path}")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        player_map = {}
        # Adaptado para la estructura típica de tracking providers (Opta, SkillCorner, etc.)
        # Si el JSON es un objeto directo de equipos o una lista
        iterator = data.values() if isinstance(data, dict) else data
        
        for team_data in iterator:
            # Manejo flexible de claves por si varían
            team_id = team_data.get('team_id') or team_data.get('id')
            team_name = team_data.get('team_name') or team_data.get('name')
            
            for player in team_data.get('players', []):
                p_id = player.get('player_id') or player.get('id')
                if p_id:
                    player_map[p_id] = {
                        'team_id': team_id,
                        'team_name': team_name,
                        'player_name': player.get('player_name') or player.get('name') or player.get('nickname')
                    }
        return player_map
    except Exception as e:
        print(f"   ⚠️ Advertencia: No se pudo cargar mapa de IDs ({e}). Se enviarán IDs crudos.")
        return {}

def publish_message(publisher, topic_path, data, data_type):
    try:
        message_json = json.dumps(data, default=str) 
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, message_bytes, data_type=data_type)
    except Exception as e:
        print(f"❌ Error publicación: {e}", file=sys.stderr)

# =========================================================================
# 2. INICIALIZACIÓN
# =========================================================================
ENVIRONMENT = os.environ.get("APP_ENV", "dev") 
try:
    CONFIG = load_config(ENVIRONMENT)
except Exception as e:
    print(f"❌ Error Config: {e}"); sys.exit(1)

PROJECT_ID = CONFIG['gcp_project_id']
TOPIC_TRACKING = CONFIG['pubsub']['topic_tracking']
TOPIC_EVENTING = CONFIG['pubsub']['topic_eventing']
SPEED_MULTIPLIER = CONFIG.get('simulation_speed_multiplier', 1)

# Rutas fijas basadas en tus archivos
TRACKING_FILE = "data/tracking_file.jsonl"
EVENTING_FILE = "data/eventing_file.csv"
IDS_FILE = "data/ids_tracking.json"

print("🔌 Conectando a Google Cloud Pub/Sub...")
try:
    publisher = pubsub_v1.PublisherClient()
    path_track = publisher.topic_path(PROJECT_ID, TOPIC_TRACKING)
    path_event = publisher.topic_path(PROJECT_ID, TOPIC_EVENTING)
    print(f"✅ Conexión exitosa. Velocidad simulación: {SPEED_MULTIPLIER}x")
except Exception as e:
    print(f"❌ Error de Credenciales GCP: {e}"); sys.exit(1)

# =========================================================================
# 3. CARGA Y LIMPIEZA INTELIGENTE DE DATOS
# =========================================================================

def load_data():
    print("\n📂 Cargando Datasets...")
    
    # --- 1. IDs ---
    id_map = load_ids_map(IDS_FILE)

    # --- 2. TRACKING (JSONL) ---
    try:
        print(f"   -> Leyendo Tracking: {TRACKING_FILE}")
        # Leemos en chunks si fuera muy grande, pero 89k cabe en memoria
        track_df = pd.read_json(TRACKING_FILE, lines=True)
    except ValueError as e:
        print(f"❌ Error leyendo JSONL. Verifica el formato: {e}"); sys.exit(1)

    # Limpieza Tracking
    if 'timestamp' in track_df.columns:
        track_df['game_time'] = track_df['timestamp'].apply(time_to_seconds)
    else:
        print("❌ Error: No se encontró columna 'timestamp' en Tracking.")
        sys.exit(1)
    
    # Filtrar basura (registros vacíos o tiempos nulos)
    initial_len = len(track_df)
    track_df = track_df.dropna(subset=['game_time'])
    # Opcional: Filtrar si player_data está vacío o es nulo
    # track_df = track_df[track_df['player_data'].map(lambda d: len(d) > 0 if isinstance(d, list) else False)]
    
    print(f"      Limpieza Tracking: {initial_len} -> {len(track_df)} registros válidos.")

    # Enriquecer con IDs (Optimizado)
    def enrich(players):
        if not isinstance(players, list): return []
        for p in players:
            pid = p.get('player_id')
            if pid in id_map:
                p.update(id_map[pid]) # Añade nombre y equipo al objeto del jugador
        return players

    track_df['player_data'] = track_df['player_data'].apply(enrich)
    
    # --- 3. EVENTING (CSV) ---
    try:
        print(f"   -> Leyendo Eventing: {EVENTING_FILE}")
        # sep=None permite detectar ; o , automáticamente
        ev_df = pd.read_csv(EVENTING_FILE, sep=None, engine='python')
    except Exception as e:
        print(f"❌ Error leyendo CSV Eventing: {e}"); sys.exit(1)

    # Auto-detección de columna de tiempo para Eventing
    possible_cols = ['timestamp', 'time', 'Time', 'period_time', 'game_time_seconds', 'minuto']
    time_col = next((c for c in possible_cols if c in ev_df.columns), None)
    
    if time_col:
        print(f"      Columna de tiempo detectada: '{time_col}'")
        ev_df['game_time'] = ev_df[time_col].apply(time_to_seconds)
        ev_df = ev_df.dropna(subset=['game_time'])
    else:
        print(f"❌ Error: No se detectó columna de tiempo en Eventing. Columnas: {list(ev_df.columns)}")
        sys.exit(1)

    # Ordenar ambos por tiempo
    track_df = track_df.sort_values('game_time')
    ev_df = ev_df.sort_values('game_time')

    return track_df, ev_df

# =========================================================================
# 4. SIMULACIÓN
# =========================================================================

def simulate(track_df, ev_df):
    print("\n==============================================")
    print(f"      ▶️ INICIANDO PARTIDO ⚽ (Simulado)      ")
    print("==============================================")
    
    # Unificar streams
    t_list = track_df.assign(type='tracking').to_dict('records')
    e_list = ev_df.assign(type='eventing').to_dict('records')
    
    # Fusionar y ordenar por tiempo absoluto de juego
    full_stream = sorted(t_list + e_list, key=lambda x: x['game_time'])
    
    if not full_stream:
        print("❌ No hay datos para simular."); return

    start_game_time = full_stream[0]['game_time']
    last_game_time = start_game_time
    
    # Estadísticas simples
    count_t, count_e = 0, 0
    
    try:
        for record in full_stream:
            current_time = record['game_time']
            dtype = record.pop('type') # Extraer tipo y limpiar registro
            
            # Calcular espera (Delta de tiempo real / velocidad)
            wait = (current_time - last_game_time) / SPEED_MULTIPLIER
            
            if wait > 0:
                time.sleep(wait)
            
            # Publicar
            topic = path_track if dtype == 'tracking' else path_event
            publish_message(publisher, topic, record, dtype)
            
            last_game_time = current_time
            
            # Logs visuales
            if dtype == 'eventing':
                count_e += 1
                # Mostrar evento destacado
                evt_name = record.get('type', record.get('event_type', 'Evento'))
                print(f"⚽ [{current_time:.1f}s] EVENTO: {evt_name}")
            else:
                count_t += 1
                # Log de tracking cada ~1 segundo de juego (27 frames) para no saturar consola
                if count_t % 27 == 0:
                    sys.stdout.write(f"\r🏃 Tracking... T={current_time:.1f}s | Frames: {count_t}")
                    sys.stdout.flush()
                    
    except KeyboardInterrupt:
        print("\n🛑 Simulación detenida manualmente.")
    
    print(f"\n\n🏁 Fin de la transmisión. Total Tracking: {count_t}, Eventos: {count_e}")

if __name__ == "__main__":
    t_df, e_df = load_data()
    simulate(t_df, e_df)