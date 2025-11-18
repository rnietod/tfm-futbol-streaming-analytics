# publisher.py (En la Raíz del Proyecto)

import pandas as pd
import json
import time
from google.cloud import pubsub_v1
import os 
import sys
from datetime import datetime

# =========================================================================
# ⚠️ FIX DE PATH FINAL: Asegura la importación desde TACTIX_LIVE
# =========================================================================

# 1. Añadir el directorio que contiene TACTIX_LIVE al PATH.
# dirname(__file__) es la raíz del proyecto.
project_root = os.path.abspath(os.path.dirname(__file__))

# 2. Agregar la raíz al PATH para que Python pueda encontrar 'TACTIX_LIVE'
if project_root not in sys.path:
    sys.path.append(project_root)

# Ahora la importación funciona a través del paquete TACTIX_LIVE
try:
    # La ruta correcta es TACTIX_LIVE.utils.config_loader
    from TACTIX_LIVE.utils.config_loader import load_config
except ImportError as e:
    # Si falla, es probable que __init__.py o el nombre del archivo no exista.
    print(f"❌ ERROR CRÍTICO DE IMPORTACIÓN: No se pudo importar el módulo de configuración.")
    print(f"Detalle del Error: {e}")
    print("\nVerificación de la Estructura:")
    print(f"  - ¿Existe el archivo en: {os.path.join(project_root, 'TACTIX_LIVE', 'utils', 'config_loader.py')}?")
    print("  - ¿Existe TACTIX_LIVE/__init__.py y TACTIX_LIVE/utils/__init__.py?")
    sys.exit(1)


# =========================================================================
# 1. FUNCIONES DE UTILIDAD
# =========================================================================

def time_to_seconds(time_str: str) -> float | None:
    """
    Convierte un timestamp de cadena (ej: "00:12:14.80") a segundos totales.
    """
    if pd.isna(time_str) or time_str is None:
        return None

    try:
        parts = str(time_str).split(':')
        
        # El formato esperado es HH:MM:SS.ss (3 partes) o MM:SS.ss (2 partes)
        if len(parts) == 3:
            h, m, s = map(float, parts)
        elif len(parts) == 2:
            h = 0.0
            m, s = map(float, parts)
        else:
            return None

        return h * 3600 + m * 60 + s
    except Exception:
        return None 


def load_ids_map(file_path: str) -> dict:
    """Carga el archivo ids_tracking.json y retorna el mapa de IDs de jugadores."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Creamos un diccionario de mapeo player_id -> {'team_id', 'team_name', 'player_name'}
        player_map = {}
        for team_data in data.values():
            team_id = team_data.get('team_id')
            team_name = team_data.get('team_name')
            
            for player_info in team_data.get('players', []):
                player_map[player_info.get('player_id')] = {
                    'team_id': team_id,
                    'team_name': team_name,
                    'player_name': player_info.get('player_name')
                }
        return player_map
        
    except FileNotFoundError:
        print(f"❌ ERROR: Archivo de mapeo de IDs '{file_path}' no encontrado.", file=sys.stderr)
        return {}
    except Exception as e:
        print(f"❌ ERROR al cargar el archivo de IDs: {e}", file=sys.stderr)
        return {}


def publish_message(topic_path: str, data: dict, data_type: str):
    """Publica un mensaje codificado en JSON a un Topic de Pub/Sub."""
    try:
        message_json = json.dumps(data, default=str) 
        message_bytes = message_json.encode("utf-8")
        
        publisher.publish(topic_path, message_bytes, data_type=data_type)
    except Exception as e:
        print(f"❌ Error al publicar {data_type} en {topic_path}: {e}", file=sys.stderr)


# =========================================================================
# 2. CARGA DE CONFIGURACIÓN Y CLIENTES
# =========================================================================

ENVIRONMENT = os.environ.get("APP_ENV", "dev") 

try:
    CONFIG = load_config(ENVIRONMENT)
except FileNotFoundError as e:
    print(e)
    sys.exit(1)

# Variables de GCP y configuración
PROJECT_ID = CONFIG['gcp_project_id']
TOPIC_TRACKING_ID = CONFIG['pubsub']['topic_tracking']
TOPIC_EVENTING_ID = CONFIG['pubsub']['topic_eventing']
IDS_FILE = "data/ids_tracking.json" # Ruta fija del archivo de soporte

TRACKING_FILE = CONFIG['data_paths']['tracking_file']
EVENTING_FILE = CONFIG['data_paths']['eventing_file'].replace(".xlsx", ".csv") # CORRECCIÓN: Usamos CSV

SPEED_MULTIPLIER = CONFIG.get('simulation_speed_multiplier', 1)

publisher = pubsub_v1.PublisherClient()
topic_path_tracking = publisher.topic_path(PROJECT_ID, TOPIC_TRACKING_ID)
topic_path_eventing = publisher.topic_path(PROJECT_ID, TOPIC_EVENTING_ID)

print(f"✅ Publisher inicializado. Ambiente: {ENVIRONMENT.upper()}. Multiplicador de velocidad: {SPEED_MULTIPLIER}x")


# =========================================================================
# 3. FUNCIÓN DE CARGA Y PRE-PROCESAMIENTO DE DATOS
# =========================================================================

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga, limpia y prepara los datos para la simulación."""
    print(f"\nCargando datos desde:\n  - Tracking: {TRACKING_FILE}\n  - Eventing: {EVENTING_FILE}")

    try:
        # Cargar Tracking Data (JSONL)
        tracking_df = pd.read_json(TRACKING_FILE, lines=True)
        # Cargar Eventing Data (CSV)
        eventing_df = pd.read_csv(EVENTING_FILE)
        
    except FileNotFoundError:
         raise FileNotFoundError(f"Asegúrate de que tus archivos de datos existan en la carpeta 'data/'.")

    # Cargar y preparar el mapa de IDs
    player_id_map = load_ids_map(IDS_FILE)
    
    # ----------------------------------------------------------------------
    # LIMPIEZA Y CONVERSIÓN DE TRACKING DATA (89k registros)
    # ----------------------------------------------------------------------
    
    # 1. Aplicar la función de conversión de tiempo a la columna 'timestamp'
    tracking_df['game_time_seconds'] = tracking_df['timestamp'].apply(time_to_seconds)
    
    # 2. Filtrar registros nulos/vacíos (el ~15% problemático)
    # Un registro es "vacío" si el timestamp no se pudo convertir O si no tiene player_data
    tracking_df = tracking_df.dropna(subset=['game_time_seconds', 'player_data'])
    
    # 3. Eliminar la columna de string original
    tracking_df = tracking_df.drop(columns=['timestamp'])
    
    # 4. Enriquecimiento de jugador (IMPORTANTE para PySpark)
    # Esto ayuda al worker de PySpark a saber rápidamente a qué equipo pertenece un jugador
    def enrich_player_data(players_list):
        if not players_list:
            return players_list
        enriched_list = []
        for player in players_list:
            player_id = player.get('player_id')
            if player_id in player_id_map:
                player.update(player_id_map[player_id])
            enriched_list.append(player)
        return enriched_list

    tracking_df['player_data'] = tracking_df['player_data'].apply(enrich_player_data)
    
    # ----------------------------------------------------------------------
    # VERIFICACIÓN DE EVENTING DATA
    # ----------------------------------------------------------------------
    
    # La columna de tiempo para Eventing debe existir y ya estar en segundos para ser precisa.
    TIME_COLUMN = 'game_time_seconds' 
    
    if TIME_COLUMN not in eventing_df.columns:
        raise ValueError(f"❌ ERROR: Columna de tiempo '{TIME_COLUMN}' no encontrada en Eventing. Por favor, asegúrate de que exista en tu CSV.")

    # Ordenar por tiempo de juego para garantizar el orden del stream
    tracking_df = tracking_df.sort_values(by=TIME_COLUMN)
    eventing_df = eventing_df.sort_values(by=TIME_COLUMN)
    
    print(f"✅ Tracking limpio: {len(tracking_df)} registros después de filtrar registros vacíos.")
    
    return tracking_df, eventing_df


# =========================================================================
# 4. LÓGICA DE SIMULACIÓN Y EJECUCIÓN
# =========================================================================

def simulate_match(tracking_df: pd.DataFrame, eventing_df: pd.DataFrame):
    """Simula el partido combinando flujos y publicando mensajes con pausas."""
    # Código de simulate_match (sin cambios funcionales respecto al anterior, usa las mismas variables)
    
    print("\n==============================================")
    print(f"      ▶️ INICIANDO SIMULACIÓN @ {SPEED_MULTIPLIER}x ◀️     ")
    print("==============================================")
    
    tracking_list = tracking_df.assign(source='tracking').to_dict('records')
    eventing_list = eventing_df.assign(source='eventing').to_dict('records')
    all_data = sorted(tracking_list + eventing_list, key=lambda x: x['game_time_seconds'])
    
    last_game_time = all_data[0]['game_time_seconds']
    start_wall_time = time.time()
    
    for record in all_data:
        current_game_time = record['game_time_seconds']
        
        # 1. Cálculo de la PAUSA
        time_diff = current_game_time - last_game_time
        wait_time = time_diff / SPEED_MULTIPLIER
        
        if wait_time > 0:
            time.sleep(wait_time)
            
        # 2. Publicar el registro
        topic_path = topic_path_tracking if record['source'] == 'tracking' else topic_path_eventing
        data_type = record['source']
            
        record.pop('source') 
        publish_message(topic_path, record, data_type)
        
        # 3. Log y Actualización
        current_wall_time = time.time() - start_wall_time
        
        print(f"[{datetime.now().strftime('%H:%M:%S')}] GAME: {current_game_time:.2f}s | WALL: {current_wall_time:.2f}s | Publicado: {data_type.upper()}")
        
        last_game_time = current_game_time
    
    end_wall_time = time.time()
    print(f"\n--- Simulación FINALIZADA en {end_wall_time - start_wall_time:.2f} segundos. ---")


if __name__ == "__main__":
    try:
        # ⚠️ Verifica que la columna 'game_time_seconds' exista en tu CSV de eventing.
        tracking_data, eventing_data = load_data()
        simulate_match(tracking_data, eventing_data)
            
    except (FileNotFoundError, ValueError) as e:
        print(f"\n🔴 ERROR FATAL EN LA CARGA DE DATOS: {e}", file=sys.stderr)
    except Exception as e:
        print(f"\n🔴 ERROR DE CONEXIÓN O NO CONTROLADO. Revisa la autenticación de GCP.")
        print(f"Detalle: {e}", file=sys.stderr)