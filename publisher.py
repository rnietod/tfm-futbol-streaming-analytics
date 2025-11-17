# publisher.py
import pandas as pd
import json
import time
from google.cloud import pubsub_v1
import os 
from datetime import datetime
import sys

# Importamos la utilidad para cargar la configuración
try:
    from TACTIX_LIVE.utils.config_loader import load_config
except ImportError:
    print("❌ ERROR: No se puede importar TACTIX_LIVE.utils.config_loader.")
    print("Asegúrate de que la carpeta TACTIX_LIVE sea un módulo válido (con __init__.py vacío).")
    sys.exit(1)


# =========================================================================
# 1. CARGA DE CONFIGURACIÓN Y CLIENTES
# =========================================================================

# Leer el ambiente de trabajo ('dev' por defecto)
ENVIRONMENT = os.environ.get("APP_ENV", "dev") 

try:
    CONFIG = load_config(ENVIRONMENT)
except FileNotFoundError as e:
    print(e)
    sys.exit(1)

# Asignar variables de la configuración cargada
PROJECT_ID = CONFIG['gcp_project_id']
TOPIC_TRACKING_ID = CONFIG['pubsub']['topic_tracking']
TOPIC_EVENTING_ID = CONFIG['pubsub']['topic_eventing']

# Rutas de los archivos de datos
TRACKING_FILE = CONFIG['data_paths']['tracking_file']
EVENTING_FILE = CONFIG['data_paths']['eventing_file']

# Factor de velocidad de simulación (1x es tiempo real)
SPEED_MULTIPLIER = CONFIG.get('simulation_speed_multiplier', 1)

# Inicializa el Publisher Client (se autentica vía GOOGLE_APPLICATION_CREDENTIALS)
publisher = pubsub_v1.PublisherClient()
topic_path_tracking = publisher.topic_path(PROJECT_ID, TOPIC_TRACKING_ID)
topic_path_eventing = publisher.topic_path(PROJECT_ID, TOPIC_EVENTING_ID)

print(f"✅ Publisher inicializado. Ambiente: {ENVIRONMENT.upper()}. Multiplicador de velocidad: {SPEED_MULTIPLIER}x")


# =========================================================================
# 2. FUNCIONES DE PUBLICACIÓN Y CARGA DE DATOS
# =========================================================================

def publish_message(topic_path: str, data: dict, data_type: str):
    """Publica un mensaje codificado en JSON a un Topic de Pub/Sub."""
    try:
        # Usamos default=str para asegurar que todos los tipos de datos sean serializables a JSON.
        message_json = json.dumps(data, default=str) 
        message_bytes = message_json.encode("utf-8")
        
        # Enviar el mensaje, el atributo data_type es útil para filtrar en PySpark
        publisher.publish(topic_path, message_bytes, data_type=data_type)
    except Exception as e:
        print(f"❌ Error al publicar {data_type} en {topic_path}: {e}", file=sys.stderr)

def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga y pre-procesa los datos de Eventing y Tracking."""
    print(f"\nCargando datos desde:\n  - Tracking: {TRACKING_FILE}\n  - Eventing: {EVENTING_FILE}")

    try:
        # Cargar Tracking Data (JSONL)
        tracking_df = pd.read_json(TRACKING_FILE, lines=True)

        # Cargar Eventing Data (Excel)
        eventing_df = pd.read_excel(EVENTING_FILE)
        
    except FileNotFoundError:
         raise FileNotFoundError(f"Asegúrate de que tus archivos de datos existan en la carpeta 'data/'.")

    # ⚠️ DEFINICIÓN DE COLUMNA CRÍTICA: Debes verificar que este nombre exista en tus archivos.
    TIME_COLUMN = 'game_time_seconds' 
    
    if TIME_COLUMN not in tracking_df.columns or TIME_COLUMN not in eventing_df.columns:
        raise ValueError(f"❌ ERROR: Columna de tiempo '{TIME_COLUMN}' no encontrada. Revisa tus archivos.")

    # Ordenar por tiempo de juego para garantizar el orden del stream
    tracking_df = tracking_df.sort_values(by=TIME_COLUMN)
    eventing_df = eventing_df.sort_values(by=TIME_COLUMN)
    
    return tracking_df, eventing_df


# =========================================================================
# 3. LÓGICA DE SIMULACIÓN EN TIEMPO REAL
# =========================================================================

def simulate_match(tracking_df: pd.DataFrame, eventing_df: pd.DataFrame):
    """Simula el partido combinando flujos y publicando mensajes con pausas."""
    print("\n==============================================")
    print(f"      ▶️ INICIANDO SIMULACIÓN @ {SPEED_MULTIPLIER}x ◀️     ")
    print("==============================================")
    
    # Asignar fuente y combinar toda la data
    tracking_list = tracking_df.assign(source='tracking').to_dict('records')
    eventing_list = eventing_df.assign(source='eventing').to_dict('records')
    all_data = sorted(tracking_list + eventing_list, key=lambda x: x['game_time_seconds'])
    
    # Inicializar contadores de tiempo
    last_game_time = all_data[0]['game_time_seconds']
    start_wall_time = time.time() # Tiempo real de inicio del script
    
    for record in all_data:
        current_game_time = record['game_time_seconds']
        
        # 1. Cálculo de la PAUSA
        time_diff = current_game_time - last_game_time
        wait_time = time_diff / SPEED_MULTIPLIER
        
        # Pausar el script
        if wait_time > 0:
            time.sleep(wait_time)
            
        # 2. Publicar el registro
        if record['source'] == 'tracking':
            topic_path = topic_path_tracking
            data_type = 'tracking'
        else: # source == 'eventing'
            topic_path = topic_path_eventing
            data_type = 'eventing'
            
        # Publicar (eliminar 'source' antes de enviar)
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
        # ⚠️ Verifica que la columna 'game_time_seconds' exista en tus archivos antes de ejecutar.