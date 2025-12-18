import redis
import json
import os

# Configuración (Idealmente esto iría en variables de entorno)
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

def get_redis_connection():
    """
    Crea y devuelve una conexión a Redis.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        # Ping rápido para verificar conexión
        r.ping()
        return r
    except redis.ConnectionError as e:
        print(f"❌ Error fatal conectando a Redis: {e}")
        return None

def set_match_state(r, match_id, state_data):
    """
    Guarda el estado actual del partido en Redis.
    Usamos 'set' simple porque sobrescribimos el frame anterior.
    """
    key = f"match:{match_id}:live"
    # Convertimos el dict a string JSON
    r.set(key, json.dumps(state_data))

def get_match_state(r, match_id):
    """
    Recupera el último estado del partido.
    """
    key = f"match:{match_id}:live"
    data = r.get(key)
    if data:
        return json.loads(data)
    return None