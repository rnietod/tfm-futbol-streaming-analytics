from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from src.data.redis_client import get_redis_connection
import asyncio
import json
import math
from src.data.postgres_client import get_db_engine
from sqlalchemy import text

app = FastAPI()

# Configuración CORS para que el Frontend (puerto 5173) pueda hablar con el Backend (puerto 8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_conn = get_redis_connection()

# ==========================================
# 🛡️ EL ESCUDO ANTI-NAN (Sanitizer)
# ==========================================
def clean_nans(obj):
    """
    Recorre recursivamente cualquier objeto (lista, dict, valor).
    Si encuentra un float('nan') o float('inf'), lo convierte a None.
    Esto hace que Python genere 'null' en el JSON, que es válido para JS.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    elif isinstance(obj, dict):
        return {k: clean_nans(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nans(v) for v in obj]
    return obj

@app.get("/")
def read_root():
    return {"status": "TACTIX API Online 🟢"}

@app.on_event("startup")
async def startup_event():
    """
    Limpieza automática al arrancar la API.
    Asegura que no queden datos viejos en Redis de sesiones anteriores.
    """
    try:
        # Opción A: Borrado quirúrgico (solo lo de nuestra app)
        keys = []
        for k in redis_conn.scan_iter("match:*"):
            keys.append(k)
        
        if keys:
            redis_conn.delete(*keys)
            print(f"🧹 LIMPIEZA INICIAL: {len(keys)} claves viejas eliminadas de Redis.")
        else:
            print("✨ Redis está limpio. Listo para empezar.")
            
    except Exception as e:
        print(f"⚠️ Alerta: No se pudo limpiar Redis al inicio: {e}")
        
@app.delete("/admin/reset-all")
def reset_all_data():
    """
    NUCLEAR OPTION: Borra Redis + Vacía PostgreSQL (Docker)
    """
    report = {"redis": 0, "db": "Not connected"}

    # 1. Limpieza Redis
    try:
        keys = []
        for k in redis_conn.scan_iter("match:*"):
            keys.append(k)
        if keys:
            redis_conn.delete(*keys)
        report["redis"] = len(keys)
    except Exception as e:
        report["redis_error"] = str(e)

    # 2. Limpieza PostgreSQL (Docker Local)
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # TRUNCATE con CASCADE borra todo en cadena rapidísimo
            conn.execute(text("TRUNCATE TABLE matches, match_events, match_tracking, match_players CASCADE;"))
            conn.commit()
            report["db"] = "Purged (Truncate Cascade)"
    except Exception as e:
        report["db_error"] = str(e)
        print(f"❌ Error borrando DB: {e}")

    print(f"⚠️ ADMIN RESET: {report}")
    return report

@app.get("/match/{match_id}/metadata")
def get_match_metadata(match_id: str):
    """
    Endpoint para obtener la alineación inicial.
    """
    if not redis_conn:
        return {"error": "Redis no conectado"}
    
    key = f"match:{match_id}:metadata"
    data = redis_conn.get(key)
    
    if data:
        json_data = json.loads(data)
        # 🛡️ Blindaje también aquí por si la metadata tiene basura
        return clean_nans(json_data)
        
    return {"error": "Metadata no encontrada (¿Enviaste la alineación desde el Dashboard?)"}

@app.websocket("/ws/match/{match_id}")
async def websocket_endpoint(websocket: WebSocket, match_id: str):
    """
    Tubería de datos en tiempo real.
    """
    await websocket.accept()
    print(f"🟢 Cliente conectado al partido: {match_id}")
    
    # Suscribirse al Canal de Eventos
    pubsub = redis_conn.pubsub()
    pubsub.subscribe(f"match:{match_id}:events")
    
    try:
        while True:
            # --- A. TRACKING (Polling) ---
            tracking_raw = redis_conn.get(f"match:{match_id}:tracking")
            
            if tracking_raw:
                track_data = json.loads(tracking_raw)
                
                # 🛡️ Blindaje antes de enviar
                safe_payload = clean_nans(track_data)
                
                await websocket.send_json({
                    "type": "tracking",
                    "payload": safe_payload 
                })

            # --- B. EVENTOS (Push) ---
            message = pubsub.get_message(ignore_subscribe_messages=True)
            
            if message and message['type'] == 'message':
                event_data = json.loads(message['data'])
                
                # Log de control
                evt_name = event_data.get('event_type_name', 'Evento')
                minute = event_data.get('minute', '?')
                print(f"⚡ Enviando: {evt_name} ({minute}')")
                
                # 🛡️ Blindaje antes de enviar (Aquí es donde solía fallar)
                safe_payload = clean_nans(event_data)
                
                await websocket.send_json({
                    "type": "event",
                    "payload": safe_payload
                })

            # Pausa técnica para no saturar CPU
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        print(f"🔴 Cliente desconectado")
    except Exception as e:
        print(f"❌ Error crítico en WebSocket: {e}")
        await websocket.close()