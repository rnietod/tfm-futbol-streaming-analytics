from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from src.data.redis_client import get_redis_connection
import asyncio
import json

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

@app.get("/")
def read_root():
    return {"status": "TACTIX API Online 🟢"}

@app.get("/match/{match_id}/metadata")
def get_match_metadata(match_id: str):
    """
    Endpoint para obtener la alineación inicial y configuración del partido.
    El Frontend llama a esto UNA vez al cargar la página.
    """
    if not redis_conn:
        return {"error": "Redis no conectado"}
    
    key = f"match:{match_id}:metadata"
    data = redis_conn.get(key)
    
    if data:
        return json.loads(data)
    return {"error": "Metadata no encontrada (¿Enviaste la alineación desde el Dashboard?)"}

@app.websocket("/ws/match/{match_id}")
async def websocket_endpoint(websocket: WebSocket, match_id: str):
    """
    Tubería de datos en tiempo real.
    Combina Tracking (Alta frecuencia) + Eventos (Baja frecuencia).
    """
    await websocket.accept()
    print(f"🟢 Cliente conectado al partido: {match_id}")
    
    # 1. Suscribirse al Canal de Eventos (Push)
    # Esto nos avisa inmediatamente cuando hay un gol o tarjeta
    pubsub = redis_conn.pubsub()
    pubsub.subscribe(f"match:{match_id}:events")
    
    try:
        while True:
            # --- A. GESTIÓN DE TRACKING (Polling Rápido) ---
            # Leemos el último frame disponible en la memoria RAM
            tracking_raw = redis_conn.get(f"match:{match_id}:tracking")
            
            if tracking_raw:
                track_data = json.loads(tracking_raw)
                
                # Enviamos al Frontend etiquetado como "tracking"
                # El frontend usará esto para mover los puntos en el mapa
                await websocket.send_json({
                    "type": "tracking",
                    "payload": track_data 
                })

            # --- B. GESTIÓN DE EVENTOS (Lectura de Cola) ---
            # Verificamos si Redis nos ha gritado algún evento nuevo
            message = pubsub.get_message(ignore_subscribe_messages=True)
            
            if message and message['type'] == 'message':
                event_data = json.loads(message['data'])
                
                print(f"⚡ Enviando evento al frontend: {event_data.get('type_name')}")
                
                # Enviamos al Frontend etiquetado como "event"
                # El frontend usará esto para añadirlo a la lista de la izquierda
                await websocket.send_json({
                    "type": "event",
                    "payload": event_data
                })

            # Pequeña pausa técnica para dar aire a la CPU
            # (Tracking va a 25fps = 0.04s, así que 0.01s es seguro para no perder frames)
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        print(f"🔴 Cliente desconectado")
    except Exception as e:
        print(f"❌ Error crítico en WebSocket: {e}")
        await websocket.close()