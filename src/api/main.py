from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from src.data.redis_client import get_redis_connection, get_match_state
import asyncio
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Acepta conexiones de cualquier origen (React, Postman, etc.)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Conexión global a Redis (se abre al iniciar la app)
redis_conn = get_redis_connection()

@app.get("/")
def read_root():
    return {"status": "ok", "service": "Real-Time Football API"}

@app.websocket("/ws/match/{match_id}")
async def websocket_endpoint(websocket: WebSocket, match_id: str):
    """
    Endpoint de Streaming.
    El frontend se conecta aquí para recibir la lluvia de datos.
    """
    await websocket.accept()
    print(f"🟢 Cliente conectado al partido: {match_id}")
    
    last_frame_idx = -1
    
    try:
        while True:
            # 1. Leer el estado actual de Redis (ultra rápido)
            state = get_match_state(redis_conn, match_id)
            
            if state:
                current_frame = state.get("frame", -1)
                
                # 2. Solo enviamos si hay un frame nuevo (evitamos duplicados)
                if current_frame > last_frame_idx:
                    await websocket.send_json(state)
                    last_frame_idx = current_frame
            
            # 3. Pequeña pausa para no saturar el loop (Polling a Redis)
            # 0.01s = 10ms -> Suficiente para 100 FPS
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        print(f"🔴 Cliente desconectado del partido: {match_id}")
    except Exception as e:
        print(f"❌ Error en WebSocket: {e}")
        await websocket.close()