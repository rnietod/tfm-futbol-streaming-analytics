from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
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
    Endpoint para obtener la alineación inicial y datos del partido.
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # 1. Consulta SQL de Match Info
            match_query = text("""
                SELECT home_team_id, home_team_name, home_team_acronym, 
                       away_team_id, away_team_name, away_team_acronym
                FROM matches
                WHERE match_id = :mid
            """)
            match_result = conn.execute(match_query, {"mid": match_id}).fetchone()

            # 2. Consulta SQL de Jugadores
            query = text("""
                SELECT player_id, team_id, name, dorsal, position
                FROM match_players
                WHERE match_id = :mid
            """)
            result = conn.execute(query, {"mid": match_id}).fetchall()

            # Validación de Negocio
            if not result:
                return {"error": "Alineación no encontrada en Base de Datos. Verifique la carga inicial."}

            # Serialización
            players_list = []
            for row in result:
                players_list.append({
                    "player_id": row.player_id,
                    "team_id": row.team_id,
                    "short_name": row.name,
                    "number": row.dorsal,
                    "role": row.position
                })
                
            match_data = None
            if match_result:
                match_data = dict(match_result._mapping)

            print(f"✅ Metadata servida desde SQL: {len(players_list)} jugadores.")
            return {"match": match_data, "players": players_list}

    except Exception as e:
        print(f"❌ Error crítico leyendo DB: {e}")
        return {"error": "Error de conexión con Base de Datos"}


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
        print("🔴 Cliente desconectado")
    except Exception as e:
        print(f"❌ Error crítico en WebSocket: {e}")
        await websocket.close()


# ==========================================
# 📼 TIME-SHIFTING / REPLAY API (PostgreSQL)
# ==========================================
@app.get("/match/{match_id}/tracking/history")
def get_tracking_history(
    match_id: str,
    start_frame: int = Query(..., description="Frame inicial para el buffer de replay"),
    end_frame: int = Query(..., description="Frame final para el buffer de replay")
):
    """
    Recupera segmentos de tracking histórico (video) desde PostgreSQL.
    Devuelve los frames ordenados y parseados, listos para la animación.
    """
    engine = get_db_engine()
    results = []

    try:
        with engine.connect() as conn:
            # Seleccionamos solo lo necesario para mantener la respuesta ligera
            query = text("""
                SELECT frame_idx, players_data
                FROM match_tracking
                WHERE match_id = :mid
                  AND frame_idx BETWEEN :start AND :end
                ORDER BY frame_idx ASC
            """)

            rows = conn.execute(query, {
                "mid": match_id,
                "start": start_frame,
                "end": end_frame
            }).fetchall()

            for row in rows:
                # Parsing seguro del JSON almacenado como texto en DB
                p_data = row.players_data
                if isinstance(p_data, str):
                    p_data = json.loads(p_data)

                frame_obj = {
                    "frame_idx": row.frame_idx,
                    "tracking": p_data
                }
                # Sanitización obligatoria antes de enviar al frontend
                results.append(clean_nans(frame_obj))

        return results

    except Exception as e:
        print(f"❌ Error fetching tracking history: {e}")
        return {"error": "Error recuperando histórico de tracking", "details": str(e)}


@app.get("/match/{match_id}/events/history")
def get_events_history(match_id: str):
    """
    Recupera la lista COMPLETA de eventos del partido (Contexto).
    No aplica filtros de tiempo: devuelve todo lo ocurrido hasta ahora.
    """
    engine = get_db_engine()
    events = []

    try:
        with engine.connect() as conn:
            query = text("""
                SELECT *
                FROM match_events
                WHERE match_id = :mid
                ORDER BY period ASC, minute ASC, second ASC
            """)

            rows = conn.execute(query, {"mid": match_id}).fetchall()

            for row in rows:
                r = row._mapping
                # Mapeo consistente con el formato del WebSocket
                evt = {
                    "id": r['event_uuid'],
                    "index": r['event_index'],
                    "period": r['period'],
                    "minute": r['minute'],
                    "second": r['second'],
                    "timestamp": str(r['timestamp']) if r['timestamp'] else None,

                    # Estos son los que el Frontend busca desesperadamente:
                    "event_type_id": r['event_type_id'],
                    "event_type_name": r['event_type_name'],
                    "type_id": r['type_id'],
                    "type_name": r['type_name'],
                    "outcome_id": r['outcome_id'],
                    "outcome_name": r['outcome_name'],

                    "player_id": r['player_id'],
                    "player_name": r['player_name'],
                    "team_name": r['team_name'],

                    "location": [r['location_x'], r['location_y']],
                    "pass": {
                        "recipient": r['pass_recipient_name'],
                        "length": r['pass_length']
                    }
                }
                events.append(clean_nans(evt))

        return events

    except Exception as e:
        print(f"❌ Error fetching events history: {e}")
        return {"error": "Error recuperando histórico de eventos", "details": str(e)}
