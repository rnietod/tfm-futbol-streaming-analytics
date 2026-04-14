from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from src.data.redis_client import get_redis_connection
import asyncio
import json
import math
import sys
import os
import subprocess
from pathlib import Path
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
                SELECT mp.player_id, mp.team_id, mp.name, mp.dorsal, mp.position,
                       lp.deviation,
                       gp.pct_shots, gp.pct_creation, gp.pct_progression,
                       gp.pct_defense, gp.pct_workrate, gp.pct_saves, gp.pct_distribution,
                       gp.player_type as ghost_player_type
                FROM match_players mp
                LEFT JOIN player_live_projection lp ON mp.player_id = lp.player_id AND lp.match_id = :mid
                LEFT JOIN player_ghost_profile gp ON mp.player_id = gp.tracking_player_id AND gp.game_state = 'Drawing'
                WHERE mp.match_id = :mid
            """)
            result = conn.execute(query, {"mid": match_id}).fetchall()

            # Validación de Negocio
            if not result:
                return {"error": "Alineación no encontrada en Base de Datos. Verifique la carga inicial."}

            import json
            import os
            
            # Recuperar groups desde ids_tracking.json
            role_map = {}
            ids_path = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'ids_tracking.json')
            try:
                if os.path.exists(ids_path):
                    with open(ids_path, 'r', encoding='utf-8') as f:
                        tracking_data = json.load(f)
                        for p in tracking_data.get('players', []):
                            pid = str(p.get('team_player_id'))
                            pgroup = p.get('player_role', {}).get('position_group', 'Unknown')
                            role_map[pid] = pgroup
            except Exception as e:
                print(f"Error parseando ids_tracking para position_group: {e}")

            # Serialización
            players_list = []
            for row in result:
                # Calcular ghost_score promedio desde percentiles de BigQuery
                bq_pcts = []
                for col_name in ['pct_shots', 'pct_creation', 'pct_progression', 'pct_defense', 'pct_workrate', 'pct_saves', 'pct_distribution']:
                    val = getattr(row, col_name, None)
                    if val is not None:
                        bq_pcts.append(float(val))
                ghost_score = round(sum(bq_pcts) / len(bq_pcts) * 100, 1) if bq_pcts else None

                players_list.append({
                    "player_id": row.player_id,
                    "team_id": row.team_id,
                    "short_name": row.name,
                    "number": row.dorsal,
                    "role": row.position,
                    "position_group": role_map.get(str(row.player_id), "Unknown"),
                    "deviation": float(row.deviation) if row.deviation is not None else 0.0,
                    "ghost_score": ghost_score
                })
                
            match_data = None
            if match_result:
                match_data = dict(match_result._mapping)

            print(f"✅ Metadata servida desde SQL: {len(players_list)} jugadores.")
            return {"match": match_data, "players": players_list}

    except Exception as e:
        print(f"❌ Error crítico leyendo DB: {e}")
        return {"error": "Error de conexión con Base de Datos"}


@app.get("/player/{player_id}/ghost_profile")
def get_player_ghost_profile(player_id: str, game_state: str = Query('Drawing')):
    """
    Endpoint para alimentar el Radar Chart del frontend según el estado del partido
    (Winning, Drawing, Losing).
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # Join con match_players y profile en vivo
            query = text("""
                SELECT p.*,
                       lp.proj_pct_shots, lp.proj_pct_creation, lp.proj_pct_progression,
                       lp.proj_pct_defense, lp.proj_pct_workrate, lp.proj_pct_saves, lp.proj_pct_distribution,
                       lp.deviation, lp.minutes_played
                FROM player_ghost_profile p
                JOIN match_players m ON m.name = p.player_name OR CAST(m.player_id AS VARCHAR) = p.tracking_player_id::VARCHAR
                LEFT JOIN player_live_projection lp ON CAST(m.player_id AS VARCHAR) = CAST(lp.player_id AS VARCHAR)
                WHERE CAST(m.player_id AS VARCHAR) = :pid
                  AND p.game_state = :state
                LIMIT 1
            """)
            result = conn.execute(query, {
                "pid": player_id,
                "state": game_state
            }).fetchone()

            if not result:
                # Fallback genérico si no encontró para ese estado o aún no cargó
                return {"status": "processing", "message": f"Data not ready for state: {game_state}"}

            return clean_nans(dict(result._mapping))

    except Exception as e:
        print(f"❌ Error leyendo Ghost Profile: {e}")
        return {"error": "Error de BD obteniendo Ghost Profile"}



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


# ==========================================
# 🛠️ SERVICE MANAGER (Dev Tools)
# ==========================================

# Registro de procesos en memoria (se reinicia con la API)
_SERVICES: dict = {}

# Directorio raíz del proyecto (dos niveles arriba de src/api/)
PROJECT_ROOT = Path(__file__).parent.parent.parent
VENV_PYTHON = PROJECT_ROOT / "venvfutbol" / "Scripts" / "python.exe"
VENV_STREAMLIT = PROJECT_ROOT / "venvfutbol" / "Scripts" / "streamlit.exe"

SERVICE_COMMANDS = {
    "worker": [str(VENV_PYTHON), str(PROJECT_ROOT / "src" / "data" / "worker_persist.py")],
    "dashboard": [str(VENV_STREAMLIT), "run", str(PROJECT_ROOT / "src" / "streaming" / "dashboard.py"), "--server.headless", "true"],
    # La propia API no puede iniciarse a sí misma; se marca siempre como running
    "api": None,
}


@app.get("/admin/services/status")
def get_services_status():
    """Devuelve el estado (running/stopped) de cada servicio gestionado."""
    status = {}
    for svc_id, proc in _SERVICES.items():
        if proc is None:
            status[svc_id] = False
        else:
            status[svc_id] = proc.poll() is None  # None = sigue en marcha

    # La API siempre está corriendo (somos nosotros)
    status["api"] = True
    return status


@app.post("/admin/services/start/{service_id}")
def start_service(service_id: str):
    """Arranca un servicio en background."""
    if service_id == "api":
        return {"ok": False, "message": "La API no puede reiniciarse a sí misma"}

    if service_id not in SERVICE_COMMANDS:
        return {"ok": False, "message": f"Servicio desconocido: {service_id}"}

    # Si ya está corriendo, no lanzamos otro
    existing = _SERVICES.get(service_id)
    if existing and existing.poll() is None:
        return {"ok": True, "message": f"{service_id} ya está en marcha"}

    cmd = SERVICE_COMMANDS[service_id]
    try:
        env = os.environ.copy()
        env['PYTHONUTF8'] = '1'
        env['PYTHONIOENCODING'] = 'utf-8'
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=env,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        _SERVICES[service_id] = proc
        print(f"[OK] Servicio iniciado: {service_id} (PID {proc.pid})")
        return {"ok": True, "pid": proc.pid}
    except Exception as e:
        print(f"[ERROR] Al iniciar {service_id}: {e}")
        return {"ok": False, "message": str(e)}


@app.post("/admin/services/stop/{service_id}")
def stop_service(service_id: str):
    """Detiene un servicio gestionado."""
    if service_id == "api":
        return {"ok": False, "message": "No puedes parar la API desde aquí"}

    proc = _SERVICES.get(service_id)
    if not proc or proc.poll() is not None:
        return {"ok": True, "message": f"{service_id} ya estaba detenido"}

    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        proc.kill()

    _SERVICES[service_id] = None
    print(f"[STOP] Servicio detenido: {service_id}")
    return {"ok": True}


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
