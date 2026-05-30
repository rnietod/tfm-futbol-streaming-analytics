from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from src.data.redis_client import get_redis_connection
import asyncio
import json
import math
import os
import csv
from src.data.postgres_client import get_db_engine
from sqlalchemy import text
import subprocess
import sys
from fastapi import HTTPException

# --- Player ID Mapping (opta <-> tracking) ---
_BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_MAPPING_PATH = os.path.join(_BASE_DIR, 'data', 'dim_player_mapping.csv')
_OPTA_TO_TRACKING = {}  # opta_player_id -> tracking_player_id
try:
    with open(_MAPPING_PATH, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            opta = row.get('opta_player_id', '').split('.')[0]  # remove .0
            tracking = row.get('tracking_player_id', '')
            if opta and tracking:
                _OPTA_TO_TRACKING[opta] = tracking
    print(f"📋 Player mapping loaded: {len(_OPTA_TO_TRACKING)} entries")
except Exception as e:
    print(f"⚠️ Could not load player mapping: {e}")

# --- Process Manager Registry ---
process_registry = {}


SERVICES_CONFIG = {
    "streamlit_dashboard": {
        "command": [
            sys.executable,
            "-m",
            "streamlit",
            "run",
            "src/streaming/dashboard.py",
            "--server.port",
            "8501",
            "--server.headless",
            "true"
        ],
        "name": "Streamlit Dashboard"
    },
    "worker_persist": {
        "command": [sys.executable, "src/data/worker_persist.py"],
        "name": "Persistence Worker"
    }
}

app = FastAPI()

# ConfiguraciÃ³n CORS para que el Frontend (puerto 5173) pueda hablar con el Backend (puerto 8000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_conn = get_redis_connection()


# ==========================================
# ðŸ›¡ï¸ EL ESCUDO ANTI-NAN (Sanitizer)
# ==========================================
def clean_nans(obj):
    """
    Recorre recursivamente cualquier objeto (lista, dict, valor).
    Si encuentra un float('nan') o float('inf'), lo convierte a None.
    Esto hace que Python genere 'null' en el JSON, que es vÃ¡lido para JS.
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
    return {"status": "TACTIX API Online ðŸŸ¢"}


@app.on_event("startup")
async def startup_event():
    """
    Limpieza automÃ¡tica al arrancar la API.
    Asegura que no queden datos viejos en Redis de sesiones anteriores.
    """
    try:
        # OpciÃ³n A: Borrado quirÃºrgico (solo lo de nuestra app)
        keys = []
        for k in redis_conn.scan_iter("match:*"):
            keys.append(k)

        if keys:
            redis_conn.delete(*keys)
            print(f"ðŸ§¹ LIMPIEZA INICIAL: {len(keys)} claves viejas eliminadas de Redis.")
        else:
            print("âœ¨ Redis estÃ¡ limpio. Listo para empezar.")

    except Exception as e:
        print(f"âš ï¸ Alerta: No se pudo limpiar Redis al inicio: {e}")


@app.delete("/admin/reset-all")
def reset_all_data():
    """
    NUCLEAR OPTION: Borra Redis + VacÃ­a PostgreSQL (Docker)
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
            # TRUNCATE con CASCADE borra todo en cadena rapidÃ­simo
            conn.execute(text("TRUNCATE TABLE matches, match_events, match_tracking, match_players CASCADE;"))
            conn.commit()
            report["db"] = "Purged (Truncate Cascade)"
    except Exception as e:
        report["db_error"] = str(e)
        print(f"âŒ Error borrando DB: {e}")

    print(f"âš ï¸ ADMIN RESET: {report}")
    return report


@app.get("/match/{match_id}/metadata")
def get_match_metadata(match_id: str):
    """
    Endpoint para obtener la alineaciÃ³n inicial.
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # 0. Info del partido (nombres de equipo)
            match_row = conn.execute(text(
                "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
            ), {"mid": match_id}).fetchone()

            # 1. Consulta SQL Directa
            query = text("""
                SELECT player_id, team_id, name, dorsal, position
                FROM match_players
                WHERE match_id = :mid
            """)

            result = conn.execute(query, {"mid": match_id}).fetchall()

            if not result:
                return {"error": "Alineacion no encontrada en Base de Datos."}

            players_list = []
            team_ids = set()
            for row in result:
                team_ids.add(row.team_id)
                players_list.append({
                    "player_id": row.player_id,
                    "team_id": row.team_id,
                    "short_name": row.name,
                    "number": row.dorsal,
                    "role": row.position
                })

            # Construir mapa team_id -> team_name
            team_ids_sorted = sorted(team_ids)
            home_name = match_row.home_team_name if match_row else "HOME"
            away_name = match_row.away_team_name if match_row else "AWAY"

            teams = {}
            if len(team_ids_sorted) >= 2:
                # El equipo local (ej. Real Madrid) en este dataset resulta tener el ID mayor
                teams[str(team_ids_sorted[1])] = home_name
                teams[str(team_ids_sorted[0])] = away_name
            elif len(team_ids_sorted) == 1:
                teams[str(team_ids_sorted[0])] = home_name

            print(f"Metadata servida: {len(players_list)} jugadores, equipos: {teams}")
            return {"players": players_list, "teams": teams, "homeName": home_name, "awayName": away_name}

    except Exception as e:
        print(f"â Œ Error crÃ­tico leyendo DB: {e}")
        return {"error": "Error de conexiÃ³n con Base de Datos"}


@app.get("/match/{match_id}/player/{player_id}/profile")
def get_player_profile(match_id: str, player_id: int, state: str = "Drawing"):
    """
    Obtiene el perfil ghost de BigQuery basado en el estado del partido (Winning, Losing, Drawing).
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            query = text("""
                SELECT
                    pct_goals, pct_shots, pct_xg, pct_creation, pct_progression, pct_defense
                FROM player_ghost_profile
                WHERE tracking_player_id = :pid AND game_state = :state
                LIMIT 1
            """)
            result = conn.execute(query, {"pid": player_id, "state": state}).fetchone()
            if result:
                return {
                    "stats": [
                        {"metric": "GOALS", "value": int((result.pct_goals or 0) * 100), "fullMark": 100},
                        {"metric": "SHOTS", "value": int((result.pct_shots or 0) * 100), "fullMark": 100},
                        {"metric": "xG", "value": int((result.pct_xg or 0) * 100), "fullMark": 100},
                        {"metric": "CREA", "value": int((result.pct_creation or 0) * 100), "fullMark": 100},
                        {"metric": "PROG", "value": int((result.pct_progression or 0) * 100), "fullMark": 100},
                        {"metric": "DEF", "value": int((result.pct_defense or 0) * 100), "fullMark": 100}
                    ]
                }
            return {"stats": None}
    except Exception as e:
        print(f"❌ Error obteniendo perfil de jugador: {e}")
        return {"error": "Error interno al consultar el perfil"}


# --- Process Management Endpoints ---


@app.get("/admin/services")
def list_services():
    """Returns the status of all available microservices."""
    response = {}
    for svc_id, config in SERVICES_CONFIG.items():
        proc = process_registry.get(svc_id)
        if proc and proc.poll() is None:
            response[svc_id] = {"name": config["name"], "status": "running", "pid": proc.pid}
        else:
            response[svc_id] = {"name": config["name"], "status": "stopped", "pid": None}
    return response


@app.post("/admin/services/{service_id}/start")
def start_service(service_id: str):
    if service_id not in SERVICES_CONFIG:
        raise HTTPException(status_code=404, detail="Service not found")

    proc = process_registry.get(service_id)
    if proc and proc.poll() is None:
        return {"status": "already_running", "pid": proc.pid}

    # Start process
    cmd = SERVICES_CONFIG[service_id]["command"]
    new_proc = subprocess.Popen(cmd, cwd=_BASE_DIR)
    process_registry[service_id] = new_proc
    return {"status": "started", "pid": new_proc.pid}


@app.post("/admin/services/{service_id}/stop")
def stop_service(service_id: str):
    if service_id not in SERVICES_CONFIG:
        raise HTTPException(status_code=404, detail="Service not found")

    proc = process_registry.get(service_id)
    if not proc or proc.poll() is not None:
        return {"status": "already_stopped"}

    # Stop process
    try:
        if os.name == 'nt':
            subprocess.run(
                ['taskkill', '/F', '/T', '/PID', str(proc.pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
        else:
            proc.kill()
    except Exception as e:
        print(f"Error killing {service_id}: {e}")

    process_registry[service_id] = None
    return {"status": "stopped"}


@app.websocket("/ws/match/{match_id}")
async def websocket_endpoint(websocket: WebSocket, match_id: str):
    """
    TuberÃ­a de datos en tiempo real.
    """
    await websocket.accept()
    print(f"ðŸŸ¢ Cliente conectado al partido: {match_id}")

    # Suscribirse al Canal de Eventos
    pubsub = redis_conn.pubsub()
    pubsub.subscribe(f"match:{match_id}:events")

    try:
        while True:
            # --- A. TRACKING (Polling) ---
            tracking_raw = redis_conn.get(f"match:{match_id}:tracking")

            if tracking_raw:
                track_data = json.loads(tracking_raw)

                # ðŸ›¡ï¸ Blindaje antes de enviar
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
                print(f"âš¡ Enviando: {evt_name} ({minute}')")

                # ðŸ›¡ï¸ Blindaje antes de enviar (AquÃ­ es donde solÃ­a fallar)
                safe_payload = clean_nans(event_data)

                await websocket.send_json({
                    "type": "event",
                    "payload": safe_payload
                })

            # Pausa tÃ©cnica para no saturar CPU
            await asyncio.sleep(0.01)

    except WebSocketDisconnect:
        print("ðŸ”´ Cliente desconectado")
    except Exception as e:
        print(f"âŒ Error crÃ­tico en WebSocket: {e}")
        await websocket.close()


# ==========================================
# ðŸ“¼ TIME-SHIFTING / REPLAY API (PostgreSQL)
# ==========================================
@app.get("/match/{match_id}/tracking/history")
def get_tracking_history(
    match_id: str,
    start_frame: int = Query(..., description="Frame inicial para el buffer de replay"),
    end_frame: int = Query(..., description="Frame final para el buffer de replay")
):
    """
    Recupera segmentos de tracking histÃ³rico (video) desde PostgreSQL.
    Devuelve los frames ordenados y parseados, listos para la animaciÃ³n.
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
                # SanitizaciÃ³n obligatoria antes de enviar al frontend
                results.append(clean_nans(frame_obj))

        return results

    except Exception as e:
        print(f"âŒ Error fetching tracking history: {e}")
        return {"error": "Error recuperando histÃ³rico de tracking", "details": str(e)}


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
        print(f"âŒ Error fetching events history: {e}")
        return {"error": "Error recuperando histÃ³rico de eventos", "details": str(e)}


# ==========================================
# ðŸ“Š MATCH STATS HUB API
# ==========================================

@app.get("/match/{match_id}/stats")
def get_match_stats(match_id: str):
    """
    EstadÃ­sticas agregadas del partido para el Match Stats Hub.
    Usa SUM(CASE WHEN) para compatibilidad con pg8000.
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            match_info = conn.execute(text(
                "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
            ), {"mid": match_id}).fetchone()

            if not match_info:
                return {"error": "Match not found"}

            home_short = match_info.home_team_name or "HOME"
            away_short = match_info.away_team_name or "AWAY"

            team_rows = conn.execute(text("""
                SELECT
                    team_name,
                    SUM(CASE WHEN event_type_id = 16 THEN 1 ELSE 0 END) AS total_shots,
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97)
                        THEN 1 ELSE 0 END) AS shots_on_target,
                    SUM(CASE WHEN event_type_id = 16 AND outcome_id = 97 THEN 1 ELSE 0 END) AS goals,
                    SUM(CASE WHEN event_type_id = 30 THEN 1 ELSE 0 END) AS total_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL THEN 1 ELSE 0 END) AS accurate_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10 THEN 1 ELSE 0 END) AS progressive_passes,
                    SUM(CASE WHEN event_type_id = 10 THEN 1 ELSE 0 END) AS interceptions,
                    SUM(CASE WHEN event_type_id = 4 AND outcome_name = 'Won' THEN 1 ELSE 0 END) AS duels_won,
                    SUM(CASE WHEN event_type_id = 9 THEN 1 ELSE 0 END) AS clearances,
                    SUM(xg) AS total_xg,
                    SUM(CASE WHEN xg > 0.3 THEN 1 ELSE 0 END) AS big_chances,
                    SUM(CASE WHEN xg > 0.3 AND outcome_name != 'Goal' THEN 1 ELSE 0 END) AS big_chances_missed,
                    SUM(CASE WHEN event_type_id = 22 THEN 1 ELSE 0 END) AS fouls,
                    SUM(CASE WHEN type_id = 61 THEN 1 ELSE 0 END) AS corners
                FROM match_events
                WHERE match_id = :mid AND team_name IS NOT NULL
                GROUP BY team_name
            """), {"mid": match_id}).fetchall()

            if not team_rows:
                return {"error": "No event data found"}

            all_passes = sum(int(r.total_passes or 0) for r in team_rows) or 1

            # Pass network & avg positions — computed purely from match_events
            # (match_players uses different ID system so JOINs don't match)
            pass_network_rows = []
            avg_pos_rows = []
            try:
                pass_network_rows = conn.execute(text("""
                    SELECT team_name, player_name as from_name, player_id as from_player_id,
                           pass_recipient_name as to_name, COUNT(*) as count
                    FROM match_events
                    WHERE match_id = :mid AND event_type_id = 30 AND outcome_id IS NULL
                          AND pass_recipient_name IS NOT NULL AND player_name IS NOT NULL
                    GROUP BY team_name, player_name, player_id, pass_recipient_name
                """), {"mid": match_id}).fetchall()

                avg_pos_rows = conn.execute(text("""
                    SELECT team_name, player_name as name, player_id,
                           AVG(location_x) as avg_x, AVG(location_y) as avg_y, COUNT(*) as touches
                    FROM match_events
                    WHERE match_id = :mid AND location_x IS NOT NULL AND player_name IS NOT NULL
                    GROUP BY team_name, player_name, player_id
                """), {"mid": match_id}).fetchall()
            except Exception as net_err:
                print(f"⚠️ Pass network query error (non-fatal): {net_err}")

            teams_data = {}
            for r in team_rows:
                tp = int(r.total_passes or 0)
                ap = int(r.accurate_passes or 0)
                poss = round((tp / all_passes) * 100)
                pacc = round((ap / tp) * 100) if tp > 0 else 0

                teams_data[r.team_name] = {
                    "teamName": r.team_name,
                    "teamShort": "",
                    "goals": int(r.goals or 0),
                    "topStats": {
                        "xG": round(float(r.total_xg or 0), 2),
                        "totalShots": int(r.total_shots or 0),
                        "shotsOnTarget": int(r.shots_on_target or 0),
                        "possession": poss,
                        "bigChances": int(r.big_chances or 0),
                        "bigChancesMissed": int(r.big_chances_missed or 0),
                        "totalPasses": tp,
                        "accuratePasses": ap,
                        "passAccuracy": pacc,
                        "fouls": int(r.fouls or 0),
                        "corners": int(r.corners or 0)
                    },
                    "passing": {
                        "accuratePasses": ap,
                        "passAccuracy": pacc,
                        "progressivePasses": int(r.progressive_passes or 0),
                    },
                    "defense": {
                        "interceptions": int(r.interceptions or 0),
                        "tacklesWon": int(r.duels_won or 0),
                        "clearances": int(r.clearances or 0),
                    },
                    "passNetwork": [],
                    "averagePositions": []
                }

            # Build a name→dorsal map from match_players for jersey numbers
            dorsal_map = {}
            try:
                mp_rows = conn.execute(text(
                    "SELECT name, dorsal FROM match_players WHERE match_id = :mid AND dorsal IS NOT NULL"
                ), {"mid": match_id}).fetchall()
                for mp in mp_rows:
                    if mp.name and mp.dorsal:
                        dorsal_map[mp.name.lower()] = int(mp.dorsal)
            except Exception:
                pass

            def find_dorsal(full_name):
                """Try to match a full StatsBomb name to a match_players short name via last name."""
                if not full_name:
                    return None
                fn_lower = full_name.lower()
                # Direct match
                if fn_lower in dorsal_map:
                    return dorsal_map[fn_lower]
                # Last-name match: extract last word of full name and check if any key contains it
                parts = fn_lower.split()
                for last in reversed(parts):
                    if len(last) < 3:
                        continue
                    for key, dorsal in dorsal_map.items():
                        if last in key or key.endswith(last):
                            return dorsal
                return None

            for pr in pass_network_rows:
                if pr.team_name in teams_data:
                    teams_data[pr.team_name]["passNetwork"].append({
                        "from_player_id": pr.from_player_id,
                        "from_name": pr.from_name,
                        "to_name": pr.to_name,
                        "count": pr.count
                    })

            for ar in avg_pos_rows:
                if ar.team_name in teams_data:
                    # Normalize coords from 120x80 to 100x100
                    teams_data[ar.team_name]["averagePositions"].append({
                        "player_id": ar.player_id,
                        "name": ar.name,
                        "number": find_dorsal(ar.name),
                        "x": round(float(ar.avg_x) / 1.2, 1),
                        "y": round(float(ar.avg_y) / 0.8, 1)
                    })

            team_a_data = {}
            team_b_data = {}
            team_keys = list(teams_data.keys())

            for name, data in teams_data.items():
                nl = name.lower()
                hl = home_short.lower()
                al = away_short.lower()
                if hl in nl or nl.startswith(hl[:3]):
                    data["teamShort"] = home_short
                    team_a_data = data
                elif al in nl or nl.startswith(al[:3]):
                    data["teamShort"] = away_short
                    team_b_data = data
                else:
                    data["teamShort"] = name[:3].upper()

            if not team_a_data and len(team_keys) > 0:
                team_a_data = teams_data[team_keys[0]]
                team_a_data["teamShort"] = home_short
            if not team_b_data and len(team_keys) > 1:
                team_b_data = teams_data[team_keys[1]]
                team_b_data["teamShort"] = away_short

            player_rows = conn.execute(text("""
                SELECT
                    e.player_id, e.player_name, e.team_name,
                    SUM(CASE WHEN event_type_id = 16 AND outcome_id = 97 THEN 1 ELSE 0 END) AS goals,
                    SUM(CASE WHEN event_type_id = 16 THEN 1 ELSE 0 END) AS shots,
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97)
                        THEN 1 ELSE 0 END) AS shots_on_target,
                    SUM(CASE WHEN event_type_id = 30 THEN 1 ELSE 0 END) AS total_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL THEN 1 ELSE 0 END) AS passes_completed,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10 THEN 1 ELSE 0 END) AS progressive_passes,
                    SUM(CASE WHEN event_type_id = 4 THEN 1 ELSE 0 END) AS duels,
                    SUM(CASE WHEN event_type_id = 4 AND outcome_name = 'Won' THEN 1 ELSE 0 END) AS duels_won,
                    SUM(CASE WHEN event_type_id = 10 THEN 1 ELSE 0 END) AS interceptions,
                    SUM(CASE WHEN event_type_id IN (30, 42, 43, 16, 14, 38, 6) THEN 1 ELSE 0 END) AS touches,
                    MAX(minute) AS last_minute
                FROM match_events e
                WHERE e.match_id = :mid AND e.player_id IS NOT NULL
                GROUP BY e.player_id, e.player_name, e.team_name
            """), {"mid": match_id}).fetchall()

            roster_rows = conn.execute(text(
                "SELECT player_id, team_id, name, dorsal, position FROM match_players WHERE match_id = :mid"
            ), {"mid": match_id}).fetchall()

            # Build roster map keyed by tracking_player_id
            roster_by_tracking = {}
            for r in roster_rows:
                roster_by_tracking[str(r.player_id)] = {
                    "team_id": r.team_id, "name": r.name,
                    "number": r.dorsal, "position": r.position,
                }

            players = {}
            for r in player_rows:
                pid = str(r.player_id)  # This is the opta_player_id
                # Translate opta -> tracking to find roster info
                tracking_id = _OPTA_TO_TRACKING.get(pid, pid)
                ro = roster_by_tracking.get(tracking_id, {})
                tp = int(r.total_passes or 0)
                pc = int(r.passes_completed or 0)
                pacc = round((pc / tp) * 100) if tp > 0 else 0
                dn = ro.get("name") or r.player_name or "Unknown"

                players[pid] = {
                    "info": {
                        "id": pid, "name": r.player_name or dn, "shortName": dn,
                        "number": ro.get("number", 0), "position": ro.get("position", "?"),
                        "teamId": str(ro.get("team_id", "")), "teamName": r.team_name or "",
                    },
                    "stats": {
                        "minutesPlayed": int(r.last_minute or 0), "goals": int(r.goals or 0),
                        "assists": 0, "shots": int(r.shots or 0),
                        "shotsOnTarget": int(r.shots_on_target or 0), "xG": 0,
                        "passesCompleted": pc, "passAccuracy": pacc, "keyPasses": 0,
                        "progressivePasses": int(r.progressive_passes or 0),
                        "tackles": int(r.duels or 0), "interceptions": int(r.interceptions or 0),
                        "duelsWon": int(r.duels_won or 0), "aerialDuelsWon": 0,
                        "touches": int(r.touches or 0), "distanceCovered": 0,
                    },
                }

            print(f"ðŸ“Š Stats Hub: {len(teams_data)} equipos, {len(players)} jugadores")
            return clean_nans({"teamA": team_a_data, "teamB": team_b_data, "players": players})

    except Exception as e:
        print(f"âŒ Error fetching match stats: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@app.get("/match/{match_id}/player/{player_id}/pitch")
def get_player_pitch_data(match_id: str, player_id: str):
    """
    Datos de visualizaciÃ³n en campo: heatmap, red de pases, touch map.
    Coordenadas normalizadas 0-100 (desde StatsBomb 0-120 x 0-80).
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            heat_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL AND location_y IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            heatmap = [{
                "x": round(float(r.location_x) / 1.2, 1),
                "y": round(float(r.location_y) / 0.8, 1),
                "intensity": 0.7 if r.event_type_id in (16, 14, 30) else 0.4,
            } for r in heat_rows]

            pass_rows = conn.execute(text("""
                SELECT location_x, location_y, end_location_x, end_location_y, outcome_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND event_type_id = 30
                  AND location_x IS NOT NULL AND end_location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            pass_network = [{
                "from_x": round(float(r.location_x) / 1.2, 1),
                "from_y": round(float(r.location_y) / 0.8, 1),
                "to_x": round(float(r.end_location_x) / 1.2, 1),
                "to_y": round(float(r.end_location_y) / 0.8, 1),
                "count": 1,
                "successful": r.outcome_id is None,
            } for r in pass_rows]

            touch_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            type_map = {
                30: "pass", 16: "shot", 14: "dribble", 43: "cross",
                4: "tackle", 10: "tackle", 42: "reception", 2: "reception",
                6: "tackle", 9: "tackle",
            }

            touch_map = [{
                "x": round(float(r.location_x) / 1.2, 1),
                "y": round(float(r.location_y) / 0.8, 1),
                "eventType": type_map.get(r.event_type_id, "reception"),
            } for r in touch_rows]

            print(f"🗺️ Pitch: player {player_id} -> {len(heatmap)}h {len(pass_network)}p {len(touch_map)}t")
            return clean_nans({"heatmap": heatmap, "passNetwork": pass_network, "touchMap": touch_map})

    except Exception as e:
        print(f"❌ Error fetching player pitch data: {e}")
        return {"error": str(e)}
