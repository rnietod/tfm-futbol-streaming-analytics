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
    Endpoint para obtener la alineación inicial.
    """
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            # 1. Consulta SQL Directa
            # Buscamos los jugadores asociados a este partido
            query = text("""
                SELECT player_id, team_id, name, dorsal, position
                FROM match_players
                WHERE match_id = :mid
            """)

            result = conn.execute(query, {"mid": match_id}).fetchall()

            # 2. Validación de Negocio
            if not result:
                # Si Postgres no tiene datos, es que el proceso de carga falló antes
                return {"error": "Alineación no encontrada en Base de Datos. Verifique la carga inicial."}

            # 3. Serialización (Mapping DB -> Frontend)
            # Transformamos las columnas de la DB al JSON que espera React
            players_list = []
            for row in result:
                players_list.append({
                    "player_id": row.player_id,     # React key
                    "team_id": row.team_id,         # Para colores
                    "short_name": row.name,         # Display
                    "number": row.dorsal,           # Dorsal
                    "role": row.position            # Posición (GK, etc)
                })

            print(f"✅ Metadata servida desde SQL: {len(players_list)} jugadores.")
            return {"players": players_list}

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


# ==========================================
# 📊 MATCH STATS HUB API
# ==========================================

@app.get("/match/{match_id}/stats")
def get_match_stats(match_id: str):
    """
    Estadísticas agregadas del partido para el Match Stats Hub.
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
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97) THEN 1 ELSE 0 END) AS shots_on_target,
                    SUM(CASE WHEN event_type_id = 16 AND outcome_id = 97 THEN 1 ELSE 0 END) AS goals,
                    SUM(CASE WHEN event_type_id = 30 THEN 1 ELSE 0 END) AS total_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL THEN 1 ELSE 0 END) AS accurate_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10 THEN 1 ELSE 0 END) AS progressive_passes,
                    SUM(CASE WHEN event_type_id = 10 THEN 1 ELSE 0 END) AS interceptions,
                    SUM(CASE WHEN event_type_id = 4 AND outcome_name = 'Won' THEN 1 ELSE 0 END) AS duels_won,
                    SUM(CASE WHEN event_type_id = 9 THEN 1 ELSE 0 END) AS clearances
                FROM match_events
                WHERE match_id = :mid AND team_name IS NOT NULL
                GROUP BY team_name
            """), {"mid": match_id}).fetchall()

            if not team_rows:
                return {"error": "No event data found"}

            all_passes = sum(int(r.total_passes or 0) for r in team_rows) or 1

            teams_data = {}
            for r in team_rows:
                tp = int(r.total_passes or 0)
                ap = int(r.accurate_passes or 0)
                poss = round((tp / all_passes) * 100)
                pacc = round((ap / tp) * 100) if tp > 0 else 0

                teams_data[r.team_name] = {
                    "teamName": r.team_name,
                    "teamShort": "",
                    "topStats": {
                        "xG": 0,
                        "totalShots": int(r.total_shots or 0),
                        "shotsOnTarget": int(r.shots_on_target or 0),
                        "possession": poss,
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
                }

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
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97) THEN 1 ELSE 0 END) AS shots_on_target,
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

            roster_map = {}
            for r in roster_rows:
                roster_map[str(r.player_id)] = {
                    "team_id": r.team_id, "name": r.name,
                    "number": r.dorsal, "position": r.position,
                }

            players = {}
            for r in player_rows:
                pid = str(r.player_id)
                ro = roster_map.get(pid, {})
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

            print(f"📊 Stats Hub: {len(teams_data)} equipos, {len(players)} jugadores")
            return clean_nans({"teamA": team_a_data, "teamB": team_b_data, "players": players})

    except Exception as e:
        print(f"❌ Error fetching match stats: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@app.get("/match/{match_id}/player/{player_id}/pitch")
def get_player_pitch_data(match_id: str, player_id: str):
    """
    Datos de visualización en campo: heatmap, red de pases, touch map.
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



# ==========================================
# 📊 MATCH STATS HUB API
# ==========================================

@app.get("/match/{match_id}/stats")
def get_match_stats(match_id: str):
    """
    Estadísticas agregadas del partido para el Match Stats Hub.
    Devuelve: comparación de equipos + stats por jugador.
    Usa SUM(CASE WHEN) en lugar de FILTER para compatibilidad con pg8000.
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # 1. Info del partido
            match_info = conn.execute(text(
                "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
            ), {"mid": match_id}).fetchone()

            if not match_info:
                return {"error": "Match not found"}

            home_short = match_info.home_team_name or "HOME"
            away_short = match_info.away_team_name or "AWAY"

            # 2. Stats agregadas por equipo (SUM/CASE para pg8000)
            team_rows = conn.execute(text("""
                SELECT
                    team_name,
                    SUM(CASE WHEN event_type_id = 16 THEN 1 ELSE 0 END) AS total_shots,
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97) THEN 1 ELSE 0 END) AS shots_on_target,
                    SUM(CASE WHEN event_type_id = 16 AND outcome_id = 97 THEN 1 ELSE 0 END) AS goals,
                    SUM(CASE WHEN event_type_id = 30 THEN 1 ELSE 0 END) AS total_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL THEN 1 ELSE 0 END) AS accurate_passes,
                    SUM(CASE WHEN event_type_id = 30 AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10 THEN 1 ELSE 0 END) AS progressive_passes,
                    SUM(CASE WHEN event_type_id = 10 THEN 1 ELSE 0 END) AS interceptions,
                    SUM(CASE WHEN event_type_id = 4 THEN 1 ELSE 0 END) AS duels_total,
                    SUM(CASE WHEN event_type_id = 4 AND outcome_name = 'Won' THEN 1 ELSE 0 END) AS duels_won,
                    SUM(CASE WHEN event_type_id = 9 THEN 1 ELSE 0 END) AS clearances,
                    SUM(CASE WHEN event_type_id = 6 THEN 1 ELSE 0 END) AS blocks
                FROM match_events
                WHERE match_id = :mid AND team_name IS NOT NULL
                GROUP BY team_name
            """), {"mid": match_id}).fetchall()

            if not team_rows:
                return {"error": "No event data found"}

            # Posesión ~ proporción de pases
            all_passes = sum(int(r.total_passes or 0) for r in team_rows) or 1

            teams_data = {}
            for r in team_rows:
                tp = int(r.total_passes or 0)
                ap = int(r.accurate_passes or 0)
                poss = round((tp / all_passes) * 100)
                pacc = round((ap / tp) * 100) if tp > 0 else 0

                teams_data[r.team_name] = {
                    "teamName": r.team_name,
                    "teamShort": "",
                    "topStats": {
                        "xG": 0,
                        "totalShots": int(r.total_shots or 0),
                        "shotsOnTarget": int(r.shots_on_target or 0),
                        "possession": poss,
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
                }

            # Asignar short names home/away
            team_a_data = {}
            team_b_data = {}
            team_keys = list(teams_data.keys())

            for name, data in teams_data.items():
                name_lower = name.lower()
                home_lower = home_short.lower()
                away_lower = away_short.lower()

                if home_lower in name_lower or name_lower.startswith(home_lower[:3]):
                    data["teamShort"] = home_short
                    team_a_data = data
                elif away_lower in name_lower or name_lower.startswith(away_lower[:3]):
                    data["teamShort"] = away_short
                    team_b_data = data
                else:
                    data["teamShort"] = name[:3].upper()

            # Fallback si el matching no funcionó
            if not team_a_data and len(team_keys) > 0:
                team_a_data = teams_data[team_keys[0]]
                team_a_data["teamShort"] = home_short
            if not team_b_data and len(team_keys) > 1:
                team_b_data = teams_data[team_keys[1]]
                team_b_data["teamShort"] = away_short

            # 3. Stats por jugador
            player_rows = conn.execute(text("""
                SELECT
                    e.player_id,
                    e.player_name,
                    e.team_name,
                    SUM(CASE WHEN event_type_id = 16 AND outcome_id = 97 THEN 1 ELSE 0 END) AS goals,
                    SUM(CASE WHEN event_type_id = 16 THEN 1 ELSE 0 END) AS shots,
                    SUM(CASE WHEN event_type_id = 16 AND (type_id = 88 OR outcome_id = 97) THEN 1 ELSE 0 END) AS shots_on_target,
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

            # 4. Info de roster (dorsal, posición)
            roster_rows = conn.execute(text("""
                SELECT player_id, team_id, name, dorsal, position
                FROM match_players
                WHERE match_id = :mid
            """), {"mid": match_id}).fetchall()

            roster_map = {}
            for r in roster_rows:
                roster_map[str(r.player_id)] = {
                    "team_id": r.team_id,
                    "name": r.name,
                    "number": r.dorsal,
                    "position": r.position,
                }

            # 5. Construir lista de jugadores
            players = {}
            for r in player_rows:
                pid = str(r.player_id)
                roster = roster_map.get(pid, {})
                tp = int(r.total_passes or 0)
                pc = int(r.passes_completed or 0)
                pacc = round((pc / tp) * 100) if tp > 0 else 0
                display_name = roster.get("name") or r.player_name or "Unknown"

                players[pid] = {
                    "info": {
                        "id": pid,
                        "name": r.player_name or display_name,
                        "shortName": display_name,
                        "number": roster.get("number", 0),
                        "position": roster.get("position", "?"),
                        "teamId": str(roster.get("team_id", "")),
                        "teamName": r.team_name or "",
                    },
                    "stats": {
                        "minutesPlayed": int(r.last_minute or 0),
                        "goals": int(r.goals or 0),
                        "assists": 0,
                        "shots": int(r.shots or 0),
                        "shotsOnTarget": int(r.shots_on_target or 0),
                        "xG": 0,
                        "passesCompleted": pc,
                        "passAccuracy": pacc,
                        "keyPasses": 0,
                        "progressivePasses": int(r.progressive_passes or 0),
                        "tackles": int(r.duels or 0),
                        "interceptions": int(r.interceptions or 0),
                        "duelsWon": int(r.duels_won or 0),
                        "aerialDuelsWon": 0,
                        "touches": int(r.touches or 0),
                        "distanceCovered": 0,
                    },
                }

            print(f"📊 Stats Hub: {len(teams_data)} equipos, {len(players)} jugadores")

            return clean_nans({
                "teamA": team_a_data,
                "teamB": team_b_data,
                "players": players,
            })

    except Exception as e:
        print(f"❌ Error fetching match stats: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@app.get("/match/{match_id}/player/{player_id}/pitch")
def get_player_pitch_data(match_id: str, player_id: str):
    """
    Datos de visualización en campo para un jugador: heatmap, red de pases, touch map.
    Coordenadas normalizadas a 0-100 (desde StatsBomb 0-120 x 0-80).
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Heatmap: todas las acciones con ubicación
            heat_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL AND location_y IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            heatmap = []
            for r in heat_rows:
                lx = float(r.location_x) if r.location_x else 0
                ly = float(r.location_y) if r.location_y else 0
                heatmap.append({
                    "x": round(lx / 1.2, 1),
                    "y": round(ly / 0.8, 1),
                    "intensity": 0.7 if r.event_type_id in (16, 14, 30) else 0.4,
                })

            # Pass Network: pases con origen y destino
            pass_rows = conn.execute(text("""
                SELECT location_x, location_y,
                       end_location_x, end_location_y,
                       outcome_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND event_type_id = 30
                  AND location_x IS NOT NULL
                  AND end_location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            pass_network = []
            for r in pass_rows:
                pass_network.append({
                    "from_x": round(float(r.location_x) / 1.2, 1),
                    "from_y": round(float(r.location_y) / 0.8, 1),
                    "to_x":   round(float(r.end_location_x) / 1.2, 1),
                    "to_y":   round(float(r.end_location_y) / 0.8, 1),
                    "count":  1,
                    "successful": r.outcome_id is None,
                })

            # Touch Map: todos los eventos categorizados
            touch_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            type_to_touch = {
                30: "pass", 16: "shot", 14: "dribble", 43: "cross",
                4: "tackle", 10: "tackle", 42: "reception", 2: "reception",
                6: "tackle", 9: "tackle",
            }

            touch_map = []
            for r in touch_rows:
                touch_map.append({
                    "x": round(float(r.location_x) / 1.2, 1),
                    "y": round(float(r.location_y) / 0.8, 1),
                    "eventType": type_to_touch.get(r.event_type_id, "reception"),
                })

            print(f"🗺️ Pitch: player {player_id} -> {len(heatmap)} heat, {len(pass_network)} passes, {len(touch_map)} touches")

            return clean_nans({
                "heatmap": heatmap,
                "passNetwork": pass_network,
                "touchMap": touch_map,
    except Exception as e:
        print(f"❌ Error fetching player pitch data: {e}")
        return {"error": str(e)}

    """
    Estadísticas agregadas del partido para el Match Stats Hub.
    Devuelve: comparación de equipos + stats por jugador.
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # 1. Info del partido
            match_info = conn.execute(text(
                "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
            ), {"mid": match_id}).fetchone()

            if not match_info:
                return {"error": "Match not found"}

            home_short = match_info.home_team_name or "HOME"
            away_short = match_info.away_team_name or "AWAY"

            # 2. Stats agregadas por equipo
            team_rows = conn.execute(text("""
                SELECT
                    team_name,
                    COUNT(*) FILTER (WHERE event_type_id = 16) AS total_shots,
                    COUNT(*) FILTER (WHERE event_type_id = 16
                        AND (type_id = 88 OR outcome_id = 97)) AS shots_on_target,
                    COUNT(*) FILTER (WHERE event_type_id = 16
                        AND outcome_id = 97) AS goals,
                    COUNT(*) FILTER (WHERE event_type_id = 30) AS total_passes,
                    COUNT(*) FILTER (WHERE event_type_id = 30
                        AND outcome_id IS NULL) AS accurate_passes,
                    COUNT(*) FILTER (WHERE event_type_id = 30
                        AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL
                        AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10) AS progressive_passes,
                    COUNT(*) FILTER (WHERE event_type_id = 10) AS interceptions,
                    COUNT(*) FILTER (WHERE event_type_id = 4) AS duels_total,
                    COUNT(*) FILTER (WHERE event_type_id = 4
                        AND outcome_name = 'Won') AS duels_won,
                    COUNT(*) FILTER (WHERE event_type_id = 9) AS clearances,
                    COUNT(*) FILTER (WHERE event_type_id = 6) AS blocks,
                    COUNT(*) FILTER (WHERE event_type_id = 2) AS ball_recoveries
                FROM match_events
                WHERE match_id = :mid AND team_name IS NOT NULL
                GROUP BY team_name
            """), {"mid": match_id}).fetchall()

            # Posesión ~ proporción de pases
            all_passes = sum(r.total_passes for r in team_rows) or 1

            # Determinar home/away mapeo
            # team_rows[0].team_name, team_rows[1].team_name son los full names
            full_names = [r.team_name for r in team_rows]

            teams_data = {}
            for idx, r in enumerate(team_rows):
                poss = round((r.total_passes / all_passes) * 100)
                pacc = round((r.accurate_passes / r.total_passes) * 100) if r.total_passes > 0 else 0

                teams_data[r.team_name] = {
                    "teamName": r.team_name,
                    "teamShort": "",  # se rellena abajo
                    "topStats": {
                        "xG": 0,
                        "totalShots": r.total_shots or 0,
                        "shotsOnTarget": r.shots_on_target or 0,
                        "possession": poss,
                    },
                    "passing": {
                        "accuratePasses": r.accurate_passes or 0,
                        "passAccuracy": pacc,
                        "progressivePasses": r.progressive_passes or 0,
                    },
                    "defense": {
                        "interceptions": r.interceptions or 0,
                        "tacklesWon": r.duels_won or 0,
                        "clearances": r.clearances or 0,
                    },
                }

            # Asignar short names home/away
            team_a_data = {}
            team_b_data = {}
            for name, data in teams_data.items():
                # Match por short name (case-insensitive, parcial)
                name_lower = name.lower()
                home_lower = home_short.lower()
                away_lower = away_short.lower()

                if home_lower in name_lower or name_lower.startswith(home_lower[:3]):
                    data["teamShort"] = home_short
                    team_a_data = data
                elif away_lower in name_lower or name_lower.startswith(away_lower[:3]):
                    data["teamShort"] = away_short
                    team_b_data = data
                else:
                    # Fallback: primer equipo = A, segundo = B
                    data["teamShort"] = name[:3].upper()

            # Si el matching falló, asignar por orden
            if not team_a_data and len(teams_data) > 0:
                keys = list(teams_data.keys())
                team_a_data = teams_data[keys[0]]
                team_a_data["teamShort"] = home_short
            if not team_b_data and len(teams_data) > 1:
                keys = list(teams_data.keys())
                team_b_data = teams_data[keys[1]]
                team_b_data["teamShort"] = away_short

            # 3. Stats por jugador
            player_rows = conn.execute(text("""
                SELECT
                    e.player_id,
                    e.player_name,
                    e.team_name,
                    COUNT(*) FILTER (WHERE event_type_id = 16
                        AND outcome_id = 97) AS goals,
                    COUNT(*) FILTER (WHERE event_type_id = 16) AS shots,
                    COUNT(*) FILTER (WHERE event_type_id = 16
                        AND (type_id = 88 OR outcome_id = 97)) AS shots_on_target,
                    COUNT(*) FILTER (WHERE event_type_id = 30) AS total_passes,
                    COUNT(*) FILTER (WHERE event_type_id = 30
                        AND outcome_id IS NULL) AS passes_completed,
                    COUNT(*) FILTER (WHERE event_type_id = 30
                        AND outcome_id IS NULL
                        AND end_location_x IS NOT NULL
                        AND location_x IS NOT NULL
                        AND (end_location_x - location_x) > 10) AS progressive_passes,
                    COUNT(*) FILTER (WHERE event_type_id = 4) AS duels,
                    COUNT(*) FILTER (WHERE event_type_id = 4
                        AND outcome_name = 'Won') AS duels_won,
                    COUNT(*) FILTER (WHERE event_type_id = 10) AS interceptions,
                    COUNT(*) FILTER (WHERE event_type_id = 9) AS clearances,
                    COUNT(*) FILTER (WHERE event_type_id IN (30, 42, 43, 16, 14, 38, 6)) AS touches,
                    MAX(minute) AS last_minute
                FROM match_events e
                WHERE e.match_id = :mid AND e.player_id IS NOT NULL
                GROUP BY e.player_id, e.player_name, e.team_name
            """), {"mid": match_id}).fetchall()

            # 4. Info de roster (dorsal, posición)
            roster_rows = conn.execute(text("""
                SELECT player_id, team_id, name, dorsal, position
                FROM match_players
                WHERE match_id = :mid
            """), {"mid": match_id}).fetchall()

            roster_map = {}
            for r in roster_rows:
                roster_map[str(r.player_id)] = {
                    "team_id": r.team_id,
                    "name": r.name,
                    "number": r.dorsal,
                    "position": r.position,
                }

            # 5. Construir lista de jugadores
            players = {}
            for r in player_rows:
                pid = str(r.player_id)
                roster = roster_map.get(pid, {})
                pacc = round((r.passes_completed / r.total_passes) * 100) if r.total_passes > 0 else 0
                display_name = roster.get("name") or r.player_name or "Unknown"

                players[pid] = {
                    "info": {
                        "id": pid,
                        "name": r.player_name or display_name,
                        "shortName": display_name,
                        "number": roster.get("number", 0),
                        "position": roster.get("position", "?"),
                        "teamId": str(roster.get("team_id", "")),
                        "teamName": r.team_name or "",
                    },
                    "stats": {
                        "minutesPlayed": r.last_minute or 0,
                        "goals": r.goals or 0,
                        "assists": 0,
                        "shots": r.shots or 0,
                        "shotsOnTarget": r.shots_on_target or 0,
                        "xG": 0,
                        "passesCompleted": r.passes_completed or 0,
                        "passAccuracy": pacc,
                        "keyPasses": 0,
                        "progressivePasses": r.progressive_passes or 0,
                        "tackles": r.duels or 0,
                        "interceptions": r.interceptions or 0,
                        "duelsWon": r.duels_won or 0,
                        "aerialDuelsWon": 0,
                        "touches": r.touches or 0,
                        "distanceCovered": 0,
                    },
                }

            print(f"📊 Stats Hub: {len(teams_data)} equipos, {len(players)} jugadores")

            return clean_nans({
                "teamA": team_a_data,
                "teamB": team_b_data,
                "players": players,
            })

    except Exception as e:
        print(f"❌ Error fetching match stats: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e)}


@app.get("/match/{match_id}/player/{player_id}/pitch")
def get_player_pitch_data(match_id: str, player_id: str):
    """
    Datos de visualización en campo para un jugador: heatmap, red de pases, touch map.
    Coordenadas normalizadas a 0-100 (desde StatsBomb 0-120 x 0-80).
    """
    engine = get_db_engine()

    try:
        with engine.connect() as conn:
            # Heatmap: todas las acciones con ubicación
            heat_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL AND location_y IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            heatmap = []
            for r in heat_rows:
                heatmap.append({
                    "x": round(r.location_x / 1.2, 1),   # 0-120 -> 0-100
                    "y": round(r.location_y / 0.8, 1),   # 0-80  -> 0-100
                    "intensity": 0.7 if r.event_type_id in (16, 14, 30) else 0.4,
                })

            # Pass Network: pases con origen y destino
            pass_rows = conn.execute(text("""
                SELECT location_x, location_y,
                       end_location_x, end_location_y,
                       outcome_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND event_type_id = 30
                  AND location_x IS NOT NULL
                  AND end_location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            pass_network = []
            for r in pass_rows:
                pass_network.append({
                    "from_x": round(r.location_x / 1.2, 1),
                    "from_y": round(r.location_y / 0.8, 1),
                    "to_x":   round(r.end_location_x / 1.2, 1),
                    "to_y":   round(r.end_location_y / 0.8, 1),
                    "count":  1,
                    "successful": r.outcome_id is None,
                })

            # Touch Map: todos los eventos categorizados
            touch_rows = conn.execute(text("""
                SELECT location_x, location_y, event_type_id
                FROM match_events
                WHERE match_id = :mid AND player_id = :pid
                  AND location_x IS NOT NULL
            """), {"mid": match_id, "pid": player_id}).fetchall()

            type_to_touch = {
                30: "pass",
                16: "shot",
                14: "dribble",
                43: "cross",
                4:  "tackle",
                10: "tackle",
                42: "reception",
                2:  "reception",
                6:  "tackle",
                9:  "tackle",
            }

            touch_map = []
            for r in touch_rows:
                touch_map.append({
                    "x": round(r.location_x / 1.2, 1),
                    "y": round(r.location_y / 0.8, 1),
                    "eventType": type_to_touch.get(r.event_type_id, "reception"),
                })

            print(f"🗺️ Pitch data for player {player_id}: {len(heatmap)} heat, {len(pass_network)} passes, {len(touch_map)} touches")

            return clean_nans({
                "heatmap": heatmap,
                "passNetwork": pass_network,
                "touchMap": touch_map,
            })

    except Exception as e:
        print(f"❌ Error fetching player pitch data: {e}")
        return {"error": str(e)}
