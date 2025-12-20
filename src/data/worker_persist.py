# src/data/worker_persist.py
import json
import time
import signal
import sys
import math
from datetime import datetime
from redis_client import get_redis_connection
from postgres_client import get_db_engine
from sqlalchemy import text

# Configuración
MATCH_ID = "test_match"
BUFFER_SIZE = 50 

class PersistenceWorker:
    def __init__(self):
        self.running = True
        self.redis = get_redis_connection()
        self.db_engine = get_db_engine()
        self.pubsub = self.redis.pubsub()
        
        self.tracking_buffer = []
        self.metadata_saved = False

        self.last_saved_frame_idx = -1
        
        self._check_db_connection()
        signal.signal(signal.SIGINT, self.stop)

    def _log(self, msg, type="INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[{ts}] [{type}] {msg}")

    def _check_db_connection(self):
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._log("Base de Datos Local CONECTADA 🟢")
        except Exception as e:
            self._log(f"NO HAY CONEXIÓN A DB: {e}", "CRITICAL")
            sys.exit(1)

    def stop(self, signum, frame):
        self._log("Deteniendo Worker...", "WARN")
        self.running = False

    # 🧼 EL LIMPIADOR DE TOXINAS (Vital para evitar error 'Token NaN')
    def clean_nans(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        elif isinstance(obj, str):
            if obj == "NaT": return None
        elif isinstance(obj, dict):
            return {k: self.clean_nans(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.clean_nans(v) for v in obj]
        return obj

    def save_metadata(self):
        if self.metadata_saved: return
        
        raw = self.redis.get(f"match:{MATCH_ID}:metadata")
        if not raw:
            if int(time.time()) % 5 == 0:
                self._log("⏳ Esperando Metadata (Dale a 'Enviar Alineación')...", "WAIT")
            return

        try:
            data = self.clean_nans(json.loads(raw))
            
            with self.db_engine.begin() as conn:
                # 1. Matches
                conn.execute(text("""
                    INSERT INTO matches (match_id, home_team_name, away_team_name, match_date, stadium)
                    VALUES (:mid, :home, :away, :date, :stad)
                    ON CONFLICT (match_id) DO NOTHING
                """), {
                    "mid": MATCH_ID, 
                    "home": data['home_team']['short_name'], 
                    "away": data['away_team']['short_name'],
                    "date": data.get('match_date') or datetime.now(),
                    "stad": data.get('stadium')
                })
                
                # 2. Players
                players = []
                for p in data['players']:
                    players.append({
                        "mid": MATCH_ID, "pid": str(p['id']),
                        "tid": str(p['team_id']),
                        "name": p['short_name'],
                        "num": p['number'],
                        "pos": p['role']
                    })
                
                if players:
                    conn.execute(text("""
                        INSERT INTO match_players (match_id, player_id, team_id, name, dorsal, position)
                        VALUES (:mid, :pid, :tid, :name, :num, :pos)
                        ON CONFLICT (match_id, player_id) DO NOTHING
                    """), players)
            
            self._log("✅ METADATA GUARDADA", "SUCCESS")
            self.metadata_saved = True

        except Exception as e:
            self._log(f"Error Metadata: {e}", "ERROR")

    def save_event(self, msg):
        try:
            ev = self.clean_nans(json.loads(msg['data']))
            
            with self.db_engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO match_events (
                        event_uuid, match_id, timestamp, period, minute, second,
                        event_type_id, event_type_name, type_id, type_name, 
                        outcome_id, outcome_name,
                        team_name, player_id, player_name,
                        location_x, location_y, 
                        end_location_x, end_location_y, end_location_z,
                        pass_length, pass_angle, pass_recipient_name, 
                        pass_height_name, body_part_name
                    )
                    VALUES (
                        :uuid, :mid, :ts, :per, :min, :sec,
                        :et_id, :et_name, :t_id, :t_name, 
                        :o_id, :o_name,
                        :team, :pid, :pname,
                        :x, :y, 
                        :end_x, :end_y, :end_z,
                        :p_len, :p_ang, :p_rec, 
                        :p_height, :body
                    )
                    ON CONFLICT (event_uuid) DO NOTHING
                """), {
                    "uuid": ev.get('id') or f"{MATCH_ID}-{time.time()}",
                    "mid": MATCH_ID,
                    "ts": ev.get('timestamp'),
                    "per": ev.get('period'),
                    "min": ev.get('minute'),
                    "sec": ev.get('second'),
                    
                    # IDs Clave
                    "et_id": ev.get('event_type_id'),
                    "et_name": ev.get('event_type_name'),
                    "t_id": ev.get('type_id'),      # Subtipo (ej: Penalty)
                    "t_name": ev.get('type_name'),  # Nombre Subtipo
                    "o_id": ev.get('outcome_id'),   # Resultado (ej: Gol)
                    "o_name": ev.get('outcome_name'),

                    # Contexto
                    "team": ev.get('team_name'),
                    "pid": str(ev.get('player_id')) if ev.get('player_id') else None,
                    "pname": ev.get('player_name'),

                    # Coordenadas
                    "x": ev.get('location_x'),
                    "y": ev.get('location_y'),
                    "end_x": ev.get('end_location_x'),
                    "end_y": ev.get('end_location_y'),
                    "end_z": ev.get('end_location_z'),

                    # Detalles Pase
                    "p_len": ev.get('pass_length'),
                    "p_ang": ev.get('pass_angle'),
                    "p_rec": ev.get('pass_recipient_name'),
                    "p_height": ev.get('pass_height_name'),
                    "body": ev.get('body_part_name')
                })
            
            # Log más detallado para debug
            tipo = ev.get('event_type_name', 'Evento')
            subtipo = f"({ev.get('type_name')})" if ev.get('type_name') else ""
            self._log(f"⚽ Guardado: {tipo} {subtipo} - Min {ev.get('minute')}")
            
        except Exception as e:
            self._log(f"Error saving event: {e}", "ERROR")

    def run(self):
        self._log("👷 WORKER ACTIVO - Modo Inspección Superado", "INFO")
        self.pubsub.subscribe(f"match:{MATCH_ID}:events")
        
        while self.running:
            # 1. Metadata es prioridad
            if not self.metadata_saved:
                self.save_metadata()
                time.sleep(1)
                continue

            # 2. Eventos
            while True:
                msg = self.pubsub.get_message(ignore_subscribe_messages=True)
                if msg: self.save_event(msg)
                else: break
            
            # 3. Tracking (EL FLUJO PESADO)
            raw_track = self.redis.get(f"match:{MATCH_ID}:tracking")
            if raw_track:
                try:
                    dirty_obj = json.loads(raw_track)
                    clean_obj = self.clean_nans(dirty_obj)
                    
                    # Extraer frame. Si no existe, IGNORAR (no inventar timestamp)
                    frame_idx = clean_obj.get("frame") or clean_obj.get("frame_idx")
                    
                    if frame_idx is not None:
                        frame_idx = int(frame_idx)
                        
                        # ✨ LA CLAVE: Si es el mismo frame que ya guardé, PASAR
                        if frame_idx > self.last_saved_frame_idx:
                            
                            self.tracking_buffer.append({
                                "mid": MATCH_ID, 
                                "idx": frame_idx, 
                                "ts": datetime.now(), 
                                "data": json.dumps(clean_obj) 
                            })
                            
                            # Actualizamos el puntero de memoria
                            self.last_saved_frame_idx = frame_idx
                            
                except Exception:
                    pass

            # Flush a Base de Datos
            if len(self.tracking_buffer) >= BUFFER_SIZE:
                try:
                    with self.db_engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO match_tracking (match_id, frame_idx, timestamp, players_data) 
                            VALUES (:mid, :idx, :ts, :data)
                        """), self.tracking_buffer)
                    
                    # Mensaje de éxito
                    last_frame = self.tracking_buffer[-1]['idx']
                    self._log(f"📡 Tracking Guardado (Bloque de {len(self.tracking_buffer)}). Último Frame: {last_frame}", "DEBUG")
                    self.tracking_buffer = []
                    
                except Exception as e:
                    self._log(f"Error SQL Tracking: {e}", "ERROR")
                    self.tracking_buffer = []
            
            time.sleep(0.02) # ~50hz

if __name__ == "__main__":
    PersistenceWorker().run()