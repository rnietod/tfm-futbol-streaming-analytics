# src/streaming/engine.py
import pandas as pd
import json
import time
import threading
import os
from datetime import datetime
from src.data.redis_client import get_redis_connection

# Configuración de Canales Redis
MATCH_ID = "test_match"
KEY_TRACKING = f"match:{MATCH_ID}:tracking"
KEY_EVENTS = f"match:{MATCH_ID}:events"
KEY_METADATA = f"match:{MATCH_ID}:metadata"


class SimulationEngine:
    def __init__(self, env="dev"):
        self.env = env
        self.speed_multiplier = 1.0
        self.running = False
        self.current_time = -1.0
        self.current_period = 0

        # Listas Maestras
        self.tracking_stream = []
        self.eventing_stream = []
        self.raw_ids_data = None

        self.ids_map = {}
        self._thread = None
        self.total_game_time = 1

        # Logs
        self.simple_logs = []
        self.sent_tracking_log = []
        self.sent_eventing_log = []
        self.total_tracking = 0
        self.total_events = 0
        self.metrics = {'tracking': {'count': 0, 'total_latency': 0.0}, 'eventing': {'count': 0, 'total_latency': 0.0}}

        self.errors = 0
        self.latency_ms = 0
        self.last_log = ""

        # Conexión Redis
        self.redis = get_redis_connection()
        self.status_message = "Redis Conectado 🟢" if self.redis else "Error Redis 🔴"

        # --- CORRECCIÓN DE RUTAS: Calculamos la raíz del proyecto ---
        # 1. Obtenemos donde está este archivo (src/streaming)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # 2. Subimos dos niveles (src -> raiz)
        self.project_root = os.path.dirname(os.path.dirname(current_dir))
        self.data_dir = os.path.join(self.project_root, "data")

        print(f"📂 Directorio de Datos detectado: {self.data_dir}")

    def _log(self, message):
        ts = datetime.now().strftime("%H:%M:%S")
        self.simple_logs.insert(0, f"[{ts}] {message}")
        if len(self.simple_logs) > 50:
            self.simple_logs.pop()

    @staticmethod
    def _time_to_seconds(time_val):
        if pd.isna(time_val) or time_val is None:
            return None
        if isinstance(time_val, (int, float)):
            return float(time_val)
        time_str = str(time_val).strip()
        if " " in time_str:
            time_str = time_str.split(" ")[-1]
        try:
            parts = time_str.split(':')
            if len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            elif len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            else:
                return float(time_str)
        except Exception:
            return None

    def load_data(self):
        self.status_message = "Cargando datos..."
        self._log("Iniciando carga y limpieza de archivos...")
        self.tracking_stream = []
        self.eventing_stream = []

        try:
            track_file = os.path.join(self.data_dir, "tracking_file.jsonl")
            ev_file = os.path.join(self.data_dir, "eventing_file.csv")
            ids_file = os.path.join(self.data_dir, "ids_tracking.json")

            # ==========================================
            # 1. IDs (Metadatos JSON) - Extracción Quirúrgica
            # ==========================================
            if not os.path.exists(ids_file):
                raise FileNotFoundError(f"No encuentro: {ids_file}")

            with open(ids_file, 'r', encoding='utf-8') as f:
                raw_json = json.load(f)

                # Construimos un objeto limpio siguiendo tus JSONPaths
                clean_metadata = {
                    "home_team": {
                        "id": raw_json.get("home_team", {}).get("id"),                 # $.home_team.id
                        "short_name": raw_json.get("home_team", {}).get("short_name"), # $.home_team.short_name
                        "acronym": raw_json.get("home_team", {}).get("acronym")        # $.home_team.acronym
                    },
                    "away_team": {
                        "id": raw_json.get("away_team", {}).get("id"),                 # $.away_team.id
                        "short_name": raw_json.get("away_team", {}).get("short_name"), # $.away_team.short_name
                        "acronym": raw_json.get("away_team", {}).get("acronym")        # $.away_team.acronym
                    },
                    "players": []
                }

                # Procesamos el array de jugadores
                # Path base: $.players
                raw_players = raw_json.get("players", [])
                for p in raw_players:
                    clean_player = {
                        "short_name": p.get("short_name"),                    # $.players[x].short_name
                        "id": p.get("id"),                                    # $.players[x].id
                        "team_id": p.get("team_id"),                          # $.players[x].team_id
                        "number": p.get("number"),                            # $.players[x].number
                        "role": p.get("player_role", {}).get("acronym")       # $.players[x].player_role.acronym
                    }
                    clean_metadata["players"].append(clean_player)

                    # Actualizamos el mapa interno para el Engine
                    if clean_player["id"]:
                        self.ids_map[str(clean_player["id"])] = clean_player

                self.raw_ids_data = clean_metadata

            # ==========================================
            # 2. TRACKING (Sin cambios mayores)
            # ==========================================
            if not os.path.exists(track_file):
                raise FileNotFoundError(f"Falta {track_file}")

            t_df = pd.read_json(track_file, lines=True)
            t_df['timestamp'] = t_df['timestamp'].astype(str)
            t_df['game_time'] = t_df['timestamp'].apply(self._time_to_seconds)
            if 'period' not in t_df.columns:
                t_df['period'] = 1

            self.tracking_stream = t_df.to_dict('records')
            self.total_game_time = t_df['game_time'].max() if not t_df['game_time'].isna().all() else 1

            # ==========================================
            # 3. EVENTING (CSV) - Minería de Datos
            # ==========================================
            if os.path.exists(ev_file):
                e_df = pd.read_csv(ev_file, sep=None, engine='python')

                # Lista exacta de columnas solicitadas
                target_columns = [
                    "id", "index",
                    "timestamp", "period", "minute", "event_type_name", "event_type_id",
                    "team_name", "player_name", "location_x", "location_y",
                    "end_location_y", "end_location_x", "end_location_z",
                    "pass_recipient_name", "pass_length", "pass_angle",
                    "pass_height_name", "pass_cross", "pass_cut_back",
                    "pass_switch", "body_part_name", "outcome_id", "outcome_name",
                    "type_id", "type_name"
                ]

                # 1. Validar existencia (Fail Fast si faltan las críticas)
                critical_cols = ["timestamp", "period"]
                missing_critical = [c for c in critical_cols if c not in e_df.columns]
                if missing_critical:
                    raise ValueError(f"Faltan columnas críticas en CSV: {missing_critical}")

                # 2. Calcular tiempo para el motor (game_time)
                e_df['game_time'] = e_df['timestamp'].apply(self._time_to_seconds)
                e_df = e_df.dropna(subset=['game_time'])

                # 3. Filtrar y rellenar columnas faltantes (para no romper si falta una opcional)
                # Si una columna opcional (ej: pass_cross) no viene, la creamos con None
                for col in target_columns:
                    if col not in e_df.columns:
                        e_df[col] = None

                # 4. Seleccionar SOLO las columnas de interés + game_time
                final_cols = target_columns + ['game_time']
                e_df = e_df[final_cols]

                integers_to_fix = ['event_type_id', 'type_id', 'outcome_id']

                for col in integers_to_fix:
                    if col in e_df.columns:
                        # astype('Int64') es la clave mágica de Pandas moderno
                        e_df[col] = e_df[col].astype('Int64')

                # 5. Ordenar para el streaming
                e_df = e_df.sort_values(by=['period', 'game_time'])

                e_df = e_df.where(pd.notnull(e_df), None)

                self.eventing_stream = e_df.to_dict('records')

            self.status_message = (
                f"Datos Pulidos: {len(self.tracking_stream)} frames | "
                f"{len(self.eventing_stream)} eventos"
            )
            self._log("Carga completada y datos estructurados.")
            return True

        except Exception as e:
            error_msg = f"Error Carga: {str(e)}"
            self.status_message = error_msg
            self._log(f"❌ {error_msg}")
            print(f"❌ ERROR DETALLADO: {e}")
            self.errors += 1
            return False

    def set_speed(self, speed: float):
        self.speed_multiplier = max(0.1, speed)

    def send_alignment(self):
        # Intentamos cargar si no está cargado
        if not self.raw_ids_data:
            success = self.load_data()
            if not success:
                return False

        try:
            # Enviamos a Redis
            if self.redis:
                self.redis.set(KEY_METADATA, json.dumps(self.raw_ids_data))
                self.status_message = "Alineación Publicada 📋"
                self._log("Metadata enviada a Redis (match:metadata)")
                return True
            else:
                self._log("❌ No hay conexión a Redis")
                return False
        except Exception as e:
            self._log(f"❌ Error Redis: {e}")
            self.errors += 1
            return False

    def start_stream(self):
        if not self.tracking_stream:
            if not self.load_data():
                return
        if not self.running:
            self.running = True
            self._thread = threading.Thread(target=self._stream_loop)
            self._thread.start()

    def stop_stream(self):
        self.running = False
        self.status_message = "Detenido por usuario"

    def _stream_loop(self):
        track_idx = 0
        event_idx = 0
        total_track = len(self.tracking_stream)
        total_event = len(self.eventing_stream)

        # Control de Tiempo y Delta
        last_tracking_time = None  # Para calcular el sleep entre frames
        FRAME_DURATION = 0.1       # Fallback si no hay timestamp

        # Control de Sincronización Eventos (Absoluto vs Relativo)
        current_period_start_time = 0.0
        last_processed_period = 1

        self._log("▶️ Streaming iniciado (Sincronizado)...")

        while self.running and track_idx < total_track:
            start_proc = time.time()

            # 1. Obtener Frame de Tracking
            track_record = self.tracking_stream[track_idx]
            current_game_time_abs = track_record.get('game_time')  # Tiempo ABSOLUTO (e.g., 2800s)
            p_val = track_record.get('period')

            # --- LÓGICA DE TIEMPO Y SLEEP (Tu requerimiento de 0.1s / 0.05s) ---
            if pd.isna(current_game_time_abs):
                # Si no hay tiempo, usamos velocidad crucero por defecto
                wait = FRAME_DURATION
                self.status_message = f"WAITING (Frame {track_idx})"
            else:
                self.current_time = current_game_time_abs
                self.current_period = int(p_val) if pd.notna(p_val) else 1
                self.status_message = f"LIVE P{self.current_period} 🔴"

                # Detectar cambio de periodo para resetear la referencia de eventos
                if self.current_period != last_processed_period:
                    current_period_start_time = current_game_time_abs
                    last_processed_period = self.current_period
                    self._log(
                        f"🔄 Cambio de Periodo detectado: P{self.current_period} "
                        f"inicia en t={current_game_time_abs}"
                    )

                # Calculamos el DELTA exacto con el frame anterior
                if last_tracking_time is not None:
                    delta = current_game_time_abs - last_tracking_time
                    # Protección contra saltos gigantes o negativos (ej: pausas largas o errores)
                    if 0 <= delta < 5.0:
                        wait = delta
                    else:
                        wait = FRAME_DURATION
                else:
                    wait = 0  # Primer frame sale disparado

                last_tracking_time = current_game_time_abs

            # Aplicar espera (Sleep) ajustada por el multiplicador de velocidad
            if wait > 0:
                time.sleep(wait / self.speed_multiplier)

            # --- LÓGICA DE EVENTOS (Tu requerimiento de sincronización) ---
            # Calculamos el tiempo RELATIVO del tracking para compararlo con el evento
            # Si estamos en P1 (inicio 0): 10.5 - 0 = 10.5
            # Si estamos en P2 (inicio 2700): 2710.5 - 2700 = 10.5 -> COINCIDE con evento P2
            current_time_rel = self.current_time - current_period_start_time

            while event_idx < total_event:
                event_record = self.eventing_stream[event_idx]
                ev_time_rel = event_record['game_time']  # Esto viene reiniciado (0-45)
                ev_period = event_record['period']

                # CASO 1: Eventos atrasados (de periodos anteriores o segundos previos)
                # Se envían inmediatamente para "ponerse al día"
                if ev_period < self.current_period:
                    self._publish_event(event_record)
                    event_idx += 1

                # CASO 2: Eventos del periodo actual
                # Comparamos peras con peras (Tiempo Relativo vs Tiempo Relativo)
                elif ev_period == self.current_period:
                    # Usamos un margen pequeño (0.05) para asegurar que no se quede atrás por milisegundos
                    if ev_time_rel <= (current_time_rel + 0.05):
                        self._publish_event(event_record)
                        event_idx += 1
                    else:
                        # El evento es futuro, paramos de buscar en la lista
                        break

                # CASO 3: Eventos de periodos futuros (no deberían estar aquí por el sort, pero por seguridad)
                else:
                    break

            # 3. Publicar Tracking
            self._publish_tracking(track_record)
            track_idx += 1

            # Latencia real de procesamiento
            self.latency_ms = (time.time() - start_proc) * 1000

        self.running = False
        self.status_message = "Fin del Partido"
        self._log("🏁 Streaming finalizado.")

    def _publish_tracking(self, record):
        if not self.redis:
            return
        try:
            # Limpieza para no enviar datos internos de Python
            payload = {k: v for k, v in record.items() if k not in ['game_time', 'converted_time']}
            self.redis.set(KEY_TRACKING, json.dumps(payload))

            self.total_tracking += 1
            self.metrics['tracking']['count'] += 1
        except Exception:
            self.errors += 1

    def _publish_event(self, record):
        if not self.redis:
            return
        try:
            payload = {k: v for k, v in record.items() if k not in ['game_time']}
            self.redis.publish(KEY_EVENTS, json.dumps(payload))

            self.total_events += 1
            self.metrics['eventing']['count'] += 1

            evt_type = payload.get('event_type_name') or payload.get('type')
            self.sent_eventing_log.append({
                'Time': f"{self.current_time:.1f}",
                'Type': evt_type,
                'Player': payload.get('player_name')
            })
        except Exception:
            self.errors += 1
