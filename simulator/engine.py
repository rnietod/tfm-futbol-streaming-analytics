# simulator/engine.py (Versión 5.3 - Sincronización por Periodo)
import pandas as pd
import json
import time
import threading
import os
import sys
from google.cloud import pubsub_v1
import google.auth
from datetime import datetime

# 1. SETUP PATH
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from TACTIX_LIVE.utils.config_loader import load_config
except ImportError:
    def load_config(env): return {}


class SimulationEngine:
    def __init__(self, env="dev"):
        self.env = env
        self.config = load_config(env)
        self.project_id = self.config.get('gcp_project_id', '')
        self.topic_tracking = self.config.get('pubsub', {}).get('topic_tracking', '')
        self.topic_eventing = self.config.get('pubsub', {}).get('topic_eventing', '')

        self.speed_multiplier = 1.0
        self.running = False
        self.current_time = -1.0
        self.current_period = 0  # Nuevo estado para UI

        # Listas Maestras
        self.tracking_stream = []
        self.eventing_stream = []

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

        self.publisher = None
        self._connect_gcp()

    def _log(self, message):
        ts = datetime.now().strftime("%H:%M:%S")
        self.simple_logs.insert(0, f"[{ts}] {message}")
        if len(self.simple_logs) > 50:
            self.simple_logs.pop()

    def _connect_gcp(self):
        try:
            creds, _ = google.auth.default()
            self.publisher = pubsub_v1.PublisherClient(credentials=creds)
            self.path_track = self.publisher.topic_path(self.project_id, self.topic_tracking)
            self.path_event = self.publisher.topic_path(self.project_id, self.topic_eventing)
            self.status_message = "Conectado a GCP 🟢"
        except Exception as e:
            self.status_message = f"Error GCP: {str(e)} 🔴"
            self.errors += 1

    @staticmethod
    def _time_to_seconds(time_val):
        """
        Limpia fechas (2025-11-20) y convierte HH:MM:SS o MM:SS a segundos.
        """
        if pd.isna(time_val) or time_val is None:
            return None

        # Si ya es número
        if isinstance(time_val, (int, float)):
            return float(time_val)

        # Si es string
        time_str = str(time_val).strip()

        # 1. ELIMINAR FECHA SI EXISTE (ej: "2025-11-20 00:00:50.000")
        if " " in time_str:
            time_str = time_str.split(" ")[-1]  # Tomar la parte de la hora

        try:
            parts = time_str.split(':')
            if len(parts) == 3:  # HH:MM:SS.ss
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            elif len(parts) == 2:  # MM:SS.ss
                return float(parts[0]) * 60 + float(parts[1])
            else:
                return float(time_str)
        except BaseException:
            return None

    def load_data(self):
        self.status_message = "Cargando datos..."
        self._log("Cargando (Sincronización por Periodo)...")

        self.tracking_stream = []
        self.eventing_stream = []
        self.sent_tracking_log = []
        self.sent_eventing_log = []
        self.total_tracking = 0
        self.total_events = 0
        self.metrics = {'tracking': {'count': 0, 'total_latency': 0.0}, 'eventing': {'count': 0, 'total_latency': 0.0}}

        try:
            track_file = "data/tracking_file.jsonl"
            ev_file = "data/eventing_file.csv"
            ids_file = "data/ids_tracking.json"

            # 1. IDs
            if os.path.exists(ids_file):
                with open(ids_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    iterator = data.values() if isinstance(data, dict) else data
                    for team in iterator:
                        if not isinstance(team, dict):
                            continue
                        t_id, t_name = team.get('team_id') or team.get('id'), team.get('team_name') or team.get('name')
                        for p in team.get('players', []):
                            pid = p.get('player_id') or p.get('id')
                            if pid:
                                self.ids_map[pid] = {
                                    'team_id': t_id, 'team_name': t_name, 'player_name': p.get('player_name')}

            # 2. TRACKING (MASTER)
            t_df = pd.read_json(track_file, lines=True, dtype=False)

            # Asegurar que timestamp sea string para limpiarlo
            t_df['timestamp'] = t_df['timestamp'].astype(str)
            t_df['game_time'] = t_df['timestamp'].apply(self._time_to_seconds)

            # Rellenar periodo si falta (asumimos 1 si es null, para no romper orden)
            if 'period' not in t_df.columns:
                t_df['period'] = 1
            t_df['period'] = t_df['period'].fillna(1).astype(int)

            # Enriquecer
            def enrich(players):
                if not isinstance(players, list):
                    return []
                for p in players:
                    pid = p.get('player_id')
                    if pid in self.ids_map:
                        p.update(self.ids_map[pid])
                return players
            t_df['player_data'] = t_df['player_data'].apply(enrich)

            # NO ORDENAMOS EL TRACKING (Respetamos la secuencia visual del archivo JSONL)
            self.tracking_stream = t_df.to_dict('records')

            # Tiempo total (suma aproximada)
            max_t = t_df['game_time'].max()
            if pd.notna(max_t):
                self.total_game_time = max_t

            # 3. EVENTING (QUEUE)
            e_df = pd.read_csv(ev_file, sep=None, engine='python')
            t_col = next((c for c in ['game_time_seconds', 'timestamp', 'time'] if c in e_df.columns), None)
            p_col = next((c for c in ['period', 'period_id', 'half'] if c in e_df.columns), None)

            if t_col:
                e_df[t_col] = e_df[t_col].astype(str)
                e_df['game_time'] = e_df[t_col].apply(self._time_to_seconds)
                e_df = e_df.dropna(subset=['game_time'])

                # Normalizar columna periodo
                if p_col:
                    e_df['period'] = e_df[p_col].fillna(1).astype(int)
                else:
                    e_df['period'] = 1  # Default

                # 🟢 CLAVE: Ordenar Eventos por (Periodo, Tiempo)
                e_df = e_df.sort_values(by=['period', 'game_time'])
                self.eventing_stream = e_df.to_dict('records')

            self.status_message = f"Listo. Track: {len(self.tracking_stream)} | Event: {len(self.eventing_stream)}"
            self._log("Carga sincronizada completada.")
            return True

        except Exception as e:
            self.status_message = f"Error Carga: {str(e)}"
            self._log(f"❌ Error fatal: {e}")
            self.errors += 1
            return False

    def set_speed(self, speed: float):
        self.speed_multiplier = max(1.0, speed)

    def send_alignment(self):
        if not self.tracking_stream:
            if not self.load_data():
                return False
        self.status_message = "Alineación Enviada ✅"
        return True

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
        self.status_message = "Pausado ⏹️"

    def _stream_loop(self):
        track_idx = 0
        event_idx = 0
        total_track = len(self.tracking_stream)
        total_event = len(self.eventing_stream)

        last_valid_game_time = 0.0
        current_track_period = 1
        FRAME_DURATION = 0.04  # 25 fps

        self._log("▶️ Iniciando Master Clock...")

        while self.running and track_idx < total_track:
            # 1. Leer Frame Actual
            track_record = self.tracking_stream[track_idx]
            current_game_time = track_record.get('game_time')

            # Leer el periodo del frame (si es nulo, asumimos el último conocido o 1)
            p_val = track_record.get('period')
            if pd.notna(p_val):
                current_track_period = int(p_val)

            # 2. Control de Tiempo
            is_valid_time = pd.notna(current_game_time) and current_game_time is not None

            if not is_valid_time:
                self.status_message = f"WAITING (P{current_track_period})"
                self.current_time = -1
                time.sleep(FRAME_DURATION / self.speed_multiplier)
            else:
                self.status_message = f"LIVE P{current_track_period} 🔴"
                self.current_time = current_game_time
                self.current_period = current_track_period

                delta = current_game_time - last_valid_game_time
                # Si cambiamos de periodo o hay un salto grande, no esperamos el delta
                if delta < 0 or delta > 5.0:
                    wait = FRAME_DURATION
                else:
                    wait = delta

                if wait > 0:
                    time.sleep(wait / self.speed_multiplier)
                last_valid_game_time = current_game_time

                # 3. INYECCIÓN DE EVENTOS (Sincronizada por Periodo y Tiempo)
                while event_idx < total_event:
                    event_record = self.eventing_stream[event_idx]
                    ev_time = event_record['game_time']
                    ev_period = event_record['period']

                    # Lógica de prioridad:
                    # - Si el evento es de un periodo ANTERIOR, ya debió salir -> Enviarlo ya.
                    # - Si es del MISMO periodo y su tiempo <= tiempo actual -> Enviarlo ya.
                    should_send = False

                    if ev_period < current_track_period:
                        should_send = True
                    elif ev_period == current_track_period and ev_time <= (current_game_time + 0.05):
                        should_send = True

                    if should_send:
                        self._publish_event(event_record)
                        event_idx += 1
                    else:
                        # El evento es futuro (mismo periodo, tiempo mayor) o de un periodo futuro
                        break

            self._publish_tracking(track_record)
            track_idx += 1

        self.running = False
        self.status_message = "Fin de Secuencia"
        self._log("🏁 Partido finalizado.")

    # --- Helpers (Iguales) ---
    def _publish_tracking(self, record):
        try:
            payload = record.copy()
            if 'game_time' in payload:
                del payload['game_time']
            if 'converted_time' in payload:
                del payload['converted_time']  # Limpieza extra

            data_str = json.dumps(payload, default=str).encode("utf-8")
            start = time.time()
            self.publisher.publish(self.path_track, data_str)
            lat = (time.time() - start) * 1000

            self.metrics['tracking']['count'] += 1
            self.metrics['tracking']['total_latency'] += lat
            self.total_tracking += 1

            self.sent_tracking_log.append({
                'Frame': payload.get('frame'),
                'Time': payload.get('timestamp', 'NULL'),  # Mostrar el string original
                'Period': payload.get('period'),
                'Latencia': int(lat)
            })
            if len(self.sent_tracking_log) > 2000:
                self.sent_tracking_log.pop(0)

        except Exception:
            self.errors += 1

    def _publish_event(self, record):
        try:
            payload = record.copy()
            if 'game_time' in payload:
                del payload['game_time']

            data_str = json.dumps(payload, default=str).encode("utf-8")
            start = time.time()
            self.publisher.publish(self.path_event, data_str)
            lat = (time.time() - start) * 1000

            self.metrics['eventing']['count'] += 1
            self.metrics['eventing']['total_latency'] += lat
            self.total_events += 1

            evt_type = payload.get('type_name') or payload.get('type') or 'Evento'
            self.sent_eventing_log.append({
                'Period': payload.get('period'),
                'Evento': evt_type,
                'Time': f"{self.current_time:.2f}",
                'Latencia': int(lat)
            })
            self.last_log = f"⚡ P{payload.get('period')} {evt_type} @ {self.current_time:.1f}s"

        except Exception:
            self.errors += 1
