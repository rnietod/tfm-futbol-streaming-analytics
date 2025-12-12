# src/streaming/engine.py
import pandas as pd
import json
import time
import threading
import os
import sys
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
        if pd.isna(time_val) or time_val is None: return None
        if isinstance(time_val, (int, float)): return float(time_val)
        time_str = str(time_val).strip()
        if " " in time_str: time_str = time_str.split(" ")[-1]
        try:
            parts = time_str.split(':')
            if len(parts) == 3: return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            elif len(parts) == 2: return float(parts[0]) * 60 + float(parts[1])
            else: return float(time_str)
        except: return None

    def load_data(self):
        self.status_message = "Cargando datos..."
        self._log("Iniciando carga de archivos...")
        self.tracking_stream = []
        self.eventing_stream = []
        
        try:
            # Usamos las rutas absolutas calculadas en __init__
            track_file = os.path.join(self.data_dir, "tracking_file.jsonl")
            ev_file = os.path.join(self.data_dir, "eventing_file.csv")
            ids_file = os.path.join(self.data_dir, "ids_tracking.json")

            # Verificación de existencia para dar un error claro
            if not os.path.exists(ids_file):
                raise FileNotFoundError(f"No encuentro: {ids_file}")

            # 1. IDs (Alineación)
            with open(ids_file, 'r', encoding='utf-8') as f:
                self.raw_ids_data = json.load(f)
                
                # Mapeo de IDs para uso interno (Engine)
                data = self.raw_ids_data
                # Detectamos si es la lista plana o el objeto partido
                if isinstance(data, dict) and 'players' in data:
                    iterator = data['players']
                elif isinstance(data, dict):
                    iterator = data.values()
                else:
                    iterator = data # Asumimos lista
                
                if iterator:
                    for item in iterator:
                        if isinstance(item, dict): 
                            pid = item.get('id') or item.get('player_id')
                            if pid: self.ids_map[str(pid)] = item

            # 2. TRACKING
            if not os.path.exists(track_file): raise FileNotFoundError(f"Falta {track_file}")
            
            t_df = pd.read_json(track_file, lines=True)
            t_df['timestamp'] = t_df['timestamp'].astype(str)
            t_df['game_time'] = t_df['timestamp'].apply(self._time_to_seconds)
            
            if 'period' not in t_df.columns: t_df['period'] = 1
            t_df['period'] = t_df['period'].fillna(1).astype(int)

            self.tracking_stream = t_df.to_dict('records')
            self.total_game_time = t_df['game_time'].max() if not t_df['game_time'].isna().all() else 1

            # 3. EVENTING
            if os.path.exists(ev_file):
                e_df = pd.read_csv(ev_file, sep=None, engine='python')
                t_col = next((c for c in ['game_time_seconds', 'timestamp', 'time'] if c in e_df.columns), None)
                p_col = next((c for c in ['period', 'period_id', 'half'] if c in e_df.columns), None)

                if t_col:
                    e_df[t_col] = e_df[t_col].astype(str)
                    e_df['game_time'] = e_df[t_col].apply(self._time_to_seconds)
                    e_df = e_df.dropna(subset=['game_time'])
                    e_df['period'] = e_df[p_col].fillna(1).astype(int) if p_col else 1
                    e_df = e_df.sort_values(by=['period', 'game_time'])
                    self.eventing_stream = e_df.to_dict('records')

            self.status_message = f"Datos Listos: {len(self.tracking_stream)} frames"
            self._log("Carga completada exitosamente.")
            return True

        except Exception as e:
            error_msg = f"Error Carga: {str(e)}"
            self.status_message = error_msg
            self._log(f"❌ {error_msg}")
            print(f"❌ ERROR DETALLADO EN ENGINE: {e}") # Para ver en la terminal
            self.errors += 1
            return False

    def set_speed(self, speed: float):
        self.speed_multiplier = max(0.1, speed)

    def send_alignment(self):
        # Intentamos cargar si no está cargado
        if not self.raw_ids_data:
            success = self.load_data()
            if not success: return False
        
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
            if not self.load_data(): return
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
        last_valid_game_time = 0.0
        
        FRAME_DURATION = 0.1 # 100ms si es null

        self._log("▶️ Streaming iniciado...")

        while self.running and track_idx < total_track:
            start_proc = time.time()
            
            track_record = self.tracking_stream[track_idx]
            current_game_time = track_record.get('game_time')
            p_val = track_record.get('period')
            
            # Lógica de tiempo
            if pd.isna(current_game_time):
                self.status_message = f"WAITING (Frame {track_idx})"
                wait = FRAME_DURATION
            else:
                self.status_message = f"LIVE P{p_val} 🔴"
                self.current_time = current_game_time
                self.current_period = int(p_val) if pd.notna(p_val) else 1
                
                delta = current_game_time - last_valid_game_time
                wait = delta if (0 <= delta < 5.0) else FRAME_DURATION
                last_valid_game_time = current_game_time

            if wait > 0:
                time.sleep(wait / self.speed_multiplier)

            # Eventos
            while event_idx < total_event:
                event_record = self.eventing_stream[event_idx]
                ev_time = event_record['game_time']
                ev_period = event_record['period']

                if ev_period < self.current_period:
                    self._publish_event(event_record)
                    event_idx += 1
                elif ev_period == self.current_period and ev_time <= (self.current_time + 0.05):
                    self._publish_event(event_record)
                    event_idx += 1
                else:
                    break 

            # Tracking
            self._publish_tracking(track_record)
            track_idx += 1
            
            self.latency_ms = (time.time() - start_proc) * 1000

        self.running = False
        self.status_message = "Fin del Partido"
        self._log("🏁 Streaming finalizado.")

    def _publish_tracking(self, record):
        if not self.redis: return
        try:
            # Limpieza para no enviar datos internos de Python
            payload = {k: v for k, v in record.items() if k not in ['game_time', 'converted_time']}
            self.redis.set(KEY_TRACKING, json.dumps(payload))
            
            self.total_tracking += 1
            self.metrics['tracking']['count'] += 1
        except Exception: self.errors += 1

    def _publish_event(self, record):
        if not self.redis: return
        try:
            payload = {k: v for k, v in record.items() if k not in ['game_time']}
            self.redis.publish(KEY_EVENTS, json.dumps(payload))
            
            self.total_events += 1
            self.metrics['eventing']['count'] += 1
            
            evt_type = payload.get('type_name') or payload.get('type')
            self.sent_eventing_log.append({
                'Time': f"{self.current_time:.1f}", 
                'Type': evt_type,
                'Player': payload.get('player_name')
            })
        except Exception: self.errors += 1