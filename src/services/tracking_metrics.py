# src/services/tracking_metrics.py
"""
Motor de Métricas Físicas de Tracking por Jugador
===================================================
Lee los frames almacenados en la tabla `match_tracking` de PostgreSQL
(guardados progresivamente por el worker_persist durante el streaming) y
calcula métricas de carga física:

    - Distancia total recorrida (m)
    - Velocidad máxima (m/s y km/h) + segundo en que se alcanzó
    - Distancia Carrera  : tramos a > 75 % de la v_max personal
    - Distancia Trotada  : tramos al 25 – 75 % de la v_max personal
    - Distancia Caminada : tramos a < 25 % de la v_max personal
    - Velocidad instantánea por segundo (para gráficas)

Parámetros clave
----------------
- Ventana de suavizado : 10 frames  (≈ 1 s a 10 fps)
- Umbrales de zona     : dinámicos, basados en la v_max personal de cada jugador
- Fuente de datos      : tabla `match_tracking` (frames JSONB)
- Gap máximo permitido : 2 s entre frames consecutivos de un jugador
                         (protege contra baches de detección)
"""

import json
import math
import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from src.data.postgres_client import get_db_engine

logger = logging.getLogger(__name__)

# ─── Constantes de configuración ──────────────────────────────────────────────
SMOOTH_WINDOW = 10          # frames (~1 s)
MAX_GAP_SECONDS = 2.0       # saltos mayores se ignoran
SPRINT_THRESHOLD = 0.75     # > 75 % de v_max personal → Carrera
RUN_THRESHOLD = 0.25        # > 25 % de v_max personal → Trote  (lo de abajo → Caminata)
MS_TO_KMH = 3.6


# ─── Clase principal ───────────────────────────────────────────────────────────
class TrackingMetricsEngine:
    """
    Calcula métricas físicas de tracking a partir de los frames almacenados
    en la tabla `match_tracking` de PostgreSQL.
    """

    # ── Lectura de datos ───────────────────────────────────────────────────────

    @staticmethod
    def _load_frames_from_db(match_id: str) -> List[Dict[str, Any]]:
        """
        Recupera todos los frames de `match_tracking` ordenados por frame_idx.
        Cada fila contiene `frame_idx` (int) y `players_data` (JSONB / dict).

        El formato de `players_data` almacenado por worker_persist es el frame
        completo del JSONL:
            {
              "frame": 1340,
              "timestamp": "00:00:00.00",
              "period": 1,
              "player_data": [
                {"x": -43.45, "y": 0.7, "player_id": 4713, "is_detected": true},
                ...
              ]
            }
        """
        engine = get_db_engine()
        frames: List[Dict[str, Any]] = []

        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT frame_idx, players_data
                    FROM match_tracking
                    WHERE match_id = :mid
                    ORDER BY frame_idx ASC
                """),
                {"mid": match_id}
            ).fetchall()

        for row in rows:
            pd = row.players_data
            if isinstance(pd, str):
                pd = json.loads(pd)
            frames.append({"frame_idx": row.frame_idx, "data": pd})

        logger.info(f"[TrackingMetrics] {len(frames)} frames cargados para match={match_id}")
        return frames

    # ── Parseo de timestamp ────────────────────────────────────────────────────

    @staticmethod
    def _ts_to_seconds(ts_val: Any) -> Optional[float]:
        """Convierte 'HH:MM:SS.ss' o float a segundos."""
        if ts_val is None:
            return None
        if isinstance(ts_val, (int, float)):
            return float(ts_val)
        ts = str(ts_val).strip()
        # Quitar parte de fecha si viene con espacio
        if " " in ts:
            ts = ts.split(" ")[-1]
        try:
            parts = ts.split(":")
            if len(parts) == 3:
                return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
            elif len(parts) == 2:
                return float(parts[0]) * 60 + float(parts[1])
            return float(ts)
        except Exception:
            return None

    # ── Suavizado ─────────────────────────────────────────────────────────────

    @staticmethod
    def _smooth(values: List[float], window: int = SMOOTH_WINDOW) -> List[float]:
        """
        Media móvil simple de tamaño `window`.
        Los primeros valores (arranque) usan una ventana más pequeña para
        no perder datos al inicio del partido.
        """
        smoothed = []
        half = window // 2
        n = len(values)
        for i in range(n):
            lo = max(0, i - half)
            hi = min(n, i + half + 1)
            chunk = values[lo:hi]
            smoothed.append(sum(chunk) / len(chunk) if chunk else 0.0)
        return smoothed

    # ── Clasificación de zona ─────────────────────────────────────────────────

    @staticmethod
    def _classify_zone(speed: float, v_max: float) -> str:
        """Devuelve 'sprint', 'run' o 'walk' según % de v_max personal."""
        if v_max <= 0:
            return "walk"
        ratio = speed / v_max
        if ratio > SPRINT_THRESHOLD:
            return "sprint"
        if ratio > RUN_THRESHOLD:
            return "run"
        return "walk"

    # ── Resampleo a 1 fps ─────────────────────────────────────────────────────

    @staticmethod
    def _resample_to_seconds(
        second_speeds: Dict[int, List[float]]
    ) -> List[Dict[str, float]]:
        """
        Recibe un dict {segundo_entero: [lista de speeds en ese segundo]}
        y devuelve una lista ordenada con la velocidad media por segundo.
        """
        result = []
        for sec in sorted(second_speeds.keys()):
            speeds = second_speeds[sec]
            avg_ms = sum(speeds) / len(speeds) if speeds else 0.0
            result.append({
                "second": sec,
                "speed_ms": round(avg_ms, 3),
                "speed_kmh": round(avg_ms * MS_TO_KMH, 2),
            })
        return result

    # ── Motor principal ────────────────────────────────────────────────────────

    def compute_for_match(self, match_id: str) -> Dict[str, Any]:
        """
        Punto de entrada principal.
        Lee los frames de PostgreSQL y devuelve las métricas por jugador.

        Returns
        -------
        {
          "match_id": str,
          "frames_processed": int,
          "players": {
            "<player_id>": {
              "player_id": int,
              "total_distance_m": float,
              "max_speed_ms": float,
              "max_speed_kmh": float,
              "max_speed_at_second": int,
              "max_speed_at_period": int,
              "distance_sprint_m": float,
              "distance_run_m": float,
              "distance_walk_m": float,
              "frames_processed": int,
              "speed_per_second": [ { "second", "speed_ms", "speed_kmh" } ]
            }
          }
        }
        """
        frames = self._load_frames_from_db(match_id)
        if not frames:
            logger.warning(f"[TrackingMetrics] Sin frames en DB para match={match_id}")
            return {"match_id": match_id, "frames_processed": 0, "players": {}}

        # ── Paso 1: Construir histórico de posiciones por jugador ──────────────
        # player_history[pid] = [ (frame_idx, timestamp_s, period, x, y), ... ]
        player_history: Dict[int, List[Tuple]] = defaultdict(list)

        for frame_obj in frames:
            frame_idx = frame_obj["frame_idx"]
            data = frame_obj["data"]

            # El timestamp viene del JSON interno del frame
            ts_raw = data.get("timestamp")
            period = data.get("period") or 1
            ts_sec = self._ts_to_seconds(ts_raw)

            player_data = data.get("player_data", [])
            for pd in player_data:
                pid = pd.get("player_id")
                x = pd.get("x")
                y = pd.get("y")

                if pid is None or x is None or y is None:
                    continue

                try:
                    pid = int(pid)
                    x = float(x)
                    y = float(y)
                except (ValueError, TypeError):
                    continue

                player_history[pid].append((frame_idx, ts_sec, int(period), x, y))

        # ── Paso 2: Calcular velocidades brutas frame a frame ──────────────────
        # player_speeds[pid] = [ (frame_idx, ts_sec, period, distance, speed_ms) ]
        player_speeds: Dict[int, List[Tuple]] = {}

        for pid, history in player_history.items():
            # Ordenar por frame_idx (ya deberían estar ordenados, pero por seguridad)
            history.sort(key=lambda h: h[0])
            speeds = []

            for i in range(1, len(history)):
                f_prev = history[i - 1]
                f_curr = history[i]

                # Desempaquetar
                _, ts_prev, period_prev, x_prev, y_prev = f_prev
                fi_curr, ts_curr, period_curr, x_curr, y_curr = f_curr

                # ── Calcular Δt ──────────────────────────────────────────────
                if ts_prev is not None and ts_curr is not None:
                    dt = ts_curr - ts_prev
                    # Si hay cambio de periodo, el timestamp se reinicia
                    # → forzamos dt positivo pequeño para no crear velocidades monstruosas
                    if period_curr != period_prev:
                        dt = 0.1  # Asumimos continuidad suave entre periodos
                    if dt <= 0 or dt > MAX_GAP_SECONDS:
                        # Frame duplicado o salto grande (jugador fuera de campo)
                        speeds.append((fi_curr, ts_curr, period_curr, 0.0, 0.0))
                        continue
                else:
                    # Sin timestamp: asumimos 10fps → Δt = 0.1s
                    dt = 0.1

                # ── Calcular distancia y velocidad ───────────────────────────
                dist = math.sqrt((x_curr - x_prev) ** 2 + (y_curr - y_prev) ** 2)
                speed = dist / dt  # m/s

                # Filtro de velocidad máxima física: un humano no pasa de ~12 m/s (≈43 km/h)
                if speed > 12.0:
                    speed = 0.0
                    dist = 0.0

                speeds.append((fi_curr, ts_curr, period_curr, dist, speed))

            player_speeds[pid] = speeds

        # ── Paso 3: Suavizado de velocidades ──────────────────────────────────
        # player_smooth[pid] = misma estructura que player_speeds pero con speed suavizada
        player_smooth: Dict[int, List[Tuple]] = {}

        for pid, speeds in player_speeds.items():
            raw_speeds = [s[4] for s in speeds]
            smoothed = self._smooth(raw_speeds, SMOOTH_WINDOW)
            player_smooth[pid] = [
                (speeds[i][0], speeds[i][1], speeds[i][2], speeds[i][3], smoothed[i])
                for i in range(len(speeds))
            ]

        # ── Paso 4: Calcular v_max personal (post-suavizado) ──────────────────
        player_vmax: Dict[int, Tuple[float, int, int]] = {}
        # Tuple: (v_max_ms, second_when, period_when)

        for pid, smooth_data in player_smooth.items():
            v_max = 0.0
            v_max_ts = 0
            v_max_period = 1

            for fi, ts, period, dist, speed in smooth_data:
                if speed > v_max:
                    v_max = speed
                    v_max_ts = int(ts) if ts is not None else 0
                    v_max_period = period

            player_vmax[pid] = (v_max, v_max_ts, v_max_period)

        # ── Paso 5: Agregar métricas y resamplear a 1fps ─────────────────────
        result_players: Dict[str, Any] = {}

        for pid, smooth_data in player_smooth.items():
            v_max, v_max_ts, v_max_period = player_vmax[pid]

            total_dist = 0.0
            dist_sprint = 0.0
            dist_run = 0.0
            dist_walk = 0.0

            # Para resampleo: segundo_entero → lista de speeds en ese segundo
            second_speeds: Dict[int, List[float]] = defaultdict(list)

            for fi, ts, period, dist, speed in smooth_data:
                total_dist += dist

                zone = self._classify_zone(speed, v_max)
                if zone == "sprint":
                    dist_sprint += dist
                elif zone == "run":
                    dist_run += dist
                else:
                    dist_walk += dist

                # Agrupar para resampleo
                if ts is not None:
                    sec_key = int(ts)
                    second_speeds[sec_key].append(speed)

            speed_per_second = self._resample_to_seconds(second_speeds)

            result_players[str(pid)] = {
                "player_id": pid,
                "total_distance_m": round(total_dist, 2),
                "max_speed_ms": round(v_max, 3),
                "max_speed_kmh": round(v_max * MS_TO_KMH, 2),
                "max_speed_at_second": v_max_ts,
                "max_speed_at_period": v_max_period,
                "distance_sprint_m": round(dist_sprint, 2),
                "distance_run_m": round(dist_run, 2),
                "distance_walk_m": round(dist_walk, 2),
                "frames_processed": len(smooth_data),
                "speed_per_second": speed_per_second,
            }

        logger.info(
            f"[TrackingMetrics] match={match_id} | "
            f"jugadores={len(result_players)} | frames={len(frames)}"
        )

        return {
            "match_id": match_id,
            "frames_processed": len(frames),
            "players": result_players,
        }

    def compute_for_player(
        self, match_id: str, player_id: int
    ) -> Optional[Dict[str, Any]]:
        """
        Atajo para obtener métricas de un único jugador.
        Calcula todo el partido y filtra el jugador solicitado.
        Si ya está en cache Redis o DB, es preferible usar esa versión.
        """
        result = self.compute_for_match(match_id)
        return result["players"].get(str(player_id))
