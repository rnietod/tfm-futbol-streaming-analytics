"""
Ghost Engine — desviación del rendimiento en vivo respecto a la norma histórica
del PROPIO jugador, medida en unidades de desviación estándar (z-score).

Idea
----
Para cada métrica relevante a la posición:

    live_p30 = (valor_acumulado_en_vivo / minutos_jugados) * 30
    z        = (live_p30 - media_p30) / max(std_p30, σ_floor)

donde media_p30 y std_p30 vienen de `player_ghost_baseline` (Postgres), que se
construye en `scripts/build_ghost_baseline.py` desde el histórico de BigQuery por
(jugador, game_state). El z compuesto (media ponderada por importancia de la
posición) es el número que muestra el ticker y alimenta el radar/comparativa.

El "cold start damper" amortigua el ruido de los primeros minutos (z≈0 hasta el
minuto 5, rampa lineal hasta el 20).
"""

import logging
from typing import Dict, Any, List
from sqlalchemy import text
from src.data.postgres_client import get_db_engine

logger = logging.getLogger(__name__)

# --- Pesos de importancia por grupo de posición (sobre el z de cada métrica) ---
WEIGHTS_CONFIG = {
    "FW":  {"xg": 1.5, "shots": 1.2, "key_passes": 0.6, "recoveries": 0.4},
    "MID": {"key_passes": 1.4, "progressive": 1.0, "recoveries": 0.9,
            "interceptions": 0.7, "xg": 0.7, "shots": 0.6},
    "DEF": {"recoveries": 1.4, "interceptions": 1.3, "progressive": 0.9, "shots": 0.2},
    "GK":  {"recoveries": 1.0, "progressive": 1.0, "interceptions": 0.6},
}

ROLE_MAP = {
    "CF": "FW", "ST": "FW", "LW": "FW", "RW": "FW", "SS": "FW", "RF": "FW", "LF": "FW",
    "CAM": "MID", "CM": "MID", "CDM": "MID", "DM": "MID", "RDM": "MID", "LDM": "MID",
    "LM": "MID", "RM": "MID", "AM": "MID",
    "CB": "DEF", "LCB": "DEF", "RCB": "DEF", "LB": "DEF", "RB": "DEF",
    "LWB": "DEF", "RWB": "DEF",
    "GK": "GK",
}

# Métricas con baseline en player_ghost_baseline.
METRIC_KEYS = ["xg", "shots", "key_passes", "progressive", "recoveries", "interceptions", "goals"]

# Piso de σ por métrica: evita z explosivos cuando la dispersión histórica es casi nula.
SIGMA_FLOOR = {
    "xg": 0.05, "shots": 0.30, "key_passes": 0.25, "progressive": 0.50,
    "recoveries": 0.50, "interceptions": 0.30, "goals": 0.08,
}

# Por debajo de esto el baseline por game_state es poco fiable -> fallback a 'Overall'.
MIN_MATCHES = 5

# Caché en memoria: { "match_id:game_state": { tracking_id: profile } }
_GHOST_CACHE: Dict[str, Dict[int, Any]] = {}


def _row_metrics(row_map) -> Dict[str, Dict[str, float]]:
    """Extrae {metric: {mean, std}} de una fila de player_ghost_baseline."""
    out = {}
    for m in METRIC_KEYS:
        out[m] = {
            "mean": row_map.get(f"{m}_p30_mean"),
            "std": row_map.get(f"{m}_p30_std"),
        }
    return out


class GhostEngine:

    @staticmethod
    def load_ghost_profiles(match_id: str, game_state: str = "Overall"):
        """
        Carga los perfiles base de los jugadores del partido para un game_state,
        con fallback a 'Overall' cuando el row específico no existe o tiene pocos
        partidos. Cachea por (match_id, game_state).
        """
        cache_key = f"{match_id}:{game_state}"
        if cache_key in _GHOST_CACHE:
            return

        engine = get_db_engine()
        profiles: Dict[int, Any] = {}
        try:
            with engine.connect() as conn:
                query = text("""
                    SELECT mp.position AS role, b.*
                    FROM match_players mp
                    JOIN player_ghost_baseline b
                      ON b.tracking_player_id = mp.player_id
                    WHERE mp.match_id = :mid
                      AND b.game_state IN (:gs, 'Overall')
                """)
                rows = conn.execute(query, {"mid": match_id, "gs": game_state}).fetchall()

                # Agrupar por jugador (puede traer fila del estado + fila Overall)
                by_player: Dict[int, Dict[str, Any]] = {}
                for r in rows:
                    d = dict(r._mapping)
                    tid = d["tracking_player_id"]
                    slot = by_player.setdefault(tid, {"role": d["role"], "state": None, "overall": None})
                    if d["game_state"] == "Overall":
                        slot["overall"] = d
                    else:
                        slot["state"] = d

                for tid, slot in by_player.items():
                    state, overall = slot["state"], slot["overall"]
                    chosen = state if (state and (state.get("n_matches") or 0) >= MIN_MATCHES) else (overall or state)
                    if not chosen:
                        continue
                    profiles[tid] = {
                        "role": slot["role"],
                        "n_matches": chosen.get("n_matches"),
                        "game_state_used": chosen.get("game_state"),
                        "metrics": _row_metrics(chosen),
                    }

            _GHOST_CACHE[cache_key] = profiles
            logger.info("👻 Ghost baseline cargado %s: %d perfiles", cache_key, len(profiles))
        except Exception as e:
            logger.error("❌ Error cargando Ghost baseline: %s", e)
            _GHOST_CACHE[cache_key] = {}

    @staticmethod
    def get_profile(match_id: str, game_state: str, tracking_id: int):
        """Devuelve el perfil cacheado de un jugador (cargándolo si hace falta)."""
        GhostEngine.load_ghost_profiles(match_id, game_state)
        return _GHOST_CACHE.get(f"{match_id}:{game_state}", {}).get(tracking_id)

    @staticmethod
    def calculate_deviation(live_val, minutes_played, mean_p30, std_p30, metric_key):
        """
        Devuelve (expected_p30, live_p30, z_amortiguado, status).
        z = (live_p30 - mean) / max(std, floor), con cold-start damper.
        """
        mean_p30 = float(mean_p30) if mean_p30 is not None else 0.0

        if minutes_played is None or minutes_played <= 0:
            return mean_p30, 0.0, 0.0, "calibrating"

        live_p30 = (float(live_val or 0) / minutes_played) * 30.0
        std = max(float(std_p30 or 0.0), SIGMA_FLOOR.get(metric_key, 0.30))
        z = (live_p30 - mean_p30) / std

        # Cold start damper
        if minutes_played <= 5:
            damp, status = 0.0, "calibrating"
        elif minutes_played <= 20:
            damp, status = (minutes_played - 5) / 15.0, "active"
        else:
            damp, status = 1.0, "active"

        return mean_p30, live_p30, z * damp, status

    @staticmethod
    def player_breakdown(profile: Dict, live: Dict) -> Dict:
        """
        Calcula el desglose por métrica + z compuesto de UN jugador.
        `profile`: salida de load_ghost_profiles[tid]
        `live`: {minutes, xg, shots, key_passes, progressive, recoveries, interceptions, goals, name, number, team_id}
        """
        role_group = ROLE_MAP.get((profile.get("role") or "").upper(), "MID")
        weights = WEIGHTS_CONFIG.get(role_group, WEIGHTS_CONFIG["MID"])
        minutes = live.get("minutes", 0)

        metrics_out = {}
        weighted_sum = 0.0
        total_weight = 0.0
        calibrating = False

        for metric, weight in weights.items():
            # Si la métrica no está disponible en vivo (p.ej. key_passes sin flag
            # en match_events), se omite del z compuesto en lugar de penalizar.
            if live.get(metric) is None:
                continue
            base = profile["metrics"].get(metric, {})
            mean, std = base.get("mean"), base.get("std")
            expected, live_p30, z, status = GhostEngine.calculate_deviation(
                live.get(metric, 0), minutes, mean, std, metric
            )
            if status == "calibrating":
                calibrating = True
            metrics_out[metric] = {
                "live": round(float(live.get(metric, 0) or 0), 2),
                "live_p30": round(live_p30, 3),
                "expected": round(expected, 3),
                "std": round(float(std or 0.0), 3),
                "z": round(z, 3),
                "weight": weight,
            }
            weighted_sum += z * weight
            total_weight += weight

        composite_z = (weighted_sum / total_weight) if total_weight > 0 else 0.0
        status = "calibrating" if calibrating else "active"
        # Score 0-10 derivado (6 = en su media; +1σ ≈ +1.5 pts)
        overall_score = max(0.0, min(10.0, round(6.0 + composite_z * 1.5, 1)))

        return {
            "position_group": role_group,
            "n_matches": profile.get("n_matches"),
            "metrics": metrics_out,
            "deviation_sigma": round(composite_z, 2),
            "overall_score": overall_score,
            "status": status,
        }

    @staticmethod
    def analyze_match(match_id: str, live_stats: List[Dict], game_state: str = "Overall"):
        """
        Cruza stats en vivo (lista de dicts por jugador) contra el baseline.
        Devuelve el ranking de desviaciones para el ticker.
        """
        GhostEngine.load_ghost_profiles(match_id, game_state)
        profiles = _GHOST_CACHE.get(f"{match_id}:{game_state}", {})

        current_minute = max([p.get("minutes", 0) for p in live_stats], default=0)
        players = []

        for live in live_stats:
            tid = live.get("player_id")
            profile = profiles.get(tid)
            if not profile:
                continue
            bd = GhostEngine.player_breakdown(profile, live)
            players.append({
                "player_id": tid,
                "player_name": live.get("name"),
                "number": live.get("number"),
                "team_id": live.get("team_id"),
                **bd,
            })

        players.sort(key=lambda p: abs(p["deviation_sigma"]), reverse=True)
        return {"match_minute": current_minute, "game_state": game_state, "players": players}
