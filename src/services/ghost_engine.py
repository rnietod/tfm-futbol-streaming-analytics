import logging
import math
from typing import Dict, Any, List
from sqlalchemy import text
from src.data.postgres_client import get_db_engine

logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN DE PESOS (Business Logic) ---
WEIGHTS_CONFIG = {
    "FW": {"w_xg_p30": 1.5, "w_shots_p30": 1.2, "w_key_passes_p30": 0.5, "w_recoveries_p30": 0.5},
    "MID": {"w_xg_p30": 0.8, "w_shots_p30": 0.8, "w_key_passes_p30": 1.5, "w_recoveries_p30": 1.2},
    "DEF": {"w_xg_p30": 0.2, "w_shots_p30": 0.2, "w_progressive_p30": 1.0, "w_recoveries_p30": 1.5},
    "GK":  {"w_saves_vol": 1.5, "w_reflex_saves": 1.5, "w_dist_accuracy": 1.0}
}

ROLE_MAP = {
    "CF": "FW", "ST": "FW", "LW": "FW", "RW": "FW",
    "CAM": "MID", "CM": "MID", "CDM": "MID", "LM": "MID", "RM": "MID",
    "CB": "DEF", "LB": "DEF", "RB": "DEF", "LWB": "DEF", "RWB": "DEF",
    "GK": "GK"
}

# --- CACHÉ EN MEMORIA (Singleton Pattern simplificado) ---
# Estructura: { match_id: { player_id: { perfil_historico } } }
_GHOST_CACHE: Dict[str, Dict[int, Any]] = {}

class GhostEngine:
    
    @staticmethod
    def load_ghost_profiles(match_id: str):
        """
        PRE-LOAD: Consulta la Base de Datos (mart_ghost_profile) UNA SOLA VEZ
        y guarda los perfiles en memoria para acceso O(1).
        """
        if match_id in _GHOST_CACHE:
            logger.info(f"👻 Ghost Profiles ya cargados para {match_id} (Cache Hit)")
            return

        engine = get_db_engine()
        profiles_cache = {}

        try:
            with engine.connect() as conn:
                # 1. Obtenemos los jugadores del partido
                # 2. Hacemos JOIN con el perfil fantasma (mart_ghost_profile)
                # Nota: Asumimos que la tabla mart_ghost_profile existe en Postgres.
                # Si no existe (entorno dev limpio), fallará y manejaremos la excepción.
                query = text("""
                    SELECT 
                        mp.player_id,
                        mp.position as role,
                        mg.*
                    FROM match_players mp
                    LEFT JOIN mart_ghost_profile mg ON mp.player_id = mg.player_id
                    WHERE mp.match_id = :mid
                """)
                
                rows = conn.execute(query, {"mid": match_id}).fetchall()
                
                for row in rows:
                    # Convertimos la fila a diccionario
                    data = row._mapping
                    if data['player_id']:
                        profiles_cache[data['player_id']] = dict(data)
                
                _GHOST_CACHE[match_id] = profiles_cache
                logger.info(f"👻 Ghost Profiles cargados para {match_id}: {len(profiles_cache)} perfiles.")

        except Exception as e:
            logger.error(f"❌ Error cargando Ghost Profiles: {e}")
            # Fallback: Cache vacío para no romper el flujo
            _GHOST_CACHE[match_id] = {}

    @staticmethod
    def calculate_deviation(live_val: float, p30_val: float, minutes_played: int, metric_key: str):
        """
        EL ALGORITMO: Time Scaling + Cold Start Damper
        """
        # Protección contra nulls de la DB
        live_val = float(live_val or 0)
        p30_val = float(p30_val or 0)

        if minutes_played == 0:
            return 0.0, 0.0, "calibrating"

        # 1. Time Scaling (Regla de 3 simple: Qué se espera para X minutos)
        expected_now = (p30_val / 30.0) * minutes_played
        raw_deviation = live_val - expected_now

        # Excepción: No castigar inactividad en métricas de evento si es 0
        if live_val == 0 and "xg" in metric_key: # Ejemplo simple
             return expected_now, 0.0, "active"

        # 2. Cold Start Damper (Amortiguador)
        dampener = 1.0
        status = "active"

        if minutes_played <= 5:
            dampener = 0.0
            status = "calibrating"
        elif minutes_played <= 20:
            # Rampa lineal de 0.0 a 1.0 entre el minuto 5 y el 20
            dampener = (minutes_played - 5) / 15.0
        
        final_deviation = raw_deviation * dampener
        
        return expected_now, final_deviation, status

    @staticmethod
    def analyze_match(match_id: str, live_stats: List[Dict]):
        """
        Cruza datos LIVE (Stats agregadas) vs GHOST (Histórico Caché)
        """
        # Asegurar que tenemos caché
        if match_id not in _GHOST_CACHE:
            GhostEngine.load_ghost_profiles(match_id)
        
        ghost_cache = _GHOST_CACHE.get(match_id, {})
        analyzed_players = []
        
        # Minuto actual del partido (el máximo registrado por cualquier jugador activo)
        current_minute = max([p.get('minutes', 0) for p in live_stats]) if live_stats else 0

        for p_live in live_stats:
            pid = p_live['player_id']
            # Recuperar perfil histórico
            p_ghost = ghost_cache.get(pid)
            
            # Si no hay perfil histórico (jugador nuevo), saltamos o usamos defaults
            if not p_ghost:
                continue

            role_raw = p_live.get('role', 'MID')
            pos_group = ROLE_MAP.get(role_raw, 'MID')
            weights = WEIGHTS_CONFIG.get(pos_group, WEIGHTS_CONFIG['MID'])
            
            player_result = {
                "player_id": pid,
                "player_name": p_live.get('name'),
                "position_group": pos_group,
                "metrics": {},
                "overall_score": 6.0, # Base
                "status": "active"
            }

            weighted_sum = 0.0
            total_weight = 0.0
            is_calibrating = False

            # Iteramos sobre las métricas configuradas para esa posición
            for metric_ghost_key, weight in weights.items():
                # Mapeo de nombres: w_xg_p30 (DB Histórico) -> xg (Live Stats)
                # Quitamos el prefijo 'w_' y el sufijo '_p30' para buscar en live
                metric_live_key = metric_ghost_key.replace("w_", "").replace("_p30", "")
                
                # Casos especiales de nombres (si no coinciden exacto)
                if metric_live_key == "progressive": metric_live_key = "passes" # Simplificación por ahora
                
                live_val = p_live.get(metric_live_key, 0)
                hist_val = p_ghost.get(metric_ghost_key, 0)

                expected, deviation, status = GhostEngine.calculate_deviation(
                    live_val, hist_val, current_minute, metric_live_key
                )

                if status == "calibrating":
                    is_calibrating = True

                player_result["metrics"][metric_live_key] = {
                    "live": round(live_val, 2),
                    "expected": round(expected, 2),
                    "deviation": round(deviation, 3),
                    "importance_weight": weight
                }
                
                weighted_sum += deviation * weight
                total_weight += weight

            # Cálculo final del Score
            if is_calibrating:
                player_result["status"] = "calibrating"
                # En calibración, el score tiende a 6.0 (neutro)
                player_result["overall_score"] = 6.0
            else:
                # Score Base 6.0 + Desviación
                final_score = 6.0 + weighted_sum
                player_result["overall_score"] = max(0.0, min(10.0, round(final_score, 1)))

            analyzed_players.append(player_result)
            
        return {
            "match_minute": current_minute,
            "players": analyzed_players
        }