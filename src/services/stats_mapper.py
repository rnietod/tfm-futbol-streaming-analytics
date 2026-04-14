
import math

class StatsMapper:
    """
    Normalizes raw football metrics (per 90 or per 30) into 0-99 scales purely for visualization.
    Uses heuristic max values based on top 5 league standards.
    """

    # Reference Max Values (per 90 mins approximation) to reach 99 score
    # These are heuristics.
    MAX_VALUES = {
        "goals": 1.2,
        "shots": 5.5,
        "xg": 1.0,
        "assists": 0.8,
        "key_passes": 4.0,
        "passes": 90.0,
        "dribbles": 6.0,
        "tackles": 4.5,
        "interceptions": 3.0,
        "recoveries": 12.0,
        "duels_won": 10.0
    }

    @staticmethod
    def _normalize(value, max_val):
        if value is None: return 50 # Average fallback
        safe_val = float(value)
        # Linear normalization capped at 99
        score = (safe_val / max_val) * 99.0
        return min(99, max(1, int(score)))

    @staticmethod
    def map_to_fifa_scale(raw_stats: dict, position_group: str) -> list:
        """
        Converts a dictionary of raw stats (e.g., from BQ or Live Aggregation)
        into a list of 6 objects for the Radar Chart: PAC, SHO, PAS, DRI, DEF, PHY.
        
        Args:
            raw_stats: Dict with keys like 'goals', 'shots_p30', 'passes_p30', etc.
            position_group: 'FW', 'MID', 'DEF', 'GK' (used to weight attributes if needed)
        """
        
        # 0. Helper to get value with multiple potential keys (handling _p30 vs raw)
        def get_val(*keys):
            for k in keys:
                if k in raw_stats and raw_stats[k] is not None:
                    return raw_stats[k]
            return 0.0

        # 1. SHOOTING (SHO)
        # Driven by: Goals, xG, Shots
        goals = get_val('goals', 'goals_p30')
        xg = get_val('xg', 'xg_p30', 'expected_goals')
        shots = get_val('shots', 'shots_p30')
        
        # Weighted composite for Shooting
        sho_raw = (goals * 40.0) + (xg * 30.0) + (shots * 5.0) 
        # Normalize relative to a "perfect" striker performance
        sho_score = StatsMapper._normalize(sho_raw, 50.0) # Heuristic max score

        # 2. PASSING (PAS)
        pass_vol = get_val('passes', 'passes_p30', 'avg_passes')
        key_passes = get_val('key_passes', 'key_passes_p30')
        pas_raw = (pass_vol * 0.4) + (key_passes * 12.0)
        pas_score = StatsMapper._normalize(pas_raw, 80.0)

        # 3. DRIBBLING (DRI)
        dribbles = get_val('dribbles', 'dribbles_p30', 'avg_dribbles')
        # Proxy for agility/ball control if specific metrics missing
        dri_score = StatsMapper._normalize(dribbles, 6.0) 

        # 4. DEFENSE (DEF)
        interceptions = get_val('interceptions', 'interceptions_p30')
        tackles = get_val('tackles', 'tackles_p30', 'avg_tackles')
        recoveries = get_val('recoveries', 'recoveries_p30')
        def_raw = (interceptions * 10.0) + (tackles * 8.0) + (recoveries * 2.5)
        def_score = StatsMapper._normalize(def_raw, 60.0)

        # 5. PHYSICAL (PHY)
        # Often hard to track with event data alone. using Duels + Stamina proxy (minutes)
        duels = get_val('duels_won', 'duels_won_p30')
        phy_score = StatsMapper._normalize(duels, 10.0) 

        # 6. PACE (PAC)
        # Impossible to know without Tracking Data speed metrics.
        # We will use value 70 as neutral placeholder + small variance based on position
        pac_score = 70
        if position_group == "FW": pac_score = 82
        elif position_group == "DEF": pac_score = 65

        # RETURN FORMAT FOR RECHARTS
        return [
            { "metric": "PAC", "value": pac_score, "fullMark": 100 },
            { "metric": "SHO", "value": sho_score, "fullMark": 100 },
            { "metric": "PAS", "value": pas_score, "fullMark": 100 },
            { "metric": "DRI", "value": dri_score, "fullMark": 100 },
            { "metric": "DEF", "value": def_score, "fullMark": 100 },
            { "metric": "PHY", "value": phy_score, "fullMark": 100 },
        ]
