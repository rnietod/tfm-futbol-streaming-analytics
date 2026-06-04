import sys
import os

# Añadir el directorio raíz al path para poder importar src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        match_id = 'test_match'
        match_info = conn.execute(text(
            "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
        ), {"mid": match_id}).fetchone()
        home_short = match_info.home_team_name
        away_short = match_info.away_team_name

        roster_rows = conn.execute(text(
            "SELECT player_id, team_id, name, dorsal FROM match_players WHERE match_id = :mid"
        ), {"mid": match_id}).fetchall()

        roster_by_tracking = {}
        for r in roster_rows:
            roster_by_tracking[str(r.player_id)] = {
                "team_id": r.team_id, "name": r.name,
                "number": r.dorsal,
            }

        tracking_avg_rows = conn.execute(text("""
            WITH extracted AS (
                SELECT 
                    (players_data->>'period')::float as period,
                    (elem->>'player_id')::text as tracking_id,
                    (elem->>'x')::float as x,
                    (elem->>'y')::float as y
                FROM match_tracking,
                LATERAL jsonb_array_elements(players_data->'player_data') as elem
                WHERE match_id = :mid
            )
            SELECT 
                tracking_id,
                period,
                AVG(x) as avg_x,
                AVG(y) as avg_y,
                COUNT(*) as count
            FROM extracted
            WHERE tracking_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
            GROUP BY tracking_id, period
        """), {"mid": match_id}).fetchall()
        
        # Create team_id to name map
        team_ids = set(r.team_id for r in roster_rows if r.team_id is not None)
        team_ids_sorted = sorted(team_ids)
        home_team_id = team_ids_sorted[1] if len(team_ids_sorted) >= 2 else team_ids_sorted[0]
        away_team_id = team_ids_sorted[0] if len(team_ids_sorted) >= 2 else None

        results = {}
        for tr in tracking_avg_rows:
            tid = tr.tracking_id
            period = tr.period
            ro = roster_by_tracking.get(tid)
            if ro:
                team_id = ro.get("team_id")
                # Determine normalized attacking coordinate based on team and period
                if team_id == home_team_id:
                    # Home team (Atlético): plays L->R in P1, R->L in P2
                    x_norm = -tr.avg_x if period == 2.0 else tr.avg_x
                    y_norm = -tr.avg_y if period == 2.0 else tr.avg_y
                else:
                    # Away team (Real): plays R->L in M1, L->R in M2
                    x_norm = tr.avg_x if period == 2.0 else -tr.avg_x
                    y_norm = tr.avg_y if period == 2.0 else -tr.avg_y
                
                if tid not in results:
                    results[tid] = {"name": ro.get("name"), "number": ro.get("number"), "x_sum": 0.0, "y_sum": 0.0, "count": 0}
                
                results[tid]["x_sum"] += x_norm * tr.count
                results[tid]["y_sum"] += y_norm * tr.count
                results[tid]["count"] += tr.count

        print("Mapeados finales (promedio ponderado ponderando ambos tiempos):")
        for tid, data in list(results.items())[:5]:
            final_x = round(((data["x_sum"] / data["count"] + 52.5) / 105.0) * 100, 1)
            final_y = round(((data["y_sum"] / data["count"] + 34.0) / 68.0) * 100, 1)
            print(f"Player: {data['name']} (Nº {data['number']}) | final_x: {final_x} | final_y: {final_y}")

except Exception as e:
    print(f"Error: {e}")
