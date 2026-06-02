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
        # Obtener nombres de equipos
        match_info = conn.execute(text(
            "SELECT home_team_name, away_team_name FROM matches WHERE match_id = :mid"
        ), {"mid": match_id}).fetchone()
        home_short = match_info.home_team_name
        away_short = match_info.away_team_name

        roster_rows = conn.execute(text(
            "SELECT player_id, team_id, name, dorsal, position FROM match_players WHERE match_id = :mid"
        ), {"mid": match_id}).fetchall()

        roster_by_tracking = {}
        for r in roster_rows:
            roster_by_tracking[str(r.player_id)] = {
                "team_id": r.team_id, "name": r.name,
                "number": r.dorsal, "position": r.position,
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
                AVG(CASE WHEN period = 2.0 THEN -x ELSE x END) as avg_x,
                AVG(CASE WHEN period = 2.0 THEN -y ELSE y END) as avg_y
            FROM extracted
            WHERE tracking_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
            GROUP BY tracking_id
        """), {"mid": match_id}).fetchall()
        
        # Create team_id to name map
        team_ids = set(r.team_id for r in roster_rows if r.team_id is not None)
        team_ids_sorted = sorted(team_ids)
        team_id_to_name = {}
        if len(team_ids_sorted) >= 2:
            team_id_to_name[team_ids_sorted[1]] = home_short
            team_id_to_name[team_ids_sorted[0]] = away_short
        elif len(team_ids_sorted) == 1:
            team_id_to_name[team_ids_sorted[0]] = home_short

        print("Equipos:", team_id_to_name)
        
        results = []
        for tr in tracking_avg_rows:
            tid = tr.tracking_id
            ro = roster_by_tracking.get(tid)
            if ro:
                team_id = ro.get("team_id")
                tname = team_id_to_name.get(team_id)
                results.append({
                    "tracking_id": tid,
                    "name": ro.get("name"),
                    "team": tname,
                    "x": round(((float(tr.avg_x) + 52.5) / 105.0) * 100, 1),
                    "y": round(((float(tr.avg_y) + 34.0) / 68.0) * 100, 1)
                })
        
        print("\nPrimeros 5 mapeados:")
        for res in results[:5]:
            print(res)

except Exception as e:
    print(f"Error: {e}")
