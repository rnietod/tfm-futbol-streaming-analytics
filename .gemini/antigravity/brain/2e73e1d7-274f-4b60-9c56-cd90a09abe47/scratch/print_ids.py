import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        match_id = 'test_match'
        
        # 1. Fetch Event-based average positions
        avg_pos_rows = conn.execute(text("""
            SELECT player_name as name, player_id,
                   AVG(location_x) as avg_x, AVG(location_y) as avg_y
            FROM match_events
            WHERE match_id = :mid AND location_x IS NOT NULL AND player_name IS NOT NULL
            GROUP BY player_name, player_id
        """), {"mid": match_id}).fetchall()
        
        print("--- EVENTING PLAYERS (averagePositions) ---")
        for r in avg_pos_rows[:15]:
            pid = str(r.player_id) if r.player_id is not None else "None"
            print(f"Name: {str(r.name):<30} | ID: {pid:<10}")
            
        # 2. Fetch Tracking-based average positions
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
        
        # Mapping dicts
        _OPTA_TO_TRACKING = {}
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        import csv
        mapping_path = os.path.join(base_dir, 'data', 'dim_player_mapping.csv')
        with open(mapping_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                opta = row.get('opta_player_id', '').split('.')[0]
                tracking = row.get('tracking_player_id', '')
                if opta and tracking:
                    _OPTA_TO_TRACKING[opta] = tracking
                    
        _TRACKING_TO_OPTA = {v: k for k, v in _OPTA_TO_TRACKING.items()}

        print("\n--- TRACKING PLAYERS (averagePositionsTracking) ---")
        for tr in tracking_avg_rows[:15]:
            tid = tr.tracking_id
            ro = roster_by_tracking.get(tid)
            if ro:
                opta_id = _TRACKING_TO_OPTA.get(tid, tid)
                print(f"Name: {str(ro['name']):<30} | Raw Tracking ID: {str(tid):<10} | Mapped Opta ID: {str(opta_id):<10}")

except Exception as e:
    print(f"Error: {e}")
