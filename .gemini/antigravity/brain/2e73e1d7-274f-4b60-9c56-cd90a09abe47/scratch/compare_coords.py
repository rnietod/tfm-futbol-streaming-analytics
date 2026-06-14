import sys
import os

# Add root directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        match_id = 'test_match'
        
        # Build a map from short name/jersey number to StatsBomb events
        dorsal_map = {}
        mp_rows = conn.execute(text(
            "SELECT player_id, team_id, name, dorsal FROM match_players WHERE match_id = :mid"
        ), {"mid": match_id}).fetchall()
        
        roster_by_tracking = {}
        for r in mp_rows:
            roster_by_tracking[str(r.player_id)] = {
                "team_id": r.team_id,
                "name": r.name,
                "number": r.dorsal,
            }
            if r.name and r.dorsal:
                dorsal_map[int(r.dorsal)] = r.name

        # Fetch Event-based average positions
        avg_pos_rows = conn.execute(text("""
            SELECT player_name as name, player_id,
                   AVG(location_x) as avg_x, AVG(location_y) as avg_y
            FROM match_events
            WHERE match_id = :mid AND location_x IS NOT NULL AND player_name IS NOT NULL
            GROUP BY player_name, player_id
        """), {"mid": match_id}).fetchall()
        
        # Build eventing map keyed by number (via find_dorsal style mapping)
        def find_dorsal(full_name):
            if not full_name: return None
            fn_lower = full_name.lower()
            # Match number by roster short names
            for r in mp_rows:
                if not r.dorsal: continue
                # Exact or lastName match
                if r.name.lower() in fn_lower or fn_lower in r.name.lower():
                    return int(r.dorsal)
                parts = fn_lower.split()
                for part in parts:
                    if len(part) >= 4 and part in r.name.lower():
                        return int(r.dorsal)
            return None

        event_pos_by_number = {}
        for r in avg_pos_rows:
            num = find_dorsal(r.name)
            if num:
                event_pos_by_number[num] = {
                    "name": r.name,
                    "x": round(float(r.avg_x) / 1.2, 1),
                    "y": round(float(r.avg_y) / 0.8, 1)
                }

        # Fetch Tracking-based average positions
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
        
        team_ids = set(r.team_id for r in mp_rows if r.team_id is not None)
        team_ids_sorted = sorted(team_ids)
        home_team_id = team_ids_sorted[1] if len(team_ids_sorted) >= 2 else team_ids_sorted[0]

        results = {}
        for tr in tracking_avg_rows:
            tid = tr.tracking_id
            period = tr.period
            ro = roster_by_tracking.get(tid)
            if ro:
                team_id = ro.get("team_id")
                
                # Formula A: current mirroring (negates y when negating x)
                if team_id == home_team_id:
                    x_norm_A = -tr.avg_x if period == 2.0 else tr.avg_x
                    y_norm_A = -tr.avg_y if period == 2.0 else tr.avg_y
                else:
                    x_norm_A = tr.avg_x if period == 2.0 else -tr.avg_x
                    y_norm_A = tr.avg_y if period == 2.0 else -tr.avg_y
                    
                # Formula B: never negate y
                if team_id == home_team_id:
                    y_norm_B = tr.avg_y
                else:
                    y_norm_B = tr.avg_y
                
                # Formula C: negate y ALWAYS
                if team_id == home_team_id:
                    y_norm_C = -tr.avg_y
                else:
                    y_norm_C = -tr.avg_y
                
                # Formula D: invert mirroring (y_norm = -y_norm_A)
                # i.e. negate y in Period 1 instead of Period 2, etc.
                if team_id == home_team_id:
                    y_norm_D = tr.avg_y if period == 2.0 else -tr.avg_y
                else:
                    y_norm_D = -tr.avg_y if period == 2.0 else tr.avg_y

                if tid not in results:
                    results[tid] = {
                        "name": ro.get("name"),
                        "number": ro.get("number"),
                        "y_sum_A": 0.0,
                        "y_sum_B": 0.0,
                        "y_sum_C": 0.0,
                        "y_sum_D": 0.0,
                        "count": 0
                    }
                
                results[tid]["y_sum_A"] += y_norm_A * tr.count
                results[tid]["y_sum_B"] += y_norm_B * tr.count
                results[tid]["y_sum_C"] += y_norm_C * tr.count
                results[tid]["y_sum_D"] += y_norm_D * tr.count
                results[tid]["count"] += tr.count

        print(f"{'Player Name':<20} | Num | {'Ev Y':<5} | {'A (Neg in P2/P1)':<16} | {'B (Never Neg)':<14} | {'C (Always Neg)':<14} | {'D (Opposite A)':<14}")
        print("-" * 115)
        for tid, data in results.items():
            num = data["number"]
            if not num: continue
            ev_data = event_pos_by_number.get(int(num))
            if ev_data:
                y_ev = ev_data["y"]
                y_track_A = round(((data["y_sum_A"] / data["count"] + 34.0) / 68.0) * 100, 1)
                y_track_B = round(((data["y_sum_B"] / data["count"] + 34.0) / 68.0) * 100, 1)
                y_track_C = round(((data["y_sum_C"] / data["count"] + 34.0) / 68.0) * 100, 1)
                y_track_D = round(((data["y_sum_D"] / data["count"] + 34.0) / 68.0) * 100, 1)
                print(f"{data['name']:<20} | {num:<3} | {y_ev:<5} | {y_track_A:<16} | {y_track_B:<14} | {y_track_C:<14} | {y_track_D:<14}")

except Exception as e:
    print(f"Error: {e}")
