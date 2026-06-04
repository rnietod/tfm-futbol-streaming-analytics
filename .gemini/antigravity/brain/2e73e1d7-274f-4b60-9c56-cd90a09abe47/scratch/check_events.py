import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        res = conn.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(player_id) as non_null_ids,
                COUNT(player_name) as non_null_names
            FROM match_events
            WHERE match_id = 'test_match'
        """)).fetchone()
        print(f"Total events: {res.total} | Non-null player_ids: {res.non_null_ids} | Non-null player_names: {res.non_null_names}")
        
        sample = conn.execute(text("""
            SELECT player_id, player_name 
            FROM match_events 
            WHERE match_id = 'test_match' AND player_name IS NOT NULL
            LIMIT 10
        """)).fetchall()
        for s in sample:
            print(f"  player_id: {s.player_id} (type: {type(s.player_id)}) | player_name: {s.player_name}")
except Exception as e:
    print(f"Error: {e}")
