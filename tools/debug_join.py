import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
with engine.connect() as conn:
    # Check what data we have for player numbers
    r = conn.execute(text("""
        SELECT DISTINCT player_name, player_id
        FROM match_events 
        WHERE match_id='test_match' AND player_name IS NOT NULL
        LIMIT 10
    """)).fetchall()
    print("=== Events player data ===")
    for row in r:
        print(f"  name='{row[0]}', id='{row[1]}'")
    
    # Check match_players for dorsals
    r2 = conn.execute(text("""
        SELECT name, player_id, dorsal
        FROM match_players 
        WHERE match_id='test_match'
        LIMIT 10
    """)).fetchall()
    print("\n=== match_players data ===")
    for row in r2:
        print(f"  name='{row[0]}', id={row[1]}, dorsal={row[2]}")
