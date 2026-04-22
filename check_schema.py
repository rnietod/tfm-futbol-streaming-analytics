import sys
sys.path.append('src')
from data.postgres_client import get_db_engine
from sqlalchemy import text
engine = get_db_engine()
conn = engine.connect()
res = conn.execute(text("SELECT game_state, pct_goals, pct_shots, pct_xg, pct_creation, pct_progression, pct_defense FROM player_ghost_profile WHERE tracking_player_id = 24955")).fetchall()
for r in res:
    print(r)
