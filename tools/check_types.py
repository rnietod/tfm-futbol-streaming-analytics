import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
with engine.connect() as conn:
    r = conn.execute(text(
        "SELECT table_name, column_name, data_type FROM information_schema.columns "
        "WHERE table_name IN ('match_events','match_players') "
        "AND column_name IN ('player_id','match_id','event_type_id','outcome_id','type_id') "
        "ORDER BY table_name, column_name"
    )).fetchall()
    for row in r:
        print(f"{row[0]:20s} {row[1]:20s} {row[2]}")
