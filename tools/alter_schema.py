import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
with engine.begin() as conn:
    try:
        conn.execute(text("ALTER TABLE match_events ADD COLUMN xg NUMERIC;"))
        print("Successfully added xg column.")
    except Exception as e:
        print("Column may already exist or error occurred:", e)
