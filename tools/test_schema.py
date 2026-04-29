from src.data.postgres_client import get_db_engine
from sqlalchemy import text
engine = get_db_engine()
with engine.connect() as conn:
    res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='match_events'")).fetchall()
    print([r[0] for r in res])
