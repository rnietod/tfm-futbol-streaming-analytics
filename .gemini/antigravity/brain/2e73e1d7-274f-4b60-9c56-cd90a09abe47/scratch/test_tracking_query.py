import sys
import os

# Añadir el directorio raíz al path para poder importar src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        query = text("""
            WITH extracted AS (
                SELECT 
                    (players_data->>'period')::float as period,
                    (elem->>'player_id')::text as tracking_id,
                    (elem->>'x')::float as x,
                    (elem->>'y')::float as y
                FROM match_tracking,
                LATERAL jsonb_array_elements(players_data->'player_data') as elem
                WHERE match_id = 'test_match'
            )
            SELECT 
                tracking_id,
                AVG(CASE WHEN period = 2.0 THEN -x ELSE x END) as avg_x,
                AVG(CASE WHEN period = 2.0 THEN -y ELSE y END) as avg_y,
                COUNT(*) as count
            FROM extracted
            WHERE tracking_id IS NOT NULL AND x IS NOT NULL AND y IS NOT NULL
            GROUP BY tracking_id
            LIMIT 10
        """)
        rows = conn.execute(query).fetchall()
        print("Promedios de Tracking (primeros 10):")
        for r in rows:
            print(f"Player ID: {r.tracking_id} | avg_x: {r.avg_x:.2f} | avg_y: {r.avg_y:.2f} | count: {r.count}")
except Exception as e:
    print(f"Error: {e}")
