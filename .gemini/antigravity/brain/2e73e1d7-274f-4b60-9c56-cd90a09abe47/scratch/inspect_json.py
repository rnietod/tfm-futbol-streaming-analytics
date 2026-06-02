import sys
import os
import json

# Añadir el directorio raíz al path para poder importar src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..')))

from src.data.postgres_client import get_db_engine
from sqlalchemy import text

engine = get_db_engine()
try:
    with engine.connect() as conn:
        sample = conn.execute(text("""
            SELECT players_data FROM match_tracking LIMIT 1
        """)).fetchone()
        if sample:
            data = sample[0]
            print("Estructura de players_data:")
            print(json.dumps(data, indent=2)[:2000]) # imprimir los primeros 2000 caracteres
        else:
            print("No data found")
except Exception as e:
    print(f"Error: {e}")
