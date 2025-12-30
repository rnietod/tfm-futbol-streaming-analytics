import os
import json
import logging
import sqlalchemy
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rutas dinámicas para encontrar el config
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(BASE_DIR, 'configs/dev.json')

def get_db_engine():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config no encontrado: {CONFIG_PATH}")
        
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    
    # Leemos la config local
    db_conf = config.get('cloudsql', {})
    user = os.getenv('DB_USER', db_conf.get('db_user', 'postgres'))
    password = os.getenv('DB_PASS', db_conf.get('db_pass', 'admin123'))
    host = os.getenv('DB_HOST', db_conf.get('db_host', 'localhost'))
    dbname = os.getenv('DB_NAME', db_conf.get('db_name', 'tactix_db'))
    port = os.getenv('DB_PORT', "5432")

    # URL Estándar de PostgreSQL (sin drivers de Google)
    db_url = f"postgresql+pg8000://{user}:{password}@{host}:{port}/{dbname}"
    
    try:
        engine = sqlalchemy.create_engine(db_url, pool_pre_ping=True)
        return engine
    except Exception as e:
        logger.error(f"❌ Error creando engine: {e}")
        raise e

def test_connection():
    try:
        engine = get_db_engine()
        with engine.connect() as conn:
            res = conn.execute(text("SELECT '🟢 CONECTADO A DOCKER LOCAL'")).scalar()
            print(res)
    except Exception as e:
        print(f"❌ FALLO DE CONEXIÓN: {e}")

if __name__ == "__main__":
    test_connection()