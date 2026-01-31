import sys
import os
import logging
from sqlalchemy.orm import Session
from src.data.postgres_client import get_db_engine
from src.data.models import MatchActiveProfile, Base
from src.services.match_setup import initialize_match_profiles

# Ajuste de path para que encuentre los módulos 'src'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configuración de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_full_ghost_flow():
    """
    Prueba End-to-End:
    1. Ingestión (BQ -> Postgres)
    2. Verificación de Datos (SQLAlchemy)
    """
    print("\n🧪 --- INICIANDO TEST DE INTEGRACIÓN GHOST ENGINE ---")

    # 1. DATOS DE PRUEBA
    TEST_MATCH_ID = "test_ghost_ci_cd"

    TEST_LINEUP = [
        # Real Madrid
        {"player_id": 12253, "name": "Vinícius Jr", "role": "FW"},
        {"player_id": 24583, "name": "Jude Bellingham", "role": "MID"},
        # Atlético
        {"player_id": 946, "name": "Griezmann", "role": "FW"},
        {"player_id": 4713, "name": "Jan Oblak", "role": "GK"}
    ]

    # 2. PROBAR CONEXIÓN A BIGQUERY E INGESTIÓN
    print(f"\n📡 1. Ejecutando Ingestión desde BigQuery para {TEST_MATCH_ID}...")
    try:
        success = initialize_match_profiles(TEST_MATCH_ID, TEST_LINEUP)
        if success:
            print("✅ Ingestión reportada como EXITOSA.")
        else:
            print("❌ La ingestión falló (Revisa credenciales de Google o logs arriba).")
            return
    except Exception as e:
        print(f"❌ Excepción crítica durante ingestión: {e}")
        return

    # 3. VERIFICAR PERSISTENCIA EN POSTGRES
    print("\n🔍 2. Verificando datos en PostgreSQL (JSONB)...")
    engine = get_db_engine()

    with Session(engine) as session:
        profiles = session.query(MatchActiveProfile).filter_by(match_id=TEST_MATCH_ID).all()
        
        if not profiles:
            print("❌ ERROR: No se encontraron perfiles en la base de datos.")
            return

        print(f"✅ Se encontraron {len(profiles)} perfiles en DB.")

        for p in profiles:
            print(f"   👤 Jugador: {p.player_name} (ID: {p.player_id})")
            print(f"      📦 Payload Size: {len(str(p.full_profile_payload))} chars")

            # Verificamos si trajo algo dentro del JSON
            if p.full_profile_payload and len(p.full_profile_payload) > 0:
                print("      ✅ JSONB contiene datos.")
                # Opcional: Imprimir una clave para verificar
                # print(f"      Datos: {p.full_profile_payload.keys()}")
            else:
                print("      ⚠️ JSONB está vacío (¿El jugador no existía en BQ?)")

    print("\n🎉 --- TEST DE INTEGRACIÓN COMPLETADO ---")


if __name__ == "__main__":
    # Asegúrate de tener las credenciales de Google configuradas en tu entorno
    # export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
    test_full_ghost_flow()
