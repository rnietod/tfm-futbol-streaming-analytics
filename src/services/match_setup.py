import logging
from google.cloud import bigquery
from sqlalchemy.orm import Session
from src.data.postgres_client import get_db_engine
from src.data.models import MatchActiveProfile, Base

logger = logging.getLogger(__name__)

# Configuración del Dataset en BigQuery (Sácalo de tus configs si prefieres)
# Basado en tu archivo mart_ghost_profile.sqlx, el esquema es 'marts_football'
BQ_DATASET = "marts_football"
BQ_TABLE = "mart_ghost_profile"


def _fetch_full_profiles_from_bigquery(player_ids: list):

    if not player_ids:
        return []

    try:
        # 1. Instanciar Cliente
        client = bigquery.Client()

        # 2. Construir Query
        ids_str = ",".join(f"'{pid}'" for pid in player_ids)

        query = f"""
            SELECT * FROM `{client.project}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE player_id IN ({ids_str})
        """

        logger.info(f"🔭 Ejecutando Query en BigQuery: {query}")
        query_job = client.query(query)

        # 3. Convertir resultados a lista de dicts
        results = []
        for row in query_job:
            # dict(row) convierte la fila completa con todas sus columnas
            row_dict = dict(row)
            results.append(row_dict)

        return results

    except Exception as e:
        logger.error(f"❌ Error crítico conectando a BigQuery: {e}")
        # En caso de fallo de red, podrías decidir si fallar o devolver vacío.
        # Aquí lanzamos el error para que te enteres si la credencial falla.
        raise e


def initialize_match_profiles(match_id: str, lineup_list: list):
    """
    THE BIG INGESTION: Carga Masiva (Real).
    """
    engine = get_db_engine()
    Base.metadata.create_all(engine)

    player_ids = [p['player_id'] for p in lineup_list]
    logger.info(f"🚚 Iniciando Ingestión Real desde BigQuery para {match_id} ({len(player_ids)} jugadores)...")

    # 1. Traer TODO de BigQuery (Real)
    try:
        bq_data = _fetch_full_profiles_from_bigquery(player_ids)
    except Exception as e:
        logger.error("⚠️ Fallo en BigQuery. Abortando inicialización de perfiles.")
        return False

    # Indexamos por ID para acceso rápido
    bq_map = {p['player_id']: p for p in bq_data}

    profiles_to_insert = []

    for player in lineup_list:
        pid = player['player_id']
        role = player.get('role', 'MID')

        # Recuperamos la fila completa.
        full_payload = bq_map.get(pid)

        if not full_payload:
            logger.warning(f"⚠️ Jugador {pid} ({player.get('name')}) no tiene perfil en BigQuery. Usando vacío.")
            full_payload = {}

        # Extraemos un valor clave para indexar (ejemplo: xg_p30)
        # Ajusta esto según el nombre real de tu columna en BQ.
        # En tu SQLX vi que se llaman 'w_xg_p30'.
        xg_ref = full_payload.get("w_xg_p30", 0.0)

        profile = MatchActiveProfile(
            match_id=match_id,
            player_id=pid,
            player_name=player.get('name', 'Unknown'),
            position_group=role, 
            
            # 💾 GUARDADO DEL DUMP COMPLETO
            full_profile_payload=full_payload,
            
            # Columna indexada para búsquedas rápidas
            xg_p30_ref=float(xg_ref) if xg_ref else 0.0
        )
        profiles_to_insert.append(profile)

    # 2. Transacción Atómica en Postgres
    with Session(engine) as session:
        try:
            # Limpiamos datos previos de este partido para evitar duplicados
            session.query(MatchActiveProfile).filter(MatchActiveProfile.match_id == match_id).delete()

            session.add_all(profiles_to_insert)
            session.commit()
            logger.info(f"✅ CARGA COMPLETA: {len(profiles_to_insert)} perfiles reales de BigQuery persistidos en Postgres.")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Error guardando en Postgres: {e}")
            return False
