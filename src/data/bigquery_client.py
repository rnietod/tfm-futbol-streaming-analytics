
import os
import json
import logging
from typing import List, Optional
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rutas dinámicas para encontrar credenciales
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CREDENTIALS_PATH = os.path.join(BASE_DIR, '.df-credentials.json')

class BigQueryClient:
    def __init__(self):
        self._client = None
        self._project_id = None
        self._location = None
        self._load_config()
        self._init_client()

    def _load_config(self):
        """Carga projectId/location desde env (BQ_PROJECT_ID/BQ_LOCATION) o, como
        fallback para dev local, desde .df-credentials.json. La autenticación
        sigue siendo por ADC (gcloud auth / GOOGLE_APPLICATION_CREDENTIALS)."""
        self._project_id = os.getenv("BQ_PROJECT_ID")
        self._location = os.getenv("BQ_LOCATION")

        if not self._project_id and os.path.exists(CREDENTIALS_PATH):
            try:
                with open(CREDENTIALS_PATH, 'r') as f:
                    config = json.load(f)
                self._project_id = self._project_id or config.get('projectId')
                self._location = self._location or config.get('location')
            except Exception as e:
                logger.error(f"Error cargando configuración de BigQuery: {e}")
                raise

        if not self._project_id:
            raise ValueError(
                "Falta projectId: define BQ_PROJECT_ID (env) o crea .df-credentials.json"
            )

    def _init_client(self):
        """Inicializa el cliente de BigQuery usando ADC"""
        try:
            # Se asume que el usuario tiene credenciales configuradas (gcloud auth o GOOGLE_APPLICATION_CREDENTIALS)
            self._client = bigquery.Client(project=self._project_id, location=self._location)
            logger.info(f"Cliente BigQuery inicializado para proyecto: {self._project_id}")
        except Exception as e:
            logger.error(f"Error inicializando cliente BigQuery: {e}")
            raise

    def query(self, sql: str, params: Optional[list] = None,
              max_bytes: Optional[int] = 8_000_000_000):
        """
        Ejecuta una consulta arbitraria y devuelve (rows, bytes_billed).

        - `params`: lista de bigquery.ScalarQueryParameter/ArrayQueryParameter (opcional).
        - `max_bytes`: tope de bytes facturables como red de seguridad de coste
          (las consultas del DOFA filtran por team_name y escanean ~5-6 GB).

        Devuelve `(list[dict], int)` con las filas materializadas y los bytes facturados,
        para poder loggear el coste de cada consulta.
        """
        job_config = bigquery.QueryJobConfig()
        if params:
            job_config.query_parameters = params
        if max_bytes:
            job_config.maximum_bytes_billed = max_bytes

        job = self._client.query(sql, job_config=job_config)
        rows = [dict(r) for r in job.result()]
        return rows, (job.total_bytes_billed or 0)

    def get_player_season_profile(self, player_ids: List[str]) -> pd.DataFrame:
        """
        Obtiene el perfil de rendimiento para una lista de jugadores.
        Tabla: tfm-master-futbol.marts_football.fct_player_season_profile
        """
        if not player_ids:
            return pd.DataFrame()

        query = """
            SELECT *
            FROM `tfm-master-futbol.marts_football.fct_player_season_profile`
            WHERE player_id IN UNNEST(@player_ids)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("player_ids", "STRING", player_ids)
            ]
        )

        try:
            df = self._client.query(query, job_config=job_config).to_dataframe()
            return df
        except Exception as e:
            logger.error(f"Error consultando fct_player_season_profile: {e}")
            raise

    def get_ghost_profile(self, player_ids: List[str]) -> pd.DataFrame:
        """
        Obtiene el perfil fantasma (benchmarking) para una lista de jugadores.
        Tabla: tfm-master-futbol.marts_football.mart_ghost_profile
        """
        if not player_ids:
            return pd.DataFrame()

        query = """
            SELECT *
            FROM `tfm-master-futbol.marts_football.mart_ghost_profile`
            WHERE player_id IN UNNEST(@player_ids)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("player_ids", "STRING", player_ids)
            ]
        )

        try:
            df = self._client.query(query, job_config=job_config).to_dataframe()
            return df
        except Exception as e:
            logger.error(f"Error consultando mart_ghost_profile: {e}")
            raise
