
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
        """Carga la configuración del proyecto desde .df-credentials.json"""
        if not os.path.exists(CREDENTIALS_PATH):
            raise FileNotFoundError(f"Archivo de credenciales no encontrado: {CREDENTIALS_PATH}")
        
        try:
            with open(CREDENTIALS_PATH, 'r') as f:
                config = json.load(f)
                self._project_id = config.get('projectId')
                self._location = config.get('location')
                
            if not self._project_id:
                raise ValueError("projectId no encontrado en .df-credentials.json")
                
        except Exception as e:
            logger.error(f"Error cargando configuración de BigQuery: {e}")
            raise

    def _init_client(self):
        """Inicializa el cliente de BigQuery usando ADC"""
        try:
            # Se asume que el usuario tiene credenciales configuradas (gcloud auth o GOOGLE_APPLICATION_CREDENTIALS)
            self._client = bigquery.Client(project=self._project_id, location=self._location)
            logger.info(f"Cliente BigQuery inicializado para proyecto: {self._project_id}")
        except Exception as e:
            logger.error(f"Error inicializando cliente BigQuery: {e}")
            raise

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

    def predict_xg(self, distance: float, angle: float, body_part: str, play_pattern: str) -> float:
        """
        Predice el xG de un tiro usando el modelo ML.PREDICT de BigQuery.
        """
        query = f"""
            SELECT predicted_is_goal_probs
            FROM ML.PREDICT(
                MODEL `tfm-master-futbol.ml_football.model_xg`,
                (SELECT 
                    {distance} AS distance_to_goal, 
                    {angle} AS angle_visible, 
                    '{body_part}' AS body_part, 
                    '{play_pattern}' AS play_pattern
                )
            )
        """
        try:
            df = self._client.query(query).to_dataframe()
            if not df.empty:
                # ML.PREDICT retorna un array de structs 'predicted_is_goal_probs' con 'label' y 'prob'
                probs = df.iloc[0]['predicted_is_goal_probs']
                for p in probs:
                    if p['label'] == 1:
                        return float(p['prob'])
            return 0.0
        except Exception as e:
            logger.error(f"Error prediciendo xG: {e}")
            return 0.0
