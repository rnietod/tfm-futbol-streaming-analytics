import pandas as pd
import json
import logging
from typing import List, Dict
from thefuzz import process, fuzz  # pip install thefuzz
from unidecode import unidecode   # pip install unidecode
from google.cloud import bigquery

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - ZIZU_LOG - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN ---
PATH_CSV = 'data/eventing_file.csv'
PATH_JSON = 'data/ids_tracking.json'
BQ_TABLE = 'marts_football.fct_events_enriched'
CONFIDENCE_THRESHOLD = 75

# DICCIONARIO DE APODOS (Casos donde el nombre legal no se parece al futbolístico)
ALIAS_DB = {
    "Jorge Resurrección Merodio": "Koke",
    "Rodrigo Hernández Cascante": "Rodri",
    "Francisco Román Alarcón Suárez": "Isco",
    "Alejandro Baena Rodríguez": "Álex Baena",
    # Añadir más según surjan
}

def normalize_text(text: str) -> str:
    """Limpieza: Minusculas, sin acentos."""
    if not isinstance(text, str): return ""
    return unidecode(text.lower().strip())

def load_local_data():
    """Paso 1: Carga fuentes locales."""
    logger.info("📡 [1/4] Leyendo fuentes locales...")

    # 1. Fuente B: Opta (CSV)
    try:
        df_opta = pd.read_csv(PATH_CSV, encoding='utf-8')
        local_teams = df_opta['team_name'].dropna().unique().tolist()
        
        # Seleccionamos columnas y NORMALIZAMOS nombres legales con ALIAS_DB
        df_opta = df_opta[['player_id', 'player_name', 'team_name']].drop_duplicates().dropna()
        
        # APLICAMOS EL ALIAS AQUÍ para facilitar el match
        df_opta['search_name'] = df_opta['player_name'].map(lambda x: ALIAS_DB.get(x, x))
        
        df_opta.rename(columns={'player_id': 'opta_player_id'}, inplace=True)
        logger.info(f"✅ CSV: {len(df_opta)} jugadores.")
    except Exception as e:
        logger.error(f"❌ Error CSV: {e}")
        return None, None, None

    # 2. Fuente C: Tracking (JSON)
    try:
        with open(PATH_JSON, 'r', encoding='utf-8') as f:
            data_tracking = json.load(f)
        
        tracking_players = []
        for p in data_tracking.get('players', []):
            full_name = f"{p.get('first_name', '')} {p.get('last_name', '')}".strip()
            # Priorizamos Short Name, si no Full Name
            p_name = p.get('short_name') or full_name
            
            tracking_players.append({
                'tracking_player_id': p.get('id'),
                'tracking_name': p_name,
                'team_id': p.get('team_id')
            })
        
        df_tracking = pd.DataFrame(tracking_players)
        logger.info(f"✅ JSON: {len(df_tracking)} jugadores tracking.")
    except Exception as e:
        logger.error(f"❌ Error JSON: {e}")
        return None, None, None

    return df_opta, df_tracking, local_teams

def resolve_team_names(local_teams: List[str]) -> List[str]:
    """Paso 2: Resolver Nombres de Equipos (Local -> BQ)."""
    client = bigquery.Client()
    query_teams = f"SELECT DISTINCT team_name FROM `{BQ_TABLE}` WHERE team_name IS NOT NULL"
    
    try:
        df_bq = client.query(query_teams).to_dataframe()
        bq_teams = df_bq['team_name'].tolist()
    except:
        return local_teams

    resolved = []
    for local in local_teams:
        match, score = process.extractOne(local, bq_teams)
        resolved.append(match if score > 85 else local)
    return resolved

def fetch_bq_players(resolved_teams: List[str]) -> pd.DataFrame:
    """Paso 3: Descargar plantilla BQ."""
    if not resolved_teams: return pd.DataFrame()
    teams_sql = "', '".join(resolved_teams)
    
    query = f"""
        SELECT DISTINCT player_id as master_player_id, player_name as bq_name
        FROM `{BQ_TABLE}` WHERE team_name IN ('{teams_sql}')
    """
    try:
        client = bigquery.Client()
        return client.query(query).to_dataframe()
    except:
        return pd.DataFrame(columns=['master_player_id', 'bq_name'])

def smart_match(name, candidates_dict):
    """
    Logica de Matching Mejorada:
    1. Match Exacto (Normalizado)
    2. Token Set Ratio (Subconjuntos: 'Mbappe' en 'Mbappe Lottin')
    """
    if not name or not candidates_dict: return None, 0, None
    
    name_norm = normalize_text(name)
    
    # 1. Exacto
    for cid, cname in candidates_dict.items():
        if normalize_text(cname) == name_norm:
            return cname, 100, cid
            
    # 2. Fuzzy Set (Maneja abreviaciones y nombres extra)
    match = process.extractOne(name, candidates_dict, scorer=fuzz.token_set_ratio)
    if match:
        best_name, score, key = match
        return best_name, score, key
        
    return None, 0, None

def triangulate_identities(df_opta, df_tracking, df_bq):
    """Paso 4: Triangulación con Smart Match."""
    logger.info("🔄 [4/4] Triangulando...")
    
    track_dict = {row['tracking_player_id']: row['tracking_name'] for _, row in df_tracking.iterrows()}
    bq_dict = {row['master_player_id']: row['bq_name'] for _, row in df_bq.iterrows()} if not df_bq.empty else {}
    
    results = []
    
    for _, row in df_opta.iterrows():
        # Usamos 'search_name' (que ya tiene el alias 'Koke') para buscar
        search_name = row['search_name']
        original_name = row['player_name']
        
        # Match Local
        m_name_loc, score_loc, m_id_loc = smart_match(search_name, track_dict)
        
        # Match BQ
        m_name_bq, score_bq, m_id_bq = smart_match(search_name, bq_dict)
        
        # Score Final (Si BQ existe, pondera más)
        final_score = score_bq if score_bq > 0 else score_loc
        
        results.append({
            'master_player_id': m_id_bq,
            'opta_player_id': row['opta_player_id'],
            'tracking_player_id': m_id_loc,
            'player_name_opta': original_name,
            'player_name_normalized': m_name_bq or m_name_loc or original_name,
            'confidence_score': final_score,
            'status': 'MATCHED' if final_score >= CONFIDENCE_THRESHOLD else 'REVIEW'
        })
        
    return pd.DataFrame(results)

def main():
    df_opta, df_tracking, local_teams = load_local_data()
    if df_opta is None: return

    resolved_teams = resolve_team_names(local_teams)
    df_bq = fetch_bq_players(resolved_teams)
    
    df_final = triangulate_identities(df_opta, df_tracking, df_bq)
    
    path = 'data/dim_player_mapping.csv'
    df_final.to_csv(path, index=False)

    print("\n" + "="*60)
    print(f"📊 REPORTE FINAL (Umbral: {CONFIDENCE_THRESHOLD}%)")
    print("="*60)
    
    matched = df_final[df_final['confidence_score'] >= CONFIDENCE_THRESHOLD]
    review = df_final[df_final['confidence_score'] < CONFIDENCE_THRESHOLD]
    
    print(f"✅ Matched: {len(matched)}")
    print(f"⚠️  Review : {len(review)}")
    
    if not review.empty:
        print("\nCasos Pendientes de Revisión:")
        print(review[['player_name_opta', 'player_name_normalized', 'confidence_score']].to_string(index=False))
    else:
        print("\n🎉 ¡PLENO! Todos los jugadores identificados correctamente.")
        print(matched[['player_name_opta', 'player_name_normalized', 'confidence_score']].head().to_string(index=False))

if __name__ == "__main__":
    main()