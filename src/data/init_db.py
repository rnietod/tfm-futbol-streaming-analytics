from sqlalchemy import text
from postgres_client import get_db_engine
import logging

logging.basicConfig(level=logging.INFO)

def init_tables():
    engine = get_db_engine()
    
    ddl = """
    -- 1. PARTIDOS (Metadata)
    CREATE TABLE IF NOT EXISTS matches (
        match_id VARCHAR(50) PRIMARY KEY,
        home_team_id VARCHAR(50),
        home_team_name VARCHAR(100),
        away_team_id VARCHAR(50),
        away_team_name VARCHAR(100),
        match_date TIMESTAMP,
        stadium VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. JUGADORES (Alineación)
    CREATE TABLE IF NOT EXISTS match_players (
        match_id VARCHAR(50),
        player_id VARCHAR(50),
        team_id VARCHAR(50),
        name VARCHAR(100),
        dorsal INTEGER,
        position VARCHAR(10),
        PRIMARY KEY (match_id, player_id)
    );

    -- 3. EVENTOS (Analytics)
    CREATE TABLE IF NOT EXISTS match_events (
        event_uuid VARCHAR(50) PRIMARY KEY,
        match_id VARCHAR(50),
        timestamp TIME,
        period INTEGER,
        minute INTEGER,
        second INTEGER,
        
        -- IDs Críticos para el Frontend
        event_type_id INTEGER,
        event_type_name VARCHAR(100),
        type_id INTEGER,
        type_name VARCHAR(100),
        outcome_id INTEGER,
        outcome_name VARCHAR(50),
        
        -- Datos de Jugador/Equipo
        team_id VARCHAR(50),
        team_name VARCHAR(100),
        player_id VARCHAR(50),
        player_name VARCHAR(100),
        
        -- Coordenadas
        location_x FLOAT, location_y FLOAT,
        end_location_x FLOAT, end_location_y FLOAT, end_location_z FLOAT,
        
        -- Detalles de Pase (Opcionales)
        pass_length FLOAT,
        pass_angle FLOAT,
        pass_recipient_name VARCHAR(100),
        pass_height_name VARCHAR(50),
        body_part_name VARCHAR(50),
        
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 4. TRACKING (Bulk Data - JSONB)
    CREATE TABLE IF NOT EXISTS match_tracking (
        tracking_id SERIAL PRIMARY KEY,
        match_id VARCHAR(50),
        frame_idx INTEGER,
        timestamp TIMESTAMP,
        players_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_tracking_match ON match_tracking(match_id, frame_idx);
    """
    
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        print("✅ Tablas creadas en Docker Local.")

if __name__ == "__main__":
    init_tables()