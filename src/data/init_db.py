from sqlalchemy import text
from postgres_client import get_db_engine
import logging

logging.basicConfig(level=logging.INFO)

def init_tables():
    engine = get_db_engine()
    
    ddl = """
    DROP TABLE IF EXISTS match_tracking CASCADE;
    DROP TABLE IF EXISTS match_events CASCADE;
    DROP TABLE IF EXISTS match_players CASCADE;
    DROP TABLE IF EXISTS matches CASCADE;
    DROP TABLE IF EXISTS player_season_profile CASCADE;
    DROP TABLE IF EXISTS player_ghost_profile CASCADE;
    DROP TABLE IF EXISTS player_live_projection CASCADE;

    -- 1. PARTIDOS (Metadata)
    CREATE TABLE IF NOT EXISTS matches (
        match_id VARCHAR(50) PRIMARY KEY,
        home_team_id INTEGER,
        home_team_name VARCHAR(100),
        home_team_acronym VARCHAR(10),
        away_team_id INTEGER,
        away_team_name VARCHAR(100),
        away_team_acronym VARCHAR(10),
        match_date TIMESTAMP,
        stadium VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. JUGADORES (Alineación)
    CREATE TABLE IF NOT EXISTS match_players (
        match_id VARCHAR(50),
        player_id INTEGER,
        team_id INTEGER,
        name VARCHAR(100),
        dorsal INTEGER,
        position VARCHAR(10),
        PRIMARY KEY (match_id, player_id)
    );

    -- 3. EVENTOS (Analytics)
    CREATE TABLE IF NOT EXISTS match_events (
        event_uuid VARCHAR(50) PRIMARY KEY,
        event_index INTEGER,
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

    -- 5. SEASON PROFILE (BigQuery Cache)
    CREATE TABLE IF NOT EXISTS player_season_profile (
        master_player_id VARCHAR(50),
        tracking_player_id INTEGER,
        opta_player_id INTEGER,
        player_name VARCHAR(100),
        season VARCHAR(50),
        team_id VARCHAR(50),
        game_state VARCHAR(50),
        matches_played INTEGER,
        last_match_date TIMESTAMP,
        goals INTEGER,
        shots_total INTEGER,
        shots_on_target INTEGER,
        shots_blocked INTEGER,
        total_xg FLOAT,
        shots_inside_box INTEGER,
        shots_outside_box INTEGER,
        passes_total INTEGER,
        passes_completed INTEGER,
        passes_vertical INTEGER,
        passes_horizontal INTEGER,
        passes_short INTEGER,
        passes_long INTEGER,
        key_passes INTEGER,
        assists INTEGER,
        crosses INTEGER,
        dribbles_attempted INTEGER,
        dribbles_completed INTEGER,
        dispossessed INTEGER,
        interceptions INTEGER,
        tackles INTEGER,
        recoveries INTEGER,
        fouls_committed INTEGER,
        minutes_played INTEGER,
        shot_accuracy_pct FLOAT,
        pass_accuracy_pct FLOAT,
        dribble_success_pct FLOAT,
        goals_p30 FLOAT,
        xg_p30 FLOAT,
        shots_p30 FLOAT,
        passes_vertical_p30 FLOAT,
        key_passes_p30 FLOAT,
        interceptions_p30 FLOAT,
        recoveries_p30 FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (master_player_id, season, team_id, game_state)
    );

    -- 6. GHOST PROFILE (BigQuery Benchmarking Cache)
    CREATE TABLE IF NOT EXISTS player_ghost_profile (
        master_player_id VARCHAR(50),
        game_state VARCHAR(50),
        tracking_player_id INTEGER,
        opta_player_id INTEGER,
        player_name VARCHAR(100),
        current_team_id VARCHAR(50),
        player_type VARCHAR(50),
        w_goals_p30 FLOAT,
        w_xg_p30 FLOAT,
        w_shots_p30 FLOAT,
        w_key_passes_p30 FLOAT,
        w_progressive_p30 FLOAT,
        w_dribble_pct FLOAT,
        w_interceptions_p30 FLOAT,
        w_recoveries_p30 FLOAT,
        w_saves_vol FLOAT,
        w_reflex_saves FLOAT,
        w_long_dist_share FLOAT,
        w_dist_accuracy FLOAT,
        pct_xg FLOAT,
        pct_goals FLOAT,
        pct_shots FLOAT,
        pct_creation FLOAT,
        pct_progression FLOAT,
        pct_defense FLOAT,
        pct_workrate FLOAT,
        pct_saves FLOAT,
        pct_distribution FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (master_player_id, game_state)
    );
    -- 7. VISIÓN EN VIVO (PostgreSQL Projection)
    CREATE TABLE IF NOT EXISTS player_live_projection (
        match_id VARCHAR(50),
        player_id INTEGER,
        minutes_played FLOAT,
        proj_pct_shots FLOAT,
        proj_pct_creation FLOAT,
        proj_pct_progression FLOAT,
        proj_pct_defense FLOAT,
        proj_pct_workrate FLOAT,
        proj_pct_saves FLOAT,
        proj_pct_distribution FLOAT,
        deviation FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (match_id, player_id)
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(ddl))
        conn.commit()
        print("Tablas creadas en Docker Local.")

if __name__ == "__main__":
    init_tables()