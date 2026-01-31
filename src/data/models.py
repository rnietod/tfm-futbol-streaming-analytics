# src/data/models.py
from sqlalchemy import Column, String, Integer, Float, JSON, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class MatchActiveProfile(Base):
    """
    LA EXPECTATIVA (The Ghost):
    Ahora con capacidad ilimitada gracias a JSONB.
    """
    __tablename__ = 'match_active_profiles'

    match_id = Column(String, primary_key=True)
    player_id = Column(Integer, primary_key=True)
    player_name = Column(String)
    position_group = Column(String)
    
    # 🌟 LA JOYA DE LA CORONA:
    full_profile_payload = Column(JSON, default={}) 
    
    # Índices Tácticos (Opcional: Mantenemos solo los drivers principales para búsquedas rápidas SQL)
    xg_p30_ref = Column(Float, default=0.0) 
    
    created_at = Column(DateTime, default=datetime.utcnow)

class MatchLiveStats(Base):
    """
    LA REALIDAD (The Live Snapshot):
    Historial minuto a minuto.
    """
    __tablename__ = 'match_live_stats'

    match_id = Column(String, primary_key=True)
    player_id = Column(Integer, primary_key=True)
    minute = Column(Integer, primary_key=True)
    
    # Acumulados
    shots_accum = Column(Integer, default=0)
    passes_accum = Column(Integer, default=0)
    goals_accum = Column(Integer, default=0)
    recoveries_accum = Column(Integer, default=0)
    distance_accum = Column(Float, default=0.0)
    xg_accum = Column(Float, default=0.0)
    
    # Score instantáneo del Ghost Engine (para gráficas de evolución)
    ghost_score = Column(Float, nullable=True)
    
    timestamp = Column(DateTime, default=datetime.utcnow)