# app/models/container_position.py
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Date, Index
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

from app.models.base import Base

class ContainerPosition(Base):
    __tablename__ = "container_positions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Información temporal
    fecha = Column(Date, nullable=False, index=True)
    turno = Column(Integer, nullable=False, index=True)  # 1, 2, 3
    semana_iso = Column(String(10), nullable=False, index=True)  # 2022-01-03
    
    # Datos del contenedor
    gkey = Column(String(50), nullable=False, index=True)
    posicion = Column(String(20), nullable=False, index=True)  # C455D5
    
    # Desglose de posición para facilitar búsquedas
    patio = Column(String(5), nullable=False, index=True)      # C
    bloque = Column(String(5), nullable=False, index=True)     # 4
    bahia = Column(Integer, nullable=False, index=True)        # 55
    fila = Column(String(1), nullable=False, index=True)       # D
    tier = Column(Integer, nullable=False, index=True)         # 5
    
    # Atributos del contenedor
    category = Column(String(10), nullable=False, index=True)  # IMPRT/EXPRT/STRGE
    tiempo_permanencia = Column(Integer, nullable=True)
    requires_power = Column(Boolean, default=False)
    nominal_length = Column(Integer, nullable=False)  # 20 o 40
    hazardous = Column(Boolean, default=False)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    # Índice único para evitar duplicados
    __table_args__ = (
        Index('idx_container_position_unique', 'fecha', 'turno', 'gkey', unique=True),
        Index('idx_container_position_bloque_fecha', 'bloque', 'fecha', 'turno'),
        Index('idx_container_position_patio_fecha', 'patio', 'fecha', 'turno'),
        {'postgresql_tablespace': 'pg_default'}
    )