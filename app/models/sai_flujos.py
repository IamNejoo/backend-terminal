# app/models/sai_flujos.py
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text, Index, Time
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.models.base import Base

class SAIConfiguration(Base):
    """Configuración de datos SAI cargados"""
    __tablename__ = "sai_configurations"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    fecha = Column(DateTime, nullable=False, index=True)
    semana = Column(Integer, nullable=False, index=True)
    participacion = Column(Integer, nullable=False, default=68)
    con_dispersion = Column(Boolean, nullable=False, default=True)
    fecha_carga = Column(DateTime, default=datetime.utcnow)
    
    # Relaciones
    flujos = relationship("SAIFlujo", back_populates="configuration", cascade="all, delete-orphan")
    volumen_bloques = relationship("SAIVolumenBloque", back_populates="configuration", cascade="all, delete-orphan")
    volumen_segregaciones = relationship("SAIVolumenSegregacion", back_populates="configuration", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_sai_config_fecha_semana', 'fecha', 'semana'),
    )

class SAIFlujo(Base):
    """Flujos originales del archivo"""
    __tablename__ = "sai_flujos"
    
    id = Column(Integer, primary_key=True)
    config_id = Column(UUID(as_uuid=True), ForeignKey("sai_configurations.id"), nullable=False)
    
    # Datos temporales - ACTUALIZADO
    ime_time = Column(DateTime, nullable=False, index=True)  # Fecha y hora completa
    hora_exacta = Column(Time, nullable=False, index=True)   # Solo la hora (HH:MM:SS)
    turno = Column(Integer, nullable=False, index=True)       # 1, 2, 3
    hora_turno = Column(String(10))                           # "08-00", "15-30", "23-00"
    
    # Datos de movimiento
    ime_fm = Column(String(20))
    ime_to = Column(String(20))
    ime_move_kind = Column(String(10))
    
    # Criterios (mapeo con segregaciones)
    criterio_i = Column(String(100))
    criterio_ii = Column(String(100), index=True)  # Principal para mapeo
    criterio_iii = Column(String(100))
    
    # Datos adicionales
    iu_category = Column(String(20))
    ig_hazardous = Column(Boolean, default=False)
    iu_requires_power = Column(Boolean, default=False)
    
    configuration = relationship("SAIConfiguration", back_populates="flujos")
    
    __table_args__ = (
        Index('idx_sai_flujos_bloque_criterio', 'ime_to', 'criterio_ii'),
        Index('idx_sai_flujos_tiempo', 'ime_time', 'turno'),
        Index('idx_sai_flujos_hora_exacta', 'hora_exacta'),
    )

class SAIVolumenBloque(Base):
    """Volumen por bloque y turno"""
    __tablename__ = "sai_volumen_bloques"
    
    id = Column(Integer, primary_key=True)
    config_id = Column(UUID(as_uuid=True), ForeignKey("sai_configurations.id"), nullable=False)
    
    fecha = Column(DateTime, nullable=False)
    turno = Column(Integer, nullable=False)
    hora_turno = Column(String(10))  # Agregado para consistencia
    
    # Volumen por bloque en TEUs - ACTUALIZADO CON TODOS LOS BLOQUES
    c1 = Column(Integer, default=0)
    c2 = Column(Integer, default=0)
    c3 = Column(Integer, default=0)
    c4 = Column(Integer, default=0)
    c5 = Column(Integer, default=0)
    c6 = Column(Integer, default=0)
    c7 = Column(Integer, default=0)
    c8 = Column(Integer, default=0)
    c9 = Column(Integer, default=0)
    h1 = Column(Integer, default=0)
    h2 = Column(Integer, default=0)
    h3 = Column(Integer, default=0)
    h4 = Column(Integer, default=0)
    h5 = Column(Integer, default=0)
    t1 = Column(Integer, default=0)
    t2 = Column(Integer, default=0)
    t3 = Column(Integer, default=0)
    t4 = Column(Integer, default=0)
    
    configuration = relationship("SAIConfiguration", back_populates="volumen_bloques")

class SAIVolumenSegregacion(Base):
    """Volumen por bloque, segregación y turno"""
    __tablename__ = "sai_volumen_segregaciones"
    
    id = Column(Integer, primary_key=True)
    config_id = Column(UUID(as_uuid=True), ForeignKey("sai_configurations.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    segregacion_id = Column(String(10), nullable=False, index=True)  # S1, S2, etc
    segregacion_nombre = Column(String(100))
    
    # Volumen por turno
    turno_1 = Column(Integer, default=0)
    turno_2 = Column(Integer, default=0)
    turno_3 = Column(Integer, default=0)
    total = Column(Integer, default=0)
    
    configuration = relationship("SAIConfiguration", back_populates="volumen_segregaciones")
    
    __table_args__ = (
        Index('idx_sai_vol_seg_bloque_seg', 'bloque', 'segregacion_id'),
    )

class SAISegregacion(Base):
    """Catálogo de segregaciones"""
    __tablename__ = "sai_segregaciones"
    
    id = Column(String(10), primary_key=True)  # S1, S2, etc
    nombre = Column(String(100), nullable=False, unique=True)
    teus = Column(Integer, nullable=False)  # 1 o 2
    tipo = Column(String(10))  # '20' o '40'
    categoria = Column(String(20))  # 'dry', 'reefer'
    direccion = Column(String(10))  # 'impo', 'expo'
    color = Column(String(7))  # Hex color
    
    fecha_carga = Column(DateTime, default=datetime.utcnow)

class SAICapacidadBloque(Base):
    """Capacidades de bloques"""
    __tablename__ = "sai_capacidades_bloques"
    
    bloque = Column(String(10), primary_key=True)
    capacidad_teus = Column(Integer, nullable=False)
    capacidad_contenedores = Column(Integer, nullable=False)
    bahias_totales = Column(Integer, nullable=False)
    bahias_reefer = Column(Integer, default=0)
    contenedores_por_bahia = Column(Integer, nullable=False)  # VS_b
    
    fecha_carga = Column(DateTime, default=datetime.utcnow)

class SAIMapeoCriterios(Base):
    """Mapeo entre criterios y segregaciones"""
    __tablename__ = "sai_mapeo_criterios"
    
    id = Column(Integer, primary_key=True)
    criterio = Column(String(100), nullable=False, unique=True, index=True)
    segregacion_id = Column(String(10), ForeignKey("sai_segregaciones.id"), nullable=False)
    
    # Para análisis
    frecuencia_uso = Column(Integer, default=0)
    fecha_ultimo_uso = Column(DateTime)
    
    segregacion = relationship("SAISegregacion")