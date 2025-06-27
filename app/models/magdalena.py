# app/models/magdalena.py
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.models.base import Base

class MagdalenaRun(Base):
    __tablename__ = "magdalena_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    semana = Column(Integer, nullable=False, index=True)
    participacion = Column(Integer, nullable=False, index=True)
    con_dispersion = Column(Boolean, nullable=False, index=True)
    fecha_carga = Column(DateTime, default=datetime.utcnow)
    
    # Metadatos
    total_movimientos = Column(Integer, default=0)
    total_bloques = Column(Integer, default=0)
    total_segregaciones = Column(Integer, default=0)
    periodos = Column(Integer, default=0)
    
    # Relaciones
    general_data = relationship("MagdalenaGeneral", back_populates="run", cascade="all, delete-orphan")
    ocupacion_data = relationship("MagdalenaOcupacion", back_populates="run", cascade="all, delete-orphan")
    workload_data = relationship("MagdalenaWorkload", back_populates="run", cascade="all, delete-orphan")
    bahias_data = relationship("MagdalenaBahias", back_populates="run", cascade="all, delete-orphan")
    volumen_data = relationship("MagdalenaVolumen", back_populates="run", cascade="all, delete-orphan")
    
    __table_args__ = (
        {'postgresql_tablespace': 'pg_default'}
    )

class MagdalenaGeneral(Base):
    __tablename__ = "magdalena_general"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("magdalena_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    periodo = Column(Integer, nullable=False, index=True)
    segregacion = Column(String(50), nullable=False)
    
    recepcion = Column(Integer, default=0)
    carga = Column(Integer, default=0)
    descarga = Column(Integer, default=0)
    entrega = Column(Integer, default=0)
    
    run = relationship("MagdalenaRun", back_populates="general_data")

class MagdalenaOcupacion(Base):
    __tablename__ = "magdalena_ocupacion"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("magdalena_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    periodo = Column(Integer, nullable=False, index=True)
    volumen_teus = Column(Float, default=0)
    capacidad_bloque = Column(Float, default=0)
    
    run = relationship("MagdalenaRun", back_populates="ocupacion_data")

class MagdalenaWorkload(Base):
    __tablename__ = "magdalena_workload"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("magdalena_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    periodo = Column(Integer, nullable=False, index=True)
    carga_trabajo = Column(Float, default=0)
    
    run = relationship("MagdalenaRun", back_populates="workload_data")

class MagdalenaBahias(Base):
    __tablename__ = "magdalena_bahias"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("magdalena_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    periodo = Column(Integer, nullable=False, index=True)
    segregacion = Column(String(50), nullable=False, index=True)
    bahias_ocupadas = Column(Integer, default=0)
    
    run = relationship("MagdalenaRun", back_populates="bahias_data")

class MagdalenaVolumen(Base):
    __tablename__ = "magdalena_volumen"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("magdalena_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False, index=True)
    periodo = Column(Integer, nullable=False, index=True)
    segregacion = Column(String(50), nullable=False, index=True)
    volumen = Column(Float, default=0)
    
    run = relationship("MagdalenaRun", back_populates="volumen_data")

# Tablas para datos de instancia
class MagdalenaInstancia(Base):
    __tablename__ = "magdalena_instancia"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    semana = Column(Integer, nullable=False, index=True)
    participacion = Column(Integer, nullable=False, index=True)
    con_dispersion = Column(Boolean, nullable=False, index=True)
    
    capacidades_bloques = Column(JSON)  # {bloque: capacidad}
    teus_segregaciones = Column(JSON)   # {segregacion: teus}
    info_segregaciones = Column(JSON)   # {segregacion: {id, nombre, teu}}
    
    fecha_carga = Column(DateTime, default=datetime.utcnow)

# Tabla para datos reales
class MagdalenaRealData(Base):
    __tablename__ = "magdalena_real_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    semana = Column(Integer, nullable=False, index=True)
    
    total_movimientos = Column(Integer, default=0)
    reubicaciones = Column(Integer, default=0)
    movimientos_dlvr = Column(Integer, default=0)
    movimientos_dsch = Column(Integer, default=0)
    movimientos_load = Column(Integer, default=0)
    movimientos_recv = Column(Integer, default=0)
    movimientos_othr = Column(Integer, default=0)
    
    bloques_unicos = Column(JSON)  # Lista de bloques
    turnos = Column(JSON)          # Lista de turnos
    carriers = Column(Integer, default=0)
    
    fecha_carga = Column(DateTime, default=datetime.utcnow)