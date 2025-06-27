# app/models/camila.py
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, ForeignKey, JSON, Text, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

from app.models.base import Base

class CamilaRun(Base):
    """Ejecución del modelo Camila"""
    __tablename__ = "camila_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    semana = Column(Integer, nullable=False)
    dia = Column(String(20), nullable=False)  # Monday, Tuesday, etc.
    turno = Column(Integer, nullable=False)  # 1, 2, 3
    modelo_tipo = Column(String(10), nullable=False)  # minmax, maxmin
    con_segregaciones = Column(Boolean, default=True)
    fecha_carga = Column(DateTime, default=datetime.utcnow)
    
    # Métricas generales
    funcion_objetivo = Column(Float, default=0)
    total_movimientos = Column(Integer, default=0)
    balance_workload = Column(Float, default=0)
    indice_congestion = Column(Float, default=0)
    
    # Relaciones
    flujos = relationship("CamilaFlujos", back_populates="run", cascade="all, delete-orphan")
    gruas = relationship("CamilaGruas", back_populates="run", cascade="all, delete-orphan")
    asignacion = relationship("CamilaAsignacion", back_populates="run", cascade="all, delete-orphan")
    resultados = relationship("CamilaResultados", back_populates="run", cascade="all, delete-orphan")
    real_data = relationship("CamilaRealData", back_populates="run", cascade="all, delete-orphan")
    cuotas = relationship("CamilaCuotas", back_populates="run", cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint('semana', 'dia', 'turno', 'modelo_tipo', 'con_segregaciones', 
                        name='_camila_run_unique'),
        Index('idx_camila_run_lookup', 'semana', 'dia', 'turno', 'modelo_tipo', 'con_segregaciones'),
    )

class CamilaFlujos(Base):
    """Flujos de contenedores por segregación, bloque y tiempo"""
    __tablename__ = "camila_flujos"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    variable = Column(String(10), nullable=False)  # fr_sbt, fe_sbt, fc_sbt, fd_sbt
    segregacion = Column(String(10), nullable=False)
    bloque = Column(String(10), nullable=False)
    tiempo = Column(Integer, nullable=False)
    valor = Column(Float, default=0)
    
    run = relationship("CamilaRun", back_populates="flujos")
    
    __table_args__ = (
        Index('idx_camila_flujos_lookup', 'run_id', 'variable', 'bloque', 'tiempo'),
    )

class CamilaGruas(Base):
    """Asignación de grúas (variable ygbt)"""
    __tablename__ = "camila_gruas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    grua = Column(String(10), nullable=False)  # g1, g2, ..., g12
    bloque = Column(String(10), nullable=False)  # b1, b2, ..., b9
    tiempo = Column(Integer, nullable=False)  # 1-8
    valor = Column(Integer, default=0)  # 0 o 1 (binario)
    
    run = relationship("CamilaRun", back_populates="gruas")
    
    __table_args__ = (
        Index('idx_camila_gruas_lookup', 'run_id', 'grua', 'tiempo'),
    )

class CamilaAsignacion(Base):
    """Matriz de asignación visual de grúas"""
    __tablename__ = "camila_asignacion"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    tiempo = Column(Integer, nullable=False)  # 1-8
    grua = Column(String(10), nullable=False)  # g1-g12
    bloque_asignado = Column(String(50), nullable=False)  # b1-b9 o null
    movimientos_realizados = Column(Integer, default=0)
    
    run = relationship("CamilaRun", back_populates="asignacion")
    
    __table_args__ = (
        Index('idx_camila_asignacion_lookup', 'run_id', 'tiempo', 'grua'),
    )

class CamilaResultados(Base):
    """Resultados consolidados del modelo"""
    __tablename__ = "camila_resultados"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    # Métricas calculadas
    total_flujos = Column(JSON)  # Matriz [bloques][tiempos]
    capacidad = Column(JSON)  # Matriz [bloques][tiempos]
    disponibilidad = Column(JSON)  # Matriz [bloques][tiempos]
    
    # KPIs
    participacion_bloques = Column(JSON)  # Array de % por bloque
    participacion_tiempo = Column(JSON)  # Array de % por tiempo
    desviacion_std_bloques = Column(Float)
    desviacion_std_tiempo = Column(Float)
    
    # Cuotas recomendadas
    cuotas_recomendadas = Column(JSON)  # Matriz [bloques][tiempos]
    
    run = relationship("CamilaRun", back_populates="resultados")

class CamilaRealData(Base):
    """Datos reales históricos para comparación"""
    __tablename__ = "camila_real_data"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False)
    tiempo = Column(Integer, nullable=False)
    movimientos = Column(Integer, default=0)
    
    run = relationship("CamilaRun", back_populates="real_data")
    
    __table_args__ = (
        Index('idx_camila_real_lookup', 'run_id', 'bloque', 'tiempo'),
    )

class CamilaCuotas(Base):
    """Cálculo de cuotas y disponibilidad"""
    __tablename__ = "camila_cuotas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    bloque = Column(String(10), nullable=False)
    tiempo = Column(Integer, nullable=False)
    disponibilidad = Column(Integer, default=0)
    cuota_recomendada = Column(Integer, default=0)
    
    run = relationship("CamilaRun", back_populates="cuotas")