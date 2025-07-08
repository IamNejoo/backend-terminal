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
    
    # Archivo fuente
    archivo_resultado = Column(String(255))
    archivo_instancia = Column(String(255))
    
    # Métricas generales
    funcion_objetivo = Column(Float, default=0)  # min_diff_val
    total_movimientos = Column(Integer, default=0)
    balance_workload = Column(Float, default=0)
    indice_congestion = Column(Float, default=0)
    
    # Relaciones
    variables = relationship("CamilaVariable", back_populates="run", cascade="all, delete-orphan")
    parametros = relationship("CamilaParametro", back_populates="run", cascade="all, delete-orphan")
    metricas = relationship("CamilaMetrica", back_populates="run", cascade="all, delete-orphan")
    
    __table_args__ = (
        UniqueConstraint('semana', 'dia', 'turno', 'modelo_tipo', 'con_segregaciones', 
                        name='_camila_run_unique'),
        Index('idx_camila_run_lookup', 'semana', 'dia', 'turno', 'modelo_tipo', 'con_segregaciones'),
    )

class CamilaVariable(Base):
    """Variables del modelo de optimización"""
    __tablename__ = "camila_variables"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    # Datos originales del archivo
    variable = Column(String(20), nullable=False)  # fr_sbt, fe_sbt, ygbt, alpha_gbt, Z_gb, min_diff_val
    indice = Column(String(100))  # "('s3', 'b3', 1)" o vacio para min_diff_val
    valor = Column(Float, default=0)
    
    # Campos parseados del índice (nullable porque dependen del tipo de variable)
    segregacion = Column(String(10))  # s1, s2, etc
    grua = Column(String(10))  # g1, g2, etc
    bloque = Column(String(10))  # b1, b2, etc
    tiempo = Column(Integer)  # 1-8
    
    # Tipo de variable para facilitar queries
    tipo_variable = Column(String(20))  # flujo_recepcion, flujo_entrega, asignacion_grua, etc
    
    run = relationship("CamilaRun", back_populates="variables")
    
    __table_args__ = (
        Index('idx_camila_var_lookup', 'run_id', 'variable', 'bloque', 'tiempo'),
        Index('idx_camila_var_tipo', 'run_id', 'tipo_variable'),
    )

class CamilaParametro(Base):
    """Parámetros del modelo desde el archivo de instancia"""
    __tablename__ = "camila_parametros"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    # Parámetros generales
    parametro = Column(String(50), nullable=False)  # mu, W, K, Rmax, etc
    valor = Column(Float)
    descripcion = Column(Text)
    
    # Para parámetros indexados (como AEbs, DMEst, etc)
    indices = Column(JSON)  # {"bloque": "b1", "segregacion": "s3"}
    
    run = relationship("CamilaRun", back_populates="parametros")
    
    __table_args__ = (
        Index('idx_camila_param_lookup', 'run_id', 'parametro'),
    )

class CamilaMetrica(Base):
    """Métricas calculadas y agregadas"""
    __tablename__ = "camila_metricas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    # Métricas por bloque
    metricas_bloque = Column(JSON)  # {"b1": {"movimientos": 100, "gruas": 5, ...}, ...}
    
    # Métricas por grúa
    metricas_grua = Column(JSON)  # {"g1": {"periodos_activos": 6, "bloques": ["b1", "b3"], ...}, ...}
    
    # Métricas por tiempo
    metricas_tiempo = Column(JSON)  # {"1": {"movimientos": 50, "gruas_activas": 3, ...}, ...}
    
    # Métricas por segregación
    metricas_segregacion = Column(JSON)  # {"s3": {"movimientos": 57, "tipo": "recepcion", ...}, ...}
    
    # Matrices completas para visualización
    matriz_flujos_total = Column(JSON)  # [bloques][tiempos]
    matriz_asignacion_gruas = Column(JSON)  # [gruas][bloques*tiempos]
    matriz_capacidad = Column(JSON)  # [bloques][tiempos]
    matriz_disponibilidad = Column(JSON)  # [bloques][tiempos]
    
    # Participación porcentual
    participacion_bloques = Column(JSON)  # Array de % por bloque
    participacion_tiempo = Column(JSON)  # Array de % por tiempo
    
    # Estadísticas
    desviacion_std_bloques = Column(Float)
    desviacion_std_tiempo = Column(Float)
    utilizacion_promedio = Column(Float)
    gruas_activas = Column(Integer)
    bloques_activos = Column(Integer)
    
    run = relationship("CamilaRun", back_populates="metricas")

# Tabla auxiliar para segregaciones (información estática)
class CamilaSegregacion(Base):
    """Información de segregaciones desde el archivo de instancia"""
    __tablename__ = "camila_segregaciones"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    run_id = Column(UUID(as_uuid=True), ForeignKey("camila_runs.id"), nullable=False)
    
    codigo = Column(String(10), nullable=False)  # s1, s2, etc
    descripcion = Column(String(100))  # expo-dry-20-MK567, etc
    tipo = Column(String(20))  # exportacion, importacion
    
    # Estadísticas de la segregación
    total_recepcion = Column(Integer, default=0)
    total_entrega = Column(Integer, default=0)
    bloques_asignados = Column(JSON)  # ["b1", "b3", ...]