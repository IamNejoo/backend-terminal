from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, JSON, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
import uuid
from datetime import datetime

from app.models.base import Base

class MagdalenaRun(Base):
    """Datos optimizados de Magdalena por periodo"""
    __tablename__ = "magdalena_runs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # Identificadores únicos
    anio = Column(Integer, nullable=False, index=True)
    semana = Column(Integer, nullable=False, index=True)
    turno = Column(Integer, nullable=False, index=True)  # 1-21
    participacion = Column(Integer, nullable=False)
    con_dispersion = Column(Boolean, nullable=False)
    
    # Métricas calculadas de los archivos
    reubicaciones_eliminadas = Column(Integer, default=0)  # YARD de flujos
    movimientos_reales = Column(Integer, default=0)  # Total flujos reales
    movimientos_optimizados = Column(Integer, default=0)  # Total General
    
    # Eficiencias calculadas
    eficiencia_ganada = Column(Float, default=0)  # Fórmula específica
    eficiencia_operacional_real = Column(Float, default=0)
    eficiencia_operacional_opt = Column(Float, default=100)
    
    # De Carga máx-min
    carga_maxima = Column(Integer, default=0)
    carga_minima = Column(Integer, default=0)
    balance_carga = Column(Float, default=0)  # Desviación estándar
    
    # De Ocupación Bloques
    ocupacion_promedio = Column(Float, default=0)
    volumen_total_teus = Column(Float, default=0)
    capacidad_total = Column(Float, default=0)
    
    # De Workload
    carga_trabajo_total = Column(Float, default=0)
    
    # Conteos
    segregaciones_activas = Column(Integer, default=0)  # Únicas en General
    bloques_activos = Column(Integer, default=0)
    
    # JSONs con detalles
    movimientos_por_tipo = Column(JSON)  # {recepcion, carga, descarga, entrega}
    movimientos_reales_tipo = Column(JSON)  # {DLVR, DSCH, LOAD, RECV, YARD}
    ocupacion_por_bloque = Column(JSON)  # {C1: %, C2: %, ...}
    workload_por_bloque = Column(JSON)  # {C1: valor, C2: valor, ...}
    bahias_por_segregacion = Column(JSON)  # Datos de bahías
    volumen_por_segregacion = Column(JSON)  # Volumen TEUs
    
    # Metadata
    fecha_carga = Column(DateTime, default=datetime.utcnow)
    archivo_resultado = Column(String(255))
    archivo_flujos = Column(String(255))
    archivo_instancia = Column(String(255))
    
    __table_args__ = (
        UniqueConstraint('anio', 'semana', 'turno', 'participacion', 'con_dispersion', 
                        name='uq_magdalena_run'),
    )