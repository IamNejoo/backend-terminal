from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text, Numeric, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.models.base import Base

class ResultadoCamila(Base):
    __tablename__ = "resultados_camila"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codigo = Column(String(50), unique=True, nullable=False)  # ej: "20220103_68_K_T01"
    fecha_inicio = Column(DateTime, nullable=False)
    fecha_fin = Column(DateTime, nullable=False)
    anio = Column(Integer, nullable=False, index=True)
    semana = Column(Integer, nullable=False, index=True)
    dia = Column(Integer, nullable=False)  # 1-7
    turno = Column(Integer, nullable=False, index=True)  # 1-21 (turno de la semana)
    turno_del_dia = Column(Integer, nullable=False)  # 1-3 (turno del día)
    participacion = Column(Integer, nullable=False, index=True)
    con_dispersion = Column(Boolean, nullable=False, index=True)
    estado = Column(String(20), default='procesando')
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    fecha_procesamiento = Column(DateTime, nullable=True)
    
    # Métricas agregadas
    total_gruas = Column(Integer, default=12)
    total_movimientos = Column(Integer, default=0)
    total_bloques_visitados = Column(Integer, default=0)
    total_segregaciones = Column(Integer, default=0)
    utilizacion_promedio = Column(Numeric(5, 2))
    coeficiente_variacion = Column(Numeric(5, 2))
    
    # Relaciones
    asignaciones_gruas = relationship("AsignacionGrua", back_populates="resultado", cascade="all, delete-orphan")
    cuotas_camiones = relationship("CuotaCamion", back_populates="resultado", cascade="all, delete-orphan")
    metricas_gruas = relationship("MetricaGrua", back_populates="resultado", cascade="all, delete-orphan")
    comparaciones = relationship("ComparacionCamila", back_populates="resultado", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_camila_fecha', 'fecha_inicio', 'fecha_fin'),
        Index('idx_camila_anio_semana_turno', 'anio', 'semana', 'turno'),
        Index('idx_camila_participacion', 'participacion', 'con_dispersion'),
    )

class AsignacionGrua(Base):
    __tablename__ = "asignaciones_gruas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    segregacion_codigo = Column(String(50), nullable=False)  # S1, S2, etc
    bloque_codigo = Column(String(10), nullable=False)  # C1, C2, etc
    periodo = Column(Integer, nullable=False)  # 1-4 (períodos dentro del turno)
    grua_asignada = Column(Integer)  # ID de la grúa (1-12)
    frecuencia = Column(Integer, default=0)  # Número de visitas
    tipo_asignacion = Column(String(20))  # regular, emergencia, etc
    
    resultado = relationship("ResultadoCamila", back_populates="asignaciones_gruas")
    
    __table_args__ = (
        Index('idx_asig_resultado_periodo', 'resultado_id', 'periodo'),
        Index('idx_asig_bloque', 'bloque_codigo'),
        Index('idx_asig_segregacion', 'segregacion_codigo'),
    )

class CuotaCamion(Base):
    __tablename__ = "cuotas_camiones"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    periodo = Column(Integer, nullable=False)
    ventana_inicio = Column(Integer, nullable=False)
    ventana_fin = Column(Integer, nullable=False)
    bloque_codigo = Column(String(10), nullable=False)
    cuota_camiones = Column(Integer, default=0)
    capacidad_maxima = Column(Integer)
    tipo_operacion = Column(String(20))  # recepcion, entrega, mixto
    segregaciones_incluidas = Column(JSON)  # Lista de segregaciones
    
    resultado = relationship("ResultadoCamila", back_populates="cuotas_camiones")
    
    __table_args__ = (
        Index('idx_cuota_resultado_periodo', 'resultado_id', 'periodo'),
        Index('idx_cuota_bloque', 'bloque_codigo'),
    )

class MetricaGrua(Base):
    __tablename__ = "metricas_gruas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    grua_id = Column(Integer, nullable=False)  # 1-12
    movimientos_asignados = Column(Integer, default=0)
    bloques_visitados = Column(Integer, default=0)
    cambios_bloque = Column(Integer, default=0)
    tiempo_productivo_hrs = Column(Numeric(5, 2))
    tiempo_improductivo_hrs = Column(Numeric(5, 2))
    utilizacion_pct = Column(Numeric(5, 2))
    distancia_recorrida_m = Column(Integer, default=0)
    
    resultado = relationship("ResultadoCamila", back_populates="metricas_gruas")
    
    __table_args__ = (
        Index('idx_metrica_resultado_grua', 'resultado_id', 'grua_id'),
    )

class ComparacionCamila(Base):
    __tablename__ = "comparaciones_camila"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    tipo_comparacion = Column(String(50), nullable=False)  # general, por_bloque, por_segregacion
    metrica = Column(String(100), nullable=False)
    valor_magdalena = Column(Numeric(15, 2))
    valor_camila = Column(Numeric(15, 2))
    diferencia = Column(Numeric(15, 2))
    porcentaje_diferencia = Column(Numeric(5, 2))
    descripcion = Column(Text)
    
    resultado = relationship("ResultadoCamila", back_populates="comparaciones")
    
    __table_args__ = (
        Index('idx_comp_resultado_tipo', 'resultado_id', 'tipo_comparacion'),
    )

class ParametroCamila(Base):
    __tablename__ = "parametros_camila"
    
    id = Column(Integer, primary_key=True)
    codigo = Column(String(20), unique=True, nullable=False)  # mu, W, K, Rmax
    descripcion = Column(String(200))
    valor = Column(Numeric(10, 2))
    unidad = Column(String(20))
    activo = Column(Boolean, default=True)

class LogCamila(Base):
    __tablename__ = "logs_camila"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    archivo_nombre = Column(String(255))
    archivo_tipo = Column(String(50))  # resultado, instancia
    fecha_procesamiento = Column(DateTime, default=datetime.utcnow)
    registros_procesados = Column(Integer, default=0)
    estado = Column(String(20))
    mensaje_error = Column(Text)
    duracion_segundos = Column(Integer)
    
    __table_args__ = (
        Index('idx_log_camila_resultado', 'resultado_id'),
        Index('idx_log_camila_fecha', 'fecha_procesamiento'),
    )