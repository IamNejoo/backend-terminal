# app/models/camila.py

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text, Numeric, Index, Enum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime
import enum

from app.models.base import Base


class EstadoProcesamiento(enum.Enum):
    PENDIENTE = "pendiente"
    PROCESANDO = "procesando"
    COMPLETADO = "completado"
    ERROR = "error"


class TipoOperacion(enum.Enum):
    RECEPCION = "recepcion"
    ENTREGA = "entrega"
    CARGA = "carga"
    DESCARGA = "descarga"
    MIXTO = "mixto"


class TipoAsignacion(enum.Enum):
    REGULAR = "regular"
    EMERGENCIA = "emergencia"
    REPOSICIONAMIENTO = "reposicionamiento"


class ResultadoCamila(Base):
    """Resultado principal de una ejecución del modelo Camila"""
    __tablename__ = "resultados_camila"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codigo = Column(String(50), unique=True, nullable=False, index=True)  # ej: "20220103_68_K_T01"
    
    # Información temporal
    fecha_inicio = Column(DateTime, nullable=False, index=True)
    fecha_fin = Column(DateTime, nullable=False)
    anio = Column(Integer, nullable=False, index=True)
    semana = Column(Integer, nullable=False, index=True)
    dia = Column(Integer, nullable=False)  # 1-7 (día de la semana)
    turno = Column(Integer, nullable=False, index=True)  # 1-21 (turno de la semana)
    turno_del_dia = Column(Integer, nullable=False)  # 1-3 (turno del día)
    
    # Configuración
    participacion = Column(Integer, nullable=False, index=True)  # 60-80
    con_dispersion = Column(Boolean, nullable=False, index=True)
    
    # Estado
    estado = Column(Enum(EstadoProcesamiento), default=EstadoProcesamiento.PROCESANDO, nullable=False)
    fecha_creacion = Column(DateTime, default=datetime.utcnow, nullable=False)
    fecha_procesamiento = Column(DateTime, nullable=True)
    
    # Métricas agregadas del modelo
    total_movimientos_modelo = Column(Integer, default=0, nullable=False)
    total_gruas_utilizadas = Column(Integer, default=0, nullable=False)
    total_bloques_visitados = Column(Integer, default=0, nullable=False)
    total_segregaciones = Column(Integer, default=0, nullable=False)
    capacidad_teorica = Column(Integer, default=0, nullable=False)
    utilizacion_modelo = Column(Numeric(5, 2), default=0, nullable=False)
    coeficiente_variacion = Column(Numeric(5, 2), default=0, nullable=False)
    
    # Métricas de comparación con realidad
    total_movimientos_real = Column(Integer, default=0, nullable=True)
    accuracy_global = Column(Numeric(5, 2), nullable=True)
    brecha_movimientos = Column(Integer, nullable=True)
    correlacion_temporal = Column(Numeric(5, 2), nullable=True)
    
    # Metadata
    archivo_resultado = Column(String(255), nullable=True)
    archivo_instancia = Column(String(255), nullable=True)
    archivo_flujos_real = Column(String(255), nullable=True)
    
    # Relaciones
    asignaciones_gruas = relationship("AsignacionGrua", back_populates="resultado", cascade="all, delete-orphan")
    cuotas_camiones = relationship("CuotaCamion", back_populates="resultado", cascade="all, delete-orphan")
    metricas_gruas = relationship("MetricaGrua", back_populates="resultado", cascade="all, delete-orphan")
    comparaciones_real = relationship("ComparacionReal", back_populates="resultado", cascade="all, delete-orphan")
    flujos_modelo = relationship("FlujoModelo", back_populates="resultado", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_camila_fecha', 'fecha_inicio', 'fecha_fin'),
        Index('idx_camila_anio_semana_turno', 'anio', 'semana', 'turno'),
        Index('idx_camila_participacion', 'participacion', 'con_dispersion'),
        Index('idx_camila_codigo', 'codigo'),
    )


class AsignacionGrua(Base):
    """Asignación de grúas a bloques por periodo según el modelo"""
    __tablename__ = "asignaciones_gruas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    
    # Identificadores
    grua_id = Column(Integer, nullable=False)  # 1-12
    bloque_codigo = Column(String(10), nullable=False, index=True)  # C1-C9
    periodo = Column(Integer, nullable=False)  # 1-8
    
    # Métricas
    asignada = Column(Boolean, default=False, nullable=False)  # ygbt = 1
    activada = Column(Boolean, default=False, nullable=False)  # alpha_gbt = 1
    movimientos_asignados = Column(Integer, default=0, nullable=False)
    tipo_asignacion = Column(Enum(TipoAsignacion), default=TipoAsignacion.REGULAR)
    
    # Relación
    resultado = relationship("ResultadoCamila", back_populates="asignaciones_gruas")
    
    __table_args__ = (
        Index('idx_asig_resultado_periodo', 'resultado_id', 'periodo'),
        Index('idx_asig_grua_bloque', 'grua_id', 'bloque_codigo'),
    )


class FlujoModelo(Base):
    """Flujos de contenedores según el modelo (fr_sbt, fe_sbt, etc.)"""
    __tablename__ = "flujos_modelo"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    
    # Identificadores
    tipo_flujo = Column(String(10), nullable=False)  # fr, fe, fc, fd
    segregacion_codigo = Column(String(50), nullable=False, index=True)  # S1, S2, etc
    bloque_codigo = Column(String(10), nullable=False, index=True)  # C1-C9
    periodo = Column(Integer, nullable=False)  # 1-8
    
    # Valores
    cantidad = Column(Integer, default=0, nullable=False)
    tipo_operacion = Column(Enum(TipoOperacion), nullable=False)
    
    # Relación
    resultado = relationship("ResultadoCamila", back_populates="flujos_modelo")
    
    __table_args__ = (
        Index('idx_flujo_resultado_periodo', 'resultado_id', 'periodo'),
        Index('idx_flujo_tipo_bloque', 'tipo_flujo', 'bloque_codigo'),
    )


class CuotaCamion(Base):
    """Cuotas de camiones por periodo y bloque"""
    __tablename__ = "cuotas_camiones"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    
    # Identificadores
    periodo = Column(Integer, nullable=False)
    bloque_codigo = Column(String(10), nullable=False, index=True)
    
    # Valores del modelo
    cuota_modelo = Column(Integer, default=0, nullable=False)
    capacidad_maxima = Column(Integer, nullable=False)
    gruas_asignadas = Column(Integer, default=0, nullable=False)
    
    # Valores reales (para comparación)
    movimientos_reales = Column(Integer, nullable=True)
    utilizacion_real = Column(Numeric(10, 2), nullable=True)  # CAMBIADO: de (5,2) a (10,2)
    
    # Metadata
    tipo_operacion = Column(Enum(TipoOperacion), default=TipoOperacion.MIXTO)
    segregaciones_incluidas = Column(JSON)  # Lista de segregaciones
    
    # Relación
    resultado = relationship("ResultadoCamila", back_populates="cuotas_camiones")
    
    __table_args__ = (
        Index('idx_cuota_resultado_periodo', 'resultado_id', 'periodo'),
        Index('idx_cuota_bloque', 'bloque_codigo'),
    )

class SegregacionMapping(Base):
    """Mapeo entre códigos de segregación del modelo y nombres reales"""
    __tablename__ = "segregaciones_mapping"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    codigo = Column(String(10), nullable=False, index=True)  # S1, S2, etc.
    nombre = Column(String(100), nullable=False)  # expo-dry-20-HAM147
    tipo = Column(String(20))  # EXPORT, IMPORT
    size = Column(Integer)  # 20, 40
    
    # Relación
    resultado = relationship("ResultadoCamila", backref="segregaciones_mapping")
    
    __table_args__ = (
        Index('idx_segregacion_resultado_codigo', 'resultado_id', 'codigo'),
    )
    
    
class MetricaGrua(Base):
    """Métricas de desempeño por grúa"""
    __tablename__ = "metricas_gruas"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    
    # Identificador
    grua_id = Column(Integer, nullable=False, index=True)  # 1-12
    
    # Métricas del modelo
    movimientos_modelo = Column(Integer, default=0, nullable=False)
    bloques_visitados = Column(Integer, default=0, nullable=False)
    periodos_activa = Column(Integer, default=0, nullable=False)
    cambios_bloque = Column(Integer, default=0, nullable=False)
    
    # Métricas calculadas
    tiempo_productivo_hrs = Column(Numeric(10, 2), default=0, nullable=False)
    tiempo_improductivo_hrs = Column(Numeric(10, 2), default=0, nullable=False)
    utilizacion_pct = Column(Numeric(10, 2), default=0, nullable=False)
    
    # Comparación con distribución real (si disponible)
    movimientos_reales_estimados = Column(Integer, nullable=True)
    diferencia_vs_real = Column(Integer, nullable=True)
    
    # Relación
    resultado = relationship("ResultadoCamila", back_populates="metricas_gruas")
    
    __table_args__ = (
        Index('idx_metrica_resultado_grua', 'resultado_id', 'grua_id'),
    )


class ComparacionReal(Base):
    """Comparación entre modelo y operación real"""
    __tablename__ = "comparaciones_real"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=False)
    
    # Tipo y métrica
    tipo_comparacion = Column(String(50), nullable=False)  # 'general', 'por_periodo', 'por_bloque', 'por_tipo'
    dimension = Column(String(50), nullable=True)  # periodo, bloque, o tipo específico
    metrica = Column(String(100), nullable=False)  # 'movimientos', 'utilizacion', etc.
    
    # Valores
    valor_modelo = Column(Numeric(15, 2), nullable=False)
    valor_real = Column(Numeric(15, 2), nullable=False)
    diferencia_absoluta = Column(Numeric(15, 2), nullable=False)
    diferencia_porcentual = Column(Numeric(10, 2), nullable=False)
    accuracy = Column(Numeric(5, 2), nullable=False)  # min(modelo,real)/max(modelo,real)*100
    
    # Metadata
    fecha_comparacion = Column(DateTime, default=datetime.utcnow, nullable=False)
    archivo_fuente_real = Column(String(255), nullable=True)
    filtros_aplicados = Column(JSON, nullable=True)  # {'tipos': ['RECV','DLVR'], 'horas': [16,17...]}
    descripcion = Column(Text, nullable=True)
    
    # Relación
    resultado = relationship("ResultadoCamila", back_populates="comparaciones_real")
    
    __table_args__ = (
        Index('idx_comp_resultado_tipo', 'resultado_id', 'tipo_comparacion'),
        Index('idx_comp_dimension', 'dimension'),
    )


class ParametroCamila(Base):
    """Parámetros del modelo Camila"""
    __tablename__ = "parametros_camila"
    
    id = Column(Integer, primary_key=True)
    codigo = Column(String(20), unique=True, nullable=False)  # mu, W, K, Rmax
    descripcion = Column(String(200))
    valor_default = Column(Numeric(10, 2), nullable=False)
    valor_actual = Column(Numeric(10, 2), nullable=False)
    unidad = Column(String(20))
    activo = Column(Boolean, default=True)
    fecha_actualizacion = Column(DateTime, default=datetime.utcnow)
    
    # Valores observados en la realidad (para calibración)
    valor_real_promedio = Column(Numeric(10, 2), nullable=True)
    valor_real_min = Column(Numeric(10, 2), nullable=True)
    valor_real_max = Column(Numeric(10, 2), nullable=True)
    
    __table_args__ = (
        Index('idx_param_codigo', 'codigo'),
    )


class LogProcesamientoCamila(Base):
    """Log de procesamiento de archivos"""
    __tablename__ = "logs_procesamiento_camila"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    resultado_id = Column(UUID(as_uuid=True), ForeignKey("resultados_camila.id"), nullable=True)
    
    # Información del proceso
    tipo_proceso = Column(String(50), nullable=False)  # 'carga_modelo', 'comparacion_real', 'calculo_metricas'
    archivo_procesado = Column(String(255), nullable=True)
    fecha_inicio = Column(DateTime, nullable=False)
    fecha_fin = Column(DateTime, nullable=True)
    duracion_segundos = Column(Integer, nullable=True)
    
    # Estado
    estado = Column(Enum(EstadoProcesamiento), nullable=False)
    registros_procesados = Column(Integer, default=0)
    registros_error = Column(Integer, default=0)
    
    # Detalles
    mensaje = Column(Text, nullable=True)
    detalle_error = Column(JSON, nullable=True)
    metricas = Column(JSON, nullable=True)  # {'tiempo_lectura': 1.2, 'memoria_mb': 45}
    
    __table_args__ = (
        Index('idx_log_resultado', 'resultado_id'),
        Index('idx_log_camila_fecha', 'fecha_inicio'),
        Index('idx_log_tipo_estado', 'tipo_proceso', 'estado'),
    )