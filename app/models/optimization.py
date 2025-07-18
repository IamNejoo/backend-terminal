# app/models/optimization.py
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, Text, Numeric, Index
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
from datetime import datetime

from app.models.base import Base

class Instancia(Base):
    __tablename__ = "instancias"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    codigo = Column(String(50), unique=True, nullable=False)  # ej: "20220103_68_K"
    fecha_inicio = Column(DateTime, nullable=False)
    fecha_fin = Column(DateTime, nullable=False)
    anio = Column(Integer, nullable=False, index=True)
    semana = Column(Integer, nullable=False, index=True)
    escenario = Column(String(100))  # ej: "Participación 68%"
    participacion = Column(Integer, nullable=False, index=True)
    con_dispersion = Column(Boolean, nullable=False, index=True)
    periodos = Column(Integer, nullable=False, default=21)
    dias = Column(Integer, nullable=False, default=7)
    turnos_por_dia = Column(Integer, nullable=False, default=3)
    estado = Column(String(20), default='completado')
    fecha_creacion = Column(DateTime, default=datetime.utcnow)
    fecha_procesamiento = Column(DateTime, nullable=True)
    observaciones = Column(Text)
    total_movimientos = Column(Integer, default=0)
    total_bloques = Column(Integer, default=0)
    total_segregaciones = Column(Integer, default=0)
    
    # Relaciones
    movimientos_reales = relationship("MovimientoReal", back_populates="instancia", cascade="all, delete-orphan")
    movimientos_modelo = relationship("MovimientoModelo", back_populates="instancia", cascade="all, delete-orphan")
    resultados = relationship("ResultadoGeneral", back_populates="instancia", uselist=False, cascade="all, delete-orphan")
    ocupacion_bloques = relationship("OcupacionBloque", back_populates="instancia", cascade="all, delete-orphan")
    carga_trabajo = relationship("CargaTrabajo", back_populates="instancia", cascade="all, delete-orphan")
    kpis_comparativos = relationship("KPIComparativo", back_populates="instancia", cascade="all, delete-orphan")
    metricas_temporales = relationship("MetricaTemporal", back_populates="instancia", cascade="all, delete-orphan")
    asignaciones_bloques = relationship("AsignacionBloque", back_populates="instancia", cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_optimization_fecha', 'fecha_inicio', 'fecha_fin'),
        Index('idx_optimization_anio_semana', 'anio', 'semana'),
        Index('idx_optimization_participacion', 'participacion', 'con_dispersion'),
    )

class Bloque(Base):
    __tablename__ = "bloques"
    
    id = Column(Integer, primary_key=True)
    codigo = Column(String(10), unique=True, nullable=False)  # C1, C2, etc.
    capacidad_teus = Column(Integer, nullable=False)
    capacidad_bahias = Column(Integer, nullable=False)
    capacidad_original = Column(Integer)  # Nueva: guardar capacidad original
    ubicacion_x = Column(Numeric(10, 2))
    ubicacion_y = Column(Numeric(10, 2))
    activo = Column(Boolean, default=True)
    
    # Relaciones
    ocupaciones = relationship("OcupacionBloque", back_populates="bloque")
    cargas_trabajo = relationship("CargaTrabajo", back_populates="bloque")
    asignaciones = relationship("AsignacionBloque", back_populates="bloque")

class Segregacion(Base):
    __tablename__ = "segregaciones"
    
    id = Column(Integer, primary_key=True)
    codigo = Column(String(50), unique=True, nullable=False)  # S1, S2, etc.
    descripcion = Column(String(200))  # expo-dry-40-EU237
    tipo = Column(String(50))  # expo/impo
    categoria = Column(String(50))  # dry/reefer
    tamano = Column(Integer)  # 20/40
    destino = Column(String(50))
    activo = Column(Boolean, default=True)
    
    # Relaciones
    movimientos_modelo = relationship("MovimientoModelo", back_populates="segregacion")
    asignaciones = relationship("AsignacionBloque", back_populates="segregacion")

class MovimientoReal(Base):
    __tablename__ = "movimientos_reales"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    fecha_hora = Column(DateTime, nullable=False)
    bloque_origen = Column(String(100))
    bloque_destino = Column(String(100))
    tipo_movimiento = Column(String(200), nullable=False)  # YARD, DLVR, RECV, LOAD, DSCH, SHFT, OTHR
    segregacion = Column(String(200))
    categoria = Column(String(100))
    contenedor_id = Column(String(100))
    turno = Column(Integer)
    dia = Column(Integer)
    periodo = Column(Integer)
    distancia_calculada = Column(Integer, default=0)  # Nueva: guardar distancia calculada
    
    instancia = relationship("Instancia", back_populates="movimientos_reales")
    
    __table_args__ = (
        Index('idx_movreal_instancia_fecha', 'instancia_id', 'fecha_hora'),
        Index('idx_movreal_tipo_movimiento', 'tipo_movimiento'),
        Index('idx_movreal_bloques', 'bloque_origen', 'bloque_destino'),
    )

class DistanciaReal(Base):
    __tablename__ = "distancias_reales"
    
    id = Column(Integer, primary_key=True)
    origen = Column(String(50), nullable=False)
    destino = Column(String(50), nullable=False)
    distancia_metros = Column(Integer, nullable=False)
    tipo_origen = Column(String(20))  # bloque, gate, sitio
    tipo_destino = Column(String(20))
    
    __table_args__ = (
        Index('idx_distancia_origen_destino', 'origen', 'destino', unique=True),
    )

class ResultadoGeneral(Base):
    __tablename__ = "resultados_generales"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False, unique=True)
    
    # Movimientos detallados
    movimientos_reales_total = Column(Integer, default=0)
    movimientos_yard_real = Column(Integer, default=0)
    movimientos_dlvr_real = Column(Integer, default=0)
    movimientos_load_real = Column(Integer, default=0)
    movimientos_recv_real = Column(Integer, default=0)
    movimientos_dsch_real = Column(Integer, default=0)
    
    # Movimientos modelo
    movimientos_optimizados = Column(Integer, default=0)
    movimientos_dlvr_modelo = Column(Integer, default=0)
    movimientos_load_modelo = Column(Integer, default=0)
    movimientos_reduccion = Column(Integer, default=0)
    movimientos_reduccion_pct = Column(Numeric(5, 2))
    
    # Distancias
    distancia_real_total = Column(Integer, default=0)
    distancia_real_load = Column(Integer, default=0)
    distancia_real_dlvr = Column(Integer, default=0)
    distancia_real_yard = Column(Integer, default=0)
    distancia_modelo_total = Column(Integer, default=0)
    distancia_modelo_load = Column(Integer, default=0)
    distancia_modelo_dlvr = Column(Integer, default=0)
    distancia_reduccion = Column(Integer, default=0)
    distancia_reduccion_pct = Column(Numeric(5, 2))
    
    # Eficiencia
    eficiencia_real = Column(Numeric(5, 2))
    eficiencia_modelo = Column(Numeric(5, 2), default=100)
    eficiencia_ganancia = Column(Numeric(5, 2))
    
    # Segregaciones
    segregaciones_total = Column(Integer, default=0)
    segregaciones_optimizadas = Column(Integer, default=0)
    
    # Carga de trabajo
    carga_trabajo_total = Column(Integer, default=0)
    variacion_carga = Column(Integer, default=0)
    balance_carga = Column(Integer, default=0)
    carga_maxima = Column(Integer, default=0)
    carga_minima = Column(Integer, default=0)
    
    # Ocupación
    ocupacion_promedio_pct = Column(Numeric(5, 2))
    ocupacion_maxima_pct = Column(Numeric(5, 2))
    ocupacion_minima_pct = Column(Numeric(5, 2))
    capacidad_total_teus = Column(Integer, default=0)
    
    # Metadata
    archivo_distancias_usado = Column(String(255))
    fecha_calculo = Column(DateTime, default=datetime.utcnow)
    
    instancia = relationship("Instancia", back_populates="resultados")

class AsignacionBloque(Base):
    __tablename__ = "asignaciones_bloques"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregaciones.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloques.id"), nullable=True)
    total_bloques_asignados = Column(Integer, default=0)
    bloques_codigos = Column(JSON)  # Lista de códigos de bloques asignados
    
    instancia = relationship("Instancia", back_populates="asignaciones_bloques")
    segregacion = relationship("Segregacion", back_populates="asignaciones")
    bloque = relationship("Bloque", back_populates="asignaciones")
    
    __table_args__ = (
        Index('idx_asignacion_instancia_segregacion', 'instancia_id', 'segregacion_id'),
    )

class MovimientoModelo(Base):
    __tablename__ = "movimientos_modelo"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregaciones.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloques.id"), nullable=False)
    periodo = Column(Integer, nullable=False)
    recepcion = Column(Integer, default=0)
    carga = Column(Integer, default=0)
    descarga = Column(Integer, default=0)
    entrega = Column(Integer, default=0)
    volumen_teus = Column(Integer, default=0)
    bahias_ocupadas = Column(Integer, default=0)
    
    instancia = relationship("Instancia", back_populates="movimientos_modelo")
    segregacion = relationship("Segregacion", back_populates="movimientos_modelo")
    bloque = relationship("Bloque")
    
    __table_args__ = (
        Index('idx_movmodelo_instancia_periodo', 'instancia_id', 'periodo'),
        Index('idx_movmodelo_bloque', 'bloque_id'),
    )

class CargaTrabajo(Base):
    __tablename__ = "carga_trabajo"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloques.id"), nullable=False)
    periodo = Column(Integer, nullable=False)
    carga_trabajo = Column(Integer, default=0)
    carga_maxima = Column(Integer)
    carga_minima = Column(Integer)
    
    instancia = relationship("Instancia", back_populates="carga_trabajo")
    bloque = relationship("Bloque", back_populates="cargas_trabajo")
    
    __table_args__ = (
        Index('idx_carga_instancia_periodo', 'instancia_id', 'periodo'),
        Index('idx_carga_bloque', 'bloque_id'),
    )

class OcupacionBloque(Base):
    __tablename__ = "ocupacion_bloques"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloques.id"), nullable=False)
    periodo = Column(Integer, nullable=False)
    turno = Column(Integer, nullable=False)
    contenedores_teus = Column(Integer, default=0)
    capacidad_bloque = Column(Integer)  # Nueva: guardar capacidad usada
    porcentaje_ocupacion = Column(Numeric(5, 2))
    estado = Column(String(20))  # activo, inactivo
    
    instancia = relationship("Instancia", back_populates="ocupacion_bloques")
    bloque = relationship("Bloque", back_populates="ocupaciones")
    
    __table_args__ = (
        Index('idx_ocupacion_instancia_periodo', 'instancia_id', 'periodo'),
        Index('idx_ocupacion_bloque', 'bloque_id'),
    )

class KPIComparativo(Base):
    __tablename__ = "kpis_comparativos"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    categoria = Column(String(50), nullable=False)  # eficiencia, distancia, movimientos
    metrica = Column(String(100), nullable=False)
    valor_real = Column(Numeric(15, 2))
    valor_modelo = Column(Numeric(15, 2))
    diferencia = Column(Numeric(15, 2))
    porcentaje_mejora = Column(Numeric(5, 2))
    unidad = Column(String(20))
    
    instancia = relationship("Instancia", back_populates="kpis_comparativos")
    
    __table_args__ = (
        Index('idx_kpi_instancia_categoria', 'instancia_id', 'categoria'),
    )

class MetricaTemporal(Base):
    __tablename__ = "metricas_temporales"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    periodo = Column(Integer)
    dia = Column(Integer)
    turno = Column(Integer)
    movimientos_real = Column(Integer, default=0)
    movimientos_yard_real = Column(Integer, default=0)
    movimientos_modelo = Column(Integer, default=0)
    distancia_real = Column(Integer, default=0)
    distancia_modelo = Column(Integer, default=0)
    carga_trabajo = Column(Integer, default=0)
    ocupacion_promedio = Column(Numeric(5, 2))
    
    instancia = relationship("Instancia", back_populates="metricas_temporales")
    
    __table_args__ = (
        Index('idx_metrica_instancia_tiempo', 'instancia_id', 'dia', 'turno'),
        Index('idx_metrica_periodo', 'periodo'),
    )

class LogProcesamiento(Base):
    __tablename__ = "logs_procesamiento"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    instancia_id = Column(UUID(as_uuid=True), ForeignKey("instancias.id"), nullable=False)
    archivo_nombre = Column(String(255))
    archivo_tipo = Column(String(50))  # resultado, flujos, distancias, instancia
    fecha_procesamiento = Column(DateTime, default=datetime.utcnow)
    registros_procesados = Column(Integer, default=0)
    estado = Column(String(20))
    mensaje_error = Column(Text)
    duracion_segundos = Column(Integer)
    
    __table_args__ = (
        Index('idx_log_instancia', 'instancia_id'),
        Index('idx_log_fecha', 'fecha_procesamiento'),
    )