# app/models/camila.py
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Date, Time, ForeignKey, JSON, ARRAY, Text, CheckConstraint, UniqueConstraint, Index, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from app.models.base import Base
import enum


class EstadoInstancia(enum.Enum):
    pendiente = "pendiente"
    ejecutando = "ejecutando"
    completado = "completado"
    error = "error"
    
    
    
class InstanciaCamila(Base):
    __tablename__ = "instancia_camila"
    
    id = Column(Integer, primary_key=True, index=True)
    # Identificación
    anio = Column(Integer, nullable=False)
    semana = Column(Integer, nullable=False)
    fecha = Column(Date, nullable=False)
    turno = Column(Integer, nullable=False)
    participacion = Column(Integer, nullable=False)
    
    # Referencias
    magdalena_instance_id = Column(Integer)
    
    # Timestamps
    fecha_creacion = Column(DateTime, default=func.now())
    fecha_ejecucion = Column(DateTime)
    tiempo_ejecucion_ms = Column(Integer)
    
    # Estado
    estado = Column(Enum(EstadoInstancia), default=EstadoInstancia.pendiente)

    mensaje_error = Column(Text)
    
    # Metadata
    version_modelo = Column(String(20))
    usuario_ejecucion = Column(String(100))
    
    # Relaciones
    parametros = relationship("ParametroGeneral", back_populates="instancia", uselist=False)
    periodos = relationship("PeriodoHora", back_populates="instancia")
    inventarios = relationship("InventarioInicial", back_populates="instancia")
    demandas = relationship("DemandaOperacion", back_populates="instancia")
    demandas_hora_magdalena = relationship("DemandaHoraMagdalena", back_populates="instancia")
    capacidades = relationship("CapacidadBloque", back_populates="instancia")
    asignaciones = relationship("AsignacionGrua", back_populates="instancia")
    flujos = relationship("FlujoOperacional", back_populates="instancia")
    cuotas = relationship("CuotaCamion", back_populates="instancia")
    disponibilidades = relationship("DisponibilidadBloque", back_populates="instancia")
    metricas = relationship("MetricaResultado", back_populates="instancia", uselist=False)
    integracion = relationship("IntegracionMagdalena", back_populates="instancia", uselist=False)
    configuraciones = relationship("ConfiguracionInstancia", back_populates="instancia")
    
    __table_args__ = (
        CheckConstraint('semana BETWEEN 1 AND 52'),
        CheckConstraint('turno BETWEEN 1 AND 21'),
        CheckConstraint('participacion BETWEEN 0 AND 100'),
        CheckConstraint("estado IN ('pendiente', 'ejecutando', 'completado', 'error')"),
        UniqueConstraint('anio', 'semana', 'turno', 'participacion'),
       Index('idx_instancia_camila_fecha', 'fecha'),  # CAMBIADO
        Index('idx_instancia_camila_estado', 'estado'),  # CAMBIADO
        Index('idx_instancia_magdalena', 'magdalena_instance_id'),
    )

class Bloque(Base):
    __tablename__ = "bloque"
    
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(10), nullable=False, unique=True)
    nombre = Column(String(50))
    patio = Column(String(20), default='Costanera')
    
    # Configuración física
    grupo_movimiento = Column(Integer)  # 1: C1-C3-C6, 2: C2-C4-C7, 3: C5-C8-C9
    bloques_adyacentes = Column(ARRAY(String))
    
    # Capacidades base
    capacidad_teus = Column(Integer)
    bahias_totales = Column(Integer, default=30)
    niveles = Column(Integer, default=7)
    
    # Estado
    activo = Column(Boolean, default=True)
    fecha_creacion = Column(DateTime, default=func.now())
    
    # Relaciones
    capacidades = relationship("CapacidadBloque", back_populates="bloque")
    asignaciones = relationship("AsignacionGrua", back_populates="bloque")
    flujos = relationship("FlujoOperacional", back_populates="bloque")
    disponibilidades = relationship("DisponibilidadBloque", back_populates="bloque")
    
    __table_args__ = (
        CheckConstraint('grupo_movimiento IN (1, 2, 3)'),
    )

class Segregacion(Base):
    __tablename__ = "segregacion"
    
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(20), nullable=False, unique=True)
    descripcion = Column(Text)
    tipo_contenedor = Column(String(10))
    tipo_carga = Column(String(20))
    categoria = Column(String(20))
    
    # Metadata
    activa = Column(Boolean, default=True)
    fecha_creacion = Column(DateTime, default=func.now())
    
    # Relaciones
    inventarios = relationship("InventarioInicial", back_populates="segregacion")
    demandas = relationship("DemandaOperacion", back_populates="segregacion")
    capacidades = relationship("CapacidadBloque", back_populates="segregacion")
    flujos = relationship("FlujoOperacional", back_populates="segregacion")
    
    __table_args__ = (
        CheckConstraint("tipo_contenedor IN ('20', '40')"),
        CheckConstraint("tipo_carga IN ('dry', 'reefer', 'imo', 'oog')"),
        CheckConstraint("categoria IN ('importacion', 'exportacion', 'transbordo')"),
        Index('idx_segregacion_camila_tipo', 'tipo_contenedor'),  # CAMBIADO
        Index('idx_segregacion_camila_categoria', 'categoria'),  # CAMBIADO
    )

class Grua(Base):
    __tablename__ = "grua"
    
    id = Column(Integer, primary_key=True, index=True)
    codigo = Column(String(20), nullable=False, unique=True)
    tipo = Column(String(20), default='RTG')
    
    # Características operacionales
    productividad_nominal = Column(Integer, default=20)
    alcance_bloques = Column(Integer, default=3)
    
    # Estado
    activa = Column(Boolean, default=True)
    en_mantenimiento = Column(Boolean, default=False)
    fecha_ultimo_mantenimiento = Column(Date)
    
    fecha_creacion = Column(DateTime, default=func.now())
    
    # Relaciones
    asignaciones = relationship("AsignacionGrua", back_populates="grua")
    productividades = relationship("ProductividadGrua", back_populates="grua")

class PeriodoHora(Base):
    __tablename__ = "periodo_hora"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    hora_relativa = Column(Integer, nullable=False)
    hora_absoluta = Column(Integer, nullable=False)
    
    # Información temporal
    hora_inicio = Column(Time, nullable=False)
    hora_fin = Column(Time, nullable=False)
    dia_semana = Column(Integer)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="periodos")
    demandas = relationship("DemandaOperacion", back_populates="periodo")
    asignaciones = relationship("AsignacionGrua", back_populates="periodo")
    flujos = relationship("FlujoOperacional", back_populates="periodo")
    cuotas = relationship("CuotaCamion", back_populates="periodo")
    disponibilidades = relationship("DisponibilidadBloque", back_populates="periodo")
    
    __table_args__ = (
        CheckConstraint('hora_relativa BETWEEN 1 AND 8'),
        CheckConstraint('hora_absoluta BETWEEN 1 AND 168'),
        CheckConstraint('dia_semana BETWEEN 1 AND 7'),
        UniqueConstraint('instancia_id', 'hora_relativa'),
    )

class ParametroGeneral(Base):
    __tablename__ = "parametro_general"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False, unique=True)
    
    # Parámetros del modelo
    r_max = Column(Integer, default=12)
    t_periodos = Column(Integer, default=8)
    mu_productividad = Column(Integer, default=20)
    w_max_gruas_bloque = Column(Integer, default=2)
    k_permanencia_min = Column(Integer, default=2)
    
    # Pesos función objetivo
    alpha_balance = Column(Float, default=1.0)
    beta_cambios = Column(Float, default=0.5)
    gamma_utilizacion = Column(Float, default=0.3)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="parametros")

# NUEVA TABLA para demandas por hora desde Magdalena
class DemandaHoraMagdalena(Base):
    """Guarda las demandas por hora desde D_params_168h de Magdalena"""
    __tablename__ = "demanda_hora_magdalena"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    magdalena_instance_id = Column(Integer)
    
    segregacion = Column(String(20))
    hora_absoluta = Column(Integer)  # 1-168
    hora_turno = Column(Integer)     # 1-8 dentro del turno
    
    # Demandas
    dr_recepcion = Column(Integer, default=0)
    dc_carga = Column(Integer, default=0)
    dd_descarga = Column(Integer, default=0)
    de_entrega = Column(Integer, default=0)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="demandas_hora_magdalena")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'segregacion', 'hora_turno'),
        Index('idx_demanda_hora_camila_instancia', 'instancia_id'),  # CAMBIADO
    )

class InventarioInicial(Base):
    __tablename__ = "inventario_inicial"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloque.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregacion.id"), nullable=False)
    
    # Inventarios
    contenedores_exportacion = Column(Integer, default=0)
    contenedores_importacion = Column(Integer, default=0)
    teus_exportacion = Column(Integer, default=0)
    teus_importacion = Column(Integer, default=0)
    
    # Fuente
    fuente = Column(String(20), default='magdalena')
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="inventarios")
    bloque = relationship("Bloque")
    segregacion = relationship("Segregacion", back_populates="inventarios")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'bloque_id', 'segregacion_id'),
        Index('idx_inventario_camila_instancia', 'instancia_id'),  # CAMBIADO
    )

class DemandaOperacion(Base):
    __tablename__ = "demanda_operacion"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregacion.id"), nullable=False)
    periodo_hora_id = Column(Integer, ForeignKey("periodo_hora.id"), nullable=False)
    
    # Demandas por tipo de operación
    demanda_carga = Column(Integer, default=0)
    demanda_descarga = Column(Integer, default=0)
    demanda_recepcion = Column(Integer, default=0)
    demanda_entrega = Column(Integer, default=0)
    
    # TEUs equivalentes
    teus_carga = Column(Integer, default=0)
    teus_descarga = Column(Integer, default=0)
    teus_recepcion = Column(Integer, default=0)
    teus_entrega = Column(Integer, default=0)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="demandas")
    segregacion = relationship("Segregacion", back_populates="demandas")
    periodo = relationship("PeriodoHora", back_populates="demandas")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'segregacion_id', 'periodo_hora_id'),
        Index('idx_demanda_camila_periodo', 'periodo_hora_id'),  # CAMBIADO
    )

class CapacidadBloque(Base):
    __tablename__ = "capacidad_bloque"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloque.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregacion.id"), nullable=False)
    
    # Capacidades
    bahias_asignadas = Column(Integer, default=0)
    capacidad_contenedores = Column(Integer, default=0)
    capacidad_teus = Column(Integer, default=0)
    
    # Ocupación inicial
    ocupacion_inicial_pct = Column(Float)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="capacidades")
    bloque = relationship("Bloque", back_populates="capacidades")
    segregacion = relationship("Segregacion", back_populates="capacidades")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'bloque_id', 'segregacion_id'),
    )

class AsignacionGrua(Base):
    __tablename__ = "asignacion_grua"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    grua_id = Column(Integer, ForeignKey("grua.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloque.id"), nullable=False)
    periodo_hora_id = Column(Integer, ForeignKey("periodo_hora.id"), nullable=False)
    
    # Variable de decisión Y(g,b,t)
    asignada = Column(Boolean, nullable=False, default=False)
    
    # Información adicional
    productividad_real = Column(Integer)
    movimientos_realizados = Column(Integer)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="asignaciones")
    grua = relationship("Grua", back_populates="asignaciones")
    bloque = relationship("Bloque", back_populates="asignaciones")
    periodo = relationship("PeriodoHora", back_populates="asignaciones")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'grua_id', 'periodo_hora_id'),
        Index('idx_asignacion_camila_grua', 'grua_id'),  # CAMBIADO
        Index('idx_asignacion_camila_bloque', 'bloque_id'),  # CAMBIADO
        Index('idx_asignacion_camila_periodo', 'periodo_hora_id'),  # CAMBIADO
    )

class FlujoOperacional(Base):
    __tablename__ = "flujo_operacional"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    segregacion_id = Column(Integer, ForeignKey("segregacion.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloque.id"), nullable=False)
    periodo_hora_id = Column(Integer, ForeignKey("periodo_hora.id"), nullable=False)
    
    # Flujos por tipo
    flujo_carga = Column(Integer, default=0)
    flujo_descarga = Column(Integer, default=0)
    flujo_recepcion = Column(Integer, default=0)
    flujo_entrega = Column(Integer, default=0)
    
    # TEUs equivalentes
    teus_carga = Column(Integer, default=0)
    teus_descarga = Column(Integer, default=0)
    teus_recepcion = Column(Integer, default=0)
    teus_entrega = Column(Integer, default=0)
    
    # Capacidad utilizada
    capacidad_utilizada = Column(Integer, default=0)
    utilizacion_pct = Column(Float)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="flujos")
    segregacion = relationship("Segregacion", back_populates="flujos")
    bloque = relationship("Bloque", back_populates="flujos")
    periodo = relationship("PeriodoHora", back_populates="flujos")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'segregacion_id', 'bloque_id', 'periodo_hora_id'),
        Index('idx_flujo_camila_periodo', 'periodo_hora_id'),  # CAMBIADO
        Index('idx_flujo_camila_bloque', 'bloque_id'),  # CAMBIADO
        Index('idx_flujo_camila_instancia_tipo', 'instancia_id', 'bloque_id'),  # CAMBIADO
    )

class CuotaCamion(Base):
    __tablename__ = "cuota_camion"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    periodo_hora_id = Column(Integer, ForeignKey("periodo_hora.id"), nullable=False)
    
    # Cuotas calculadas
    cuota_recepcion = Column(Integer, default=0)
    cuota_entrega = Column(Integer, default=0)
    cuota_total = Column(Integer, default=0)
    
    # Capacidad disponible
    capacidad_disponible = Column(Integer)
    holgura_sistema = Column(Integer)
    
    # Métricas
    utilizacion_esperada = Column(Float)
    tiempo_espera_estimado = Column(Integer)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="cuotas")
    periodo = relationship("PeriodoHora", back_populates="cuotas")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'periodo_hora_id'),
        Index('idx_cuota_camila_instancia', 'instancia_id', 'cuota_total'),  # CAMBIADO

    )

class DisponibilidadBloque(Base):
    __tablename__ = "disponibilidad_bloque"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    bloque_id = Column(Integer, ForeignKey("bloque.id"), nullable=False)
    periodo_hora_id = Column(Integer, ForeignKey("periodo_hora.id"), nullable=False)
    
    # Disponibilidad
    movimientos_disponibles = Column(Integer, default=0)
    capacidad_total = Column(Integer)
    capacidad_utilizada = Column(Integer)
    capacidad_libre = Column(Integer)
    
    # Métricas
    utilizacion_pct = Column(Float)
    congestion_index = Column(Float)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="disponibilidades")
    bloque = relationship("Bloque", back_populates="disponibilidades")
    periodo = relationship("PeriodoHora", back_populates="disponibilidades")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'bloque_id', 'periodo_hora_id'),
        Index('idx_disponibilidad_camila_congestion', 'instancia_id', 'congestion_index'),  # CAMBIADO

    )

class MetricaResultado(Base):
    __tablename__ = "metrica_resultado"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False, unique=True)
    
    # Función objetivo
    valor_funcion_objetivo = Column(Float)
    gap_optimalidad = Column(Float)
    
    # KPIs de balance
    desviacion_estandar_carga = Column(Float)
    coeficiente_variacion = Column(Float)
    indice_balance = Column(Float)
    
    # KPIs de grúas
    gruas_utilizadas_promedio = Column(Float)
    utilizacion_gruas_pct = Column(Float)
    cambios_bloque_total = Column(Integer)
    cambios_por_grua_promedio = Column(Float)
    productividad_promedio = Column(Float)
    
    # KPIs de flujo
    movimientos_totales = Column(Integer)
    cumplimiento_carga_pct = Column(Float)
    cumplimiento_descarga_pct = Column(Float)
    cumplimiento_recepcion_pct = Column(Float)
    cumplimiento_entrega_pct = Column(Float)
    
    # KPIs de congestión
    congestion_maxima = Column(Float)
    bloque_mas_congestionado = Column(String(10))
    hora_pico = Column(Integer)
    
    # KPIs de camiones
    cuota_total_turno = Column(Integer)
    cuota_promedio_hora = Column(Float)
    uniformidad_cuotas = Column(Float)
    
    # Tiempos
    fecha_calculo = Column(DateTime, default=func.now())
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="metricas")
    
    __table_args__ = (
        Index('idx_metrica_camila_fecha', 'fecha_calculo'),  # CAMBIADO
    )

class ProductividadGrua(Base):
    __tablename__ = "productividad_grua"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    grua_id = Column(Integer, ForeignKey("grua.id"), nullable=False)
    
    # Métricas agregadas
    horas_trabajadas = Column(Float)
    movimientos_totales = Column(Integer)
    productividad_real = Column(Float)
    eficiencia_pct = Column(Float)
    
    # Detalle por tipo
    movimientos_carga = Column(Integer, default=0)
    movimientos_descarga = Column(Integer, default=0)
    movimientos_recepcion = Column(Integer, default=0)
    movimientos_entrega = Column(Integer, default=0)
    
    # Cambios
    cambios_bloque = Column(Integer, default=0)
    bloques_atendidos = Column(Integer, default=0)
    
    # Relaciones
    grua = relationship("Grua", back_populates="productividades")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'grua_id'),
        Index('idx_productividad_camila_eficiencia', 'instancia_id', 'eficiencia_pct'),  # CAMBIADO

    )

class IntegracionMagdalena(Base):
    __tablename__ = "integracion_magdalena"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_camila_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False, unique=True)
    magdalena_instance_id = Column(Integer, nullable=False)
    
    # Control de importación
    fecha_importacion = Column(DateTime, default=func.now())
    estado_importacion = Column(String(20))
    mensaje_error = Column(Text)
    
    # Datos importados (snapshot)
    datos_inventario = Column(JSON)
    datos_demanda = Column(JSON)
    datos_capacidad = Column(JSON)
    
    # Archivos procesados
    archivo_instancia_magdalena = Column(String(500))
    archivo_resultado_magdalena = Column(String(500))
    
    # Validación
    checksum_datos = Column(String(64))
    validacion_coherencia = Column(Boolean, default=False)
    detalles_validacion = Column(JSON)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="integracion")
    
    __table_args__ = (
        CheckConstraint("estado_importacion IN ('pendiente', 'procesando', 'completado', 'error')"),
    )

# NUEVA TABLA para configuraciones del sistema
class ConfiguracionSistema(Base):
    __tablename__ = "configuracion_sistema"
    
    id = Column(Integer, primary_key=True, index=True)
    clave = Column(String(50), unique=True, nullable=False)
    valor = Column(String(200), nullable=False)
    tipo = Column(String(20), nullable=False)  # 'int', 'float', 'string'
    descripcion = Column(Text)
    activo = Column(Boolean, default=True)
    fecha_creacion = Column(DateTime, default=func.now())
    fecha_actualizacion = Column(DateTime, onupdate=func.now())
    
    __table_args__ = (
        CheckConstraint("tipo IN ('int', 'float', 'string', 'boolean')"),
    )

# NUEVA TABLA para configuraciones por instancia
class ConfiguracionInstancia(Base):
    __tablename__ = "configuracion_instancia"
    
    id = Column(Integer, primary_key=True, index=True)
    instancia_id = Column(Integer, ForeignKey("instancia_camila.id"), nullable=False)
    clave = Column(String(50), nullable=False)
    valor = Column(String(200), nullable=False)
    
    # Relaciones
    instancia = relationship("InstanciaCamila", back_populates="configuraciones")
    
    __table_args__ = (
        UniqueConstraint('instancia_id', 'clave'),
    )