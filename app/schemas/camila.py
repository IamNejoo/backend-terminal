# app/schemas/camila.py
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime, date, time
from enum import Enum

# Enums
class EstadoInstancia(str, Enum):
    pendiente = "pendiente"
    ejecutando = "ejecutando"
    completado = "completado"
    error = "error"

class TipoContenedor(str, Enum):
    veinte = "20"
    cuarenta = "40"

# Base Schemas
class BloqueBase(BaseModel):
    codigo: str
    nombre: Optional[str] = None
    grupo_movimiento: int
    capacidad_teus: int
    
class SegregacionBase(BaseModel):
    codigo: str
    descripcion: Optional[str] = None
    tipo_contenedor: TipoContenedor
    categoria: str

class GruaBase(BaseModel):
    codigo: str
    tipo: str = "RTG"
    productividad_nominal: int = 20

# Instance Creation
class InstanciaCamilaCreate(BaseModel):
    anio: int = Field(..., ge=2017, le=2030)
    semana: int = Field(..., ge=1, le=52)
    fecha: date
    turno: int = Field(..., ge=1, le=21)
    participacion: int = Field(..., ge=0, le=100)
    magdalena_instance_id: Optional[int] = None
    version_modelo: str = "1.0"

# Response Models
class InstanciaCamilaResponse(BaseModel):
    id: int
    anio: int
    semana: int
    fecha: date
    turno: int
    participacion: int
    estado: EstadoInstancia
    fecha_creacion: datetime
    fecha_ejecucion: Optional[datetime]
    mensaje_error: Optional[str]
    magdalena_instance_id: Optional[int]
    
    class Config:
        from_attributes = True

# Dashboard Models
class KPIBalance(BaseModel):
    funcion_objetivo: float
    coeficiente_variacion: float
    indice_balance: float
    desviacion_estandar: float

class KPIGruas(BaseModel):
    utilizacion_promedio: float
    gruas_activas_promedio: float
    productividad_promedio: float
    cambios_totales: int
    eficiencia_pct: float

class KPIFlujos(BaseModel):
    movimientos_totales: int
    cumplimiento_carga: float
    cumplimiento_descarga: float
    cumplimiento_recepcion: float
    cumplimiento_entrega: float
    distribucion: Dict[str, int]

class KPICamiones(BaseModel):
    cuota_total: int
    cuota_promedio: float
    cuota_maxima: int
    cuota_minima: int
    uniformidad: float
    tiempo_espera_promedio: int

class DashboardResponse(BaseModel):
    instancia: InstanciaCamilaResponse
    balance: KPIBalance
    gruas: KPIGruas
    flujos: KPIFlujos
    camiones: KPICamiones
    congestion_maxima: float
    bloque_mas_congestionado: str
    hora_pico: int

# Crane Assignment Models
class AsignacionGruaHora(BaseModel):
    grua: str
    bloque: str
    hora: int
    productividad: Optional[int]
    movimientos: Optional[int]

class ResumenAsignacion(BaseModel):
    gruas_por_hora: Dict[int, int]
    cambios_por_grua: Dict[str, int]
    total_cambios: int

class AsignacionGruaResponse(BaseModel):
    instancia_id: int
    asignaciones: List[AsignacionGruaHora]
    resumen: ResumenAsignacion

# Flow Models
class FlujoHora(BaseModel):
    hora: int
    carga: int
    descarga: int
    recepcion: int
    entrega: int
    total: int

class FlujoBloque(BaseModel):
    bloque: str
    carga: int
    descarga: int
    recepcion: int
    entrega: int
    total: int

class FlujosResponse(BaseModel):
    instancia_id: int
    por_hora: List[FlujoHora]
    por_bloque: List[FlujoBloque]
    totales: Dict[str, int]

# Truck Quota Models
class CuotaHora(BaseModel):
    hora: int
    hora_inicio: time
    hora_fin: time
    cuota_recepcion: int
    cuota_entrega: int
    cuota_total: int
    capacidad_disponible: int
    utilizacion_esperada: float

class CuotasResponse(BaseModel):
    instancia_id: int
    cuotas: List[CuotaHora]
    total_turno: int
    promedio_hora: float
    uniformidad: float

# Balance Models
class BalanceBloque(BaseModel):
    bloque: str
    movimientos_totales: int
    utilizacion_promedio: float
    congestion_maxima: float
    gruas_asignadas: int
    capacidad_total: int

class BalanceResponse(BaseModel):
    instancia_id: int
    bloques: List[BalanceBloque]
    coeficiente_variacion: float
    balance_score: float

# Timeline Models
class EventoTimeline(BaseModel):
    hora: int
    tipo: str
    descripcion: str
    valor: Optional[float]
    bloque: Optional[str] = None

class TimelineResponse(BaseModel):
    instancia_id: int
    eventos: List[EventoTimeline]

# File Upload Models
class UploadResponse(BaseModel):
    success: bool
    message: str
    instance_id: Optional[int] = None
    errors: List[str] = []

# Integration Models
class MagdalenaImportRequest(BaseModel):
    magdalena_instance_id: int
    anio: int
    semana: int
    turno: int = Field(..., ge=1, le=21)
    participacion: int

class MagdalenaImportResponse(BaseModel):
    success: bool
    instance_id: Optional[int]
    inventario_importado: Dict[str, int]
    demanda_importada: Dict[str, int]
    capacidad_importada: Dict[str, int]
    mensaje: str

# Validation Models
class ValidacionCoherencia(BaseModel):
    es_coherente: bool
    mensaje: str
    detalles: Dict[str, Any]

# Stats Models
class EstadisticasGenerales(BaseModel):
    total_instancias: int
    instancias_completadas: int
    instancias_error: int
    promedio_funcion_objetivo: float
    promedio_utilizacion_gruas: float
    promedio_cumplimiento: float

# List Response
class InstanciaListResponse(BaseModel):
    items: List[InstanciaCamilaResponse]
    total: int
    page: int
    size: int
    pages: int

# Detailed Metrics
class MetricasDetalladas(BaseModel):
    funcion_objetivo: float
    gap_optimalidad: Optional[float]
    tiempo_ejecucion_ms: Optional[int]
    iteraciones: Optional[int]
    detalles_balance: Dict[str, float]
    detalles_gruas: Dict[str, Any]
    detalles_flujos: Dict[str, Any]
    detalles_congestion: Dict[str, float]

# Error Response
class ErrorResponse(BaseModel):
    detail: str
    errors: Optional[List[Dict[str, Any]]] = None

# Utilidades para validación
class FileValidation(BaseModel):
    is_valid: bool
    file_type: str  # 'instance', 'results', 'magdalena' o 'unknown'
    missing_sheets: List[str]
    extra_sheets: List[str]
    errors: List[str]

# Comparación con Real
class ComparacionReal(BaseModel):
    instancia_modelo: int
    instancia_real: Optional[int]
    mejora_utilizacion: float
    mejora_productividad: float
    reduccion_cambios: float
    mejora_balance: float
    detalles: Dict[str, Any]

# Configuración
class ConfiguracionResponse(BaseModel):
    id: int
    clave: str
    valor: str
    tipo: str
    descripcion: Optional[str]
    activo: bool
    
    class Config:
        from_attributes = True

class ConfiguracionUpdate(BaseModel):
    valor: str
    activo: Optional[bool] = True

# Demanda Hora Magdalena
class DemandaHoraMagdalenaResponse(BaseModel):
    segregacion: str
    hora_turno: int
    dr_recepcion: int
    dc_carga: int
    dd_descarga: int
    de_entrega: int
    
    class Config:
        from_attributes = True

# Grúa Detalle
class GruaDetalleResponse(BaseModel):
    codigo: str
    tipo: str
    productividad_nominal: int
    activa: bool
    en_mantenimiento: bool
    asignaciones_turno: int
    productividad_real: Optional[float]
    eficiencia: Optional[float]
    
    class Config:
        from_attributes = True

# Bloque Detalle
class BloqueDetalleResponse(BaseModel):
    codigo: str
    nombre: Optional[str]
    grupo_movimiento: int
    capacidad_teus: int
    bahias_totales: int
    ocupacion_actual: Optional[float]
    gruas_asignadas: Optional[int]
    movimientos_hora: Optional[int]
    
    class Config:
        from_attributes = True

# Segregación Detalle
class SegregacionDetalleResponse(BaseModel):
    codigo: str
    descripcion: Optional[str]
    tipo_contenedor: str
    categoria: str
    inventario_inicial: Optional[int]
    movimientos_planificados: Optional[int]
    bloques_asignados: Optional[List[str]]
    
    class Config:
        from_attributes = True

# Análisis Temporal
class AnalisisTemporal(BaseModel):
    instancia_id: int
    periodo: str  # 'hora', 'turno', 'dia', 'semana'
    datos: List[Dict[str, Any]]
    tendencia: str  # 'creciente', 'decreciente', 'estable'
    proyeccion: Optional[List[Dict[str, Any]]]

# Alerta Operacional
class AlertaOperacional(BaseModel):
    tipo: str  # 'congestion', 'desbalance', 'baja_productividad'
    severidad: str  # 'baja', 'media', 'alta', 'critica'
    bloque: Optional[str]
    hora: Optional[int]
    descripcion: str
    recomendacion: str
    valor_actual: float
    valor_limite: float

class AlertasResponse(BaseModel):
    instancia_id: int
    alertas: List[AlertaOperacional]
    resumen: Dict[str, int]  # Conteo por severidad

# Optimización Sugerida
class OptimizacionSugerida(BaseModel):
    tipo: str  # 'reasignacion_grua', 'cambio_cuota', 'redistribucion_flujo'
    descripcion: str
    impacto_esperado: str
    bloques_afectados: List[str]
    horas_afectadas: List[int]
    mejora_estimada: float  # Porcentaje

class SugerenciasResponse(BaseModel):
    instancia_id: int
    sugerencias: List[OptimizacionSugerida]
    mejora_total_estimada: float

# Reporte Ejecutivo
class ReporteEjecutivo(BaseModel):
    periodo: str
    fecha_generacion: datetime
    resumen_operacional: Dict[str, Any]
    kpis_principales: Dict[str, float]
    alertas_criticas: List[str]
    comparacion_periodo_anterior: Optional[Dict[str, float]]
    recomendaciones: List[str]

# Histórico Comparativo
class HistoricoComparativo(BaseModel):
    instancia_actual: int
    instancias_comparadas: List[int]
    periodo: str
    metricas: Dict[str, List[float]]  # metrica -> [valores por instancia]
    mejora_promedio: Dict[str, float]
    mejor_instancia: Dict[str, int]  # metrica -> instancia_id

# Validadores personalizados
class CuotaValidator(BaseModel):
    cuota_recepcion: int = Field(..., ge=0)
    cuota_entrega: int = Field(..., ge=0)
    
    @validator('cuota_recepcion', 'cuota_entrega')
    def validar_cuota_positiva(cls, v):
        if v < 0:
            raise ValueError('Las cuotas deben ser positivas')
        return v

class TurnoValidator(BaseModel):
    turno: int = Field(..., ge=1, le=21)
    
    @validator('turno')
    def validar_turno(cls, v):
        if not 1 <= v <= 21:
            raise ValueError('El turno debe estar entre 1 y 21')
        return v

# Respuesta de procesamiento asíncrono
class ProcesamientoAsincronoResponse(BaseModel):
    task_id: str
    estado: str  # 'pendiente', 'procesando', 'completado', 'error'
    progreso: Optional[int]  # 0-100
    mensaje: Optional[str]
    resultado: Optional[Dict[str, Any]]
    tiempo_estimado: Optional[int]  # segundos

# Exportación de datos
class ExportacionRequest(BaseModel):
    formato: str = Field(..., pattern="^(excel|csv|json)$")
    incluir_metricas: bool = True
    incluir_flujos: bool = True
    incluir_asignaciones: bool = True
    comprimir: bool = False

class ExportacionResponse(BaseModel):
    success: bool
    archivo_url: Optional[str]
    tamaño_bytes: Optional[int]
    formato: str
    fecha_expiracion: Optional[datetime]