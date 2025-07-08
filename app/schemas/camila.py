# app/schemas/camila.py
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
from uuid import UUID

# ===================== Schemas de entrada =====================

class CamilaConfigInput(BaseModel):
    semana: int = Field(..., ge=1, le=52, description="Número de semana")
    dia: str = Field(..., description="Día de la semana en inglés")
    turno: int = Field(..., ge=1, le=3, description="Número de turno")
    modelo_tipo: str = Field(..., pattern="^(minmax|maxmin)$", description="Tipo de modelo")
    con_segregaciones: bool = Field(True, description="Incluir segregaciones")
    
    @validator('dia')
    def validate_dia(cls, v):
        valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if v not in valid_days:
            raise ValueError(f"Día debe ser uno de: {', '.join(valid_days)}")
        return v

class CamilaFileUpload(BaseModel):
    config: CamilaConfigInput
    resultado_filename: str
    instancia_filename: str

# ===================== Schemas de variables =====================

class VariableInfo(BaseModel):
    """Información de una variable del modelo"""
    variable: str
    indice: Optional[str]
    valor: float
    # Campos parseados
    segregacion: Optional[str]
    grua: Optional[str]
    bloque: Optional[str]
    tiempo: Optional[int]
    tipo_variable: str

class VariablesSummary(BaseModel):
    """Resumen de variables por tipo"""
    flujos_recepcion: List[VariableInfo]
    flujos_entrega: List[VariableInfo]
    asignacion_gruas: List[VariableInfo]
    alpha_variables: List[VariableInfo]
    z_variables: List[VariableInfo]
    funcion_objetivo: float
    total_variables: int

# ===================== Schemas de métricas =====================

class MetricasBloque(BaseModel):
    """Métricas por bloque"""
    bloque: str
    movimientos_total: int
    recepcion: int
    entrega: int
    gruas_asignadas: int
    periodos_activos: int
    participacion: float
    utilizacion: float

class MetricasGrua(BaseModel):
    """Métricas por grúa"""
    grua: str
    periodos_activos: int
    bloques_asignados: List[str]
    movimientos_teoricos: int
    utilizacion: float
    asignaciones: List[Dict[str, Any]]  # [{tiempo: 1, bloque: "b3"}, ...]

class MetricasTiempo(BaseModel):
    """Métricas por período de tiempo"""
    tiempo: int
    hora_real: str  # "08:00", "09:00", etc
    movimientos_total: int
    gruas_activas: int
    bloques_activos: int
    participacion: float

class MetricasSegregacion(BaseModel):
    """Métricas por segregación"""
    segregacion: str
    descripcion: str
    tipo: str  # exportacion/importacion
    movimientos_recepcion: int
    movimientos_entrega: int
    bloques: List[str]

# ===================== Schemas de salida principales =====================

class CamilaRunSummary(BaseModel):
    """Resumen de un run"""
    id: UUID
    semana: int
    dia: str
    turno: int
    modelo_tipo: str
    con_segregaciones: bool
    fecha_carga: datetime
    funcion_objetivo: float
    total_movimientos: int
    balance_workload: float
    indice_congestion: float
    gruas_activas: int
    bloques_activos: int
    
    class Config:
        orm_mode = True

class CamilaResults(BaseModel):
    """Resultados completos del modelo"""
    # Identificación
    run_id: UUID
    config: CamilaConfigInput
    
    # Métricas principales
    funcion_objetivo: float
    total_movimientos: int
    balance_workload: float
    indice_congestion: float
    utilizacion_sistema: float
    
    # Resumen de variables
    variables_summary: VariablesSummary
    
    # Métricas agregadas
    metricas_bloques: List[MetricasBloque]
    metricas_gruas: List[MetricasGrua]
    metricas_tiempo: List[MetricasTiempo]
    metricas_segregaciones: List[MetricasSegregacion]
    
    # Matrices para visualización
    matriz_flujos: List[List[float]]  # [9 bloques][8 tiempos]
    matriz_gruas: List[List[int]]  # [12 gruas][72 slots]
    matriz_capacidad: List[List[float]]  # [9 bloques][8 tiempos]
    matriz_disponibilidad: List[List[float]]  # [9 bloques][8 tiempos]
    
    # Distribuciones porcentuales
    participacion_bloques: List[float]  # 9 elementos
    participacion_tiempo: List[float]  # 8 elementos
    
    # Parámetros del modelo
    parametros: Dict[str, Any]  # mu, W, K, Rmax, etc

class GruaTimeline(BaseModel):
    """Timeline de una grúa"""
    grua: str
    timeline: List[Dict[str, Any]]  # [{tiempo: 1, bloque: "b3", tipo: "ygbt"}, ...]
    total_periodos: int
    bloques_unicos: int
    utilizacion: float

class BlockDetail(BaseModel):
    """Detalle de un bloque específico"""
    bloque: str
    movimientos_por_tiempo: List[int]
    gruas_por_tiempo: List[List[str]]
    capacidad_por_tiempo: List[int]
    disponibilidad_por_tiempo: List[int]
    segregaciones: List[str]
    total_movimientos: int
    utilizacion_promedio: float

# ===================== Schemas para comparaciones =====================

class ModelComparison(BaseModel):
    """Comparación entre dos modelos o configuraciones"""
    config1: CamilaConfigInput
    config2: CamilaConfigInput
    
    metricas_comparadas: Dict[str, Dict[str, float]]  # {"metrica": {"modelo1": valor, "modelo2": valor}}
    mejoras: Dict[str, float]  # {"balance": 15.2, "congestion": -8.5, ...}
    
    distribucion_bloques1: List[float]
    distribucion_bloques2: List[float]
    
    recomendacion: str
    analisis: List[str]

# ===================== Schemas para filtros y queries =====================

class CamilaFilter(BaseModel):
    semana_min: Optional[int] = Field(None, ge=1, le=52)
    semana_max: Optional[int] = Field(None, ge=1, le=52)
    dia: Optional[str] = None
    turno: Optional[int] = Field(None, ge=1, le=3)
    modelo_tipo: Optional[str] = Field(None, pattern="^(minmax|maxmin)$")
    con_segregaciones: Optional[bool] = None

class PaginationParams(BaseModel):
    skip: int = Field(0, ge=0)
    limit: int = Field(10, ge=1, le=100)
    order_by: str = Field("fecha_carga", pattern="^(fecha_carga|semana|total_movimientos|balance_workload)$")
    order_desc: bool = True

# ===================== Schemas de respuesta =====================

class CamilaRunsResponse(BaseModel):
    total: int
    items: List[CamilaRunSummary]
    page: int
    pages: int

class HealthCheck(BaseModel):
    status: str = "ok"
    service: str = "camila"
    version: str = "2.0"
    tables_exist: bool
    total_runs: int
    last_update: Optional[datetime]

class UploadResponse(BaseModel):
    message: str
    run_id: UUID
    config: CamilaConfigInput
    stats: Dict[str, Any]  # {"variables_loaded": 186, "gruas_activas": 11, ...}