# app/schemas/camila.py
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

# Schemas de entrada
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
    filename: str

# Schemas de salida
class CamilaRunSummary(BaseModel):
    id: UUID
    semana: int
    dia: str
    turno: int
    modelo_tipo: str
    con_segregaciones: bool
    fecha_carga: datetime
    total_movimientos: int
    balance_workload: float
    indice_congestion: float
    
    class Config:
        orm_mode = True

class CamilaConfiguration(BaseModel):
    semana: int
    dia: str
    turno: int
    modelo_tipo: str
    con_segregaciones: bool
    disponible: bool = True

class FlowMatrix(BaseModel):
    """Matriz de flujos [bloques][tiempos]"""
    data: List[List[float]]
    total: float
    
    @validator('data')
    def validate_matrix_size(cls, v):
        if len(v) != 9:  # 9 bloques
            raise ValueError("La matriz debe tener 9 filas (bloques)")
        for row in v:
            if len(row) != 8:  # 8 períodos
                raise ValueError("Cada fila debe tener 8 columnas (períodos)")
        return v

class GrueAssignment(BaseModel):
    """Asignación de grúas [gruas][bloques*tiempos]"""
    data: List[List[int]]
    utilization_by_grue: List[float]
    
    @validator('data')
    def validate_assignment(cls, v):
        if len(v) != 12:  # 12 grúas
            raise ValueError("Debe haber 12 grúas")
        for row in v:
            if len(row) != 72:  # 9 bloques * 8 tiempos
                raise ValueError("Cada grúa debe tener 72 slots (9 bloques * 8 tiempos)")
        return v

class CamilaKPIs(BaseModel):
    total_movimientos: int
    balance_workload: float
    indice_congestion: float
    utilizacion_promedio: float
    desviacion_std_bloques: float
    desviacion_std_tiempo: float
    participacion_bloques: List[float]
    participacion_tiempo: List[float]

class ComparisonMetrics(BaseModel):
    workload_balance_improvement: float
    congestion_reduction: float
    resource_utilization: float
    total_movements_diff: int

class CamilaResults(BaseModel):
    # Identificación
    run_id: UUID
    config: CamilaConfiguration
    
    # Asignación de grúas
    grue_assignment: GrueAssignment
    
    # Flujos por tipo
    reception_flow: FlowMatrix
    delivery_flow: FlowMatrix
    loading_flow: FlowMatrix
    unloading_flow: FlowMatrix
    total_flows: FlowMatrix
    
    # Capacidad y disponibilidad
    capacity: FlowMatrix
    availability: FlowMatrix
    
    # Cuotas recomendadas
    recommended_quotas: FlowMatrix
    
    # KPIs
    kpis: CamilaKPIs
    
    # Datos reales (si están disponibles)
    real_data: Optional[FlowMatrix] = None
    
    # Comparación (si hay datos reales)
    comparison: Optional[ComparisonMetrics] = None

class GrueDetail(BaseModel):
    grua: str
    tiempo: int
    bloque_asignado: Optional[str]
    movimientos_realizados: int
    utilizacion: float

class BlockHourDetail(BaseModel):
    bloque: str
    hora: int
    flujo_total: float
    capacidad: float
    disponibilidad: float
    cuota_recomendada: int
    gruas_asignadas: List[str]

class CamilaDetailedView(BaseModel):
    """Vista detallada para análisis específicos"""
    run_id: UUID
    config: CamilaConfiguration
    
    # Por grúa
    gruas_detail: List[GrueDetail]
    
    # Por bloque-hora
    blocks_detail: List[BlockHourDetail]
    
    # Estadísticas agregadas
    stats: Dict[str, Any]

# Schemas para filtros y queries
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

# Response schemas
class CamilaRunsResponse(BaseModel):
    total: int
    items: List[CamilaRunSummary]
    page: int
    pages: int

class HealthCheck(BaseModel):
    status: str = "ok"
    tables_exist: bool
    total_runs: int
    last_update: Optional[datetime]