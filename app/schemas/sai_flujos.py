# app/schemas/sai_flujos.py
from pydantic import BaseModel, Field, validator
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID

# Schemas de entrada
class SAIConfigurationCreate(BaseModel):
    fecha: datetime
    semana: int = Field(..., ge=1, le=52)
    participacion: int = Field(68, ge=1, le=100)
    con_dispersion: bool = True

class SAIFileUpload(BaseModel):
    flujos_file: str
    instancia_file: str
    evolucion_file: Optional[str] = None
    analisis_file: Optional[str] = None

# Schemas de segregaciones
class SegregacionInfo(BaseModel):
    id: str
    nombre: str
    teus: int
    tipo: str
    color: str
    
class SegregacionVolumen(BaseModel):
    segregacion_id: str
    segregacion_nombre: str
    volumen: int
    bahias_estimadas: int
    ocupacion_porcentaje: float

# Schema para vista de bahías
class BahiaCell(BaseModel):
    segregacion: Optional[str] = None
    color: Optional[str] = None
    percentage: float = 0
    volumen_teus: Optional[int] = None
    capacidad_teus: Optional[int] = None

class BlockBahiasView(BaseModel):
    bloque: str
    turno: int
    hora: str
    
    # Matriz 7x30
    occupancy_matrix: List[List[Optional[BahiaCell]]]
    
    # Estadísticas
    bahias_ocupadas: int
    ocupacion_real: float  # Porcentaje basado en volumen/capacidad
    segregaciones_activas: int
    total_volumen_teus: int
    capacidad_total_teus: int
    
    # Detalle por segregación
    segregaciones_stats: Dict[str, Dict[str, Any]]

class SAIMetrics(BaseModel):
    """Métricas similares a MagdalenaMetrics pero para datos SAI"""
    # Identificación
    config_id: UUID
    fecha: datetime
    semana: int
    turno: int
    
    # Métricas principales
    total_movimientos: int
    total_volumen_teus: int
    bloques_activos: int
    segregaciones_activas: int
    
    # Ocupación
    ocupacion_promedio: float
    ocupacion_por_bloque: Dict[str, float]
    
    # Distribución
    distribucion_segregaciones: List[Dict[str, Any]]
    volumen_por_bloque: Dict[str, int]
    
    # Para vista de bahías
    bahias_por_bloque: Dict[str, Dict[str, int]]  # {bloque: {segregacion: bahias}}
    volumen_por_bloque_segregacion: Dict[str, Dict[str, int]]  # {bloque: {segregacion: volumen}}
    
    # Catálogos
    segregaciones_info: Dict[str, SegregacionInfo]
    capacidades_bloques: Dict[str, Dict[str, Any]]
    segregaciones_colores: Dict[str, str]

# Schemas para queries
class SAITimeFilter(BaseModel):
    fecha_inicio: Optional[datetime] = None
    fecha_fin: Optional[datetime] = None
    semana: Optional[int] = Field(None, ge=1, le=52)
    turno: Optional[int] = Field(None, ge=1, le=3)
    bloque: Optional[str] = None

class PaginationParams(BaseModel):
    skip: int = Field(0, ge=0)
    limit: int = Field(100, ge=1, le=1000)

# Response schemas
class SAIConfigurationResponse(BaseModel):
    id: UUID
    fecha: datetime
    semana: int
    participacion: int
    con_dispersion: bool
    fecha_carga: datetime
    
class Config:
    from_attributes = True

class SAIConfigurationList(BaseModel):
    total: int
    items: List[SAIConfigurationResponse]

class SAIVolumenResponse(BaseModel):
    bloque: str
    turno: int
    volumen_teus: int
    ocupacion: float
    segregaciones: List[SegregacionVolumen]

class SAIComparisonResponse(BaseModel):
    """Comparación entre SAI real y Magdalena optimizado"""
    fecha: datetime
    semana: int
    turno: int
    
    sai_metrics: Dict[str, Any]
    magdalena_metrics: Optional[Dict[str, Any]]
    
    comparacion: Dict[str, Any]

# Schema para carga de archivos
class LoadResult(BaseModel):
    success: bool
    message: str
    config_id: Optional[UUID] = None
    errors: List[str] = []
    statistics: Dict[str, Any] = {}
    
    
