from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Any
from datetime import datetime
from uuid import UUID

class MagdalenaKPIs(BaseModel):
    """KPIs calculados del modelo"""
    reubicaciones_eliminadas: int
    eficiencia_ganada: float
    segregaciones_activas: int
    balance_carga: float
    ocupacion_promedio: float
    carga_trabajo_total: int
    movimientos_optimizados: int
    variacion_carga: int

class MovimientosComparacion(BaseModel):
    """Comparación de movimientos real vs optimizado"""
    tipo: str
    real: int
    optimizado: int
    diferencia: int
    porcentaje_mejora: float

class BloqueInfo(BaseModel):
    """Información de un bloque"""
    bloqueId: str
    ocupacionPromedio: float
    ocupacionTurno: float
    cargaTrabajo: float
    capacidad: float
    estado: str

class MagdalenaDashboard(BaseModel):
    """Response completo para el dashboard"""
    magdalenaMetrics: Dict[str, Any]
    realMetrics: Dict[str, Any]
    comparison: Dict[str, Any]
    lastUpdated: str
    dataNotAvailable: bool = False

class MagdalenaLoadRequest(BaseModel):
    """Request para cargar datos"""
    fecha: str = Field(..., pattern="^\d{4}-\d{2}-\d{2}$")  # CAMBIO: pattern en lugar de regex
    participacion: int = Field(..., ge=1, le=100)
    con_dispersion: bool

class MagdalenaFilter(BaseModel):
    """Filtros para consulta"""
    anio: int = Field(...)
    semana: int = Field(..., ge=1, le=52)
    turno: int = Field(..., ge=1, le=21)
    participacion: int = Field(...)
    dispersion: str = Field(..., pattern="^[KN]$")  # CAMBIO: pattern en lugar de regex