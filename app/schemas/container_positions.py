# app/schemas/container_positions.py
from pydantic import BaseModel
from typing import Dict, List, Optional, Any, Union
from datetime import date

class BahiaCell(BaseModel):
    segregacion: str
    color: str
    percentage: float
    gkey: Optional[str] = None
    nominal_length: Optional[int] = None
    requires_power: Optional[bool] = None
    hazardous: Optional[bool] = None

class SegregacionStats(BaseModel):
    color: str
    count: int
    bahias: int
    volumen: float
    porcentajeOcupacion: float
    tipo: str
    reefer: Optional[int] = 0
    hazardous: Optional[int] = 0

class BlockPositionsResponse(BaseModel):
    bloque: str
    turno: int
    fecha: date
    hora: Optional[str] = None
    bahiasOcupadas: int
    ocupacionReal: float
    segregacionesActivas: int
    totalVolumenTeus: int
    capacidadTotalTeus: int
    bahiasPorBloque: Dict[str, Dict[str, int]]
    volumenPorBloque: Dict[str, Dict[str, float]]
    segregacionesInfo: Dict[str, Dict[str, Any]]
    segregacionesStats: Dict[str, SegregacionStats]
    occupancyMatrix: List[List[Optional[BahiaCell]]]
    capacidadesPorBloque: Optional[Dict[str, int]] = None
    teusPorSegregacion: Optional[Dict[str, int]] = None

class ContainerMetrics(BaseModel):
    fecha: date
    turno: int
    totalMovimientos: int
    totalVolumenTeus: int
    bloquesActivos: int
    segregacionesActivas: int
    ocupacionPromedio: float
    ocupacionPorBloque: Dict[str, float]
    bahiasPorBloque: Dict[str, Any]
    volumenPorBloque: Dict[str, Any]
    segregacionesInfo: Dict[str, Dict[str, Any]]
    capacidadesPorBloque: Dict[str, int]
    teusPorSegregacion: Dict[str, int]