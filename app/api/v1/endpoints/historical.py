# app/api/v1/endpoints/historical.py
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, or_, case
from collections import defaultdict
import hashlib
import logging
import statistics
import numpy as np

from app.core.database import get_db
from app.models.historical_movements import HistoricalMovement
from app.models.container_dwell_time import ContainerDwellTime
from app.models.truck_turnaround_time import TruckTurnaroundTime

router = APIRouter()
logger = logging.getLogger(__name__)

# Constantes del frontend (desde usePortKPIs)
CAPACIDADES_BLOQUES = {
    'C1': 1008, 'C2': 1008, 'C3': 1008, 'C4': 1008, 'C5': 1008,
    'C6': 1008, 'C7': 1008, 'C8': 1008, 'C9': 1008,
    'H1': 866, 'H2': 866, 'H3': 866, 'H4': 866, 'H5': 1050,
    'T1': 714, 'T2': 714, 'T3': 714, 'T4': 714
}

CAPACIDAD_TOTAL_TERMINAL = 16254

PATIO_BLOCKS = {
    'costanera': ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9'],
    'ohiggins': ['H1', 'H2', 'H3', 'H4', 'H5'],
    'tebas': ['T1', 'T2', 'T3', 'T4']
}

# Cache simple en memoria
class InMemoryCache:
    def __init__(self):
        self._cache: Dict[str, tuple[Any, datetime]] = {}
    
    def get_key(self, **kwargs) -> str:
        """Genera clave única basada en parámetros"""
        key_str = ":".join(f"{k}={v}" for k, v in sorted(kwargs.items()))
        return hashlib.md5(key_str.encode()).hexdigest()[:16]
    
    def get(self, **kwargs) -> Optional[Any]:
        key = self.get_key(**kwargs)
        if key in self._cache:
            data, expiry = self._cache[key]
            if datetime.now() < expiry:
                return data
            del self._cache[key]
        return None
    
    def set(self, data: Any, expire_minutes: int = 60, **kwargs):
        key = self.get_key(**kwargs)
        expiry = datetime.now() + timedelta(minutes=expire_minutes)
        self._cache[key] = (data, expiry)
        # Limitar tamaño del cache
        if len(self._cache) > 100:  # máximo 100 entradas
            # Eliminar las más antiguas
            oldest_key = min(self._cache.keys(), 
                           key=lambda k: self._cache[k][1])
            del self._cache[oldest_key]

# Instancia global del cache
cache = InMemoryCache()

# Funciones auxiliares
def parse_dates(start_date: str, end_date: str):
    """Parsear fechas con formato consistente"""
    if 'T' not in start_date:
        start_date = f"{start_date}T00:00:00"
    if 'T' not in end_date:
        end_date = f"{end_date}T23:59:59"
    
    start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    
    return start_dt, end_dt

def calculate_percentile(values: List[float], percentile: float) -> float:
    """Calcular percentil de una lista de valores"""
    if not values:
        return 0
    sorted_values = sorted(values)
    index = int(len(sorted_values) * percentile / 100)
    return sorted_values[min(index, len(sorted_values) - 1)]

def aggregate_inventory_stats(movements_data: List[HistoricalMovement], unit: str = "day") -> dict:
    """
    Calcula estadísticas de inventario basadas en los datos de movimientos históricos.
    """
    if not movements_data:
        return {
            'minimo': 0,
            'maximo': 0,
            'promedio': 0,
            'rango': 0,
            'variabilidad': 0,
            'percentil_90': 0,
            'percentil_95': 0
        }
    
    # Extraer todos los valores de promedio de TEUs
    promedios_teus = [m.promedio_teus for m in movements_data if m.promedio_teus is not None]
    
    if not promedios_teus:
        return {
            'minimo': 0,
            'maximo': 0,
            'promedio': 0,
            'rango': 0,
            'variabilidad': 0,
            'percentil_90': 0,
            'percentil_95': 0
        }
    
    # Calcular estadísticas básicas
    minimo = min(promedios_teus)
    maximo = max(promedios_teus)
    promedio = statistics.mean(promedios_teus)
    rango = maximo - minimo
    
    # Calcular variabilidad (coeficiente de variación)
    if len(promedios_teus) > 1 and promedio > 0:
        std_dev = statistics.stdev(promedios_teus)
        variabilidad = (std_dev / promedio) * 100
    else:
        variabilidad = 0
    
    # Calcular percentiles
    percentil_90 = calculate_percentile(promedios_teus, 90)
    percentil_95 = calculate_percentile(promedios_teus, 95)
    
    return {
        'minimo': round(minimo, 2),
        'maximo': round(maximo, 2),
        'promedio': round(promedio, 2),
        'rango': round(rango, 2),
        'variabilidad': round(variabilidad, 2),
        'percentil_90': round(percentil_90, 2),
        'percentil_95': round(percentil_95, 2)
    }

@router.get("/kpis/comprehensive")
async def get_comprehensive_kpis(
    start_date: str = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    unit: str = Query("day", regex="^(hour|day|week|month|year)$"),
    patio_filter: Optional[str] = Query(None),
    bloque_filter: Optional[str] = Query(None),
    operation_type: Optional[str] = Query(None, description="import/export para CDT/TTT"),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener todos los KPIs integrados: Congestión + TTT + CDT + Inventario
    """
    # Verificar cache
    cached_data = cache.get(
        endpoint="comprehensive_kpis",
        start_date=start_date,
        end_date=end_date,
        unit=unit,
        patio=patio_filter or "all",
        bloque=bloque_filter or "all",
        operation=operation_type or "all"
    )
    
    if cached_data:
        logger.info("KPIs comprehensivos obtenidos del cache")
        return cached_data
    
    try:
        # Parsear fechas
        start_dt, end_dt = parse_dates(start_date, end_date)
    except ValueError:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD")
    
    # 1. OBTENER DATOS DE MOVIMIENTOS HISTÓRICOS
    query = select(HistoricalMovement).where(
        and_(
            HistoricalMovement.hora >= start_dt,
            HistoricalMovement.hora <= end_dt
        )
    )
    
    # Aplicar filtros
    if patio_filter and patio_filter in PATIO_BLOCKS:
        bloques_patio = PATIO_BLOCKS[patio_filter]
        query = query.where(HistoricalMovement.bloque.in_(bloques_patio))
    elif bloque_filter:
        query = query.where(HistoricalMovement.bloque == bloque_filter)
    
    result = await db.execute(query)
    movements_data = result.scalars().all()
    
    # 2. OBTENER DATOS DE TTT
    ttt_query = select(
        func.avg(TruckTurnaroundTime.ttt).label('promedio'),
        func.count(TruckTurnaroundTime.id).label('total'),
        func.min(TruckTurnaroundTime.ttt).label('minimo'),
        func.max(TruckTurnaroundTime.ttt).label('maximo')
    ).where(
        and_(
            TruckTurnaroundTime.pregate_ss >= start_dt,
            TruckTurnaroundTime.pregate_ss <= end_dt,
            TruckTurnaroundTime.ttt > 0,  # Excluir valores negativos
            TruckTurnaroundTime.ttt < 1440  # Excluir valores mayores a 24 horas
        )
    )
    
    if operation_type:
        ttt_query = ttt_query.where(TruckTurnaroundTime.operation_type == operation_type)
    
    # Para obtener percentiles de TTT
    ttt_values_query = select(TruckTurnaroundTime.ttt).where(
        and_(
            TruckTurnaroundTime.pregate_ss >= start_dt,
            TruckTurnaroundTime.pregate_ss <= end_dt,
            TruckTurnaroundTime.ttt > 0,
            TruckTurnaroundTime.ttt < 1440
        )
    )
    
    if operation_type:
        ttt_values_query = ttt_values_query.where(TruckTurnaroundTime.operation_type == operation_type)
    
    ttt_result = await db.execute(ttt_query)
    ttt_stats = ttt_result.first()
    
    ttt_values_result = await db.execute(ttt_values_query)
    ttt_values = [row[0] for row in ttt_values_result if row[0] is not None]
    
    # 3. OBTENER DATOS DE CDT
    cdt_query = select(
        func.avg(ContainerDwellTime.cdt_hours).label('promedio_horas'),
        func.count(ContainerDwellTime.id).label('total'),
        func.min(ContainerDwellTime.cdt_hours).label('minimo'),
        func.max(ContainerDwellTime.cdt_hours).label('maximo')
    ).where(
        and_(
            ContainerDwellTime.iufv_it >= start_dt,
            ContainerDwellTime.iufv_it <= end_dt,
            ContainerDwellTime.cdt_hours.isnot(None),
            ContainerDwellTime.cdt_hours > 0,  # Excluir valores negativos
            ContainerDwellTime.cdt_hours < 720  # Excluir valores mayores a 30 días
        )
    )
    
    if operation_type:
        cdt_query = cdt_query.where(ContainerDwellTime.operation_type == operation_type)
    
    # Para obtener percentiles de CDT
    cdt_values_query = select(ContainerDwellTime.cdt_hours).where(
        and_(
            ContainerDwellTime.iufv_it >= start_dt,
            ContainerDwellTime.iufv_it <= end_dt,
            ContainerDwellTime.cdt_hours.isnot(None),
            ContainerDwellTime.cdt_hours > 0,
            ContainerDwellTime.cdt_hours < 720
        )
    )
    
    if operation_type:
        cdt_values_query = cdt_values_query.where(ContainerDwellTime.operation_type == operation_type)
    
    cdt_result = await db.execute(cdt_query)
    cdt_stats = cdt_result.first()
    
    cdt_values_result = await db.execute(cdt_values_query)
    cdt_values = [row[0] for row in cdt_values_result if row[0] is not None]
    
    # 4. CALCULAR KPIs DE CAPACIDAD Y OCUPACIÓN
    capacidad_filtrada = CAPACIDAD_TOTAL_TERMINAL
    if patio_filter and patio_filter in PATIO_BLOCKS:
        bloques_patio = PATIO_BLOCKS[patio_filter]
        capacidad_filtrada = sum(CAPACIDADES_BLOQUES.get(b, 0) for b in bloques_patio)
    elif bloque_filter and bloque_filter in CAPACIDADES_BLOQUES:
        capacidad_filtrada = CAPACIDADES_BLOQUES[bloque_filter]
    
    # Usar los campos pre-calculados de promedio
    promedios_teus = [m.promedio_teus for m in movements_data]
    minimos_teus = [m.minimo_teus for m in movements_data]
    maximos_teus = [m.maximos_teus for m in movements_data]
    
    promedio_teus_actual = statistics.mean(promedios_teus) if promedios_teus else 0
    minimo_teus_periodo = min(minimos_teus) if minimos_teus else 0
    maximo_teus_periodo = max(maximos_teus) if maximos_teus else 0
    
    utilizacion_por_volumen = (promedio_teus_actual / capacidad_filtrada) * 100 if capacidad_filtrada > 0 else 0
    
    # Calcular variabilidad
    if promedios_teus and len(promedios_teus) > 1:
        std_dev = statistics.stdev(promedios_teus)
        coef_variacion = (std_dev / promedio_teus_actual) * 100 if promedio_teus_actual > 0 else 0
    else:
        coef_variacion = 0
    
    # 5. CALCULAR KPIs DE FLUJO
    total_gate_entrada = sum(m.gate_entrada_contenedores for m in movements_data)
    total_gate_salida = sum(m.gate_salida_contenedores for m in movements_data)
    total_muelle_entrada = sum(m.muelle_entrada_contenedores for m in movements_data)
    total_muelle_salida = sum(m.muelle_salida_contenedores for m in movements_data)
    total_remanejos = sum(m.remanejos_contenedores for m in movements_data)
    
    total_entradas = total_gate_entrada + total_muelle_entrada
    total_salidas = total_gate_salida + total_muelle_salida
    total_movimientos = total_entradas + total_salidas + total_remanejos
    
    balance_flujo = total_entradas / total_salidas if total_salidas > 0 else 1
    indice_remanejos = (total_remanejos / total_movimientos) * 100 if total_movimientos > 0 else 0
    
    # Congestión Vehicular calculada correctamente
    total_gate_movimientos = 0
    horas_con_gate_set = set()
    
    for movement in movements_data:
        mov_gate = movement.gate_entrada_contenedores + movement.gate_salida_contenedores
        if mov_gate > 0:
            total_gate_movimientos += mov_gate
            horas_con_gate_set.add(movement.hora)
    
    horas_con_gate = len(horas_con_gate_set)
    
    if horas_con_gate > 0:
        congestion_vehicular = total_gate_movimientos / horas_con_gate
    else:
        congestion_vehicular = 0
    
    # Productividad operacional
    horas_unicas = len(set(m.hora for m in movements_data))
    productividad_operacional = total_movimientos / horas_unicas if horas_unicas > 0 else 0
    
    # NUEVOS CÁLCULOS DE MOVIMIENTOS POR ÁREA - CORREGIDO
    # Determinar el contexto de los filtros
    is_terminal_view = not patio_filter and not bloque_filter
    is_patio_view = patio_filter and not bloque_filter
    is_bloque_view = bool(bloque_filter)
    
    # INICIALIZAR TODAS LAS VARIABLES ANTES DE LAS CONDICIONALES
    movimientos_gate_hora = 0
    movimientos_patio_hora = 0
    movimientos_muelle_hora = 0
    total_movimientos_gate = 0
    total_movimientos_patio = 0
    total_movimientos_muelle = 0
    label_movimientos_1 = "Movimientos Gate"
    label_movimientos_2 = "Movimientos Patio"
    label_movimientos_3 = "Movimientos Muelle"
    
    if is_terminal_view:
        # VISTA TERMINAL COMPLETA
        # 1. Movimientos en GATES (entrada + salida de todos los bloques)
        total_gate_movimientos = 0
        horas_con_gate_set = set()
        
        for movement in movements_data:
            mov_gate = movement.gate_entrada_contenedores + movement.gate_salida_contenedores
            if mov_gate > 0:
                total_gate_movimientos += mov_gate
                horas_con_gate_set.add(movement.hora)
        
        horas_con_gate = len(horas_con_gate_set)
        movimientos_gate_hora = total_gate_movimientos / horas_con_gate if horas_con_gate > 0 else 0
        total_movimientos_gate = total_gate_movimientos
        
        # 2. Movimientos en PATIO (todos los movimientos internos)
        total_movimientos_patio_calc = (
            sum(m.remanejos_contenedores for m in movements_data) +
            sum(m.patio_entrada_contenedores for m in movements_data) +
            sum(m.patio_salida_contenedores for m in movements_data) +
            sum(m.terminal_entrada_contenedores for m in movements_data) +
            sum(m.terminal_salida_contenedores for m in movements_data)
        )
        
        horas_con_patio_set = set()
        for movement in movements_data:
            mov_patio = (movement.remanejos_contenedores + 
                         movement.patio_entrada_contenedores + movement.patio_salida_contenedores +
                         movement.terminal_entrada_contenedores + movement.terminal_salida_contenedores)
            if mov_patio > 0:
                horas_con_patio_set.add(movement.hora)
        
        horas_con_patio = len(horas_con_patio_set)
        movimientos_patio_hora = total_movimientos_patio_calc / horas_con_patio if horas_con_patio > 0 else 0
        total_movimientos_patio = total_movimientos_patio_calc
        
        # 3. Movimientos en MUELLE (entrada + salida)
        total_muelle_movimientos = 0
        horas_con_muelle_set = set()
        
        for movement in movements_data:
            mov_muelle = movement.muelle_entrada_contenedores + movement.muelle_salida_contenedores
            if mov_muelle > 0:
                total_muelle_movimientos += mov_muelle
                horas_con_muelle_set.add(movement.hora)
        
        horas_con_muelle = len(horas_con_muelle_set)
        movimientos_muelle_hora = total_muelle_movimientos / horas_con_muelle if horas_con_muelle > 0 else 0
        total_movimientos_muelle = total_muelle_movimientos
    
    elif is_patio_view:
        # VISTA DE PATIO ESPECÍFICO
        # 1. Movimientos en GATES del patio
        total_gate_movimientos = 0
        horas_con_gate_set = set()
        
        for movement in movements_data:
            mov_gate = movement.gate_entrada_contenedores + movement.gate_salida_contenedores
            if mov_gate > 0:
                total_gate_movimientos += mov_gate
                horas_con_gate_set.add(movement.hora)
        
        horas_con_gate = len(horas_con_gate_set)
        movimientos_gate_hora = total_gate_movimientos / horas_con_gate if horas_con_gate > 0 else 0
        total_movimientos_gate = total_gate_movimientos
        
        # 2. Movimientos INTERNOS del patio (remanejos + entre bloques del mismo patio)
        total_movimientos_internos = (
            sum(m.remanejos_contenedores for m in movements_data) +
            sum(m.patio_entrada_contenedores for m in movements_data) +
            sum(m.patio_salida_contenedores for m in movements_data)
        )
        
        horas_con_internos_set = set()
        for movement in movements_data:
            mov_internos = (movement.remanejos_contenedores + 
                            movement.patio_entrada_contenedores + 
                            movement.patio_salida_contenedores)
            if mov_internos > 0:
                horas_con_internos_set.add(movement.hora)
        
        horas_con_internos = len(horas_con_internos_set)
        movimientos_internos_hora = total_movimientos_internos / horas_con_internos if horas_con_internos > 0 else 0
        
        # Para vista de patio, el segundo KPI son movimientos internos
        movimientos_patio_hora = movimientos_internos_hora
        total_movimientos_patio = total_movimientos_internos
        label_movimientos_2 = "Movimientos Internos"
        
        # 3. Movimientos INTER-PATIO (hacia/desde otros patios)
        total_movimientos_inter_patio = (
            sum(m.terminal_entrada_contenedores for m in movements_data) +
            sum(m.terminal_salida_contenedores for m in movements_data)
        )
        
        horas_con_inter_patio_set = set()
        for movement in movements_data:
            mov_inter = movement.terminal_entrada_contenedores + movement.terminal_salida_contenedores
            if mov_inter > 0:
                horas_con_inter_patio_set.add(movement.hora)
        
        horas_con_inter_patio = len(horas_con_inter_patio_set)
        movimientos_inter_patio_hora = total_movimientos_inter_patio / horas_con_inter_patio if horas_con_inter_patio > 0 else 0
        
        # En vista patio, el tercer KPI son movimientos entre patios en lugar de muelle
        movimientos_muelle_hora = movimientos_inter_patio_hora
        total_movimientos_muelle = total_movimientos_inter_patio
        label_movimientos_3 = "Inter-Patios"
    
    elif is_bloque_view:
        # VISTA DE BLOQUE ESPECÍFICO
        # 1. Movimientos en GATES del bloque
        total_gate_movimientos = 0
        horas_con_gate_set = set()
        
        for movement in movements_data:
            mov_gate = movement.gate_entrada_contenedores + movement.gate_salida_contenedores
            if mov_gate > 0:
                total_gate_movimientos += mov_gate
                horas_con_gate_set.add(movement.hora)
        
        horas_con_gate = len(horas_con_gate_set)
        movimientos_gate_hora = total_gate_movimientos / horas_con_gate if horas_con_gate > 0 else 0
        total_movimientos_gate = total_gate_movimientos
        
        # 2. REMANEJOS dentro del bloque
        total_remanejos_bloque = sum(m.remanejos_contenedores for m in movements_data)
        
        horas_con_remanejos_set = set()
        for movement in movements_data:
            if movement.remanejos_contenedores > 0:
                horas_con_remanejos_set.add(movement.hora)
        
        horas_con_remanejos = len(horas_con_remanejos_set)
        remanejos_hora = total_remanejos_bloque / horas_con_remanejos if horas_con_remanejos > 0 else 0
        
        # Para vista de bloque, el segundo KPI son solo remanejos
        movimientos_patio_hora = remanejos_hora
        total_movimientos_patio = total_remanejos_bloque
        label_movimientos_2 = "Remanejos"
        
        # 3. Movimientos CON OTROS BLOQUES (todos los demás movimientos)
        total_otros_bloques = (
            sum(m.patio_entrada_contenedores for m in movements_data) +
            sum(m.patio_salida_contenedores for m in movements_data) +
            sum(m.terminal_entrada_contenedores for m in movements_data) +
            sum(m.terminal_salida_contenedores for m in movements_data) +
            sum(m.muelle_entrada_contenedores for m in movements_data) +
            sum(m.muelle_salida_contenedores for m in movements_data)
        )
        
        horas_con_otros_set = set()
        for movement in movements_data:
            mov_otros = (movement.patio_entrada_contenedores + movement.patio_salida_contenedores +
                         movement.terminal_entrada_contenedores + movement.terminal_salida_contenedores +
                         movement.muelle_entrada_contenedores + movement.muelle_salida_contenedores)
            if mov_otros > 0:
                horas_con_otros_set.add(movement.hora)
        
        horas_con_otros = len(horas_con_otros_set)
        otros_bloques_hora = total_otros_bloques / horas_con_otros if horas_con_otros > 0 else 0
        
        # En vista bloque, el tercer KPI son movimientos con otros bloques
        movimientos_muelle_hora = otros_bloques_hora
        total_movimientos_muelle = total_otros_bloques
        label_movimientos_3 = "Otros Bloques"
    
    # 6. MÉTRICAS DE INVENTARIO
    inventario_stats = aggregate_inventory_stats(movements_data, unit)
    
    # Calcular índice de estabilidad del inventario
    if inventario_stats['promedio'] > 0:
        estabilidad_inventario = 100 - inventario_stats['variabilidad']
    else:
        estabilidad_inventario = 0
    
    # 7. CALCULAR RELACIONES ENTRE KPIs
    kpi_relations = {}
    
    # Relación Congestión-Productividad
    if congestion_vehicular > productividad_operacional * 0.5:
        kpi_relations['congestionProductividadStatus'] = 'critical'
    elif congestion_vehicular > productividad_operacional * 0.4:
        kpi_relations['congestionProductividadStatus'] = 'warning'
    else:
        kpi_relations['congestionProductividadStatus'] = 'normal'
    
    # Relación Utilización-Remanejos
    if utilizacion_por_volumen > 80 and indice_remanejos > 5:
        kpi_relations['utilizacionRemanejosStatus'] = 'critical'
    elif utilizacion_por_volumen > 70 and indice_remanejos > 3:
        kpi_relations['utilizacionRemanejosStatus'] = 'warning'
    elif utilizacion_por_volumen < 50 and indice_remanejos < 2:
        kpi_relations['utilizacionRemanejosStatus'] = 'good'
    else:
        kpi_relations['utilizacionRemanejosStatus'] = 'normal'
    
    # Relación Balance-Utilización
    if balance_flujo > 1.3 and utilizacion_por_volumen > 80:
        kpi_relations['balanceUtilizacionStatus'] = 'critical'
    elif balance_flujo > 1.2 and utilizacion_por_volumen > 70:
        kpi_relations['balanceUtilizacionStatus'] = 'warning'
    elif balance_flujo >= 0.8 and balance_flujo <= 1.2 and utilizacion_por_volumen < 70:
        kpi_relations['balanceUtilizacionStatus'] = 'good'
    else:
        kpi_relations['balanceUtilizacionStatus'] = 'normal'
    
    # Nueva relación: TTT vs Congestión
    if ttt_stats and ttt_stats.promedio:
        if ttt_stats.promedio > 90 and congestion_vehicular < 30:
            kpi_relations['tttCongestionStatus'] = 'warning'
        elif ttt_stats.promedio < 30 and congestion_vehicular > 50:
            kpi_relations['tttCongestionStatus'] = 'good'
        else:
            kpi_relations['tttCongestionStatus'] = 'normal'
    
    # 8. CONSTRUIR RESPUESTA COMPLETA
    result = {
        # KPIs de Capacidad y Ocupación
        'capacidad': {
            'utilizacionPorVolumen': round(utilizacion_por_volumen, 2),
            'promedioTeus': round(promedio_teus_actual, 2),
            'minimoTeus': minimo_teus_periodo,
            'maximoTeus': maximo_teus_periodo,
            'rangoOperativo': maximo_teus_periodo - minimo_teus_periodo,
            'coeficienteVariacion': round(coef_variacion, 2),
            'capacidadTotal': capacidad_filtrada,
            'horasCriticas': len([p for p in promedios_teus if p > capacidad_filtrada * 0.85])
        },
        
        # KPIs de Inventario
        'inventario': {
            'minimo': inventario_stats['minimo'],
            'maximo': inventario_stats['maximo'],
            'promedio': inventario_stats['promedio'],
            'rango': inventario_stats['rango'],
            'variabilidad': inventario_stats['variabilidad'],
            'estabilidad': round(estabilidad_inventario, 2),
            'percentil90': inventario_stats['percentil_90'],
            'percentil95': inventario_stats['percentil_95'],
            'utilizacionPico': round((inventario_stats['percentil_95'] / capacidad_filtrada) * 100, 2) if capacidad_filtrada > 0 else 0
        },
        
        # KPIs de Flujos y Productividad
        'flujos': {
            'gateEntrada': total_gate_entrada,
            'gateSalida': total_gate_salida,
            'congestionVehicular': round(congestion_vehicular, 2),
            'horasConGate': horas_con_gate,
            'muelleEntrada': total_muelle_entrada,
            'muelleSalida': total_muelle_salida,
            'totalMovimientos': total_movimientos,
            'balanceFlujo': round(balance_flujo, 2),
            'indiceRemanejos': round(indice_remanejos, 2),
            'productividadOperacional': round(productividad_operacional, 2),
            # CAMPOS ADAPTATIVOS CORREGIDOS
            'movimientosGateHora': round(movimientos_gate_hora, 2),
            'movimientosPatioHora': round(movimientos_patio_hora, 2),
            'movimientosMuelleHora': round(movimientos_muelle_hora, 2),
            'totalMovimientosGate': total_movimientos_gate,
            'totalMovimientosPatio': total_movimientos_patio,
            'totalMovimientosMuelle': total_movimientos_muelle,
            # Labels dinámicas para el frontend
            'labelMovimientos1': label_movimientos_1,
            'labelMovimientos2': label_movimientos_2,
            'labelMovimientos3': label_movimientos_3,
            # Contexto actual
            'vistaContexto': 'terminal' if is_terminal_view else ('patio' if is_patio_view else 'bloque')
        },
        
        # KPIs de Tiempos de Servicio
        'tiemposServicio': {
            'ttt': {
                'promedio': round(ttt_stats.promedio, 2) if ttt_stats and ttt_stats.promedio else 0,
                'minimo': round(ttt_stats.minimo, 2) if ttt_stats and ttt_stats.minimo else 0,
                'maximo': round(ttt_stats.maximo, 2) if ttt_stats and ttt_stats.maximo else 0,
                'mediana': round(calculate_percentile(ttt_values, 50), 2) if ttt_values else 0,
                'p90': round(calculate_percentile(ttt_values, 90), 2) if ttt_values else 0,
                'p95': round(calculate_percentile(ttt_values, 95), 2) if ttt_values else 0,
                'totalCamiones': ttt_stats.total if ttt_stats else 0,
                'camionesEficientes': len([v for v in ttt_values if v < 60]) if ttt_values else 0
            },
            'cdt': {
                'promedioHoras': round(cdt_stats.promedio_horas, 2) if cdt_stats and cdt_stats.promedio_horas else 0,
                'promedioDias': round(cdt_stats.promedio_horas / 24, 2) if cdt_stats and cdt_stats.promedio_horas else 0,
                'minimo': round(cdt_stats.minimo / 24, 2) if cdt_stats and cdt_stats.minimo else 0,
                'maximo': round(cdt_stats.maximo / 24, 2) if cdt_stats and cdt_stats.maximo else 0,
                'mediana': round(calculate_percentile(cdt_values, 50) / 24, 2) if cdt_values else 0,
                'p90': round(calculate_percentile(cdt_values, 90) / 24, 2) if cdt_values else 0,
                'p95': round(calculate_percentile(cdt_values, 95) / 24, 2) if cdt_values else 0,
                'totalContenedores': cdt_stats.total if cdt_stats else 0,
                'criticos': len([c for c in cdt_values if c > 168])  # > 7 días
            }
        },
        
        # Relaciones entre KPIs
        'kpiRelations': kpi_relations,
        
        # Metadata
        'metadata': {
            'periodo': {
                'inicio': start_dt.isoformat(),
                'fin': end_dt.isoformat(),
                'granularidad': unit,
                'diasAnalizados': (end_dt - start_dt).days + 1
            },
            'totalRegistros': len(movements_data),
            'horasUnicas': horas_unicas,
            'filtros': {
                'patio': patio_filter,
                'bloque': bloque_filter,
                'operacion': operation_type
            },
            'calidad': {
                'completitudMovimientos': round((len(movements_data) / (horas_unicas * 18)) * 100, 2) if horas_unicas > 0 else 0,
                'registrosTTT': ttt_stats.total if ttt_stats else 0,
                'registrosCDT': cdt_stats.total if cdt_stats else 0
            }
        }
    }
    
    # Guardar en cache
    cache.set(result, expire_minutes=30, endpoint="comprehensive_kpis", **{
        'start_date': start_date,
        'end_date': end_date,
        'unit': unit,
        'patio': patio_filter or "all",
        'bloque': bloque_filter or "all",
        'operation': operation_type or "all"
    })
    
    return result

# Incluir también el endpoint de movements por si acaso
@router.get("/movements")
async def get_historical_movements(
    start_date: str = Query(..., description="Fecha inicio (YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS)"),
    end_date: str = Query(..., description="Fecha fin (YYYY-MM-DD o YYYY-MM-DDTHH:MM:SS)"),
    bloque: Optional[str] = Query(None, description="Filtrar por bloque"),
    patio: Optional[str] = Query(None, description="Filtrar por patio"),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener movimientos históricos con filtros y agregación inteligente
    """
    # Verificar cache primero
    cached_data = cache.get(
        endpoint="movements",
        start_date=start_date,
        end_date=end_date,
        bloque=bloque or "all",
        patio=patio or "all"
    )
    
    if cached_data:
        logger.info("Datos obtenidos del cache")
        return cached_data
    
    try:
        # Parsear fechas
        start_dt, end_dt = parse_dates(start_date, end_date)
        
        # Calcular diferencia de días
        days_diff = (end_dt - start_dt).days
        
        # ESTRATEGIA DE AGREGACIÓN SEGÚN EL RANGO
        if days_diff > 90:  # Más de 3 meses: agregar por semana
            interval = "week"
            date_trunc = func.date_trunc('week', HistoricalMovement.hora)
        elif days_diff > 7:  # Más de una semana: agregar por día
            interval = "day"
            date_trunc = func.date_trunc('day', HistoricalMovement.hora)
        elif days_diff > 1:  # Más de un día: agregar por hora
            interval = "hour"
            date_trunc = func.date_trunc('hour', HistoricalMovement.hora)
        else:  # Un día o menos: datos completos
            interval = None
            date_trunc = None
        
        if interval:  # Usar agregación
            query = select(
                HistoricalMovement.bloque,
                date_trunc.label('periodo'),
                func.sum(HistoricalMovement.gate_entrada_contenedores).label('gate_entrada_contenedores'),
                func.sum(HistoricalMovement.gate_entrada_teus).label('gate_entrada_teus'),
                func.sum(HistoricalMovement.gate_salida_contenedores).label('gate_salida_contenedores'),
                func.sum(HistoricalMovement.gate_salida_teus).label('gate_salida_teus'),
                func.sum(HistoricalMovement.muelle_entrada_contenedores).label('muelle_entrada_contenedores'),
                func.sum(HistoricalMovement.muelle_entrada_teus).label('muelle_entrada_teus'),
                func.sum(HistoricalMovement.muelle_salida_contenedores).label('muelle_salida_contenedores'),
                func.sum(HistoricalMovement.muelle_salida_teus).label('muelle_salida_teus'),
                func.sum(HistoricalMovement.remanejos_contenedores).label('remanejos_contenedores'),
                func.sum(HistoricalMovement.remanejos_teus).label('remanejos_teus'),
                func.sum(HistoricalMovement.patio_entrada_contenedores).label('patio_entrada_contenedores'),
                func.sum(HistoricalMovement.patio_entrada_teus).label('patio_entrada_teus'),
                func.sum(HistoricalMovement.patio_salida_contenedores).label('patio_salida_contenedores'),
                func.sum(HistoricalMovement.patio_salida_teus).label('patio_salida_teus'),
                func.sum(HistoricalMovement.terminal_entrada_contenedores).label('terminal_entrada_contenedores'),
                func.sum(HistoricalMovement.terminal_entrada_teus).label('terminal_entrada_teus'),
                func.sum(HistoricalMovement.terminal_salida_contenedores).label('terminal_salida_contenedores'),
                func.sum(HistoricalMovement.terminal_salida_teus).label('terminal_salida_teus'),
                func.avg(HistoricalMovement.promedio_contenedores).label('promedio_contenedores'),
                func.avg(HistoricalMovement.promedio_teus).label('promedio_teus'),
                func.max(HistoricalMovement.maximo_contenedores).label('maximo_contenedores'),
                func.max(HistoricalMovement.maximos_teus).label('maximos_teus'),
                func.min(HistoricalMovement.minimo_contenedores).label('minimo_contenedores'),
                func.min(HistoricalMovement.minimo_teus).label('minimo_teus')
            ).where(
                and_(
                    HistoricalMovement.hora >= start_dt,
                    HistoricalMovement.hora <= end_dt
                )
            ).group_by(
                HistoricalMovement.bloque,
                date_trunc
            ).order_by(date_trunc)
            
            # Aplicar filtros
            if bloque:
                query = query.where(HistoricalMovement.bloque == bloque)
            elif patio and patio in PATIO_BLOCKS:
                bloques_patio = PATIO_BLOCKS[patio]
                query = query.where(HistoricalMovement.bloque.in_(bloques_patio))
            
            result = await db.execute(query)
            rows = result.all()
            
            data = []
            for row in rows:
                data.append({
                    'bloque': row.bloque,
                    'hora': row.periodo.isoformat(),
                    'gateEntradaContenedores': int(row.gate_entrada_contenedores or 0),
                    'gateEntradaTeus': int(row.gate_entrada_teus or 0),
                    'gateSalidaContenedores': int(row.gate_salida_contenedores or 0),
                    'gateSalidaTeus': int(row.gate_salida_teus or 0),
                    'muelleEntradaContenedores': int(row.muelle_entrada_contenedores or 0),
                    'muelleEntradaTeus': int(row.muelle_entrada_teus or 0),
                    'muelleSalidaContenedores': int(row.muelle_salida_contenedores or 0),
                    'muelleSalidaTeus': int(row.muelle_salida_teus or 0),
                    'remanejosContenedores': int(row.remanejos_contenedores or 0),
                    'remanejosTeus': int(row.remanejos_teus or 0),
                    'patioEntradaContenedores': int(row.patio_entrada_contenedores or 0),
                    'patioEntradaTeus': int(row.patio_entrada_teus or 0),
                    'patioSalidaContenedores': int(row.patio_salida_contenedores or 0),
                    'patioSalidaTeus': int(row.patio_salida_teus or 0),
                    'terminalEntradaContenedores': int(row.terminal_entrada_contenedores or 0),
                    'terminalEntradaTeus': int(row.terminal_entrada_teus or 0),
                    'terminalSalidaContenedores': int(row.terminal_salida_contenedores or 0),
                    'terminalSalidaTeus': int(row.terminal_salida_teus or 0),
                    'minimoContenedores': int(row.minimo_contenedores or 0),
                    'minimoTeus': int(row.minimo_teus or 0),
                    'maximoContenedores': int(row.maximo_contenedores or 0),
                    'maximosTeus': int(row.maximos_teus or 0),
                    'promedioContenedores': float(row.promedio_contenedores or 0),
                    'promedioTeus': float(row.promedio_teus or 0)
                })
            
            logger.info(f"Agregación {interval}: {len(data)} registros devueltos")
            
        else:  # Datos sin agregar (rangos pequeños)
            query = select(HistoricalMovement).where(
                and_(
                    HistoricalMovement.hora >= start_dt,
                    HistoricalMovement.hora <= end_dt
                )
            ).order_by(HistoricalMovement.hora).limit(1000)  # Limitar a 1000
            
            # Aplicar filtros
            if bloque:
                query = query.where(HistoricalMovement.bloque == bloque)
            elif patio and patio in PATIO_BLOCKS:
                bloques_patio = PATIO_BLOCKS[patio]
                query = query.where(HistoricalMovement.bloque.in_(bloques_patio))
            
            result = await db.execute(query)
            movements = result.scalars().all()
            
            if not movements:
                return []
            
            data = [{
                'bloque': m.bloque,
                'hora': m.hora.isoformat(),
                'gateEntradaContenedores': m.gate_entrada_contenedores,
                'gateEntradaTeus': m.gate_entrada_teus,
                'gateSalidaContenedores': m.gate_salida_contenedores,
                'gateSalidaTeus': m.gate_salida_teus,
                'muelleEntradaContenedores': m.muelle_entrada_contenedores,
                'muelleEntradaTeus': m.muelle_entrada_teus,
                'muelleSalidaContenedores': m.muelle_salida_contenedores,
                'muelleSalidaTeus': m.muelle_salida_teus,
                'remanejosContenedores': m.remanejos_contenedores,
                'remanejosTeus': m.remanejos_teus,
                'patioEntradaContenedores': m.patio_entrada_contenedores,
                'patioEntradaTeus': m.patio_entrada_teus,
                'patioSalidaContenedores': m.patio_salida_contenedores,
                'patioSalidaTeus': m.patio_salida_teus,
                'terminalEntradaContenedores': m.terminal_entrada_contenedores,
                'terminalEntradaTeus': m.terminal_entrada_teus,
                'terminalSalidaContenedores': m.terminal_salida_contenedores,
                'terminalSalidaTeus': m.terminal_salida_teus,
                'minimoContenedores': m.minimo_contenedores,
                'minimoTeus': m.minimo_teus,
                'maximoContenedores': m.maximo_contenedores,
                'maximosTeus': m.maximos_teus,
                'promedioContenedores': m.promedio_contenedores,
                'promedioTeus': m.promedio_teus
            } for m in movements]
        
        # Guardar en cache
        cache.set(
            data,
            expire_minutes=60,  # 1 hora
            endpoint="movements",
            start_date=start_date,
            end_date=end_date,
            bloque=bloque or "all",
            patio=patio or "all"
        )
        
        return data
        
    except Exception as e:
        logger.error(f"Error en get_historical_movements: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")