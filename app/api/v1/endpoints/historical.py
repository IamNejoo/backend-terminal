# app/api/v1/endpoints/historical.py
from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, or_
import hashlib
import logging

from app.core.database import get_db
from app.models.historical_movements import HistoricalMovement

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
        if 'T' not in start_date:
            start_date = f"{start_date}T00:00:00"
        if 'T' not in end_date:
            end_date = f"{end_date}T23:59:59"
            
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
        
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
            if patio and patio in PATIO_BLOCKS:
                bloques_patio = PATIO_BLOCKS[patio]
                query = query.where(HistoricalMovement.bloque.in_(bloques_patio))
            elif bloque:
                query = query.where(HistoricalMovement.bloque == bloque)
            
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
            if patio and patio in PATIO_BLOCKS:
                bloques_patio = PATIO_BLOCKS[patio]
                query = query.where(HistoricalMovement.bloque.in_(bloques_patio))
            elif bloque:
                query = query.where(HistoricalMovement.bloque == bloque)
            
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

@router.get("/kpis")
async def calculate_kpis(
    start_date: str = Query(..., description="Fecha inicio (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Fecha fin (YYYY-MM-DD)"),
    unit: str = Query("day", regex="^(hour|day|week|month|year)$"),
    patio_filter: Optional[str] = Query(None),
    bloque_filter: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """
    Calcular los 6 KPIs principales como en usePortKPIs
    """
    # Verificar cache
    cached_data = cache.get(
        endpoint="kpis",
        start_date=start_date,
        end_date=end_date,
        unit=unit,
        patio=patio_filter or "all",
        bloque=bloque_filter or "all"
    )
    
    if cached_data:
        logger.info("KPIs obtenidos del cache")
        return cached_data
    
    try:
        if 'T' not in start_date:
            start_date = f"{start_date}T00:00:00"
        if 'T' not in end_date:
            end_date = f"{end_date}T23:59:59"
            
        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
    except ValueError:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD")
    
    # Obtener datos filtrados
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
    data = result.scalars().all()
    
    if not data:
        kpis_result = {
            'utilizacionPorVolumen': 0,
            'congestionVehicular': 0,
            'balanceFlujo': 1,
            'productividadOperacional': 0,
            'indiceRemanejo': 0,
            'saturacionOperacional': 0,
            'kpiRelations': {
                'congestionProductividadStatus': 'normal',
                'utilizacionRemanejosStatus': 'normal',
                'balanceUtilizacionStatus': 'normal'
            },
            'detalles': {}
        }
        cache.set(kpis_result, expire_minutes=30, endpoint="kpis", **{
            'start_date': start_date,
            'end_date': end_date,
            'unit': unit,
            'patio': patio_filter or "all",
            'bloque': bloque_filter or "all"
        })
        return kpis_result
    
    # 1. UTILIZACIÓN POR VOLUMEN
    ocupacion_por_bloque = {}
    data_by_bloque = {}
    
    for movement in data:
        if movement.bloque not in data_by_bloque:
            data_by_bloque[movement.bloque] = []
        data_by_bloque[movement.bloque].append(movement)
    
    for bloque, registros in data_by_bloque.items():
        suma_teus = sum(r.promedio_teus for r in registros)
        promedio_teus = suma_teus / len(registros) if registros else 0
        ocupacion_por_bloque[bloque] = promedio_teus
    
    # Calcular capacidad según filtros
    capacidad_filtrada = CAPACIDAD_TOTAL_TERMINAL
    if patio_filter and patio_filter in PATIO_BLOCKS:
        bloques_patio = PATIO_BLOCKS[patio_filter]
        capacidad_filtrada = sum(CAPACIDADES_BLOQUES.get(b, 0) for b in bloques_patio)
    elif bloque_filter and bloque_filter in CAPACIDADES_BLOQUES:
        capacidad_filtrada = CAPACIDADES_BLOQUES[bloque_filter]
    
    ocupacion_total = sum(ocupacion_por_bloque.values())
    utilizacion_por_volumen = (ocupacion_total / capacidad_filtrada) * 100 if capacidad_filtrada > 0 else 0
    
    # 2. CONGESTIÓN VEHICULAR
    movimientos_gate_por_hora = {}
    for movement in data:
        hora = movement.hora.strftime('%H')
        if hora not in movimientos_gate_por_hora:
            movimientos_gate_por_hora[hora] = 0
        movimientos_gate_por_hora[hora] += (
            movement.gate_entrada_contenedores + 
            movement.gate_salida_contenedores
        )
    
    horas_con_movimientos = len([m for m in movimientos_gate_por_hora.values() if m > 0])
    total_movimientos_gate = sum(movimientos_gate_por_hora.values())
    congestion_vehicular = (
        total_movimientos_gate / horas_con_movimientos 
        if horas_con_movimientos > 0 else 0
    )
    
    # 3. BALANCE DE FLUJO
    total_entradas = sum(
        m.gate_entrada_contenedores + m.muelle_entrada_contenedores 
        for m in data
    )
    total_salidas = sum(
        m.gate_salida_contenedores + m.muelle_salida_contenedores 
        for m in data
    )
    balance_flujo = total_entradas / total_salidas if total_salidas > 0 else 1
    
    # 4. PRODUCTIVIDAD OPERACIONAL
    total_movimientos_terminal = total_entradas + total_salidas
    horas_unicas = len(set(m.hora for m in data))
    productividad_operacional = (
        total_movimientos_terminal / horas_unicas 
        if horas_unicas > 0 else 0
    )
    
    # 5. ÍNDICE DE REMANEJO
    total_remanejos = sum(m.remanejos_contenedores for m in data)
    total_movimientos = total_movimientos_terminal + total_remanejos
    indice_remanejo = (
        (total_remanejos / total_movimientos) * 100 
        if total_movimientos > 0 else 0
    )
    
    # 6. SATURACIÓN OPERACIONAL
    maximo_historico = max((m.maximos_teus for m in data), default=0)
    promedio_actual = data[-1].promedio_teus if data else 0
    saturacion_operacional = (
        (promedio_actual / maximo_historico) * 100 
        if maximo_historico > 0 else 0
    )
    
    # CALCULAR RELACIONES ENTRE KPIs
    kpi_relations = {}
    
    # 1. Relación Congestión-Productividad
    if congestion_vehicular > productividad_operacional * 2:
        kpi_relations['congestionProductividadStatus'] = 'critical'
    elif congestion_vehicular > productividad_operacional * 1.5:
        kpi_relations['congestionProductividadStatus'] = 'warning'
    elif congestion_vehicular < 30 and productividad_operacional < 50:
        kpi_relations['congestionProductividadStatus'] = 'warning'  # Posible falta de recursos
    else:
        kpi_relations['congestionProductividadStatus'] = 'normal'
    
    # 2. Relación Utilización-Remanejos
    if utilizacion_por_volumen > 80 and indice_remanejo > 5:
        kpi_relations['utilizacionRemanejosStatus'] = 'critical'
    elif utilizacion_por_volumen > 70 and indice_remanejo > 3:
        kpi_relations['utilizacionRemanejosStatus'] = 'warning'
    elif utilizacion_por_volumen < 50 and indice_remanejo < 2:
        kpi_relations['utilizacionRemanejosStatus'] = 'good'
    else:
        kpi_relations['utilizacionRemanejosStatus'] = 'normal'
    
    # 3. Relación Balance-Utilización
    if balance_flujo > 1.3 and utilizacion_por_volumen > 80:
        kpi_relations['balanceUtilizacionStatus'] = 'critical'
    elif balance_flujo > 1.2 and utilizacion_por_volumen > 70:
        kpi_relations['balanceUtilizacionStatus'] = 'warning'
    elif balance_flujo >= 0.8 and balance_flujo <= 1.2 and utilizacion_por_volumen < 70:
        kpi_relations['balanceUtilizacionStatus'] = 'good'
    else:
        kpi_relations['balanceUtilizacionStatus'] = 'normal'
    
    kpis_result = {
        'utilizacionPorVolumen': round(utilizacion_por_volumen, 2),
        'congestionVehicular': round(congestion_vehicular, 2),
        'balanceFlujo': round(balance_flujo, 2),
        'productividadOperacional': round(productividad_operacional, 2),
        'indiceRemanejo': round(indice_remanejo, 2),
        'saturacionOperacional': round(saturacion_operacional, 2),
        'kpiRelations': kpi_relations,  # AGREGAR ESTA LÍNEA
        'detalles': {
            'totalRegistros': len(data),
            'rangoFechas': {
                'inicio': start_dt.isoformat(),
                'fin': end_dt.isoformat()
            },
            'ocupacionPorBloque': ocupacion_por_bloque,
            'horasConActividad': horas_con_movimientos,
            'totalMovimientos': total_movimientos
        }
    }
    
    # Guardar en cache
    cache.set(kpis_result, expire_minutes=30, endpoint="kpis", **{
        'start_date': start_date,
        'end_date': end_date,
        'unit': unit,
        'patio': patio_filter or "all",
        'bloque': bloque_filter or "all"
    })
    
    return kpis_result


@router.get("/summary")
async def get_summary(
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener resumen de datos disponibles
    """
    # Verificar cache
    cached_data = cache.get(endpoint="summary")
    if cached_data:
        return cached_data
    
    # Contar registros totales
    count_query = select(func.count(HistoricalMovement.id))
    count_result = await db.execute(count_query)
    total_records = count_result.scalar()
    
    # Obtener rango de fechas
    date_query = select(
        func.min(HistoricalMovement.hora),
        func.max(HistoricalMovement.hora)
    )
    date_result = await db.execute(date_query)
    min_date, max_date = date_result.first()
    
    # Obtener bloques únicos
    bloques_query = select(HistoricalMovement.bloque).distinct()
    bloques_result = await db.execute(bloques_query)
    bloques = [b[0] for b in bloques_result.all()]
    
    summary_result = {
        'totalRecords': total_records,
        'dateRange': {
            'start': min_date.isoformat() if min_date else None,
            'end': max_date.isoformat() if max_date else None
        },
        'bloques': bloques,
        'patios': list(PATIO_BLOCKS.keys()),
        'capacidades': CAPACIDADES_BLOQUES
    }
    
    # Guardar en cache por 1 hora
    cache.set(summary_result, expire_minutes=60, endpoint="summary")
    
    return summary_result

@router.post("/cache/clear")
async def clear_cache():
    """
    Limpiar el cache manualmente
    """
    cache._cache.clear()
    return {"message": "Cache limpiado exitosamente"}