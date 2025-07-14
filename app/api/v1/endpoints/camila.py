from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, distinct
from sqlalchemy.orm import selectinload
import logging
from uuid import UUID

from app.core.database import get_db
from app.models.camila import (
    ResultadoCamila, AsignacionGrua, CuotaCamion,
    MetricaGrua, ComparacionCamila, ParametroCamila
)

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/dashboard")
async def get_camila_dashboard(
    anio: int = Query(..., ge=2017, le=2023),
    semana: int = Query(..., ge=1, le=52),
    turno: int = Query(..., ge=1, le=21),
    participacion: int = Query(..., description="60-80"),
    dispersion: str = Query(..., regex="^[KN]$", description="K=con dispersión, N=sin dispersión"),
    db: AsyncSession = Depends(get_db)
):
    """Obtener dashboard de Camila para un turno específico"""
    
    con_dispersion = dispersion == 'K'
    
    # Buscar resultado
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.turno == turno,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == 'completado'
        )
    )
    
    result = await db.execute(query)
    resultado = result.scalar_one_or_none()
    
    if not resultado:
        raise HTTPException(404, f"No hay datos para {anio} S{semana} T{turno} P{participacion}{dispersion}")
    
    # Obtener asignaciones
    asig_query = await db.execute(
        select(AsignacionGrua)
        .where(AsignacionGrua.resultado_id == resultado.id)
        .order_by(AsignacionGrua.periodo, AsignacionGrua.bloque_codigo)
    )
    asignaciones = asig_query.scalars().all()
    
    # Obtener cuotas
    cuotas_query = await db.execute(
        select(CuotaCamion)
        .where(CuotaCamion.resultado_id == resultado.id)
        .order_by(CuotaCamion.periodo, CuotaCamion.bloque_codigo)
    )
    cuotas = cuotas_query.scalars().all()
    
    # Obtener métricas
    metricas_query = await db.execute(
        select(MetricaGrua)
        .where(MetricaGrua.resultado_id == resultado.id)
        .order_by(MetricaGrua.grua_id)
    )
    metricas = metricas_query.scalars().all()
    
    # Obtener comparaciones
    comp_query = await db.execute(
        select(ComparacionCamila)
        .where(ComparacionCamila.resultado_id == resultado.id)
    )
    comparaciones = comp_query.scalars().all()
    
    # Procesar asignaciones por periodo
    asignaciones_por_periodo = {}
    for asig in asignaciones:
        if asig.periodo not in asignaciones_por_periodo:
            asignaciones_por_periodo[asig.periodo] = []
        asignaciones_por_periodo[asig.periodo].append({
            'segregacion': asig.segregacion_codigo,
            'bloque': asig.bloque_codigo,
            'frecuencia': asig.frecuencia
        })
    
    # Procesar cuotas por periodo
    cuotas_por_periodo = {}
    for cuota in cuotas:
        if cuota.periodo not in cuotas_por_periodo:
            cuotas_por_periodo[cuota.periodo] = []
        cuotas_por_periodo[cuota.periodo].append({
            'bloque': cuota.bloque_codigo,
            'cuota': cuota.cuota_camiones,
            'capacidad': cuota.capacidad_maxima,
            'tipo': cuota.tipo_operacion
        })
    
    # Procesar comparaciones
    comparaciones_dict = {
        'general': {},
        'por_bloque': {},
        'balance': {}
    }
    for comp in comparaciones:
        if comp.tipo_comparacion == 'general':
            comparaciones_dict['general'][comp.metrica] = {
                'magdalena': float(comp.valor_magdalena or 0),
                'camila': float(comp.valor_camila or 0),
                'diferencia': float(comp.diferencia or 0),
                'porcentaje': float(comp.porcentaje_diferencia or 0)
            }
        elif comp.tipo_comparacion == 'por_bloque':
            bloque = comp.metrica.replace('movimientos_', '')
            comparaciones_dict['por_bloque'][bloque] = {
                'magdalena': float(comp.valor_magdalena or 0),
                'camila': float(comp.valor_camila or 0),
                'diferencia': float(comp.diferencia or 0)
            }
        elif comp.tipo_comparacion == 'balance':
            comparaciones_dict['balance'] = {
                'magdalena': float(comp.valor_magdalena or 0),
                'camila': float(comp.valor_camila or 0),
                'mejora': float(comp.diferencia or 0)
            }
    
    # Construir respuesta
    response = {
        'metadata': {
            'resultado_id': str(resultado.id),
            'codigo': resultado.codigo,
            'anio': resultado.anio,
            'semana': resultado.semana,
            'dia': resultado.dia,
            'turno': resultado.turno,
            'turno_del_dia': resultado.turno_del_dia,
            'participacion': resultado.participacion,
            'con_dispersion': resultado.con_dispersion,
            'fecha_inicio': resultado.fecha_inicio.isoformat(),
            'fecha_fin': resultado.fecha_fin.isoformat(),
            'fecha_procesamiento': resultado.fecha_procesamiento.isoformat() if resultado.fecha_procesamiento else None
        },
        'metricas_principales': {
            'total_movimientos': resultado.total_movimientos,
            'gruas_utilizadas': resultado.total_gruas,
            'bloques_visitados': resultado.total_bloques_visitados,
            'segregaciones_atendidas': resultado.total_segregaciones,
            'utilizacion_promedio': float(resultado.utilizacion_promedio or 0),
            'coeficiente_variacion': float(resultado.coeficiente_variacion or 0)
        },
        'asignaciones_por_periodo': asignaciones_por_periodo,
        'cuotas_por_periodo': cuotas_por_periodo,
        'metricas_por_grua': [
            {
                'grua_id': m.grua_id,
                'movimientos': m.movimientos_asignados,
                'bloques_visitados': m.bloques_visitados,
                'utilizacion': float(m.utilizacion_pct or 0),
                'tiempo_productivo': float(m.tiempo_productivo_hrs or 0),
                'tiempo_improductivo': float(m.tiempo_improductivo_hrs or 0)
            }
            for m in metricas
        ],
        'comparacion_con_magdalena': comparaciones_dict,
        'resumen_operacional': {
            'ventanas_tiempo': len(cuotas_por_periodo),
            'total_cuotas_camiones': sum(c.cuota_camiones for c in cuotas),
            'promedio_frecuencia_visitas': sum(a.frecuencia for a in asignaciones) / len(asignaciones) if asignaciones else 0,
            'bloques_mas_visitados': _get_top_bloques(asignaciones, 3)
        }
    }
    
    return response

@router.get("/comparacion-temporal")
async def get_comparacion_temporal(
    anio: int = Query(...),
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """Obtener comparación temporal de todos los turnos de una semana"""
    
    con_dispersion = dispersion == 'K'
    
    # Obtener todos los turnos de la semana
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == 'completado'
        )
    ).order_by(ResultadoCamila.turno)
    
    result = await db.execute(query)
    resultados = result.scalars().all()
    
    if not resultados:
        raise HTTPException(404, "No hay datos para los parámetros especificados")
    
    # Construir serie temporal
    serie_temporal = []
    
    for res in resultados:
        # Obtener comparación general para cada turno
        comp_result = await db.execute(
            select(ComparacionCamila).where(
                and_(
                    ComparacionCamila.resultado_id == res.id,
                    ComparacionCamila.tipo_comparacion == 'general',
                    ComparacionCamila.metrica == 'movimientos_totales'
                )
            )
        )
        comparacion = comp_result.scalar_one_or_none()
        
        serie_temporal.append({
            'turno': res.turno,
            'dia': res.dia,
            'turno_del_dia': res.turno_del_dia,
            'fecha_hora': res.fecha_inicio.isoformat(),
            'movimientos_magdalena': float(comparacion.valor_magdalena) if comparacion else 0,
            'movimientos_camila': float(comparacion.valor_camila) if comparacion else 0,
            'utilizacion': float(res.utilizacion_promedio or 0),
            'coeficiente_variacion': float(res.coeficiente_variacion or 0)
        })
    
    # Calcular estadísticas agregadas
    total_magdalena = sum(t['movimientos_magdalena'] for t in serie_temporal)
    total_camila = sum(t['movimientos_camila'] for t in serie_temporal)
    promedio_utilizacion = sum(t['utilizacion'] for t in serie_temporal) / len(serie_temporal) if serie_temporal else 0
    promedio_cv = sum(t['coeficiente_variacion'] for t in serie_temporal) / len(serie_temporal) if serie_temporal else 0
    
    return {
        'metadata': {
            'anio': anio,
            'semana': semana,
            'participacion': participacion,
            'dispersion': dispersion,
            'total_turnos': len(resultados)
        },
        'serie_temporal': serie_temporal,
        'estadisticas_semanales': {
            'total_movimientos_magdalena': total_magdalena,
            'total_movimientos_camila': total_camila,
            'diferencia_total': total_camila - total_magdalena,
            'promedio_utilizacion': promedio_utilizacion,
            'promedio_coeficiente_variacion': promedio_cv,
            'turnos_procesados': len(resultados),
            'turnos_faltantes': 21 - len(resultados)
        }
    }

@router.get("/cuotas-camiones/{resultado_id}")
async def get_cuotas_detalle(
    resultado_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Obtener detalle de cuotas de camiones para un resultado específico"""
    
    # Verificar que existe
    resultado = await db.get(ResultadoCamila, resultado_id)
    if not resultado:
        raise HTTPException(404, "Resultado no encontrado")
    
    # Obtener cuotas
    cuotas_result = await db.execute(
        select(CuotaCamion)
        .where(CuotaCamion.resultado_id == resultado_id)
        .order_by(CuotaCamion.periodo, CuotaCamion.bloque_codigo)
    )
    cuotas = cuotas_result.scalars().all()
    
    # Organizar por periodo y bloque
    cuotas_por_periodo = {}
    for cuota in cuotas:
        if cuota.periodo not in cuotas_por_periodo:
            cuotas_por_periodo[cuota.periodo] = {
                'periodo': cuota.periodo,
                'ventana_inicio': cuota.ventana_inicio,
                'ventana_fin': cuota.ventana_fin,
                'bloques': []
            }
        
        cuotas_por_periodo[cuota.periodo]['bloques'].append({
            'bloque': cuota.bloque_codigo,
            'cuota': cuota.cuota_camiones,
            'capacidad': cuota.capacidad_maxima,
            'utilizacion': (cuota.cuota_camiones / cuota.capacidad_maxima * 100) if cuota.capacidad_maxima > 0 else 0,
            'tipo_operacion': cuota.tipo_operacion,
            'segregaciones': cuota.segregaciones_incluidas or []
        })
    
    # Calcular totales
    total_cuotas = sum(c.cuota_camiones for c in cuotas)
    total_capacidad = sum(c.capacidad_maxima for c in cuotas)
    
    return {
        'resultado_id': str(resultado_id),
        'turno': resultado.turno,
        'fecha': resultado.fecha_inicio.isoformat(),
        'cuotas_por_periodo': list(cuotas_por_periodo.values()),
        'resumen': {
            'total_cuotas': total_cuotas,
            'total_capacidad': total_capacidad,
            'utilizacion_global': (total_cuotas / total_capacidad * 100) if total_capacidad > 0 else 0,
            'periodos_activos': len(cuotas_por_periodo),
            'bloques_activos': len(set(c.bloque_codigo for c in cuotas))
        }
    }

# Continuación de app/api/v1/endpoints/camila.py

@router.get("/metricas-gruas")
async def get_metricas_gruas(
    anio: int = Query(...),
    semana: int = Query(...),
    turno: Optional[int] = Query(None),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """Obtener métricas de grúas agregadas"""
    
    con_dispersion = dispersion == 'K'
    
    # Query base
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == 'completado'
        )
    )
    
    if turno:
        query = query.where(ResultadoCamila.turno == turno)
    
    result = await db.execute(query)
    resultados = result.scalars().all()
    
    if not resultados:
        raise HTTPException(404, "No hay datos para los parámetros especificados")
    
    # Obtener todas las métricas
    resultado_ids = [r.id for r in resultados]
    
    metricas_result = await db.execute(
        select(MetricaGrua)
        .where(MetricaGrua.resultado_id.in_(resultado_ids))
        .order_by(MetricaGrua.grua_id)
    )
    metricas = metricas_result.scalars().all()
    
    # Agregar métricas por grúa
    metricas_por_grua = {}
    for metrica in metricas:
        if metrica.grua_id not in metricas_por_grua:
            metricas_por_grua[metrica.grua_id] = {
                'grua_id': metrica.grua_id,
                'movimientos_total': 0,
                'bloques_visitados_total': 0,
                'tiempo_productivo_total': 0,
                'tiempo_improductivo_total': 0,
                'turnos_trabajados': 0,
                'utilizacion_promedio': []
            }
        
        grua_stats = metricas_por_grua[metrica.grua_id]
        grua_stats['movimientos_total'] += metrica.movimientos_asignados
        grua_stats['bloques_visitados_total'] += metrica.bloques_visitados
        grua_stats['tiempo_productivo_total'] += float(metrica.tiempo_productivo_hrs or 0)
        grua_stats['tiempo_improductivo_total'] += float(metrica.tiempo_improductivo_hrs or 0)
        grua_stats['turnos_trabajados'] += 1
        grua_stats['utilizacion_promedio'].append(float(metrica.utilizacion_pct or 0))
    
    # Calcular promedios
    for grua_stats in metricas_por_grua.values():
        grua_stats['utilizacion_promedio'] = (
            sum(grua_stats['utilizacion_promedio']) / len(grua_stats['utilizacion_promedio'])
            if grua_stats['utilizacion_promedio'] else 0
        )
        grua_stats['movimientos_por_turno'] = (
            grua_stats['movimientos_total'] / grua_stats['turnos_trabajados']
            if grua_stats['turnos_trabajados'] > 0 else 0
        )
    
    # Estadísticas globales
    total_movimientos = sum(m.movimientos_asignados for m in metricas)
    promedio_utilizacion = (
        sum(float(m.utilizacion_pct or 0) for m in metricas) / len(metricas)
        if metricas else 0
    )
    
    return {
        'metadata': {
            'anio': anio,
            'semana': semana,
            'turno': turno,
            'participacion': participacion,
            'dispersion': dispersion,
            'turnos_analizados': len(resultados)
        },
        'metricas_por_grua': list(metricas_por_grua.values()),
        'estadisticas_globales': {
            'total_movimientos': total_movimientos,
            'promedio_utilizacion': promedio_utilizacion,
            'gruas_activas': len(metricas_por_grua),
            'movimientos_por_grua': total_movimientos / len(metricas_por_grua) if metricas_por_grua else 0,
            'balance_trabajo': _calculate_balance(metricas_por_grua)
        }
    }

@router.get("/resultados")
async def get_resultados_disponibles(
    anio: Optional[int] = Query(None),
    semana: Optional[int] = Query(None),
    turno: Optional[int] = Query(None),
    participacion: Optional[int] = Query(None),
    con_dispersion: Optional[bool] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    """Listar resultados de Camila disponibles con filtros"""
    
    query = select(ResultadoCamila).where(ResultadoCamila.estado == 'completado')
    
    if anio:
        query = query.where(ResultadoCamila.anio == anio)
    if semana:
        query = query.where(ResultadoCamila.semana == semana)
    if turno:
        query = query.where(ResultadoCamila.turno == turno)
    if participacion:
        query = query.where(ResultadoCamila.participacion == participacion)
    if con_dispersion is not None:
        query = query.where(ResultadoCamila.con_dispersion == con_dispersion)
    
    # Total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Paginación
    query = query.order_by(
        ResultadoCamila.anio.desc(),
        ResultadoCamila.semana.desc(),
        ResultadoCamila.turno
    ).limit(limit).offset(offset)
    
    result = await db.execute(query)
    resultados = result.scalars().all()
    
    return {
        'total': total,
        'limit': limit,
        'offset': offset,
        'resultados': [
            {
                'id': str(res.id),
                'codigo': res.codigo,
                'anio': res.anio,
                'semana': res.semana,
                'dia': res.dia,
                'turno': res.turno,
                'turno_del_dia': res.turno_del_dia,
                'participacion': res.participacion,
                'dispersion': 'K' if res.con_dispersion else 'N',
                'fecha_inicio': res.fecha_inicio.isoformat(),
                'total_movimientos': res.total_movimientos,
                'utilizacion': float(res.utilizacion_promedio or 0)
            }
            for res in resultados
        ]
    }

@router.get("/estadisticas")
async def get_estadisticas_camila(db: AsyncSession = Depends(get_db)):
    """Obtener estadísticas globales de Camila"""
    
    # Estadísticas por año
    stats_anio = await db.execute(
        select(
            ResultadoCamila.anio,
            func.count(ResultadoCamila.id).label('total_resultados'),
            func.count(distinct(ResultadoCamila.semana)).label('semanas_unicas'),
            func.avg(ResultadoCamila.utilizacion_promedio).label('utilizacion_promedio'),
            func.avg(ResultadoCamila.coeficiente_variacion).label('cv_promedio'),
            func.sum(ResultadoCamila.total_movimientos).label('movimientos_totales')
        ).where(
            ResultadoCamila.estado == 'completado'
        ).group_by(ResultadoCamila.anio).order_by(ResultadoCamila.anio)
    )
    
    # Comparaciones agregadas
    comp_stats = await db.execute(
        select(
            func.avg(ComparacionCamila.porcentaje_diferencia).label('mejora_promedio')
        ).where(
            and_(
                ComparacionCamila.tipo_comparacion == 'balance',
                ComparacionCamila.metrica == 'coeficiente_variacion'
            )
        )
    )
    mejora_balance = comp_stats.scalar() or 0
    
    # Total de registros
    totales = await db.execute(
        select(
            func.count(distinct(ResultadoCamila.id)).label('total_resultados'),
            func.sum(ResultadoCamila.total_movimientos).label('movimientos_totales'),
            func.avg(ResultadoCamila.utilizacion_promedio).label('utilizacion_global')
        ).where(ResultadoCamila.estado == 'completado')
    )
    
    total_stats = totales.one()
    
    return {
        'resumen_global': {
            'total_resultados': total_stats.total_resultados or 0,
            'movimientos_procesados': total_stats.movimientos_totales or 0,
            'utilizacion_promedio': float(total_stats.utilizacion_global or 0),
            'mejora_balance_promedio': float(mejora_balance)
        },
        'estadisticas_por_anio': [
            {
                'anio': row.anio,
                'resultados': row.total_resultados,
                'semanas': row.semanas_unicas,
                'utilizacion_promedio': float(row.utilizacion_promedio or 0),
                'cv_promedio': float(row.cv_promedio or 0),
                'movimientos_total': row.movimientos_totales or 0
            }
            for row in stats_anio
        ]
    }

@router.get("/comparacion-modelos")
async def get_comparacion_modelos(
    anio: int = Query(...),
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """Comparar resultados de Magdalena vs Camila para una semana completa"""
    
    con_dispersion = dispersion == 'K'
    
    # Obtener todos los resultados de Camila de la semana
    camila_query = await db.execute(
        select(ResultadoCamila).where(
            and_(
                ResultadoCamila.anio == anio,
                ResultadoCamila.semana == semana,
                ResultadoCamila.participacion == participacion,
                ResultadoCamila.con_dispersion == con_dispersion,
                ResultadoCamila.estado == 'completado'
            )
        ).order_by(ResultadoCamila.turno)
    )
    resultados_camila = camila_query.scalars().all()
    
    if not resultados_camila:
        raise HTTPException(404, "No hay datos de Camila para los parámetros especificados")
    
    # Obtener comparaciones para cada turno
    comparaciones_por_turno = []
    totales = {
        'magdalena': 0,
        'camila': 0,
        'utilizacion_acum': 0,
        'cv_acum': 0
    }
    
    for res in resultados_camila:
        # Obtener comparación general
        comp_result = await db.execute(
            select(ComparacionCamila).where(
                and_(
                    ComparacionCamila.resultado_id == res.id,
                    ComparacionCamila.tipo_comparacion == 'general',
                    ComparacionCamila.metrica == 'movimientos_totales'
                )
            )
        )
        comp = comp_result.scalar_one_or_none()
        
        # Obtener comparación de balance
        balance_result = await db.execute(
            select(ComparacionCamila).where(
                and_(
                    ComparacionCamila.resultado_id == res.id,
                    ComparacionCamila.tipo_comparacion == 'balance'
                )
            )
        )
        balance = balance_result.scalar_one_or_none()
        
        turno_data = {
            'turno': res.turno,
            'dia': res.dia,
            'turno_del_dia': res.turno_del_dia,
            'movimientos': {
                'magdalena': float(comp.valor_magdalena) if comp else 0,
                'camila': float(comp.valor_camila) if comp else 0,
                'diferencia': float(comp.diferencia) if comp else 0
            },
            'balance': {
                'cv_magdalena': float(balance.valor_magdalena) if balance else 0,
                'cv_camila': float(balance.valor_camila) if balance else 0,
                'mejora': float(balance.diferencia) if balance else 0
            },
            'utilizacion_camila': float(res.utilizacion_promedio or 0)
        }
        
        comparaciones_por_turno.append(turno_data)
        
        # Acumular totales
        if comp:
            totales['magdalena'] += float(comp.valor_magdalena)
            totales['camila'] += float(comp.valor_camila)
        totales['utilizacion_acum'] += float(res.utilizacion_promedio or 0)
        totales['cv_acum'] += float(res.coeficiente_variacion or 0)
    
    # Calcular promedios y totales
    num_turnos = len(resultados_camila)
    
    return {
        'metadata': {
            'anio': anio,
            'semana': semana,
            'participacion': participacion,
            'dispersion': dispersion,
            'turnos_procesados': num_turnos,
            'turnos_totales': 21
        },
        'comparaciones_por_turno': comparaciones_por_turno,
        'resumen_semanal': {
            'movimientos': {
                'total_magdalena': totales['magdalena'],
                'total_camila': totales['camila'],
                'diferencia_total': totales['camila'] - totales['magdalena'],
                'porcentaje_diferencia': (
                    ((totales['camila'] - totales['magdalena']) / totales['magdalena'] * 100)
                    if totales['magdalena'] > 0 else 0
                )
            },
            'eficiencia': {
                'utilizacion_promedio_camila': totales['utilizacion_acum'] / num_turnos if num_turnos > 0 else 0,
                'cv_promedio_camila': totales['cv_acum'] / num_turnos if num_turnos > 0 else 0
            },
            'cobertura': {
                'turnos_con_datos': num_turnos,
                'turnos_faltantes': 21 - num_turnos,
                'porcentaje_cobertura': (num_turnos / 21 * 100)
            }
        },
        'analisis': {
            'turnos_mayor_diferencia': _get_turnos_mayor_diferencia(comparaciones_por_turno, 5),
            'turnos_mejor_balance': _get_turnos_mejor_balance(comparaciones_por_turno, 5),
            'patron_temporal': _analyze_temporal_pattern(comparaciones_por_turno)
        }
    }

# Funciones auxiliares
def _get_top_bloques(asignaciones: List[AsignacionGrua], top_n: int) -> List[Dict]:
    """Obtener los bloques más visitados"""
    frecuencia_por_bloque = {}
    for asig in asignaciones:
        if asig.bloque_codigo not in frecuencia_por_bloque:
            frecuencia_por_bloque[asig.bloque_codigo] = 0
        frecuencia_por_bloque[asig.bloque_codigo] += asig.frecuencia
    
    sorted_bloques = sorted(
        frecuencia_por_bloque.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]
    
    return [
        {'bloque': bloque, 'frecuencia': freq}
        for bloque, freq in sorted_bloques
    ]

def _calculate_balance(metricas_por_grua: Dict) -> float:
    """Calcular balance de trabajo entre grúas"""
    if not metricas_por_grua:
        return 0
    
    movimientos = [g['movimientos_total'] for g in metricas_por_grua.values()]
    if not movimientos:
        return 0
    
    promedio = sum(movimientos) / len(movimientos)
    if promedio == 0:
        return 0
    
    desviacion = (sum((x - promedio) ** 2 for x in movimientos) / len(movimientos)) ** 0.5
    return (desviacion / promedio) * 100

def _get_turnos_mayor_diferencia(comparaciones: List[Dict], top_n: int) -> List[Dict]:
    """Obtener turnos con mayor diferencia entre modelos"""
    sorted_turnos = sorted(
        comparaciones,
        key=lambda x: abs(x['movimientos']['diferencia']),
        reverse=True
    )[:top_n]
    
    return [
        {
            'turno': t['turno'],
            'diferencia': t['movimientos']['diferencia'],
            'porcentaje': (
                (t['movimientos']['diferencia'] / t['movimientos']['magdalena'] * 100)
                if t['movimientos']['magdalena'] > 0 else 0
            )
        }
        for t in sorted_turnos
    ]

def _get_turnos_mejor_balance(comparaciones: List[Dict], top_n: int) -> List[Dict]:
    """Obtener turnos con mejor balance (menor CV)"""
    sorted_turnos = sorted(
        comparaciones,
        key=lambda x: x['balance']['cv_camila']
    )[:top_n]
    
    return [
        {
            'turno': t['turno'],
            'cv_camila': t['balance']['cv_camila'],
            'mejora_vs_magdalena': abs(t['balance']['mejora'])
        }
        for t in sorted_turnos
    ]

def _analyze_temporal_pattern(comparaciones: List[Dict]) -> Dict:
    """Analizar patrones temporales en las diferencias"""
    if not comparaciones:
        return {}
    
    # Agrupar por turno del día
    por_turno_dia = {1: [], 2: [], 3: []}
    for comp in comparaciones:
        turno_dia = comp['turno_del_dia']
        if turno_dia in por_turno_dia:
            por_turno_dia[turno_dia].append(comp['movimientos']['diferencia'])
    
    # Calcular promedios
    promedios = {}
    for turno, valores in por_turno_dia.items():
        if valores:
            promedios[f'turno_{turno}'] = {
                'promedio_diferencia': sum(valores) / len(valores),
                'num_observaciones': len(valores)
            }
    
    return {
        'por_turno_del_dia': promedios,
        'observacion': _get_pattern_observation(promedios)
    }

def _get_pattern_observation(promedios: Dict) -> str:
    """Generar observación sobre el patrón temporal"""
    if not promedios:
        return "No hay suficientes datos para identificar patrones"
    
    # Simplificado - en producción sería más sofisticado
    max_turno = max(promedios.items(), key=lambda x: x[1]['promedio_diferencia'])[0]
    
    turnos = {
        'turno_1': 'turno de la mañana (08:00-16:00)',
        'turno_2': 'turno de la tarde (16:00-00:00)',
        'turno_3': 'turno de la noche (00:00-08:00)'
    }
    
    return f"Mayor diferencia promedio en el {turnos.get(max_turno, 'turno ' + max_turno)}"