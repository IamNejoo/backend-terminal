# app/api/v1/endpoints/camila.py

from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, distinct, or_
from sqlalchemy.orm import selectinload
import logging
from uuid import UUID
import numpy as np

from app.core.database import get_db
from app.models.camila import (
    ResultadoCamila, AsignacionGrua, CuotaCamion, MetricaGrua,
    ComparacionReal, ParametroCamila, FlujoModelo, LogProcesamientoCamila,
    EstadoProcesamiento, TipoOperacion, TipoAsignacion
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/dashboard")
async def get_camila_dashboard(
    anio: int = Query(..., ge=2017, le=2023, description="Año"),
    semana: int = Query(..., ge=1, le=52, description="Número de semana"),
    turno: int = Query(..., ge=1, le=21, description="Turno de la semana (1-21)"),
    participacion: int = Query(..., ge=60, le=80, description="Porcentaje de participación"),
    dispersion: str = Query(..., regex="^[KN]$", description="K=con dispersión, N=sin dispersión"),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener dashboard completo de Camila para un turno específico.
    Incluye métricas del modelo y comparación con datos reales.
    """
    
    con_dispersion = dispersion == 'K'
    
    # Buscar resultado
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.turno == turno,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO
        )
    )
    
    result = await db.execute(query)
    resultado = result.scalar_one_or_none()
    
    if not resultado:
        raise HTTPException(404, f"No hay datos para {anio} S{semana} T{turno} P{participacion}{dispersion}")
    
    # Obtener datos relacionados
    # 1. Flujos del modelo
    flujos_query = await db.execute(
        select(FlujoModelo)
        .where(FlujoModelo.resultado_id == resultado.id)
        .order_by(FlujoModelo.periodo, FlujoModelo.bloque_codigo)
    )
    flujos = flujos_query.scalars().all()
    
    # 2. Asignaciones de grúas
    asig_query = await db.execute(
        select(AsignacionGrua)
        .where(AsignacionGrua.resultado_id == resultado.id)
        .order_by(AsignacionGrua.periodo, AsignacionGrua.grua_id)
    )
    asignaciones = asig_query.scalars().all()
    
    # 3. Cuotas de camiones
    cuotas_query = await db.execute(
        select(CuotaCamion)
        .where(CuotaCamion.resultado_id == resultado.id)
        .order_by(CuotaCamion.periodo, CuotaCamion.bloque_codigo)
    )
    cuotas = cuotas_query.scalars().all()
    
    # 4. Métricas por grúa
    metricas_query = await db.execute(
        select(MetricaGrua)
        .where(MetricaGrua.resultado_id == resultado.id)
        .order_by(MetricaGrua.grua_id)
    )
    metricas = metricas_query.scalars().all()
    
    # 5. Comparaciones con realidad
    comp_query = await db.execute(
        select(ComparacionReal)
        .where(ComparacionReal.resultado_id == resultado.id)
    )
    comparaciones = comp_query.scalars().all()
    
    # Procesar datos para el dashboard
    
    # Matriz de asignación por periodo-bloque
    matriz_asignacion = {}
    for flujo in flujos:
        key = (flujo.periodo, flujo.bloque_codigo)
        if key not in matriz_asignacion:
            matriz_asignacion[key] = 0
        matriz_asignacion[key] += flujo.cantidad
    
    # Distribución por bloque
    distribucion_bloques = {}
    for flujo in flujos:
        if flujo.bloque_codigo not in distribucion_bloques:
            distribucion_bloques[flujo.bloque_codigo] = 0
        distribucion_bloques[flujo.bloque_codigo] += flujo.cantidad
    
    # Cuotas por periodo con utilización
    cuotas_por_periodo = {}
    for cuota in cuotas:
        if cuota.periodo not in cuotas_por_periodo:
            cuotas_por_periodo[cuota.periodo] = {
                'periodo': cuota.periodo,
                'cuota_total': 0,
                'capacidad_total': 0,
                'movimientos_reales': 0,
                'bloques': []
            }
        
        cuotas_por_periodo[cuota.periodo]['cuota_total'] += cuota.cuota_modelo
        cuotas_por_periodo[cuota.periodo]['capacidad_total'] += cuota.capacidad_maxima
        cuotas_por_periodo[cuota.periodo]['movimientos_reales'] += cuota.movimientos_reales or 0
        
        cuotas_por_periodo[cuota.periodo]['bloques'].append({
            'bloque': cuota.bloque_codigo,
            'cuota': cuota.cuota_modelo,
            'capacidad': cuota.capacidad_maxima,
            'gruas': cuota.gruas_asignadas,
            'real': cuota.movimientos_reales,
            'utilizacion_modelo': (cuota.cuota_modelo / cuota.capacidad_maxima * 100) if cuota.capacidad_maxima > 0 else 0,
            'utilizacion_real': cuota.utilizacion_real or 0
        })
    
    # Métricas de grúas
    metricas_gruas_list = []
    for metrica in metricas:
        metricas_gruas_list.append({
            'grua_id': metrica.grua_id,
            'movimientos': metrica.movimientos_modelo,
            'bloques_visitados': metrica.bloques_visitados,
            'periodos_activa': metrica.periodos_activa,
            'utilizacion': float(metrica.utilizacion_pct),
            'tiempo_productivo': float(metrica.tiempo_productivo_hrs),
            'tiempo_improductivo': float(metrica.tiempo_improductivo_hrs),
            'movimientos_reales_est': metrica.movimientos_reales_estimados
        })
    
    # Procesar comparaciones con realidad
    comparaciones_dict = {
        'general': {},
        'por_periodo': {},
        'por_bloque': {},
        'accuracy_metrics': {}
    }
    
    for comp in comparaciones:
        if comp.tipo_comparacion == 'general':
            comparaciones_dict['general'][comp.metrica] = {
                'modelo': float(comp.valor_modelo),
                'real': float(comp.valor_real),
                'diferencia': float(comp.diferencia_absoluta),
                'porcentaje': float(comp.diferencia_porcentual),
                'accuracy': float(comp.accuracy)
            }
        elif comp.tipo_comparacion == 'por_periodo':
            periodo = comp.dimension
            comparaciones_dict['por_periodo'][periodo] = {
                'modelo': float(comp.valor_modelo),
                'real': float(comp.valor_real),
                'diferencia': float(comp.diferencia_absoluta),
                'accuracy': float(comp.accuracy)
            }
        elif comp.tipo_comparacion == 'por_bloque':
            bloque = comp.dimension
            comparaciones_dict['por_bloque'][bloque] = {
                'modelo': float(comp.valor_modelo),
                'real': float(comp.valor_real),
                'diferencia': float(comp.diferencia_absoluta),
                'accuracy': float(comp.accuracy)
            }
    
    # Timeline de operaciones
    timeline_data = []
    for periodo in range(1, 9):
        periodo_data = cuotas_por_periodo.get(periodo, {
            'cuota_total': 0,
            'capacidad_total': 0,
            'movimientos_reales': 0
        })
        
        # Calcular hora real del periodo
        turno_del_dia = resultado.turno_del_dia
        hora_base = {1: 8, 2: 16, 3: 0}[turno_del_dia]
        hora_periodo = (hora_base + periodo - 1) % 24
        
        timeline_data.append({
            'periodo': periodo,
            'hora': f"{hora_periodo:02d}:00",
            'movimientos_modelo': periodo_data.get('cuota_total', 0),
            'movimientos_real': periodo_data.get('movimientos_reales', 0),
            'capacidad': periodo_data.get('capacidad_total', 0),
            'bloques_activos': len([b for b in periodo_data.get('bloques', []) if b['cuota'] > 0])
        })
    
    # Construir respuesta completa
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
            'fecha_procesamiento': resultado.fecha_procesamiento.isoformat() if resultado.fecha_procesamiento else None,
            'archivos': {
                'resultado': resultado.archivo_resultado,
                'instancia': resultado.archivo_instancia,
                'flujos_real': resultado.archivo_flujos_real
            }
        },
        'metricas_principales': {
            'total_movimientos_modelo': resultado.total_movimientos_modelo,
            'total_movimientos_real': resultado.total_movimientos_real or 0,
            'accuracy_global': float(resultado.accuracy_global or 0),
            'brecha_movimientos': resultado.brecha_movimientos or 0,
            'gruas_utilizadas': resultado.total_gruas_utilizadas,
            'bloques_visitados': resultado.total_bloques_visitados,
            'segregaciones_atendidas': resultado.total_segregaciones,
            'utilizacion_modelo': float(resultado.utilizacion_modelo),
            'coeficiente_variacion': float(resultado.coeficiente_variacion),
            'capacidad_teorica': resultado.capacidad_teorica
        },
        'distribucion_bloques': distribucion_bloques,
        'matriz_asignacion': {
            f"P{p}-{b}": cantidad 
            for (p, b), cantidad in matriz_asignacion.items()
        },
        'cuotas_por_periodo': list(cuotas_por_periodo.values()),
        'metricas_gruas': metricas_gruas_list,
        'comparacion_real': comparaciones_dict,
        'timeline': timeline_data,
        'resumen_operacional': {
            'periodos_activos': len(cuotas_por_periodo),
            'bloques_mas_visitados': _get_top_bloques(distribucion_bloques, 3),
            'balance_gruas': _calculate_balance(metricas_gruas_list),
            'precision_modelo': _calculate_model_precision(comparaciones_dict)
        }
    }
    
    return response


@router.get("/comparacion-temporal")
async def get_comparacion_temporal(
    anio: int = Query(...),
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    incluir_detalles: bool = Query(False, description="Incluir detalles por turno"),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener comparación temporal de todos los turnos de una semana.
    Muestra evolución de accuracy y métricas a lo largo de la semana.
    """
    
    con_dispersion = dispersion == 'K'
    
    # Obtener todos los turnos de la semana
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO
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
            select(ComparacionReal).where(
                and_(
                    ComparacionReal.resultado_id == res.id,
                    ComparacionReal.tipo_comparacion == 'general',
                    ComparacionReal.metrica == 'movimientos_totales'
                )
            )
        )
        comparacion = comp_result.scalar_one_or_none()
        
        turno_data = {
            'turno': res.turno,
            'dia': res.dia,
            'turno_del_dia': res.turno_del_dia,
            'fecha_hora': res.fecha_inicio.isoformat(),
            'movimientos_modelo': res.total_movimientos_modelo,
            'movimientos_real': res.total_movimientos_real or 0,
            'accuracy': float(res.accuracy_global or 0),
            'utilizacion_modelo': float(res.utilizacion_modelo),
            'utilizacion_real': float(comparacion.valor_real / res.capacidad_teorica * 100) if comparacion and res.capacidad_teorica > 0 else 0,
            'coeficiente_variacion': float(res.coeficiente_variacion)
        }
        
        # Si se solicitan detalles, agregar comparación por periodo
        if incluir_detalles and comparacion:
            periodos_result = await db.execute(
                select(ComparacionReal).where(
                    and_(
                        ComparacionReal.resultado_id == res.id,
                        ComparacionReal.tipo_comparacion == 'por_periodo'
                    )
                ).order_by(ComparacionReal.dimension)
            )
            periodos = periodos_result.scalars().all()
            
            turno_data['detalle_periodos'] = [
                {
                    'periodo': int(p.dimension),
                    'modelo': float(p.valor_modelo),
                    'real': float(p.valor_real),
                    'accuracy': float(p.accuracy)
                }
                for p in periodos
            ]
        
        serie_temporal.append(turno_data)
    
    # Calcular estadísticas agregadas
    total_modelo = sum(t['movimientos_modelo'] for t in serie_temporal)
    total_real = sum(t['movimientos_real'] for t in serie_temporal)
    promedio_accuracy = np.mean([t['accuracy'] for t in serie_temporal if t['accuracy'] > 0])
    promedio_utilizacion_modelo = np.mean([t['utilizacion_modelo'] for t in serie_temporal])
    promedio_utilizacion_real = np.mean([t['utilizacion_real'] for t in serie_temporal])
    promedio_cv = np.mean([t['coeficiente_variacion'] for t in serie_temporal])
    
    # Análisis de patrones
    accuracy_por_turno_dia = {}
    for t in serie_temporal:
        td = t['turno_del_dia']
        if td not in accuracy_por_turno_dia:
            accuracy_por_turno_dia[td] = []
        accuracy_por_turno_dia[td].append(t['accuracy'])
    
    patron_turno = {}
    for td, accuracies in accuracy_por_turno_dia.items():
        patron_turno[f'turno_{td}'] = {
            'promedio_accuracy': np.mean(accuracies) if accuracies else 0,
            'desviacion': np.std(accuracies) if len(accuracies) > 1 else 0,
            'num_observaciones': len(accuracies)
        }
    
    return {
        'metadata': {
            'anio': anio,
            'semana': semana,
            'participacion': participacion,
            'dispersion': dispersion,
            'turnos_procesados': len(resultados),
            'turnos_totales': 21
        },
        'serie_temporal': serie_temporal,
        'estadisticas_semanales': {
            'totales': {
                'movimientos_modelo': total_modelo,
                'movimientos_real': total_real,
                'brecha_total': total_real - total_modelo,
                'accuracy_global': (min(total_modelo, total_real) / max(total_modelo, total_real) * 100) if max(total_modelo, total_real) > 0 else 0
            },
            'promedios': {
                'accuracy': promedio_accuracy,
                'utilizacion_modelo': promedio_utilizacion_modelo,
                'utilizacion_real': promedio_utilizacion_real,
                'coeficiente_variacion': promedio_cv
            },
            'cobertura': {
                'turnos_con_datos': len(resultados),
                'turnos_faltantes': 21 - len(resultados),
                'porcentaje_cobertura': (len(resultados) / 21 * 100)
            }
        },
        'analisis_patrones': {
            'por_turno_del_dia': patron_turno,
            'mejor_turno': _get_mejor_turno(serie_temporal),
            'peor_turno': _get_peor_turno(serie_temporal),
            'tendencia': _analyze_tendencia(serie_temporal)
        }
    }


@router.get("/metricas-gruas")
async def get_metricas_gruas(
    anio: int = Query(...),
    semana: int = Query(...),
    turno: Optional[int] = Query(None),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener métricas agregadas de grúas para uno o todos los turnos.
    """
    
    con_dispersion = dispersion == 'K'
    
    # Query base
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.anio == anio,
            ResultadoCamila.semana == semana,
            ResultadoCamila.participacion == participacion,
            ResultadoCamila.con_dispersion == con_dispersion,
            ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO
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
                'bloques_visitados_set': set(),
                'periodos_activa_total': 0,
                'tiempo_productivo_total': 0,
                'tiempo_improductivo_total': 0,
                'turnos_trabajados': 0,
                'utilizaciones': []
            }
        
        grua_stats = metricas_por_grua[metrica.grua_id]
        grua_stats['movimientos_total'] += metrica.movimientos_modelo
        grua_stats['bloques_visitados_set'].update(
            [f"C{i}" for i in range(1, metrica.bloques_visitados + 1)]
        )
        grua_stats['periodos_activa_total'] += metrica.periodos_activa
        grua_stats['tiempo_productivo_total'] += float(metrica.tiempo_productivo_hrs)
        grua_stats['tiempo_improductivo_total'] += float(metrica.tiempo_improductivo_hrs)
        grua_stats['turnos_trabajados'] += 1 if metrica.movimientos_modelo > 0 else 0
        grua_stats['utilizaciones'].append(float(metrica.utilizacion_pct))
    
    # Calcular estadísticas finales
    gruas_stats_list = []
    for grua_id, stats in metricas_por_grua.items():
        gruas_stats_list.append({
            'grua_id': grua_id,
            'movimientos_total': stats['movimientos_total'],
            'bloques_visitados_total': len(stats['bloques_visitados_set']),
            'movimientos_por_turno': stats['movimientos_total'] / len(resultados),
            'turnos_activa': stats['turnos_trabajados'],
            'turnos_inactiva': len(resultados) - stats['turnos_trabajados'],
            'utilizacion_promedio': np.mean(stats['utilizaciones']) if stats['utilizaciones'] else 0,
            'utilizacion_max': max(stats['utilizaciones']) if stats['utilizaciones'] else 0,
            'utilizacion_min': min(stats['utilizaciones']) if stats['utilizaciones'] else 0,
            'tiempo_productivo_total': stats['tiempo_productivo_total'],
            'tiempo_improductivo_total': stats['tiempo_improductivo_total']
        })
    
    # Ordenar por movimientos totales
    gruas_stats_list.sort(key=lambda x: x['movimientos_total'], reverse=True)
    
    # Estadísticas globales
    total_movimientos = sum(g['movimientos_total'] for g in gruas_stats_list)
    promedio_utilizacion = np.mean([g['utilizacion_promedio'] for g in gruas_stats_list])
    
    return {
        'metadata': {
            'anio': anio,
            'semana': semana,
            'turno': turno,
            'participacion': participacion,
            'dispersion': dispersion,
            'turnos_analizados': len(resultados)
        },
        'metricas_por_grua': gruas_stats_list,
        'estadisticas_globales': {
            'total_movimientos': total_movimientos,
            'promedio_utilizacion': promedio_utilizacion,
            'gruas_con_trabajo': len([g for g in gruas_stats_list if g['movimientos_total'] > 0]),
            'gruas_sin_trabajo': len([g for g in gruas_stats_list if g['movimientos_total'] == 0]),
            'movimientos_por_grua': total_movimientos / 12,
            'balance_trabajo': _calculate_balance(gruas_stats_list),
            'grua_mas_ocupada': gruas_stats_list[0]['grua_id'] if gruas_stats_list else None,
            'grua_menos_ocupada': gruas_stats_list[-1]['grua_id'] if gruas_stats_list else None
        },
        'distribucion_trabajo': _get_distribucion_trabajo(gruas_stats_list)
    }


@router.get("/cuotas/{resultado_id}")
async def get_cuotas_detalle(
    resultado_id: UUID = Path(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener detalle de cuotas de camiones para un resultado específico.
    Incluye comparación con movimientos reales.
    """
    
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
    cuotas_detalle = []
    resumen_por_periodo = {}
    
    for cuota in cuotas:
        cuota_data = {
            'periodo': cuota.periodo,
            'bloque': cuota.bloque_codigo,
            'cuota_modelo': cuota.cuota_modelo,
            'capacidad_maxima': cuota.capacidad_maxima,
            'gruas_asignadas': cuota.gruas_asignadas,
            'movimientos_reales': cuota.movimientos_reales or 0,
            'utilizacion_modelo': (cuota.cuota_modelo / cuota.capacidad_maxima * 100) if cuota.capacidad_maxima > 0 else 0,
            'utilizacion_real': cuota.utilizacion_real or 0,
            'brecha': (cuota.movimientos_reales or 0) - cuota.cuota_modelo,
            'tipo_operacion': cuota.tipo_operacion.value,
            'segregaciones': cuota.segregaciones_incluidas or []
        }
        cuotas_detalle.append(cuota_data)
        
        # Agregar a resumen por periodo
        if cuota.periodo not in resumen_por_periodo:
            resumen_por_periodo[cuota.periodo] = {
                'periodo': cuota.periodo,
                'cuota_total': 0,
                'capacidad_total': 0,
                'movimientos_reales_total': 0,
                'bloques_activos': 0,
                'gruas_totales': 0
            }
        
        resumen_por_periodo[cuota.periodo]['cuota_total'] += cuota.cuota_modelo
        resumen_por_periodo[cuota.periodo]['capacidad_total'] += cuota.capacidad_maxima
        resumen_por_periodo[cuota.periodo]['movimientos_reales_total'] += cuota.movimientos_reales or 0
        resumen_por_periodo[cuota.periodo]['gruas_totales'] += cuota.gruas_asignadas
        if cuota.cuota_modelo > 0:
            resumen_por_periodo[cuota.periodo]['bloques_activos'] += 1
    
    # Calcular totales
    total_cuotas = sum(c.cuota_modelo for c in cuotas)
    total_capacidad = sum(c.capacidad_maxima for c in cuotas)
    total_real = sum(c.movimientos_reales or 0 for c in cuotas)
    
    return {
        'resultado_id': str(resultado_id),
        'turno': resultado.turno,
        'fecha': resultado.fecha_inicio.isoformat(),
        'cuotas_detalle': cuotas_detalle,
        'resumen_por_periodo': list(resumen_por_periodo.values()),
        'resumen_global': {
            'total_cuota_modelo': total_cuotas,
            'total_capacidad': total_capacidad,
            'total_movimientos_reales': total_real,
            'utilizacion_modelo': (total_cuotas / total_capacidad * 100) if total_capacidad > 0 else 0,
            'utilizacion_real': (total_real / total_capacidad * 100) if total_capacidad > 0 else 0,
            'accuracy': (min(total_cuotas, total_real) / max(total_cuotas, total_real) * 100) if max(total_cuotas, total_real) > 0 else 0,
            'periodos_activos': len(resumen_por_periodo),
            'bloques_unicos': len(set(c.bloque_codigo for c in cuotas))
        }
    }


@router.get("/analisis-accuracy")
async def get_analisis_accuracy(
    anio: Optional[int] = Query(None),
    semana: Optional[int] = Query(None),
    participacion: Optional[int] = Query(None),
    min_accuracy: Optional[float] = Query(None, ge=0, le=100),
    max_accuracy: Optional[float] = Query(None, ge=0, le=100),
    limit: int = Query(100, le=1000),
    db: AsyncSession = Depends(get_db)
):
    """
    Analizar accuracy del modelo vs realidad con diferentes filtros.
    """
    
    # Query base
    query = select(ResultadoCamila).where(
        and_(
            ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO,
            ResultadoCamila.accuracy_global.isnot(None)
        )
    )
    
    # Aplicar filtros
    if anio:
        query = query.where(ResultadoCamila.anio == anio)
    if semana:
        query = query.where(ResultadoCamila.semana == semana)
    if participacion:
        query = query.where(ResultadoCamila.participacion == participacion)
    if min_accuracy is not None:
        query = query.where(ResultadoCamila.accuracy_global >= min_accuracy)
    if max_accuracy is not None:
        query = query.where(ResultadoCamila.accuracy_global <= max_accuracy)
    
    # Ordenar por accuracy
    query = query.order_by(ResultadoCamila.accuracy_global.desc()).limit(limit)
    
    result = await db.execute(query)
    resultados = result.scalars().all()
    
    if not resultados:
        raise HTTPException(404, "No hay resultados con datos de accuracy")
    
    # Construir respuesta
    resultados_list = []
    for res in resultados:
        resultados_list.append({
            'id': str(res.id),
            'codigo': res.codigo,
            'anio': res.anio,
            'semana': res.semana,
            'turno': res.turno,
            'participacion': res.participacion,
            'accuracy': float(res.accuracy_global),
            'movimientos_modelo': res.total_movimientos_modelo,
            'movimientos_real': res.total_movimientos_real,
            'brecha': res.brecha_movimientos,
            'utilizacion_modelo': float(res.utilizacion_modelo)
        })
    
    # Calcular estadísticas
    accuracies = [r['accuracy'] for r in resultados_list]
    brechas = [r['brecha'] for r in resultados_list]
    
    # Análisis por rango de accuracy
    rangos = {
        'excelente': len([a for a in accuracies if a >= 80]),
        'bueno': len([a for a in accuracies if 60 <= a < 80]),
        'regular': len([a for a in accuracies if 40 <= a < 60]),
        'bajo': len([a for a in accuracies if 20 <= a < 40]),
        'muy_bajo': len([a for a in accuracies if a < 20])
    }
    
    return {
        'total_resultados': len(resultados_list),
        'estadisticas': {
            'accuracy_promedio': np.mean(accuracies),
            'accuracy_mediana': np.median(accuracies),
            'accuracy_min': min(accuracies),
            'accuracy_max': max(accuracies),
            'desviacion_estandar': np.std(accuracies),
            'brecha_promedio': np.mean(brechas),
            'brecha_max': max(brechas),
            'brecha_min': min(brechas)
        },
        'distribucion_accuracy': rangos,
        'resultados': resultados_list,
        'recomendaciones': _get_recomendaciones_accuracy(accuracies, brechas)
    }


@router.get("/estadisticas")
async def get_estadisticas_generales(db: AsyncSession = Depends(get_db)):
    """
    Obtener estadísticas generales del modelo Camila.
    """
    
    # Estadísticas por año
    stats_anio = await db.execute(
        select(
            ResultadoCamila.anio,
            func.count(ResultadoCamila.id).label('total_resultados'),
            func.count(distinct(ResultadoCamila.semana)).label('semanas_unicas'),
            func.avg(ResultadoCamila.utilizacion_modelo).label('utilizacion_promedio'),
            func.avg(ResultadoCamila.coeficiente_variacion).label('cv_promedio'),
            func.avg(ResultadoCamila.accuracy_global).label('accuracy_promedio'),
            func.sum(ResultadoCamila.total_movimientos_modelo).label('movimientos_modelo_total'),
            func.sum(ResultadoCamila.total_movimientos_real).label('movimientos_real_total')
        ).where(
            ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO
        ).group_by(ResultadoCamila.anio).order_by(ResultadoCamila.anio)
    )
    
    # Comparaciones agregadas
    comp_stats = await db.execute(
        select(
            ComparacionReal.tipo_comparacion,
            func.count(ComparacionReal.id).label('total_comparaciones'),
            func.avg(ComparacionReal.accuracy).label('accuracy_promedio'),
            func.avg(ComparacionReal.diferencia_porcentual).label('diferencia_promedio')
        ).group_by(ComparacionReal.tipo_comparacion)
    )
    
    # Total de registros por tabla
    totales = await db.execute(
        select(
            func.count(distinct(ResultadoCamila.id)).label('total_resultados'),
            func.sum(ResultadoCamila.total_movimientos_modelo).label('movimientos_modelo_total'),
            func.sum(ResultadoCamila.total_movimientos_real).label('movimientos_real_total'),
            func.avg(ResultadoCamila.utilizacion_modelo).label('utilizacion_global'),
            func.avg(ResultadoCamila.accuracy_global).label('accuracy_global')
        ).where(ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO)
    )
    
    total_stats = totales.one()
    
    # Contar registros en tablas relacionadas
    counts = {}
    for tabla, modelo in [
        ('flujos_modelo', FlujoModelo),
        ('asignaciones_gruas', AsignacionGrua),
        ('cuotas_camiones', CuotaCamion),
        ('metricas_gruas', MetricaGrua),
        ('comparaciones_real', ComparacionReal)
    ]:
        count_result = await db.execute(select(func.count(modelo.id)))
        counts[tabla] = count_result.scalar()
    
    # Parámetros del modelo
    params_result = await db.execute(select(ParametroCamila))
    parametros = params_result.scalars().all()
    
    return {
        'resumen_global': {
            'total_resultados': total_stats.total_resultados or 0,
            'movimientos_modelo_total': total_stats.movimientos_modelo_total or 0,
            'movimientos_real_total': total_stats.movimientos_real_total or 0,
            'utilizacion_promedio': float(total_stats.utilizacion_global or 0),
            'accuracy_promedio': float(total_stats.accuracy_global or 0),
            'registros_por_tabla': counts
        },
        'estadisticas_por_anio': [
            {
                'anio': row.anio,
                'resultados': row.total_resultados,
                'semanas': row.semanas_unicas,
                'utilizacion_promedio': float(row.utilizacion_promedio or 0),
                'cv_promedio': float(row.cv_promedio or 0),
                'accuracy_promedio': float(row.accuracy_promedio or 0),
                'movimientos_modelo': row.movimientos_modelo_total or 0,
                'movimientos_real': row.movimientos_real_total or 0
            }
            for row in stats_anio
        ],
        'comparaciones_por_tipo': [
            {
                'tipo': row.tipo_comparacion,
                'total': row.total_comparaciones,
                'accuracy_promedio': float(row.accuracy_promedio or 0),
                'diferencia_promedio': float(row.diferencia_promedio or 0)
            }
            for row in comp_stats
        ],
        'parametros_modelo': [
            {
                'codigo': p.codigo,
                'descripcion': p.descripcion,
                'valor_actual': float(p.valor_actual),
                'valor_default': float(p.valor_default),
                'unidad': p.unidad
            }
            for p in parametros
        ]
    }


@router.get("/resultados")
async def get_resultados_disponibles(
    anio: Optional[int] = Query(None),
    semana: Optional[int] = Query(None),
    turno: Optional[int] = Query(None),
    participacion: Optional[int] = Query(None),
    con_dispersion: Optional[bool] = Query(None),
    con_comparacion_real: Optional[bool] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    ordenar_por: str = Query("fecha", regex="^(fecha|accuracy|utilizacion)$"),
    orden: str = Query("desc", regex="^(asc|desc)$"),
    db: AsyncSession = Depends(get_db)
):
    """
    Listar resultados de Camila disponibles con filtros avanzados.
    """
    
    query = select(ResultadoCamila).where(
        ResultadoCamila.estado == EstadoProcesamiento.COMPLETADO
    )
    
    # Aplicar filtros
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
    if con_comparacion_real is not None:
        if con_comparacion_real:
            query = query.where(ResultadoCamila.total_movimientos_real.isnot(None))
        else:
            query = query.where(ResultadoCamila.total_movimientos_real.is_(None))
    
    # Total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Ordenamiento
    if ordenar_por == "fecha":
        order_column = ResultadoCamila.fecha_inicio
    elif ordenar_por == "accuracy":
        order_column = ResultadoCamila.accuracy_global
    else:  # utilizacion
        order_column = ResultadoCamila.utilizacion_modelo
    
    if orden == "desc":
        query = query.order_by(order_column.desc())
    else:
        query = query.order_by(order_column.asc())
    
    # Paginación
    query = query.limit(limit).offset(offset)
    
    result = await db.execute(query)
    resultados = result.scalars().all()
    
    return {
        'total': total,
        'limit': limit,
        'offset': offset,
        'ordenar_por': ordenar_por,
        'orden': orden,
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
                'movimientos_modelo': res.total_movimientos_modelo,
                'movimientos_real': res.total_movimientos_real,
                'accuracy': float(res.accuracy_global) if res.accuracy_global else None,
                'utilizacion': float(res.utilizacion_modelo),
                'tiene_comparacion_real': res.total_movimientos_real is not None
            }
            for res in resultados
        ]
    }


@router.get("/logs/{resultado_id}")
async def get_logs_procesamiento(
    resultado_id: UUID = Path(...),
    db: AsyncSession = Depends(get_db)
):
    """
    Obtener logs de procesamiento para un resultado específico.
    """
    
    # Verificar que existe el resultado
    resultado = await db.get(ResultadoCamila, resultado_id)
    if not resultado:
        raise HTTPException(404, "Resultado no encontrado")
    
    # Obtener logs
    logs_result = await db.execute(
        select(LogProcesamientoCamila)
        .where(LogProcesamientoCamila.resultado_id == resultado_id)
        .order_by(LogProcesamientoCamila.fecha_inicio.desc())
    )
    logs = logs_result.scalars().all()
    
    return {
        'resultado_id': str(resultado_id),
        'codigo': resultado.codigo,
        'total_logs': len(logs),
        'logs': [
            {
                'id': str(log.id),
                'tipo_proceso': log.tipo_proceso,
                'archivo': log.archivo_procesado,
                'fecha_inicio': log.fecha_inicio.isoformat(),
                'fecha_fin': log.fecha_fin.isoformat() if log.fecha_fin else None,
                'duracion_segundos': log.duracion_segundos,
                'estado': log.estado.value,
                'registros_procesados': log.registros_procesados,
                'registros_error': log.registros_error,
                'mensaje': log.mensaje,
                'detalle_error': log.detalle_error,
                'metricas': log.metricas
            }
            for log in logs
        ]
    }


# Funciones auxiliares

def _get_top_bloques(distribucion: Dict[str, int], top_n: int) -> List[Dict]:
    """Obtener los bloques más visitados"""
    sorted_bloques = sorted(
        distribucion.items(),
        key=lambda x: x[1],
        reverse=True
    )[:top_n]
    
    return [
        {'bloque': bloque, 'movimientos': movimientos}
        for bloque, movimientos in sorted_bloques
    ]


def _calculate_balance(metricas: List[Dict]) -> float:
    """Calcular balance de trabajo entre grúas"""
    if not metricas:
        return 0
    
    movimientos = [m.get('movimientos', m.get('movimientos_total', 0)) for m in metricas]
    if not movimientos or sum(movimientos) == 0:
        return 0
    
    promedio = np.mean(movimientos)
    if promedio == 0:
        return 0
    
    desviacion = np.std(movimientos)
    return round((desviacion / promedio) * 100, 2)


def _calculate_model_precision(comparaciones: Dict) -> Dict:
    """Calcular métricas de precisión del modelo"""
    if not comparaciones.get('general'):
        return {'accuracy': 0, 'sesgo': 0, 'categoria': 'sin_datos'}
    
    general = comparaciones['general'].get('movimientos_totales', {})
    accuracy = general.get('accuracy', 0)
    diferencia_pct = general.get('porcentaje', 0)
    
    # Categorizar precisión
    if accuracy >= 80:
        categoria = 'excelente'
    elif accuracy >= 60:
        categoria = 'bueno'
    elif accuracy >= 40:
        categoria = 'regular'
    else:
        categoria = 'bajo'
    
    # Determinar sesgo
    if diferencia_pct > 10:
        sesgo = 'subestima'
    elif diferencia_pct < -10:
        sesgo = 'sobreestima'
    else:
        sesgo = 'balanceado'
    
    return {
        'accuracy': accuracy,
        'categoria': categoria,
        'sesgo': sesgo,
        'diferencia_porcentual': diferencia_pct
    }


def _get_mejor_turno(serie: List[Dict]) -> Dict:
    """Obtener el turno con mejor accuracy"""
    if not serie:
        return {}
    
    mejor = max(serie, key=lambda x: x.get('accuracy', 0))
    return {
        'turno': mejor['turno'],
        'accuracy': mejor['accuracy'],
        'fecha': mejor['fecha_hora']
    }


def _get_peor_turno(serie: List[Dict]) -> Dict:
    """Obtener el turno con peor accuracy"""
    if not serie:
        return {}
    
    # Filtrar turnos con accuracy > 0
    serie_con_accuracy = [t for t in serie if t.get('accuracy', 0) > 0]
    if not serie_con_accuracy:
        return {}
    
    peor = min(serie_con_accuracy, key=lambda x: x.get('accuracy', 0))
    return {
        'turno': peor['turno'],
        'accuracy': peor['accuracy'],
        'fecha': peor['fecha_hora']
    }


def _analyze_tendencia(serie: List[Dict]) -> Dict:
    """Analizar tendencia en la serie temporal"""
    if len(serie) < 3:
        return {'tipo': 'insuficientes_datos'}
    
    # Obtener accuracies ordenadas por turno
    accuracies = [t['accuracy'] for t in sorted(serie, key=lambda x: x['turno'])]
    accuracies_validas = [a for a in accuracies if a > 0]
    
    if len(accuracies_validas) < 3:
        return {'tipo': 'insuficientes_datos'}
    
    # Calcular tendencia simple
    primera_mitad = np.mean(accuracies_validas[:len(accuracies_validas)//2])
    segunda_mitad = np.mean(accuracies_validas[len(accuracies_validas)//2:])
    
    diferencia = segunda_mitad - primera_mitad
    
    if abs(diferencia) < 5:
        tipo = 'estable'
    elif diferencia > 0:
        tipo = 'mejorando'
    else:
        tipo = 'empeorando'
    
    return {
        'tipo': tipo,
        'cambio_porcentual': round(diferencia, 1),
        'primera_mitad_promedio': round(primera_mitad, 1),
        'segunda_mitad_promedio': round(segunda_mitad, 1)
    }


def _get_distribucion_trabajo(gruas_stats: List[Dict]) -> Dict:
    """Analizar distribución de trabajo entre grúas"""
    if not gruas_stats:
        return {}
    
    movimientos = [g['movimientos_total'] for g in gruas_stats]
    total = sum(movimientos)
    
    if total == 0:
        return {
            'tipo': 'sin_trabajo',
            'indice_gini': 0
        }
    
    # Calcular índice de Gini simplificado
    movimientos_sorted = sorted(movimientos)
    n = len(movimientos_sorted)
    index = np.arange(1, n + 1)
    gini = (2 * np.sum(index * movimientos_sorted)) / (n * np.sum(movimientos_sorted)) - (n + 1) / n
    
    # Categorizar distribución
    if gini < 0.2:
        tipo = 'muy_equitativa'
    elif gini < 0.4:
        tipo = 'equitativa'
    elif gini < 0.6:
        tipo = 'moderada'
    else:
        tipo = 'desigual'
    
    return {
        'tipo': tipo,
        'indice_gini': round(gini, 3),
        'gruas_80_20': _calculate_80_20(movimientos)  # ¿Cuántas grúas hacen 80% del trabajo?
    }


def _calculate_80_20(movimientos: List[int]) -> int:
    """Calcular cuántas grúas hacen el 80% del trabajo"""
    if not movimientos or sum(movimientos) == 0:
        return 0
    
    sorted_mov = sorted(movimientos, reverse=True)
    total = sum(sorted_mov)
    target = total * 0.8
    
    cumsum = 0
    for i, mov in enumerate(sorted_mov):
        cumsum += mov
        if cumsum >= target:
            return i + 1
    
    return len(movimientos)


def _get_recomendaciones_accuracy(accuracies: List[float], brechas: List[float]) -> List[str]:
    """Generar recomendaciones basadas en accuracy y brechas"""
    recomendaciones = []
    
    avg_accuracy = np.mean(accuracies)
    avg_brecha = np.mean(brechas)
    
    if avg_accuracy < 30:
        recomendaciones.append("El modelo tiene baja precisión. Revisar parámetros μ, K y W.")
    
    if avg_brecha > 0:
        recomendaciones.append(f"El modelo subestima en promedio {abs(avg_brecha):.0f} movimientos por turno.")
    else:
        recomendaciones.append(f"El modelo sobreestima en promedio {abs(avg_brecha):.0f} movimientos por turno.")
    
    if np.std(accuracies) > 20:
        recomendaciones.append("Alta variabilidad en accuracy. El modelo es inconsistente.")
    
    if avg_accuracy > 70:
        recomendaciones.append("Buen desempeño general del modelo.")
    
    return recomendaciones