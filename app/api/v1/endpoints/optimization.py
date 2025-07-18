# app/api/v1/endpoints/optimization.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, distinct
from sqlalchemy.orm import selectinload
import tempfile
import shutil
import os
import numpy as np  # Agregado al inicio
from app.core.database import get_db
from app.models.optimization import (
    Instancia, Bloque, Segregacion, MovimientoReal,
    MovimientoModelo, DistanciaReal, ResultadoGeneral,
    AsignacionBloque, CargaTrabajo, OcupacionBloque,
    KPIComparativo, MetricaTemporal, LogProcesamiento
)
from app.services.optimization_loader import OptimizationLoader
import logging
from uuid import UUID

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/dashboard")
async def get_optimization_dashboard(
    anio: int = Query(..., ge=2017, le=2023),
    semana: int = Query(..., ge=1, le=52),
    participacion: int = Query(..., description="60-80"),
    dispersion: str = Query(..., regex="^[KN]$", description="K=con dispersión, N=sin dispersión"),
    db: AsyncSession = Depends(get_db)
):
    """Obtener dashboard completo con KPIs de optimización - VERSIÓN CORREGIDA"""
    
    con_dispersion = dispersion == 'K'
    
    # Buscar instancia
    query = select(Instancia).where(
        and_(
            Instancia.anio == anio,
            Instancia.semana == semana,
            Instancia.participacion == participacion,
            Instancia.con_dispersion == con_dispersion,
            Instancia.estado == 'completado'
        )
    ).options(selectinload(Instancia.resultados))
    
    result = await db.execute(query)
    instancia = result.scalar_one_or_none()
    
    if not instancia:
        raise HTTPException(404, f"No hay datos para {anio} S{semana} P{participacion}{dispersion}")
    
    # Obtener resultados generales
    resultados = instancia.resultados
    if not resultados:
        raise HTTPException(404, "No hay resultados procesados para esta instancia")
    
    # Obtener KPIs comparativos
    kpis_query = await db.execute(
        select(KPIComparativo).where(KPIComparativo.instancia_id == instancia.id)
    )
    kpis_list = kpis_query.scalars().all()
    
    # Organizar KPIs por categoría
    kpis_por_categoria = {}
    for kpi in kpis_list:
        if kpi.categoria not in kpis_por_categoria:
            kpis_por_categoria[kpi.categoria] = {}
        kpis_por_categoria[kpi.categoria][kpi.metrica] = {
            'valor_real': float(kpi.valor_real or 0),
            'valor_modelo': float(kpi.valor_modelo or 0),
            'diferencia': float(kpi.diferencia or 0),
            'porcentaje_mejora': float(kpi.porcentaje_mejora or 0),
            'unidad': kpi.unidad
        }
    
    # Obtener ocupación por bloque con capacidades actualizadas
    ocupacion_query = await db.execute(
        select(
            OcupacionBloque.bloque_id,
            Bloque.codigo,
            Bloque.capacidad_teus,  # NUEVO: incluir capacidad
            func.avg(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_promedio'),
            func.max(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_maxima'),
            func.min(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_minima'),
            func.avg(OcupacionBloque.contenedores_teus).label('teus_promedio')
        ).join(Bloque).where(
            OcupacionBloque.instancia_id == instancia.id
        ).group_by(OcupacionBloque.bloque_id, Bloque.codigo, Bloque.capacidad_teus)
    )
    ocupacion_bloques = ocupacion_query.all()
    
    # Obtener distribución temporal
    temporal_query = await db.execute(
        select(MetricaTemporal).where(
            MetricaTemporal.instancia_id == instancia.id
        ).order_by(MetricaTemporal.periodo)
    )
    metricas_temporales = temporal_query.scalars().all()
    
    # Obtener segregaciones activas con asignaciones
    segregaciones_query = await db.execute(
        select(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo,
            Segregacion.descripcion,
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega).label('total_movimientos'),
            func.count(distinct(MovimientoModelo.bloque_id)).label('bloques_usados')
        ).join(Segregacion).where(
            MovimientoModelo.instancia_id == instancia.id
        ).group_by(MovimientoModelo.segregacion_id, Segregacion.codigo, Segregacion.descripcion)
        .having(func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                        MovimientoModelo.descarga + MovimientoModelo.entrega) > 0)
        .order_by(func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                          MovimientoModelo.descarga + MovimientoModelo.entrega).desc())
        .limit(20)
    )
    segregaciones_activas = segregaciones_query.all()
    
    # Obtener asignaciones de bloques
    asignaciones_query = await db.execute(
        select(AsignacionBloque).where(
            AsignacionBloque.instancia_id == instancia.id
        )
    )
    asignaciones = asignaciones_query.scalars().all()
    asignaciones_dict = {a.segregacion_id: a for a in asignaciones}
    
    # CORRECCIÓN: Usar movimientos operativos para cálculo
    movimientos_operativos_real = (
        resultados.movimientos_yard_real + 
        resultados.movimientos_dlvr_real + 
        resultados.movimientos_load_real
    )
    movimientos_operativos_modelo = (
        resultados.movimientos_dlvr_modelo + 
        resultados.movimientos_load_modelo
    )
    
    # Construir respuesta CORREGIDA
    response = {
        'metadata': {
            'instancia_id': str(instancia.id),
            'codigo': instancia.codigo,
            'anio': instancia.anio,
            'semana': instancia.semana,
            'participacion': instancia.participacion,
            'con_dispersion': instancia.con_dispersion,
            'fecha_inicio': instancia.fecha_inicio.isoformat(),
            'fecha_fin': instancia.fecha_fin.isoformat(),
            'periodos': instancia.periodos,
            'fecha_procesamiento': instancia.fecha_procesamiento.isoformat() if instancia.fecha_procesamiento else None,
            'archivo_distancias': resultados.archivo_distancias_usado  # NUEVO
        },
        'kpis_principales': {
            'eficiencia': {
                'real': float(resultados.eficiencia_real or 0),
                'optimizada': float(resultados.eficiencia_modelo or 100),
                'ganancia': float(resultados.eficiencia_ganancia or 0)
            },
            'movimientos': {
                'total_real': resultados.movimientos_reales_total,
                'operativos_real': movimientos_operativos_real,  # NUEVO
                'operativos_modelo': movimientos_operativos_modelo,  # NUEVO
                'yard_eliminados': resultados.movimientos_yard_real,
                'optimizados': resultados.movimientos_optimizados,
                'reduccion_absoluta': resultados.movimientos_reduccion,
                'reduccion_porcentaje': float(resultados.movimientos_reduccion_pct or 0),
                # Desglose detallado
                'detalle': {
                    'dlvr_real': resultados.movimientos_dlvr_real,
                    'dlvr_modelo': resultados.movimientos_dlvr_modelo,
                    'load_real': resultados.movimientos_load_real,
                    'load_modelo': resultados.movimientos_load_modelo,
                    'recv_real': resultados.movimientos_recv_real,
                    'dsch_real': resultados.movimientos_dsch_real
                }
            },
            'distancias': {
                'total_real': resultados.distancia_real_total,
                'total_modelo': resultados.distancia_modelo_total,
                'yard_eliminada': resultados.distancia_real_yard,
                'load_real': resultados.distancia_real_load,
                'load_modelo': resultados.distancia_modelo_load,
                'dlvr_real': resultados.distancia_real_dlvr,
                'dlvr_modelo': resultados.distancia_modelo_dlvr,
                'reduccion_metros': resultados.distancia_reduccion,
                'reduccion_porcentaje': float(resultados.distancia_reduccion_pct or 0),
                'distancia_ahorrada': resultados.distancia_reduccion  # CORRECCIÓN
            },
            'segregaciones': {
                'total': resultados.segregaciones_total,
                'optimizadas': resultados.segregaciones_optimizadas,
                'porcentaje': (resultados.segregaciones_optimizadas / resultados.segregaciones_total * 100) 
                             if resultados.segregaciones_total > 0 else 0
            },
            'ocupacion': {
                'promedio': float(resultados.ocupacion_promedio_pct or 0),
                'maxima': float(resultados.ocupacion_maxima_pct or 0),  # NUEVO
                'minima': float(resultados.ocupacion_minima_pct or 0),  # NUEVO
                'capacidad_total': resultados.capacidad_total_teus
            },
            'carga_trabajo': {
                'total': resultados.carga_trabajo_total,
                'maxima': resultados.carga_maxima,  # NUEVO
                'minima': resultados.carga_minima,  # NUEVO
                'variacion': resultados.variacion_carga,
                'balance': resultados.balance_carga
            }
        },
        'kpis_detallados': kpis_por_categoria,
        'ocupacion_por_bloque': [
            {
                'bloque': bloque.codigo,
                'capacidad': bloque.capacidad_teus,  # NUEVO
                'ocupacion_promedio': float(bloque.ocupacion_promedio or 0),
                'ocupacion_maxima': float(bloque.ocupacion_maxima or 0),
                'ocupacion_minima': float(bloque.ocupacion_minima or 0),
                'teus_promedio': float(bloque.teus_promedio or 0),  # NUEVO
                'utilizacion': float(bloque.teus_promedio / bloque.capacidad_teus * 100) if bloque.capacidad_teus > 0 else 0  # NUEVO
            }
            for bloque in ocupacion_bloques
        ],
        'evolucion_temporal': [
            {
                'periodo': m.periodo,
                'dia': m.dia,
                'turno': m.turno,
                'movimientos_real': m.movimientos_real,
                'movimientos_yard': m.movimientos_yard_real,
                'movimientos_modelo': m.movimientos_modelo,
                'carga_trabajo': m.carga_trabajo,  # NUEVO
                'ocupacion_promedio': float(m.ocupacion_promedio or 0)
            }
            for m in metricas_temporales
        ],
        'segregaciones_activas': [
            {
                'codigo': seg.codigo,
                'descripcion': seg.descripcion,
                'movimientos': int(seg.total_movimientos),
                'bloques_usados': seg.bloques_usados,  # NUEVO
                'bloques_asignados': asignaciones_dict.get(seg.segregacion_id).total_bloques_asignados if seg.segregacion_id in asignaciones_dict else 0  # NUEVO
            }
            for seg in segregaciones_activas
        ],
        'comparacion_resumen': {
            'eliminacion_reubicaciones': {
                'valor': resultados.movimientos_yard_real,
                'porcentaje': 100
            },
            'reduccion_movimientos_operativos': {  # CORREGIDO
                'valor': movimientos_operativos_real - movimientos_operativos_modelo,
                'porcentaje': ((movimientos_operativos_real - movimientos_operativos_modelo) / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0
            },
            'mejora_eficiencia': {
                'valor': float(resultados.eficiencia_ganancia or 0),
                'unidad': 'puntos porcentuales'
            },
            'ahorro_distancia': {
                'valor': resultados.distancia_reduccion,
                'metros_ahorrados': resultados.distancia_reduccion,
                'porcentaje': float(resultados.distancia_reduccion_pct or 0),
                'unidad': 'metros',
                'desglose': {
                    'yard_eliminada': resultados.distancia_real_yard,
                    'load_diferencia': resultados.distancia_real_load - resultados.distancia_modelo_load,
                    'dlvr_diferencia': resultados.distancia_real_dlvr - resultados.distancia_modelo_dlvr
                }
            }
        },
        # KPI destacado CORREGIDO
        'kpi_distancia_ahorrada': {
            'valor': resultados.distancia_reduccion,
            'unidad': 'metros',
            'descripcion': 'Distancia total ahorrada por la optimización',
            'equivalencia': f"{resultados.distancia_reduccion / 1000:.1f} km" if resultados.distancia_reduccion > 0 else "0 km",
            'componentes': {
                'yard_eliminada': f"{resultados.distancia_real_yard / 1000:.1f} km",
                'optimizacion_operaciones': f"{(resultados.distancia_reduccion - resultados.distancia_real_yard) / 1000:.1f} km"
            }
        }
    }
    
    return response

@router.get("/bloques/{bloque_id}/detalle")
async def get_bloque_detalle(
    bloque_id: str,
    instancia_id: UUID,
    periodo: int = Query(..., ge=1, le=21),
    db: AsyncSession = Depends(get_db)
):
    """Obtener detalle de ocupación de un bloque específico por período - MEJORADO"""
    
    # Verificar instancia
    instancia = await db.get(Instancia, instancia_id)
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
    # Obtener datos del bloque
    bloque_query = await db.execute(
        select(Bloque).where(Bloque.codigo == bloque_id)
    )
    bloque = bloque_query.scalar_one_or_none()
    if not bloque:
        raise HTTPException(404, f"Bloque {bloque_id} no encontrado")
    
    # Obtener movimientos del modelo para ese bloque y período
    movimientos_query = await db.execute(
        select(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo.label('segregacion_codigo'),
            Segregacion.descripcion,
            Segregacion.tamano,
            MovimientoModelo.volumen_teus,
            MovimientoModelo.bahias_ocupadas,
            MovimientoModelo.recepcion,
            MovimientoModelo.carga,
            MovimientoModelo.descarga,
            MovimientoModelo.entrega
        ).join(Segregacion).where(
            and_(
                MovimientoModelo.instancia_id == instancia_id,
                MovimientoModelo.bloque_id == bloque.id,
                MovimientoModelo.periodo == periodo
            )
        )
    )
    movimientos = movimientos_query.all()
    
    # Obtener ocupación específica del período
    ocupacion_query = await db.execute(
        select(OcupacionBloque).where(
            and_(
                OcupacionBloque.instancia_id == instancia_id,
                OcupacionBloque.bloque_id == bloque.id,
                OcupacionBloque.periodo == periodo
            )
        )
    )
    ocupacion = ocupacion_query.scalar_one_or_none()
    
    # Construir respuesta mejorada
    bahias_por_segregacion = {}
    volumen_por_segregacion = {}
    teus_por_segregacion = {}
    segregaciones_info = {}
    movimientos_detalle = {}
    
    for mov in movimientos:
        if mov.bahias_ocupadas > 0 or mov.volumen_teus > 0:
            bahias_por_segregacion[mov.segregacion_codigo] = mov.bahias_ocupadas
            volumen_por_segregacion[mov.segregacion_codigo] = mov.volumen_teus
            teus_por_segregacion[mov.segregacion_codigo] = 2 if mov.tamano == 40 else 1
            
            total_mov = mov.recepcion + mov.carga + mov.descarga + mov.entrega
            segregaciones_info[mov.segregacion_codigo] = {
                'descripcion': mov.descripcion or f"Contenedor {mov.tamano} pies",
                'movimientos': total_mov,
                'detalle': {
                    'recepcion': mov.recepcion,
                    'carga': mov.carga,
                    'descarga': mov.descarga,
                    'entrega': mov.entrega
                }
            }
    
    # Clave para el formato esperado
    key = f"{bloque_id}-{periodo}"
    
    return {
        'bloque': {
            'codigo': bloque_id,
            'capacidad_teus': bloque.capacidad_teus,
            'capacidad_bahias': bloque.capacidad_bahias
        },
        'periodo': periodo,
        'ocupacion_actual': {
            'contenedores': ocupacion.contenedores_teus if ocupacion else 0,
            'porcentaje': float(ocupacion.porcentaje_ocupacion) if ocupacion else 0,
            'estado': ocupacion.estado if ocupacion else 'inactivo'
        },
        'bahiasPorBloque': {
            key: bahias_por_segregacion
        },
        'volumenPorBloque': {
            key: volumen_por_segregacion
        },
        'capacidadesPorBloque': {
            bloque_id: bloque.capacidad_teus // bloque.capacidad_bahias if bloque.capacidad_bahias > 0 else 35
        },
        'teusPorSegregacion': teus_por_segregacion,
        'segregacionesInfo': segregaciones_info,
        'resumen': {
            'total_bahias_ocupadas': sum(bahias_por_segregacion.values()),
            'total_volumen_teus': sum(volumen_por_segregacion.values()),
            'segregaciones_activas': len(segregaciones_info)
        }
    }

@router.get("/dashboard/temporal")
async def get_dashboard_temporal(
    anio: int = Query(..., ge=2017, le=2023),
    semana: int = Query(..., ge=1, le=52),
    participacion: int = Query(..., description="60-80"),
    dispersion: str = Query(..., regex="^[KN]$"),
    dia: Optional[int] = Query(None, ge=1, le=7, description="Día específico (1-7)"),
    turno: Optional[int] = Query(None, ge=1, le=3, description="Turno específico (1-3)"),
    periodo_inicio: Optional[int] = Query(None, ge=1, le=21),
    periodo_fin: Optional[int] = Query(None, ge=1, le=21),
    db: AsyncSession = Depends(get_db)
):
    """Dashboard con filtros temporales avanzados - VERSIÓN MEJORADA"""
    
    con_dispersion = dispersion == 'K'
    
    # Buscar instancia
    query = select(Instancia).where(
        and_(
            Instancia.anio == anio,
            Instancia.semana == semana,
            Instancia.participacion == participacion,
            Instancia.con_dispersion == con_dispersion,
            Instancia.estado == 'completado'
        )
    )
    
    result = await db.execute(query)
    instancia = result.scalar_one_or_none()
    
    if not instancia:
        raise HTTPException(404, f"No hay datos para {anio} S{semana} P{participacion}{dispersion}")
    
    # Construir filtros temporales
    temporal_filters = [MetricaTemporal.instancia_id == instancia.id]
    
    if dia is not None:
        temporal_filters.append(MetricaTemporal.dia == dia)
    if turno is not None:
        temporal_filters.append(MetricaTemporal.turno == turno)
    if periodo_inicio is not None and periodo_fin is not None:
        temporal_filters.append(MetricaTemporal.periodo.between(periodo_inicio, periodo_fin))
    elif periodo_inicio is not None:
        temporal_filters.append(MetricaTemporal.periodo >= periodo_inicio)
    elif periodo_fin is not None:
        temporal_filters.append(MetricaTemporal.periodo <= periodo_fin)
    
    # Obtener métricas temporales filtradas
    temporal_query = await db.execute(
        select(MetricaTemporal).where(
            and_(*temporal_filters)
        ).order_by(MetricaTemporal.periodo)
    )
    metricas = temporal_query.scalars().all()
    
    # Calcular KPIs para el período filtrado
    total_real = sum(m.movimientos_real for m in metricas)
    total_yard = sum(m.movimientos_yard_real for m in metricas)
    total_modelo = sum(m.movimientos_modelo for m in metricas)
    
    # Calcular movimientos operativos
    movimientos_operativos_real = total_real  # Asumiendo que incluye todo
    movimientos_operativos_modelo = total_modelo
    
    # Obtener distancias si están disponibles
    distancia_real = sum(m.distancia_real for m in metricas if m.distancia_real)
    distancia_modelo = sum(m.distancia_modelo for m in metricas if m.distancia_modelo)
    distancia_ahorrada = distancia_real - distancia_modelo
    
    # Ocupación por bloque en el período
    ocupacion_filters = [OcupacionBloque.instancia_id == instancia.id]
    if turno:
        ocupacion_filters.append(OcupacionBloque.turno == turno)
    if periodo_inicio and periodo_fin:
        ocupacion_filters.append(OcupacionBloque.periodo.between(periodo_inicio, periodo_fin))
    
    ocupacion_query = await db.execute(
        select(
            OcupacionBloque.bloque_id,
            Bloque.codigo,
            Bloque.capacidad_teus,
            func.avg(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_promedio'),
            func.max(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_maxima'),
            func.avg(OcupacionBloque.contenedores_teus).label('teus_promedio')
        ).join(Bloque).where(
            and_(*ocupacion_filters)
        ).group_by(OcupacionBloque.bloque_id, Bloque.codigo, Bloque.capacidad_teus)
    )
    ocupacion_bloques = ocupacion_query.all()
    
    # Top segregaciones en el período
    mov_modelo_filters = [MovimientoModelo.instancia_id == instancia.id]
    if periodo_inicio and periodo_fin:
        mov_modelo_filters.append(MovimientoModelo.periodo.between(periodo_inicio, periodo_fin))
    
    seg_query = await db.execute(
        select(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo,
            Segregacion.descripcion,
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega).label('total_movimientos'),
            func.count(distinct(MovimientoModelo.bloque_id)).label('bloques_usados')
        ).join(Segregacion).where(
            and_(*mov_modelo_filters)
        ).group_by(MovimientoModelo.segregacion_id, Segregacion.codigo, Segregacion.descripcion)
        .order_by(func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                          MovimientoModelo.descarga + MovimientoModelo.entrega).desc())
        .limit(10)
    )
    top_segregaciones = seg_query.all()
    
    # Carga de trabajo por bloque
    carga_filters = [CargaTrabajo.instancia_id == instancia.id]
    if periodo_inicio and periodo_fin:
        carga_filters.append(CargaTrabajo.periodo.between(periodo_inicio, periodo_fin))
    
    carga_query = await db.execute(
        select(
            CargaTrabajo.bloque_id,
            Bloque.codigo,
            func.sum(CargaTrabajo.carga_trabajo).label('carga_total'),
            func.avg(CargaTrabajo.carga_trabajo).label('carga_promedio'),
            func.max(CargaTrabajo.carga_maxima).label('carga_maxima'),
            func.min(CargaTrabajo.carga_minima).label('carga_minima')
        ).join(Bloque).where(
            and_(*carga_filters)
        ).group_by(CargaTrabajo.bloque_id, Bloque.codigo)
    )
    cargas_trabajo = carga_query.all()
    
    # Calcular balance de carga
    cargas = [c.carga_total for c in cargas_trabajo]
    balance_carga = int(np.std(cargas)) if cargas else 0
    
    # Calcular eficiencia correctamente
    eficiencia_real = ((movimientos_operativos_real - total_yard) / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0
    reduccion_operativos = movimientos_operativos_real - movimientos_operativos_modelo
    porcentaje_reduccion = (reduccion_operativos / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0
    
    return {
        'filtros_aplicados': {
            'anio': anio,
            'semana': semana,
            'participacion': participacion,
            'dispersion': dispersion,
            'dia': dia,
            'turno': turno,
            'periodo_inicio': periodo_inicio,
            'periodo_fin': periodo_fin,
            'registros_analizados': len(metricas)
        },
        'kpis_principales': {
            'eficiencia_real': eficiencia_real,
            'eficiencia_modelo': 100,  # Sin YARD
            'eficiencia_ganada': 100 - eficiencia_real,
            'yard_eliminados': total_yard,
            'reduccion_movimientos': {
                'operativos_real': movimientos_operativos_real,
                'operativos_modelo': movimientos_operativos_modelo,
                'valor': reduccion_operativos,
                'porcentaje': porcentaje_reduccion
            },
            'distancia_ahorrada': distancia_ahorrada if distancia_ahorrada > 0 else 0,
            'movimientos_totales': total_modelo
        },
        'metricas_operacionales': {
            'segregaciones_activas': len(top_segregaciones),
            'balance_carga': balance_carga,
            'carga_maxima': max(c.carga_maxima or 0 for c in cargas_trabajo) if cargas_trabajo else 0,
            'carga_minima': min(c.carga_minima or 0 for c in cargas_trabajo) if cargas_trabajo else 0,
            'ocupacion_promedio': float(sum(b.ocupacion_promedio for b in ocupacion_bloques) / len(ocupacion_bloques)) if ocupacion_bloques else 0
        },
        'evolucion_temporal': [
            {
                'periodo': m.periodo,
                'dia': m.dia,
                'turno': m.turno,
                'hora_inicio': get_hora_turno(m.turno),
                'movimientos_real': m.movimientos_real,
                'movimientos_yard': m.movimientos_yard_real,
                'movimientos_modelo': m.movimientos_modelo,
                'carga_trabajo': m.carga_trabajo,
                'ocupacion_promedio': float(m.ocupacion_promedio or 0),
                'eficiencia_periodo': ((m.movimientos_real - m.movimientos_yard_real) / m.movimientos_real * 100) if m.movimientos_real > 0 else 0
            }
            for m in metricas
        ],
        'ocupacion_bloques': [
            {
                'bloque': b.codigo,
                'capacidad': b.capacidad_teus,
                'ocupacion_promedio': float(b.ocupacion_promedio or 0),
                'ocupacion_maxima': float(b.ocupacion_maxima or 0),
                'teus_promedio': float(b.teus_promedio or 0),
                'utilizacion': float(b.teus_promedio / b.capacidad_teus * 100) if b.capacidad_teus > 0 else 0,
                'estado': 'alto' if b.ocupacion_promedio > 80 else 'medio' if b.ocupacion_promedio > 50 else 'bajo'
            }
            for b in ocupacion_bloques
        ],
        'top_segregaciones': [
            {
                'codigo': s.codigo,
                'descripcion': s.descripcion,
                'movimientos': int(s.total_movimientos),
                'bloques_usados': s.bloques_usados,
                'porcentaje': (s.total_movimientos / total_modelo * 100) if total_modelo > 0 else 0
            }
            for s in top_segregaciones
        ],
        'carga_trabajo_bloques': [
            {
                'bloque': c.codigo,
                'carga_total': int(c.carga_total),
                'carga_promedio': float(c.carga_promedio or 0),
                'carga_maxima': int(c.carga_maxima or 0),
                'carga_minima': int(c.carga_minima or 0)
            }
            for c in cargas_trabajo
        ]
    }

def get_hora_turno(turno: int) -> str:
    """Convierte número de turno a hora de inicio"""
    turnos = {
        1: "08:00",
        2: "15:30", 
        3: "23:00"
    }
    return turnos.get(turno, "")

@router.get("/analisis/segregaciones/{instancia_id}")
async def get_analisis_segregaciones(
    instancia_id: UUID,
    top_n: int = Query(20, le=50),
    db: AsyncSession = Depends(get_db)
):
    """Análisis detallado de segregaciones para una instancia - MEJORADO"""
    
    # Verificar instancia
    instancia = await db.get(Instancia, instancia_id)
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
    # Análisis por segregación con asignaciones
    query = await db.execute(
        select(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo,
            Segregacion.descripcion,
            Segregacion.tipo,
            Segregacion.categoria,
            Segregacion.tamano,
            func.sum(MovimientoModelo.recepcion).label('total_recepcion'),
            func.sum(MovimientoModelo.carga).label('total_carga'),
            func.sum(MovimientoModelo.descarga).label('total_descarga'),
            func.sum(MovimientoModelo.entrega).label('total_entrega'),
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega).label('total_movimientos'),
            func.count(distinct(MovimientoModelo.bloque_id)).label('bloques_asignados'),
            func.count(distinct(MovimientoModelo.periodo)).label('periodos_activos'),
            func.avg(MovimientoModelo.volumen_teus).label('volumen_promedio'),
            func.max(MovimientoModelo.volumen_teus).label('volumen_maximo')
        ).join(Segregacion).where(
            MovimientoModelo.instancia_id == instancia_id
        ).group_by(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo,
            Segregacion.descripcion,
            Segregacion.tipo,
            Segregacion.categoria,
            Segregacion.tamano
        ).having(
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega) > 0
        ).order_by(
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega).desc()
        ).limit(top_n)
    )
    
    segregaciones = query.all()
    
    # Obtener asignaciones
    asignaciones_query = await db.execute(
        select(AsignacionBloque).where(
            AsignacionBloque.instancia_id == instancia_id
        )
    )
    asignaciones = asignaciones_query.scalars().all()
    asignaciones_dict = {a.segregacion_id: a for a in asignaciones}
    
    # Total de movimientos para calcular porcentajes
    total_movimientos = sum(s.total_movimientos for s in segregaciones)
    
    return {
        'instancia': {
            'id': str(instancia.id),
            'codigo': instancia.codigo,
            'total_segregaciones': instancia.total_segregaciones,
            'fecha_procesamiento': instancia.fecha_procesamiento.isoformat() if instancia.fecha_procesamiento else None
        },
        'analisis_segregaciones': [
            {
                'codigo': s.codigo,
                'descripcion': s.descripcion,
                'tipo': s.tipo,
                'categoria': s.categoria,
                'tamano': s.tamano,
                'movimientos': {
                    'recepcion': int(s.total_recepcion),
                    'carga': int(s.total_carga),
                    'descarga': int(s.total_descarga),
                    'entrega': int(s.total_entrega),
                    'total': int(s.total_movimientos)
                },
                'metricas': {
                    'bloques_asignados': s.bloques_asignados,
                    'bloques_asignados_total': asignaciones_dict.get(s.segregacion_id).total_bloques_asignados if s.segregacion_id in asignaciones_dict else s.bloques_asignados,
                    'periodos_activos': s.periodos_activos,
                    'volumen_promedio_teus': float(s.volumen_promedio or 0),
                    'volumen_maximo_teus': int(s.volumen_maximo or 0),
                    'porcentaje_del_total': (s.total_movimientos / total_movimientos * 100) if total_movimientos > 0 else 0
                },
                'promedio_por_periodo': int(s.total_movimientos / s.periodos_activos) if s.periodos_activos > 0 else 0,
                'eficiencia': {
                    'movimientos_por_bloque': float(s.total_movimientos / s.bloques_asignados) if s.bloques_asignados > 0 else 0,
                    'utilizacion_periodos': float(s.periodos_activos / 21 * 100)  # Sobre 21 periodos totales
                }
            }
            for s in segregaciones
        ],
        'resumen': {
            'segregaciones_activas': len(segregaciones),
            'total_movimientos': total_movimientos,
            'promedio_bloques_por_segregacion': float(sum(s.bloques_asignados for s in segregaciones) / len(segregaciones)) if segregaciones else 0,
            'top_5_concentracion': sum(s.total_movimientos for s in segregaciones[:5]) / total_movimientos * 100 if total_movimientos > 0 else 0
        }
    }

@router.get("/analisis/bloques/{instancia_id}")
async def get_analisis_bloques(
    instancia_id: UUID,
    periodo: Optional[int] = Query(None, ge=1, le=21),
    db: AsyncSession = Depends(get_db)
):
    """Análisis detallado de utilización de bloques - VERSIÓN MEJORADA"""
    
    # Verificar instancia
    instancia = await db.get(Instancia, instancia_id)
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
    # Filtros base
    filters = [OcupacionBloque.instancia_id == instancia_id]
    if periodo:
        filters.append(OcupacionBloque.periodo == periodo)
    
    # Estadísticas por bloque con capacidades actualizadas
    query = await db.execute(
        select(
            Bloque.codigo,
            Bloque.capacidad_teus,
            Bloque.capacidad_bahias,
            func.avg(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_promedio'),
            func.max(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_maxima'),
            func.min(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_minima'),
            func.avg(OcupacionBloque.contenedores_teus).label('teus_promedio'),
            func.max(OcupacionBloque.contenedores_teus).label('teus_maximo'),
            func.count(OcupacionBloque.id).label('registros'),
            func.count(func.nullif(OcupacionBloque.estado, 'inactivo')).label('periodos_activos')
        ).join(OcupacionBloque).where(
            and_(*filters)
        ).group_by(Bloque.codigo, Bloque.capacidad_teus, Bloque.capacidad_bahias)
        .order_by(Bloque.codigo)
    )
    
    bloques = query.all()
    
    # Obtener carga de trabajo
    carga_filters = [CargaTrabajo.instancia_id == instancia_id]
    if periodo:
        carga_filters.append(CargaTrabajo.periodo == periodo)
    
    carga_query = await db.execute(
        select(
            Bloque.codigo,
            func.sum(CargaTrabajo.carga_trabajo).label('carga_total'),
            func.avg(CargaTrabajo.carga_trabajo).label('carga_promedio'),
            func.max(CargaTrabajo.carga_maxima).label('carga_maxima'),
            func.min(CargaTrabajo.carga_minima).label('carga_minima')
        ).join(CargaTrabajo).where(
            and_(*carga_filters)
        ).group_by(Bloque.codigo)
    )
    
    cargas = {c.codigo: {
        'total': c.carga_total, 
        'promedio': c.carga_promedio,
        'maxima': c.carga_maxima,
        'minima': c.carga_minima
    } for c in carga_query}
    
    # Obtener segregaciones por bloque
    seg_por_bloque_query = await db.execute(
        select(
            Bloque.codigo,
            func.count(distinct(MovimientoModelo.segregacion_id)).label('segregaciones_distintas')
        ).join(MovimientoModelo).where(
            MovimientoModelo.instancia_id == instancia_id
        ).group_by(Bloque.codigo)
    )
    
    seg_por_bloque = {s.codigo: s.segregaciones_distintas for s in seg_por_bloque_query}
    
    return {
        'instancia': {
            'id': str(instancia.id),
            'codigo': instancia.codigo,
            'periodo_filtrado': periodo,
            'total_bloques': instancia.total_bloques
        },
        'analisis_bloques': [
            {
                'bloque': b.codigo,
                'capacidad': {
                    'teus': b.capacidad_teus,
                    'bahias': b.capacidad_bahias,
                    'teus_por_bahia': b.capacidad_teus // b.capacidad_bahias if b.capacidad_bahias > 0 else 0
                },
                'ocupacion': {
                    'promedio': float(b.ocupacion_promedio or 0),
                    'maxima': float(b.ocupacion_maxima or 0),
                    'minima': float(b.ocupacion_minima or 0),
                    'variabilidad': float(b.ocupacion_maxima - b.ocupacion_minima) if b.ocupacion_maxima and b.ocupacion_minima else 0
                },
                'utilizacion': {
                    'teus_promedio': float(b.teus_promedio or 0),
                    'teus_maximo': int(b.teus_maximo or 0),
                    'factor_utilizacion': float(b.teus_promedio / b.capacidad_teus * 100) if b.capacidad_teus > 0 else 0,
                    'periodos_activos': b.periodos_activos,
                    'tasa_actividad': float(b.periodos_activos / b.registros * 100) if b.registros > 0 else 0
                },
                'carga_trabajo': {
                    'total': int(cargas.get(b.codigo, {}).get('total', 0)),
                    'promedio': float(cargas.get(b.codigo, {}).get('promedio', 0)),
                    'maxima': int(cargas.get(b.codigo, {}).get('maxima', 0)),
                    'minima': int(cargas.get(b.codigo, {}).get('minima', 0))
                },
                'segregaciones_asignadas': seg_por_bloque.get(b.codigo, 0),
                'estado': 'critico' if b.ocupacion_promedio > 85 else 'alto' if b.ocupacion_promedio > 70 else 'medio' if b.ocupacion_promedio > 40 else 'bajo'
            }
            for b in bloques
        ],
        'resumen': {
            'ocupacion_promedio_total': float(sum(b.ocupacion_promedio for b in bloques) / len(bloques)) if bloques else 0,
            'capacidad_total_teus': sum(b.capacidad_teus for b in bloques),
            'teus_promedio_total': sum(b.teus_promedio for b in bloques),
            'utilizacion_promedio': float(sum(b.teus_promedio for b in bloques) / sum(b.capacidad_teus for b in bloques) * 100) if bloques else 0,
            'bloques_criticos': sum(1 for b in bloques if b.ocupacion_promedio > 85),
            'bloques_subutilizados': sum(1 for b in bloques if b.ocupacion_promedio < 30),
            'distribucion_carga': {
                'desviacion_estandar': float(np.std([cargas.get(b.codigo, {}).get('promedio', 0) for b in bloques])) if bloques else 0,
                'coeficiente_variacion': float(np.std([cargas.get(b.codigo, {}).get('promedio', 0) for b in bloques]) / np.mean([cargas.get(b.codigo, {}).get('promedio', 0) for b in bloques]) * 100) if bloques and np.mean([cargas.get(b.codigo, {}).get('promedio', 0) for b in bloques]) > 0 else 0
            }
        }
    }

@router.get("/metrics")  # Alias para compatibilidad
async def get_metrics_magdalena(
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    # Determinar el año basado en la semana o usar default
    anio = 2022  # O extraer de alguna configuración
    
    return await get_optimization_dashboard(
        anio=anio,
        semana=semana,
        participacion=participacion,
        dispersion=dispersion,
        db=db
    )

@router.get("/instancias")
async def get_instancias_disponibles(
    anio: Optional[int] = Query(None, ge=2017, le=2023),
    participacion: Optional[int] = Query(None),
    con_dispersion: Optional[bool] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db)
):
    """Listar instancias disponibles con filtros"""
    
    query = select(Instancia).where(Instancia.estado == 'completado')
    
    if anio:
        query = query.where(Instancia.anio == anio)
    if participacion:
        query = query.where(Instancia.participacion == participacion)
    if con_dispersion is not None:
        query = query.where(Instancia.con_dispersion == con_dispersion)
    
    # Total count
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Paginación
    query = query.order_by(Instancia.anio.desc(), Instancia.semana.desc())
    query = query.limit(limit).offset(offset)
    
    result = await db.execute(query)
    instancias = result.scalars().all()
    
    return {
        'total': total,
        'limit': limit,
        'offset': offset,
        'instancias': [
            {
                'id': str(inst.id),
                'codigo': inst.codigo,
                'anio': inst.anio,
                'semana': inst.semana,
                'participacion': inst.participacion,
                'dispersion': 'K' if inst.con_dispersion else 'N',
                'fecha_inicio': inst.fecha_inicio.isoformat(),
                'fecha_fin': inst.fecha_fin.isoformat(),
                'total_movimientos': inst.total_movimientos,
                'total_segregaciones': inst.total_segregaciones,
                'fecha_procesamiento': inst.fecha_procesamiento.isoformat() if inst.fecha_procesamiento else None
            }
            for inst in instancias
        ]
    }

@router.get("/estadisticas")
async def get_estadisticas_globales(db: AsyncSession = Depends(get_db)):
    """Obtener estadísticas globales del sistema - VERSIÓN MEJORADA"""
    
    # Estadísticas por año
    stats_anio = await db.execute(
        select(
            Instancia.anio,
            func.count(Instancia.id).label('total_instancias'),
            func.count(distinct(Instancia.semana)).label('semanas_unicas'),
            func.count(distinct(Instancia.participacion)).label('participaciones_unicas'),
            func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio'),
            func.sum(ResultadoGeneral.movimientos_yard_real).label('yard_total_eliminados'),
            func.sum(ResultadoGeneral.distancia_reduccion).label('distancia_total_ahorrada'),
            func.avg(ResultadoGeneral.movimientos_reduccion_pct).label('reduccion_movimientos_promedio')
        ).join(ResultadoGeneral).where(
            Instancia.estado == 'completado'
        ).group_by(Instancia.anio).order_by(Instancia.anio)
    )
    
    # Total de registros
    totales = await db.execute(
        select(
            func.count(distinct(Instancia.id)).label('total_instancias'),
            func.sum(ResultadoGeneral.movimientos_reales_total).label('movimientos_totales'),
            func.sum(ResultadoGeneral.movimientos_yard_real).label('yard_totales'),
            func.sum(ResultadoGeneral.distancia_real_total).label('distancia_total_procesada'),
            func.sum(ResultadoGeneral.distancia_reduccion).label('distancia_total_ahorrada'),
            func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio_global'),
            func.avg(ResultadoGeneral.movimientos_reduccion_pct).label('reduccion_promedio_global')
        ).select_from(Instancia).join(ResultadoGeneral)
    )
    
    total_stats = totales.one()
    
    # Estadísticas por participación
    stats_participacion = await db.execute(
        select(
            Instancia.participacion,
            func.count(Instancia.id).label('total_instancias'),
            func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio'),
            func.avg(ResultadoGeneral.movimientos_reduccion_pct).label('reduccion_promedio')
        ).join(ResultadoGeneral).where(
            Instancia.estado == 'completado'
        ).group_by(Instancia.participacion).order_by(Instancia.participacion)
    )
    
    return {
        'resumen_global': {
            'total_instancias': total_stats.total_instancias or 0,
            'movimientos_procesados': total_stats.movimientos_totales or 0,
            'yard_eliminados_total': total_stats.yard_totales or 0,
            'distancia_total_procesada': total_stats.distancia_total_procesada or 0,
            'distancia_total_ahorrada': total_stats.distancia_total_ahorrada or 0,
            'distancia_ahorrada_km': float((total_stats.distancia_total_ahorrada or 0) / 1000),
            'eficiencia_promedio': float(total_stats.eficiencia_promedio_global or 0),
            'reduccion_movimientos_promedio': float(total_stats.reduccion_promedio_global or 0)
        },
        'estadisticas_por_anio': [
            {
                'anio': row.anio,
                'instancias': row.total_instancias,
                'semanas': row.semanas_unicas,
                'participaciones': row.participaciones_unicas,
                'eficiencia_promedio': float(row.eficiencia_promedio or 0),
                'reduccion_movimientos_promedio': float(row.reduccion_movimientos_promedio or 0),
                'yard_eliminados': row.yard_total_eliminados or 0,
                'distancia_ahorrada_km': float((row.distancia_total_ahorrada or 0) / 1000)
            }
            for row in stats_anio
        ],
        'estadisticas_por_participacion': [
            {
                'participacion': row.participacion,
                'instancias': row.total_instancias,
                'eficiencia_promedio': float(row.eficiencia_promedio or 0),
                'reduccion_promedio': float(row.reduccion_promedio or 0)
            }
            for row in stats_participacion
        ]
    }

@router.get("/comparacion/{instancia_id}")
async def get_comparacion_detallada(
    instancia_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Obtener comparación detallada real vs modelo para una instancia - VERSIÓN CORREGIDA"""
    
    # Verificar que existe la instancia
    instancia_result = await db.execute(
        select(Instancia).where(Instancia.id == instancia_id)
        .options(selectinload(Instancia.resultados))
    )
    instancia = instancia_result.scalar_one_or_none()
    
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
    resultados = instancia.resultados
    
    # Obtener movimientos por tipo
    movimientos_real = await db.execute(
        select(
            MovimientoReal.tipo_movimiento,
            func.count(MovimientoReal.id).label('cantidad')
        ).where(
            MovimientoReal.instancia_id == instancia_id
        ).group_by(MovimientoReal.tipo_movimiento)
    )
    
    movimientos_modelo = await db.execute(
        select(
            func.sum(MovimientoModelo.recepcion).label('recepcion'),
            func.sum(MovimientoModelo.carga).label('carga'),
            func.sum(MovimientoModelo.descarga).label('descarga'),
            func.sum(MovimientoModelo.entrega).label('entrega')
        ).where(MovimientoModelo.instancia_id == instancia_id)
    )
    
    mov_real_dict = {row.tipo_movimiento: row.cantidad for row in movimientos_real}
    mov_modelo = movimientos_modelo.one()
    
    # Obtener evolución por periodo
    evolucion = await db.execute(
        select(
            MetricaTemporal.periodo,
            MetricaTemporal.dia,
            MetricaTemporal.turno,
            MetricaTemporal.movimientos_real,
            MetricaTemporal.movimientos_yard_real,
            MetricaTemporal.movimientos_modelo,
            MetricaTemporal.carga_trabajo,
            MetricaTemporal.ocupacion_promedio
        ).where(
            MetricaTemporal.instancia_id == instancia_id
        ).order_by(MetricaTemporal.periodo)
    )
    
    # Calcular movimientos operativos
    movimientos_operativos_real = sum([
        mov_real_dict.get('YARD', 0),
        mov_real_dict.get('DLVR', 0),
        mov_real_dict.get('LOAD', 0)
    ])
    
    movimientos_operativos_modelo = (mov_modelo.carga or 0) + (mov_modelo.entrega or 0)
    
    return {
        'instancia': {
            'codigo': instancia.codigo,
            'fecha_inicio': instancia.fecha_inicio.isoformat(),
            'fecha_fin': instancia.fecha_fin.isoformat(),
            'participacion': instancia.participacion,
            'con_dispersion': instancia.con_dispersion
        },
        'resumen_comparacion': {
            'movimientos_operativos': {
                'real': movimientos_operativos_real,
                'modelo': movimientos_operativos_modelo,
                'reduccion': movimientos_operativos_real - movimientos_operativos_modelo,
                'porcentaje_reduccion': ((movimientos_operativos_real - movimientos_operativos_modelo) / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0
            },
            'distancias': {
                'real': resultados.distancia_real_total if resultados else 0,
                'modelo': resultados.distancia_modelo_total if resultados else 0,
                'ahorro': resultados.distancia_reduccion if resultados else 0,
                'porcentaje_ahorro': float(resultados.distancia_reduccion_pct or 0) if resultados else 0
            }
        },
        'movimientos_por_tipo': {
            'real': mov_real_dict,
            'modelo': {
                'RECV': mov_modelo.recepcion or 0,
                'LOAD': mov_modelo.carga or 0,
                'DSCH': mov_modelo.descarga or 0,
                'DLVR': mov_modelo.entrega or 0,
                'YARD': 0
            }
        },
        'evolucion_temporal': [
            {
                'periodo': row.periodo,
                'dia': row.dia,
                'turno': row.turno,
                'hora': get_hora_turno(row.turno),
                'real': {
                    'total': row.movimientos_real,
                    'yard': row.movimientos_yard_real,
                    'utiles': row.movimientos_real - row.movimientos_yard_real
                },
                'modelo': row.movimientos_modelo,
                'reduccion': row.movimientos_real - row.movimientos_modelo,
                'carga_trabajo': row.carga_trabajo,
                'ocupacion': float(row.ocupacion_promedio or 0)
            }
            for row in evolucion
        ],
        'metricas_eficiencia': {
            'eficiencia_real': float(resultados.eficiencia_real or 0) if resultados else 0,
            'eficiencia_modelo': float(resultados.eficiencia_modelo or 100) if resultados else 100,
            'mejora': float(resultados.eficiencia_ganancia or 0) if resultados else 0
        }
    }

@router.post("/upload")
async def upload_optimization_files(
    resultado_file: UploadFile = File(...),
    instancia_file: Optional[UploadFile] = File(None),
    flujos_file: Optional[UploadFile] = File(None),
    distancias_file: Optional[UploadFile] = File(None),
    fecha_inicio: datetime = Query(...),
    semana: int = Query(...),
    anio: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(..., regex="^[KN]$"),
    db: AsyncSession = Depends(get_db)
):
    """Cargar nuevos archivos de optimización"""
    
    con_dispersion = dispersion == 'K'
    loader = OptimizationLoader(db)
    
    temp_files = []
    
    try:
        # Guardar archivos temporales
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_resultado:
            shutil.copyfileobj(resultado_file.file, tmp_resultado)
            resultado_path = tmp_resultado.name
            temp_files.append(resultado_path)
        
        instancia_path = None
        if instancia_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_instancia:
                shutil.copyfileobj(instancia_file.file, tmp_instancia)
                instancia_path = tmp_instancia.name
                temp_files.append(instancia_path)
        
        flujos_path = None
        if flujos_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_flujos:
                shutil.copyfileobj(flujos_file.file, tmp_flujos)
                flujos_path = tmp_flujos.name
                temp_files.append(flujos_path)
        
        distancias_path = None
        if distancias_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_distancias:
                shutil.copyfileobj(distancias_file.file, tmp_distancias)
                distancias_path = tmp_distancias.name
                temp_files.append(distancias_path)
        
        # Cargar datos
        instancia_id = await loader.load_optimization_results(
            resultado_filepath=resultado_path,
            instancia_filepath=instancia_path,
            flujos_filepath=flujos_path,
            distancias_filepath=distancias_path,
            fecha_inicio=fecha_inicio,
            semana=semana,
            anio=anio,
            participacion=participacion,
            con_dispersion=con_dispersion
        )
        
        await db.commit()
        
        return {
            "message": "Archivos cargados exitosamente",
            "instancia_id": str(instancia_id),
            "config": {
                "anio": anio,
                "semana": semana,
                "participacion": participacion,
                "dispersion": dispersion,
                "fecha_inicio": fecha_inicio.isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Error cargando archivos: {str(e)}")
        await db.rollback()
        raise HTTPException(500, f"Error al cargar archivos: {str(e)}")
        
    finally:
        # Limpiar archivos temporales
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except:
                pass

@router.get("/bloques")
async def get_bloques(db: AsyncSession = Depends(get_db)):
    """Obtener información de bloques con capacidades actualizadas"""
    
    result = await db.execute(
        select(Bloque).order_by(Bloque.codigo)
    )
    bloques = result.scalars().all()
    
    return [
        {
            'id': b.id,
            'codigo': b.codigo,
            'capacidad_teus': b.capacidad_teus,
            'capacidad_bahias': b.capacidad_bahias,
            'capacidad_original': b.capacidad_original,  # NUEVO
            'activo': b.activo
        }
        for b in bloques
    ]

@router.get("/segregaciones")
async def get_segregaciones(
    tipo: Optional[str] = Query(None),
    categoria: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """Obtener información de segregaciones"""
    
    query = select(Segregacion).where(Segregacion.activo == True)
    
    if tipo:
        query = query.where(Segregacion.tipo == tipo)
    if categoria:
        query = query.where(Segregacion.categoria == categoria)
    
    result = await db.execute(query.order_by(Segregacion.codigo))
    segregaciones = result.scalars().all()
    
    return [
        {
            'id': s.id,
            'codigo': s.codigo,
            'descripcion': s.descripcion,
            'tipo': s.tipo,
            'categoria': s.categoria,
            'tamano': s.tamano
        }
        for s in segregaciones
    ]

@router.get("/kpis/resumen")
async def get_kpis_resumen(
    anio: Optional[int] = Query(None),
    participacion: Optional[int] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """Obtener resumen de KPIs principales con valores correctos"""
    
    # Construir query base
    query = select(
        func.count(distinct(Instancia.id)).label('total_instancias'),
        func.avg(ResultadoGeneral.movimientos_reduccion_pct).label('reduccion_movimientos_promedio'),
        func.avg(ResultadoGeneral.distancia_reduccion_pct).label('reduccion_distancia_promedio'),
        func.sum(ResultadoGeneral.movimientos_yard_real).label('yard_eliminados_total'),
        func.sum(ResultadoGeneral.distancia_reduccion).label('distancia_ahorrada_total'),
        func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio')
    ).select_from(Instancia).join(ResultadoGeneral).where(
        Instancia.estado == 'completado'
    )
    
    if anio:
        query = query.where(Instancia.anio == anio)
    if participacion:
        query = query.where(Instancia.participacion == participacion)
    
    result = await db.execute(query)
    stats = result.one()
    
    return {
        'total_instancias': stats.total_instancias or 0,
        'kpis_promedio': {
            'reduccion_movimientos': float(stats.reduccion_movimientos_promedio or 0),
            'reduccion_distancia': float(stats.reduccion_distancia_promedio or 0),
            'eficiencia_ganada': float(stats.eficiencia_promedio or 0)
        },
        'totales_acumulados': {
            'yard_eliminados': stats.yard_eliminados_total or 0,
            'distancia_ahorrada_metros': stats.distancia_ahorrada_total or 0,
            'distancia_ahorrada_km': float((stats.distancia_ahorrada_total or 0) / 1000)
        },
        'filtros_aplicados': {
            'anio': anio,
            'participacion': participacion
        }
    }

@router.get("/diagnostico/{instancia_id}")
async def get_diagnostico_instancia(
    instancia_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Diagnóstico completo de una instancia para verificar cálculos"""
    
    # Obtener instancia con resultados
    instancia_result = await db.execute(
        select(Instancia).where(Instancia.id == instancia_id)
        .options(selectinload(Instancia.resultados))
    )
    instancia = instancia_result.scalar_one_or_none()
    
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
    resultados = instancia.resultados
    
    # Obtener totales de movimientos reales
    movs_real = await db.execute(
        select(
            func.count(MovimientoReal.id).label('total'),
            func.sum(func.cast(MovimientoReal.tipo_movimiento == 'YARD', Integer)).label('yard'),
            func.sum(func.cast(MovimientoReal.tipo_movimiento == 'DLVR', Integer)).label('dlvr'),
            func.sum(func.cast(MovimientoReal.tipo_movimiento == 'LOAD', Integer)).label('load'),
            func.sum(func.cast(MovimientoReal.tipo_movimiento == 'RECV', Integer)).label('recv'),
            func.sum(func.cast(MovimientoReal.tipo_movimiento == 'DSCH', Integer)).label('dsch')
        ).where(MovimientoReal.instancia_id == instancia_id)
    )
    real_stats = movs_real.one()
    
    # Obtener KPIs
    kpis_result = await db.execute(
        select(KPIComparativo).where(
            KPIComparativo.instancia_id == instancia_id
        ).order_by(KPIComparativo.categoria, KPIComparativo.metrica)
    )
    kpis = kpis_result.scalars().all()
    
    # Obtener capacidades de bloques
    bloques_result = await db.execute(
        select(Bloque).order_by(Bloque.codigo)
    )
    bloques = bloques_result.scalars().all()
    
    # Calcular métricas correctas
    movimientos_operativos_real = real_stats.yard + real_stats.dlvr + real_stats.load
    movimientos_operativos_modelo = (resultados.movimientos_dlvr_modelo + resultados.movimientos_load_modelo) if resultados else 0
    
    diagnostico = {
        'instancia': {
            'id': str(instancia.id),
            'codigo': instancia.codigo,
            'fecha_procesamiento': instancia.fecha_procesamiento.isoformat() if instancia.fecha_procesamiento else None
        },
        'movimientos_reales_detalle': {
            'total': real_stats.total,
            'yard': real_stats.yard,
            'dlvr': real_stats.dlvr,
            'load': real_stats.load,
            'recv': real_stats.recv,
            'dsch': real_stats.dsch,
            'operativos_total': movimientos_operativos_real
        },
        'movimientos_modelo_detalle': {
            'dlvr': resultados.movimientos_dlvr_modelo if resultados else 0,
            'load': resultados.movimientos_load_modelo if resultados else 0,
            'total': movimientos_operativos_modelo
        },
        'calculos_verificacion': {
            'reduccion_movimientos': {
                'formula': '(operativos_real - operativos_modelo) / operativos_real * 100',
                'valores': f'({movimientos_operativos_real} - {movimientos_operativos_modelo}) / {movimientos_operativos_real} * 100',
                'resultado_esperado': ((movimientos_operativos_real - movimientos_operativos_modelo) / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0,
                'resultado_guardado': float(resultados.movimientos_reduccion_pct) if resultados else 0,
                'coincide': abs(((movimientos_operativos_real - movimientos_operativos_modelo) / movimientos_operativos_real * 100) - float(resultados.movimientos_reduccion_pct or 0)) < 0.1 if resultados and movimientos_operativos_real > 0 else False
            },
            'distancia_ahorrada': {
                'total_real': resultados.distancia_real_total if resultados else 0,
                'total_modelo': resultados.distancia_modelo_total if resultados else 0,
                'diferencia': resultados.distancia_reduccion if resultados else 0,
                'porcentaje': float(resultados.distancia_reduccion_pct) if resultados else 0
            }
        },
        'capacidades_bloques': [
            {
                'codigo': b.codigo,
                'capacidad_teus': b.capacidad_teus,
                'capacidad_original': b.capacidad_original,
                'actualizada': b.capacidad_teus != b.capacidad_original if b.capacidad_original else False
            }
            for b in bloques
        ],
        'kpis_guardados': [
            {
                'categoria': kpi.categoria,
                'metrica': kpi.metrica,
                'valor_real': float(kpi.valor_real or 0),
                'valor_modelo': float(kpi.valor_modelo or 0),
                'porcentaje_mejora': float(kpi.porcentaje_mejora or 0)
            }
            for kpi in kpis
        ],
        'archivo_distancias_usado': resultados.archivo_distancias_usado if resultados else None
    }
    
    return diagnostico