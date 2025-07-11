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
    """Obtener dashboard completo con KPIs de optimización"""
    
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
    
    # Obtener ocupación por bloque
    ocupacion_query = await db.execute(
        select(
            OcupacionBloque.bloque_id,
            Bloque.codigo,
            func.avg(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_promedio'),
            func.max(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_maxima'),
            func.min(OcupacionBloque.porcentaje_ocupacion).label('ocupacion_minima')
        ).join(Bloque).where(
            OcupacionBloque.instancia_id == instancia.id
        ).group_by(OcupacionBloque.bloque_id, Bloque.codigo)
    )
    ocupacion_bloques = ocupacion_query.all()
    
    # Obtener distribución temporal
    temporal_query = await db.execute(
        select(MetricaTemporal).where(
            MetricaTemporal.instancia_id == instancia.id
        ).order_by(MetricaTemporal.periodo)
    )
    metricas_temporales = temporal_query.scalars().all()
    
    # Obtener segregaciones activas
    segregaciones_query = await db.execute(
        select(
            MovimientoModelo.segregacion_id,
            Segregacion.codigo,
            Segregacion.descripcion,
            func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                    MovimientoModelo.descarga + MovimientoModelo.entrega).label('total_movimientos')
        ).join(Segregacion).where(
            MovimientoModelo.instancia_id == instancia.id
        ).group_by(MovimientoModelo.segregacion_id, Segregacion.codigo, Segregacion.descripcion)
        .having(func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                        MovimientoModelo.descarga + MovimientoModelo.entrega) > 0)
    )
    segregaciones_activas = segregaciones_query.all()
    
    # Construir respuesta
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
            'fecha_procesamiento': instancia.fecha_procesamiento.isoformat() if instancia.fecha_procesamiento else None
        },
        'kpis_principales': {
            'eficiencia': {
                'real': float(resultados.eficiencia_real or 0),
                'optimizada': float(resultados.eficiencia_modelo or 100),
                'ganancia': float(resultados.eficiencia_ganancia or 0)
            },
            'movimientos': {
                'total_real': resultados.movimientos_reales_total,
                'yard_eliminados': resultados.movimientos_yard_real,
                'optimizados': resultados.movimientos_optimizados,
                'reduccion_porcentaje': float(resultados.movimientos_reduccion_pct or 0)
            },
            'distancias': {
                'total_real': resultados.distancia_real_total,
                'total_modelo': resultados.distancia_modelo_total,
                'yard_eliminada': resultados.distancia_real_yard,
                'reduccion_porcentaje': float(resultados.distancia_reduccion_pct or 0)
            },
            'segregaciones': {
                'total': resultados.segregaciones_total,
                'optimizadas': resultados.segregaciones_optimizadas,
                'porcentaje': (resultados.segregaciones_optimizadas / resultados.segregaciones_total * 100) 
                             if resultados.segregaciones_total > 0 else 0
            },
            'ocupacion': {
                'promedio': float(resultados.ocupacion_promedio_pct or 0),
                'capacidad_total': resultados.capacidad_total_teus
            },
            'carga_trabajo': {
                'total': resultados.carga_trabajo_total,
                'variacion': resultados.variacion_carga,
                'balance': resultados.balance_carga
            }
        },
        'kpis_detallados': kpis_por_categoria,
        'ocupacion_por_bloque': [
            {
                'bloque': bloque.codigo,
                'ocupacion_promedio': float(bloque.ocupacion_promedio or 0),
                'ocupacion_maxima': float(bloque.ocupacion_maxima or 0),
                'ocupacion_minima': float(bloque.ocupacion_minima or 0)
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
                'ocupacion_promedio': float(m.ocupacion_promedio or 0)
            }
            for m in metricas_temporales
        ],
        'segregaciones_activas': [
            {
                'codigo': seg.codigo,
                'descripcion': seg.descripcion,
                'movimientos': int(seg.total_movimientos)
            }
            for seg in segregaciones_activas
        ],
        'comparacion_resumen': {
            'eliminacion_reubicaciones': {
                'valor': resultados.movimientos_yard_real,
                'porcentaje': 100
            },
            'reduccion_movimientos': {
                'valor': resultados.movimientos_reduccion,
                'porcentaje': float(resultados.movimientos_reduccion_pct or 0)
            },
            'mejora_eficiencia': {
                'valor': float(resultados.eficiencia_ganancia or 0),
                'unidad': 'puntos porcentuales'
            },
            'ahorro_distancia': {
                'valor': resultados.distancia_reduccion,
                'porcentaje': float(resultados.distancia_reduccion_pct or 0),
                'unidad': 'metros'
            }
        }
    }
    
    return response
@router.get("/metrics")  # Alias para compatibilidad con el frontend actual
async def get_metrics_magdalena(
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    # Determinar el año basado en alguna lógica o usar un default
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
                'total_segregaciones': inst.total_segregaciones
            }
            for inst in instancias
        ]
    }

@router.get("/estadisticas")
async def get_estadisticas_globales(db: AsyncSession = Depends(get_db)):
    """Obtener estadísticas globales del sistema"""
    
    # Estadísticas por año
    stats_anio = await db.execute(
        select(
            Instancia.anio,
            func.count(Instancia.id).label('total_instancias'),
            func.count(distinct(Instancia.semana)).label('semanas_unicas'),
            func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio'),
            func.sum(ResultadoGeneral.movimientos_yard_real).label('yard_total_eliminados')
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
            func.avg(ResultadoGeneral.eficiencia_ganancia).label('eficiencia_promedio_global')
        ).select_from(Instancia).join(ResultadoGeneral)
    )
    
    total_stats = totales.one()
    
    return {
        'resumen_global': {
            'total_instancias': total_stats.total_instancias or 0,
            'movimientos_procesados': total_stats.movimientos_totales or 0,
            'yard_eliminados_total': total_stats.yard_totales or 0,
            'eficiencia_promedio': float(total_stats.eficiencia_promedio_global or 0)
        },
        'estadisticas_por_anio': [
            {
                'anio': row.anio,
                'instancias': row.total_instancias,
                'semanas': row.semanas_unicas,
                'eficiencia_promedio': float(row.eficiencia_promedio or 0),
                'yard_eliminados': row.yard_total_eliminados or 0
            }
            for row in stats_anio
        ]
    }

@router.get("/comparacion/{instancia_id}")
async def get_comparacion_detallada(
    instancia_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Obtener comparación detallada real vs modelo para una instancia"""
    
    # Verificar que existe la instancia
    instancia_result = await db.execute(
        select(Instancia).where(Instancia.id == instancia_id)
    )
    instancia = instancia_result.scalar_one_or_none()
    
    if not instancia:
        raise HTTPException(404, "Instancia no encontrada")
    
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
            MetricaTemporal.movimientos_real,
            MetricaTemporal.movimientos_yard_real,
            MetricaTemporal.movimientos_modelo,
            MetricaTemporal.ocupacion_promedio
        ).where(
            MetricaTemporal.instancia_id == instancia_id
        ).order_by(MetricaTemporal.periodo)
    )
    
    return {
        'instancia': {
            'codigo': instancia.codigo,
            'fecha_inicio': instancia.fecha_inicio.isoformat(),
            'fecha_fin': instancia.fecha_fin.isoformat()
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
                'real': {
                    'total': row.movimientos_real,
                    'yard': row.movimientos_yard_real,
                    'utiles': row.movimientos_real - row.movimientos_yard_real
                },
                'modelo': row.movimientos_modelo,
                'reduccion': row.movimientos_real - row.movimientos_modelo,
                'ocupacion': float(row.ocupacion_promedio or 0)
            }
            for row in evolucion
        ]
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
    """Obtener información de bloques"""
    
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