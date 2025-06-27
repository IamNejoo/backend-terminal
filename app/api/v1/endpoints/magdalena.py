# app/api/v1/endpoints/magdalena.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func
import tempfile
import shutil
import os
from app.core.database import get_db
from app.models.magdalena import *
from app.services.magdalena_loader import MagdalenaLoader
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Función helper para colores de segregaciones
def get_segregation_color(segregation_id: str) -> str:
    colors = [
        '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
        '#EC4899', '#14B8A6', '#F97316', '#6366F1', '#84CC16',
        '#06B6D4', '#A855F7', '#DC2626', '#059669', '#7C3AED',
        '#2563EB', '#EA580C', '#0891B2', '#9333EA', '#16A34A'
    ]
    try:
        index = int(segregation_id.replace('S', '')) % len(colors)
    except:
        index = 0
    return colors[index]

@router.get("/metrics")
async def get_magdalena_metrics(
    semana: int = Query(..., ge=1, le=52),
    participacion: int = Query(..., description="68, 69 o 70"),
    dispersion: str = Query(..., regex="^[KC]$", description="K=con dispersión, C=centralizada"),
    db: AsyncSession = Depends(get_db)
):
    """Obtener métricas completas de Magdalena (equivalente a useMagdalenaData)"""
    
    con_dispersion = dispersion == 'K'
    
    # Obtener run
    run_query = await db.execute(
        select(MagdalenaRun).where(
            MagdalenaRun.semana == semana,
            MagdalenaRun.participacion == participacion,
            MagdalenaRun.con_dispersion == con_dispersion
        )
    )
    run = run_query.scalar_one_or_none()
    
    if not run:
        raise HTTPException(404, f"No hay datos para S{semana}_P{participacion}_{dispersion}")
    
    # Obtener datos reales
    real_query = await db.execute(
        select(MagdalenaRealData).where(MagdalenaRealData.semana == semana)
    )
    real_data = real_query.scalar_one_or_none()
    
    if not real_data:
        raise HTTPException(404, f"No hay datos reales para semana {semana}")
    
    # Obtener instancia
    inst_query = await db.execute(
        select(MagdalenaInstancia).where(
            MagdalenaInstancia.semana == semana,
            MagdalenaInstancia.participacion == participacion,
            MagdalenaInstancia.con_dispersion == con_dispersion
        )
    )
    instancia = inst_query.scalar_one_or_none()
    
    # 1. Procesar datos generales
    general_query = await db.execute(
        select(MagdalenaGeneral).where(MagdalenaGeneral.run_id == run.id)
    )
    general_data = general_query.scalars().all()
    
    total_movimientos_optimizados = 0
    movimientos_optimizados_detalle = {
        'Recepcion': 0, 'Carga': 0, 'Descarga': 0, 'Entrega': 0
    }
    bloques_set = set()
    periodos_set = set()
    segregaciones_por_bloque = []
    
    for g in general_data:
        total = g.recepcion + g.carga + g.descarga + g.entrega
        if total > 0:
            movimientos_optimizados_detalle['Recepcion'] += g.recepcion
            movimientos_optimizados_detalle['Carga'] += g.carga
            movimientos_optimizados_detalle['Descarga'] += g.descarga
            movimientos_optimizados_detalle['Entrega'] += g.entrega
            total_movimientos_optimizados += total
            
            segregaciones_por_bloque.append({
                'segregacion': g.segregacion,
                'bloque': g.bloque,
                'periodo': g.periodo,
                'volumen': total
            })
        
        bloques_set.add(g.bloque)
        periodos_set.add(g.periodo)
    
    # 2. Procesar ocupación
    ocupacion_query = await db.execute(
        select(MagdalenaOcupacion).where(MagdalenaOcupacion.run_id == run.id)
    )
    ocupacion_data = ocupacion_query.scalars().all()
    
    ocupacion_por_periodo = []
    ocupacion_total = 0
    capacidad_total = 0
    
    # Agrupar por periodo
    periodo_map = {}
    for o in ocupacion_data:
        if o.periodo not in periodo_map:
            periodo_map[o.periodo] = {'volumen': 0, 'capacidad': 0}
        periodo_map[o.periodo]['volumen'] += o.volumen_teus
        periodo_map[o.periodo]['capacidad'] += o.capacidad_bloque
        ocupacion_total += o.volumen_teus
        capacidad_total += o.capacidad_bloque
    
    for periodo, data in sorted(periodo_map.items()):
        ocupacion_por_periodo.append({
            'periodo': periodo,
            'ocupacion': (data['volumen'] / data['capacidad'] * 100) if data['capacidad'] > 0 else 0,
            'capacidad': data['capacidad']
        })
    
    # 3. Procesar workload
    workload_query = await db.execute(
        select(MagdalenaWorkload).where(MagdalenaWorkload.run_id == run.id)
    )
    workload_data = workload_query.scalars().all()
    
    carga_trabajo_total = sum(w.carga_trabajo for w in workload_data)
    workload_por_bloque = []
    workload_by_bloque = {}
    
    for w in workload_data:
        workload_por_bloque.append({
            'bloque': w.bloque,
            'cargaTrabajo': w.carga_trabajo,
            'periodo': w.periodo
        })
        
        if w.bloque not in workload_by_bloque:
            workload_by_bloque[w.bloque] = []
        workload_by_bloque[w.bloque].append(w.carga_trabajo)
    
    # Calcular variación y balance
    balance_workload = 0
    if workload_by_bloque:
        promedios = [sum(cargas)/len(cargas) for cargas in workload_by_bloque.values()]
        if promedios:
            promedio_general = sum(promedios) / len(promedios)
            varianza = sum((p - promedio_general)**2 for p in promedios) / len(promedios)
            balance_workload = varianza ** 0.5
    
    # 4. Procesar bahías
    bahias_query = await db.execute(
        select(MagdalenaBahias).where(MagdalenaBahias.run_id == run.id)
    )
    bahias_data = bahias_query.scalars().all()
    
    bahias_por_bloque = {}
    for b in bahias_data:
        key = f"{b.bloque}-{b.periodo}"
        if key not in bahias_por_bloque:
            bahias_por_bloque[key] = {}
        bahias_por_bloque[key][b.segregacion] = b.bahias_ocupadas
    
    # 5. Procesar volumen
    volumen_query = await db.execute(
        select(MagdalenaVolumen).where(MagdalenaVolumen.run_id == run.id)
    )
    volumen_data = volumen_query.scalars().all()
    
    volumen_por_bloque = {}
    for v in volumen_data:
        key = f"{v.bloque}-{v.periodo}"
        if key not in volumen_por_bloque:
            volumen_por_bloque[key] = {}
        volumen_por_bloque[key][v.segregacion] = v.volumen
    
    # 6. Distribucion segregaciones
    segregacion_count = {}
    for b in bahias_data:
        if b.bahias_ocupadas > 0:
            if b.segregacion not in segregacion_count:
                segregacion_count[b.segregacion] = 0
            segregacion_count[b.segregacion] += 1
    
    distribucion_segregaciones = [
        {
            'segregacion': seg,
            'bloques': count,
            'ocupacion': 0
        }
        for seg, count in segregacion_count.items()
    ]
    
    # 7. Bloques Magdalena (ocupación por turno)
    bloques_magdalena = []
    for bloque_id in ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']:
        ocupacion_por_turno = []
        movimientos = {
            'entrega': 0, 'recepcion': 0, 'carga': 0, 'descarga': 0, 'total': 0
        }
        
        # Calcular ocupación por periodo para este bloque
        bloque_ocupacion = [o for o in ocupacion_data if o.bloque == bloque_id]
        max_periodo = max([o.periodo for o in bloque_ocupacion], default=21)
        
        for periodo in range(1, max_periodo + 1):
            periodo_data = next((o for o in bloque_ocupacion if o.periodo == periodo), None)
            if periodo_data and periodo_data.capacidad_bloque > 0:
                ocupacion = (periodo_data.volumen_teus / periodo_data.capacidad_bloque) * 100
                ocupacion_por_turno.append(int(ocupacion))
            else:
                ocupacion_por_turno.append(0)
        
        # Calcular movimientos totales del bloque
        bloque_movs = [g for g in general_data if g.bloque == bloque_id]
        for g in bloque_movs:
            movimientos['entrega'] += g.entrega
            movimientos['recepcion'] += g.recepcion
            movimientos['carga'] += g.carga
            movimientos['descarga'] += g.descarga
        movimientos['total'] = sum([movimientos[k] for k in ['entrega', 'recepcion', 'carga', 'descarga']])
        
        ocupacion_promedio = sum(ocupacion_por_turno) / len(ocupacion_por_turno) if ocupacion_por_turno else 0
        
        bloques_magdalena.append({
            'bloqueId': bloque_id,
            'ocupacionPromedio': int(ocupacion_promedio),
            'capacidad': (instancia.capacidades_bloques.get(bloque_id, 35) * 35) if instancia else 1260,
            'ocupacionPorTurno': ocupacion_por_turno,
            'movimientos': movimientos,
            'estado': 'active' if movimientos['total'] > 0 else 'maintenance'
        })
    
    # 8. Colores de segregaciones
    segregaciones_colores = {}
    all_segregaciones = set()
    
    if instancia and instancia.info_segregaciones:
        all_segregaciones.update(instancia.info_segregaciones.keys())
    
    for key, segs in bahias_por_bloque.items():
        all_segregaciones.update(segs.keys())
    
    for seg in all_segregaciones:
        if seg.startswith('S'):
            segregaciones_colores[seg] = get_segregation_color(seg)
    
    # Construir respuesta completa
    response = {
        'magdalenaMetrics': {
            'totalMovimientos': real_data.total_movimientos,
            'reubicaciones': real_data.reubicaciones,
            'eficienciaReal': 100 - (real_data.reubicaciones / real_data.total_movimientos * 100 if real_data.total_movimientos > 0 else 0),
            'totalMovimientosOptimizados': total_movimientos_optimizados,
            'reubicacionesEliminadas': real_data.reubicaciones,
            'eficienciaGanada': (real_data.reubicaciones / real_data.total_movimientos * 100 if real_data.total_movimientos > 0 else 0),
            'segregacionesActivas': len(segregacion_count),
            'bloquesAsignados': len(bloques_set),
            'distribucionSegregaciones': distribucion_segregaciones,
            'cargaTrabajoTotal': carga_trabajo_total,
            'variacionCarga': 0,  # Se puede calcular si es necesario
            'balanceWorkload': balance_workload,
            'ocupacionPromedio': (ocupacion_total / capacidad_total * 100) if capacidad_total > 0 else 0,
            'utilizacionEspacio': (ocupacion_total / capacidad_total * 100) if capacidad_total > 0 else 0,
            'movimientosReales': {
                'DLVR': real_data.movimientos_dlvr,
                'DSCH': real_data.movimientos_dsch,
                'LOAD': real_data.movimientos_load,
                'RECV': real_data.movimientos_recv,
                'OTHR': real_data.movimientos_othr,
                'YARD': real_data.reubicaciones
            },
            'movimientosOptimizadosDetalle': movimientos_optimizados_detalle,
            'periodos': max(periodos_set) if periodos_set else 0,
            'bloquesUnicos': sorted(list(bloques_set)),
            'ocupacionPorPeriodo': ocupacion_por_periodo,
            'workloadPorBloque': workload_por_bloque,
            'segregacionesPorBloque': segregaciones_por_bloque,
            'bloquesMagdalena': bloques_magdalena,
            'capacidadesPorBloque': instancia.capacidades_bloques if instancia else {},
            'teusPorSegregacion': instancia.teus_segregaciones if instancia else {},
            'segregacionesInfo': instancia.info_segregaciones if instancia else {},
            'bahiasPorBloque': bahias_por_bloque,
            'volumenPorBloque': volumen_por_bloque,
            'segregacionesColores': segregaciones_colores
        },
        'realMetrics': {
            'totalMovimientos': real_data.total_movimientos,
            'reubicaciones': real_data.reubicaciones,
            'porcentajeReubicaciones': (real_data.reubicaciones / real_data.total_movimientos * 100) if real_data.total_movimientos > 0 else 0,
            'movimientosPorTipo': {
                'DLVR': real_data.movimientos_dlvr,
                'DSCH': real_data.movimientos_dsch,
                'LOAD': real_data.movimientos_load,
                'RECV': real_data.movimientos_recv,
                'OTHR': real_data.movimientos_othr
            },
            'bloquesUnicos': real_data.bloques_unicos,
            'turnos': real_data.turnos,
            'carriers': real_data.carriers
        },
        'comparison': {
            'eliminacionReubicaciones': real_data.reubicaciones,
            'mejoraPorcentual': (real_data.reubicaciones / real_data.total_movimientos * 100) if real_data.total_movimientos > 0 else 0,
            'optimizacionSegregaciones': len(segregacion_count),
            'balanceCargaMejorado': balance_workload < 50,
            'eficienciaTotal': 100
        },
        'lastUpdated': datetime.utcnow().isoformat(),
        'dataNotAvailable': False
    }
    
    return response

@router.get("/workload")
async def get_workload_data(
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    bloque: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """Obtener datos de workload por bloque/periodo"""
    
    con_dispersion = dispersion == 'K'
    
    # Obtener run
    run_query = await db.execute(
        select(MagdalenaRun).where(
            MagdalenaRun.semana == semana,
            MagdalenaRun.participacion == participacion,
            MagdalenaRun.con_dispersion == con_dispersion
        )
    )
    run = run_query.scalar_one_or_none()
    
    if not run:
        raise HTTPException(404, "No hay datos para esta configuración")
    
    # Query workload
    query = select(MagdalenaWorkload).where(MagdalenaWorkload.run_id == run.id)
    
    if bloque:
        query = query.where(MagdalenaWorkload.bloque == bloque)
    
    result = await db.execute(query.order_by(MagdalenaWorkload.bloque, MagdalenaWorkload.periodo))
    workload_data = result.scalars().all()
    
    return [{
        'bloque': w.bloque,
        'periodo': w.periodo,
        'cargaTrabajo': w.carga_trabajo
    } for w in workload_data]

@router.get("/segregations/{bloque}/{periodo}")
async def get_segregations_detail(
    bloque: str,
    periodo: int,
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(...),
    db: AsyncSession = Depends(get_db)
):
    """Obtener detalle de segregaciones para un bloque/periodo específico"""
    
    con_dispersion = dispersion == 'K'
    
    # Obtener run
    run_query = await db.execute(
        select(MagdalenaRun).where(
            MagdalenaRun.semana == semana,
            MagdalenaRun.participacion == participacion,
            MagdalenaRun.con_dispersion == con_dispersion
        )
    )
    run = run_query.scalar_one_or_none()
    
    if not run:
        raise HTTPException(404, "No hay datos para esta configuración")
    
    # Obtener bahías
    bahias_query = await db.execute(
        select(MagdalenaBahias).where(
            and_(
                MagdalenaBahias.run_id == run.id,
                MagdalenaBahias.bloque == bloque,
                MagdalenaBahias.periodo == periodo
            )
        )
    )
    bahias = bahias_query.scalars().all()
    
    # Obtener volumen
    volumen_query = await db.execute(
        select(MagdalenaVolumen).where(
            and_(
                MagdalenaVolumen.run_id == run.id,
                MagdalenaVolumen.bloque == bloque,
                MagdalenaVolumen.periodo == periodo
            )
        )
    )
    volumenes = volumen_query.scalars().all()
    
    # Combinar datos
    volumen_dict = {v.segregacion: v.volumen for v in volumenes}
    
    result = {}
    for b in bahias:
        result[b.segregacion] = {
            'bahias': b.bahias_ocupadas,
            'volumen': volumen_dict.get(b.segregacion, 0),
            'color': get_segregation_color(b.segregacion)
        }
    
    return result

@router.post("/upload")
async def upload_magdalena_files(
    resultado_file: UploadFile = File(...),
    instancia_file: Optional[UploadFile] = File(None),
    real_data_file: Optional[UploadFile] = File(None),
    semana: int = Query(...),
    participacion: int = Query(...),
    dispersion: str = Query(..., regex="^[KC]$"),
    db: AsyncSession = Depends(get_db)
):
    """Cargar nuevos archivos de Magdalena"""
    
    con_dispersion = dispersion == 'K'
    loader = MagdalenaLoader(db)
    
    try:
        # Guardar archivo temporal de resultado
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_resultado:
            shutil.copyfileobj(resultado_file.file, tmp_resultado)
            resultado_path = tmp_resultado.name
        
        # Cargar resultado
        run_id = await loader.load_resultado_file(
            resultado_path,
            semana,
            participacion,
            con_dispersion
        )
        
        # Cargar instancia si se proporciona
        if instancia_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_instancia:
                shutil.copyfileobj(instancia_file.file, tmp_instancia)
                await loader.load_instancia_file(
                    tmp_instancia.name,
                    semana,
                    participacion,
                    con_dispersion
                )
        
        # Cargar datos reales si se proporcionan
        if real_data_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_real:
                shutil.copyfileobj(real_data_file.file, tmp_real)
                await loader.load_real_data_file(tmp_real.name, semana)
        
        return {
            "message": "Archivos cargados exitosamente",
            "run_id": str(run_id),
            "config": {
                "semana": semana,
                "participacion": participacion,
                "dispersion": dispersion
            }
        }
        
    except Exception as e:
        logger.error(f"Error cargando archivos: {str(e)}")
        raise HTTPException(500, f"Error al cargar archivos: {str(e)}")
    finally:
        # Limpiar archivos temporales
        try:
            os.unlink(resultado_path)
            if instancia_file and 'tmp_instancia' in locals():
                os.unlink(tmp_instancia.name)
            if real_data_file and 'tmp_real' in locals():
                os.unlink(tmp_real.name)
        except:
            pass

@router.get("/available")
async def get_available_configurations(db: AsyncSession = Depends(get_db)):
    """Obtener configuraciones disponibles de Magdalena"""
    
    query = await db.execute(
        select(
            MagdalenaRun.semana,
            MagdalenaRun.participacion,
            MagdalenaRun.con_dispersion
        ).distinct()
    )
    
    configs = query.all()
    
    return [{
        'semana': c.semana,
        'participacion': c.participacion,
        'dispersion': 'K' if c.con_dispersion else 'C'
    } for c in configs]