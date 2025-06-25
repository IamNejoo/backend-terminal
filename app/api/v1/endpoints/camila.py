# app/api/v1/endpoints/camila.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func
import tempfile
import shutil
import os

from app.core.database import get_db
from app.models.camila import *
from app.services.camila_loader import CamilaLoader
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/metrics")
async def get_camila_metrics(
    semana: int = Query(..., ge=1, le=52),
    dia: str = Query(..., description="Monday, Tuesday, etc."),
    turno: int = Query(..., ge=1, le=3),
    modelo_tipo: str = Query(..., regex="^(minmax|maxmin)$"),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Obtener métricas completas de Camila"""
    
    # Obtener run
    run_query = await db.execute(
        select(CamilaRun).where(
            CamilaRun.semana == semana,
            CamilaRun.dia == dia,
            CamilaRun.turno == turno,
            CamilaRun.modelo_tipo == modelo_tipo,
            CamilaRun.con_segregaciones == con_segregaciones
        )
    )
    run = run_query.scalar_one_or_none()
    
    if not run:
        raise HTTPException(404, f"No hay datos para esta configuración")
    
    # Obtener resultados
    results_query = await db.execute(
        select(CamilaResultados).where(CamilaResultados.run_id == run.id)
    )
    resultados = results_query.scalar_one_or_none()
    
    if not resultados:
        raise HTTPException(404, "No hay resultados calculados para este run")
    
    # Obtener asignación de grúas
    gruas_query = await db.execute(
        select(CamilaGruas).where(CamilaGruas.run_id == run.id)
    )
    gruas_data = gruas_query.scalars().all()
    
    # Construir matriz de asignación de grúas
    grue_assignment = [[0 for _ in range(72)] for _ in range(12)]  # 12 grúas x (9 bloques * 8 tiempos)
    
    for grua_record in gruas_data:
        if grua_record.valor == 1:
            g_idx = int(grua_record.grua.replace('g', '')) - 1
            b_idx = int(grua_record.bloque.replace('b', '')) - 1
            t_idx = grua_record.tiempo - 1
            if 0 <= g_idx < 12 and 0 <= b_idx < 9 and 0 <= t_idx < 8:
                index = b_idx * 8 + t_idx
                grue_assignment[g_idx][index] = 1
    
    # Obtener flujos por tipo
    flujos_query = await db.execute(
        select(CamilaFlujos).where(CamilaFlujos.run_id == run.id)
    )
    flujos_data = flujos_query.scalars().all()
    
    # Inicializar matrices de flujos
    reception_flow = [[0 for _ in range(8)] for _ in range(9)]
    delivery_flow = [[0 for _ in range(8)] for _ in range(9)]
    loading_flow = [[0 for _ in range(8)] for _ in range(9)]
    unloading_flow = [[0 for _ in range(8)] for _ in range(9)]
    
    # Procesar flujos
    for flujo in flujos_data:
        b_idx = int(flujo.bloque.replace('b', '')) - 1
        t_idx = flujo.tiempo - 1
        
        if 0 <= b_idx < 9 and 0 <= t_idx < 8:
            if flujo.variable == 'fr_sbt':
                reception_flow[b_idx][t_idx] += flujo.valor
            elif flujo.variable == 'fe_sbt':
                delivery_flow[b_idx][t_idx] += flujo.valor
            elif flujo.variable == 'fc_sbt':
                loading_flow[b_idx][t_idx] += flujo.valor
            elif flujo.variable == 'fd_sbt':
                unloading_flow[b_idx][t_idx] += flujo.valor
    
    # Obtener datos reales
    real_query = await db.execute(
        select(CamilaRealData).where(CamilaRealData.run_id == run.id)
    )
    real_data = real_query.scalars().all()
    
    # Construir matriz de datos reales
    real_movements = [[0 for _ in range(8)] for _ in range(9)]
    for real in real_data:
        b_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9'].index(real.bloque)
        t_idx = real.tiempo - 1
        if 0 <= b_idx < 9 and 0 <= t_idx < 8:
            real_movements[b_idx][t_idx] = real.movimientos
    
    # Calcular comparación
    real_totals = [sum(row) for row in real_movements]
    opt_totals = [sum(row) for row in resultados.total_flujos]
    
    real_std = float(np.std(real_totals))
    opt_std = float(np.std(opt_totals))
    workload_balance_improvement = ((real_std - opt_std) / real_std * 100) if real_std > 0 else 0
    
    real_max = max(real_totals) if real_totals else 0
    opt_max = max(opt_totals) if opt_totals else 0
    congestion_reduction = ((real_max - opt_max) / real_max * 100) if real_max > 0 else 0
    
    total_capacity = sum(sum(row) for row in resultados.capacidad)
    total_used = sum(sum(row) for row in resultados.total_flujos)
    resource_utilization = (total_used / total_capacity * 100) if total_capacity > 0 else 0
    
    # Construir respuesta
    response = {
        'camilaResults': {
            'grueAssignment': grue_assignment,
            'receptionFlow': reception_flow,
            'deliveryFlow': delivery_flow,
            'loadingFlow': loading_flow,
            'unloadingFlow': unloading_flow,
            'totalFlows': resultados.total_flujos,
            'capacity': resultados.capacidad,
            'availability': resultados.disponibilidad,
            'workloadBalance': run.balance_workload,
            'congestionIndex': run.indice_congestion,
            'blockParticipation': resultados.participacion_bloques,
            'timeParticipation': resultados.participacion_tiempo,
            'stdDevBlocks': resultados.desviacion_std_bloques,
            'stdDevTime': resultados.desviacion_std_tiempo,
            'recommendedQuotas': resultados.cuotas_recomendadas,
            'objectiveValue': run.funcion_objetivo,
            'modelType': run.modelo_tipo,
            'week': run.semana,
            'day': run.dia,
            'shift': run.turno
        },
        'realData': real_movements,
        'comparison': {
            'realMovements': real_movements,
            'optimizedMovements': resultados.total_flujos,
            'improvements': {
                'workloadBalance': workload_balance_improvement,
                'congestionReduction': congestion_reduction,
                'resourceUtilization': resource_utilization
            }
        }
    }
    
    return response

@router.get("/available")
async def get_available_configurations(db: AsyncSession = Depends(get_db)):
    """Obtener configuraciones disponibles de Camila"""
    
    query = await db.execute(
        select(
            CamilaRun.semana,
            CamilaRun.dia,
            CamilaRun.turno,
            CamilaRun.modelo_tipo,
            CamilaRun.con_segregaciones
        ).distinct()
    )
    
    configs = query.all()
    
    return [{
        'semana': c.semana,
        'dia': c.dia,
        'turno': c.turno,
        'modeloTipo': c.modelo_tipo,
        'conSegregaciones': c.con_segregaciones
    } for c in configs]

@router.post("/upload")
async def upload_camila_file(
    file: UploadFile = File(...),
    semana: int = Query(...),
    dia: str = Query(...),
    turno: int = Query(...),
    modelo_tipo: str = Query(..., regex="^(minmax|maxmin)$"),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Cargar un archivo de resultados de Camila"""
    
    loader = CamilaLoader(db)
    
    try:
        # Guardar archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            shutil.copyfileobj(file.file, tmp_file)
            tmp_path = tmp_file.name
        
        # Cargar archivo
        run_id = await loader.load_camila_file(
            tmp_path,
            semana,
            dia,
            turno,
            modelo_tipo,
            con_segregaciones
        )
        
        return {
            "message": "Archivo cargado exitosamente",
            "run_id": str(run_id),
            "config": {
                "semana": semana,
                "dia": dia,
                "turno": turno,
                "modelo_tipo": modelo_tipo,
                "con_segregaciones": con_segregaciones
            }
        }
        
    except Exception as e:
        logger.error(f"Error cargando archivo: {str(e)}")
        raise HTTPException(500, f"Error al cargar archivo: {str(e)}")
    finally:
        # Limpiar archivo temporal
        try:
            os.unlink(tmp_path)
        except:
            pass

@router.get("/gruas/{run_id}")
async def get_gruas_detail(
    run_id: str,
    db: AsyncSession = Depends(get_db)
):
    """Obtener detalle de asignación de grúas"""
    
    # Obtener asignación visual
    asignacion_query = await db.execute(
        select(CamilaAsignacion)
        .where(CamilaAsignacion.run_id == run_id)
        .order_by(CamilaAsignacion.tiempo, CamilaAsignacion.grua)
    )
    asignaciones = asignacion_query.scalars().all()
    
    # Organizar por tiempo
    result = {}
    for asig in asignaciones:
        if asig.tiempo not in result:
            result[asig.tiempo] = {}
        result[asig.tiempo][asig.grua] = {
            'bloque': asig.bloque_asignado,
            'movimientos': asig.movimientos_realizados
        }
    
    return result