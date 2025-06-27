# app/api/v1/endpoints/camila.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, delete, or_
import numpy as np
import logging
import tempfile
import os
from uuid import UUID

from app.core.database import get_db
from app.core.constants import (
    BLOCKS_INTERNAL, BLOCKS_DISPLAY, GRUAS, GRUA_PRODUCTIVITY,
    TIME_PERIODS, SHIFTS, FLOW_TYPES, get_block_index, get_grua_index
)
from app.models.camila import CamilaRun, CamilaFlujos, CamilaGruas, CamilaRealData
from app.services.camila_loader import CamilaLoader
router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health")
async def health_check():
    """Verificar estado del servicio"""
    return {"status": "healthy", "service": "camila"}

@router.get("/configurations")
async def get_available_configurations(db: AsyncSession = Depends(get_db)):
    """Obtener todas las configuraciones disponibles"""
    query = select(
        CamilaRun.semana,
        CamilaRun.dia,
        CamilaRun.turno,
        CamilaRun.modelo_tipo,
        CamilaRun.con_segregaciones,
        CamilaRun.total_movimientos,
        CamilaRun.balance_workload
    ).distinct()
    
    result = await db.execute(query)
    configurations = result.all()
    
    return [
        {
            "week": c.semana,
            "day": c.dia,
            "shift": c.turno,
            "modelType": c.modelo_tipo,
            "withSegregations": c.con_segregaciones,
            "totalMovements": c.total_movimientos or 0,
            "workloadBalance": c.balance_workload or 0
        }
        for c in configurations
    ]

@router.get("/results")
async def get_camila_metrics(
    semana: int = Query(..., ge=1, le=52),
    dia: str = Query(...),
    turno: int = Query(..., ge=1, le=3),
    modelo_tipo: str = Query(..., regex="^(minmax|maxmin)$"),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Obtener métricas completas de Camila"""
    
    try:
        # Buscar el run
        query = select(CamilaRun).where(
            and_(
                CamilaRun.semana == semana,
                CamilaRun.dia == dia,
                CamilaRun.turno == turno,
                CamilaRun.modelo_tipo == modelo_tipo,
                CamilaRun.con_segregaciones == con_segregaciones
            )
        )
        
        result = await db.execute(query)
        run = result.scalar_one_or_none()
        
        if not run:
            raise HTTPException(404, "No se encontraron datos para esta configuración")
        
        # Obtener flujos
        flujos_query = select(CamilaFlujos).where(CamilaFlujos.run_id == run.id)
        flujos_result = await db.execute(flujos_query)
        flujos_data = flujos_result.scalars().all()
        
        # Obtener grúas
        gruas_query = select(CamilaGruas).where(CamilaGruas.run_id == run.id)
        gruas_result = await db.execute(gruas_query)
        gruas_data = gruas_result.scalars().all()
        
        # Obtener datos reales si existen
        real_query = select(CamilaRealData).where(CamilaRealData.run_id == run.id)
        real_result = await db.execute(real_query)
        real_data_db = real_result.scalars().all()
        
        # Inicializar matrices
        reception_flow = np.zeros((9, 8))
        delivery_flow = np.zeros((9, 8))
        loading_flow = np.zeros((9, 8))
        unloading_flow = np.zeros((9, 8))
        total_flows_matrix = np.zeros((9, 8))
        
        # Procesar flujos
        for flujo in flujos_data:
            try:
                b_idx = get_block_index(flujo.bloque)
                t_idx = flujo.tiempo - 1
                
                if 0 <= b_idx < 9 and 0 <= t_idx < 8:
                    if flujo.variable == 'fr_sbt':
                        reception_flow[b_idx][t_idx] = flujo.valor
                    elif flujo.variable == 'fe_sbt':
                        delivery_flow[b_idx][t_idx] = flujo.valor
                    elif flujo.variable == 'fc_sbt':
                        loading_flow[b_idx][t_idx] = flujo.valor
                    elif flujo.variable == 'fd_sbt':
                        unloading_flow[b_idx][t_idx] = flujo.valor
                    
                    total_flows_matrix[b_idx][t_idx] += flujo.valor
            except Exception as e:
                logger.warning(f"Error procesando flujo {flujo.bloque}: {e}")
                continue
        
        # CRÍTICO: Calcular block_participation
        block_totals = np.sum(total_flows_matrix, axis=1)
        total_movements = np.sum(block_totals)
        
        if total_movements > 0:
            block_participation = (block_totals / total_movements * 100).tolist()
        else:
            block_participation = [0.0] * 9
        
        # CRÍTICO: Calcular time_participation
        time_totals = np.sum(total_flows_matrix, axis=0)
        
        if total_movements > 0:
            time_participation = (time_totals / total_movements * 100).tolist()
        else:
            time_participation = [0.0] * 8
        
        # Calcular estadísticas
        std_dev_blocks = float(np.std(block_totals))
        std_dev_time = float(np.std(time_totals))
        
        # Calcular workload balance
        avg_block = np.mean(block_totals)
        workload_balance = 100.0
        if avg_block > 0:
            cv = (std_dev_blocks / avg_block) * 100
            workload_balance = max(0, 100 - cv)
        
        # Calcular congestion index
        max_flow = float(np.max(block_totals)) if len(block_totals) > 0 else 0
        congestion_index = 1.0
        if avg_block > 0:
            congestion_index = max_flow / avg_block
        
        # Procesar asignación de grúas
        grue_assignment = [[0 for _ in range(72)] for _ in range(12)]
        
        for grua_record in gruas_data:
            if grua_record.valor == 1:
                try:
                    g_idx = get_grua_index(grua_record.grua)
                    b_idx = get_block_index(grua_record.bloque)
                    t_idx = grua_record.tiempo - 1
                    
                    if 0 <= g_idx < 12 and 0 <= b_idx < 9 and 0 <= t_idx < 8:
                        index = b_idx * 8 + t_idx
                        grue_assignment[g_idx][index] = 1
                except Exception as e:
                    logger.warning(f"Error procesando grúa {grua_record.grua}: {e}")
                    continue
        
        # Calcular capacidad basada en grúas
        capacity_matrix = np.zeros((9, 8))
        for b in range(9):
            for t in range(8):
                gruas_en_bloque = 0
                for g in range(12):
                    if grue_assignment[g][b * 8 + t] == 1:
                        gruas_en_bloque += 1
                capacity_matrix[b][t] = gruas_en_bloque * GRUA_PRODUCTIVITY
        
        # Calcular disponibilidad
        availability_matrix = np.maximum(0, capacity_matrix - total_flows_matrix)
        
        # Calcular cuotas recomendadas
        FACTOR_SEGURIDAD = 0.8
        recommended_quotas = np.round(
            reception_flow + (availability_matrix * FACTOR_SEGURIDAD)
        )
        
        # Procesar datos reales
        real_data = None
        if real_data_db:
            real_movements = np.zeros((9, 8))
            for real in real_data_db:
                try:
                    b_idx = get_block_index(real.bloque)
                    t_idx = real.tiempo - 1
                    if 0 <= b_idx < 9 and 0 <= t_idx < 8:
                        real_movements[b_idx][t_idx] = real.movimientos
                except Exception as e:
                    logger.warning(f"Error procesando dato real: {e}")
            
            real_data = {"data": real_movements.tolist()}
        
        # Calcular comparación
        comparison = None
        if real_data:
            real_totals = np.sum(real_movements, axis=1)
            real_std = np.std(real_totals)
            
            workload_balance_improvement = 0
            if real_std > 0:
                workload_balance_improvement = ((real_std - std_dev_blocks) / real_std) * 100
            
            real_max = np.max(real_totals)
            congestion_reduction = 0
            if real_max > 0:
                congestion_reduction = ((real_max - max_flow) / real_max) * 100
            
            total_capacity = np.sum(capacity_matrix)
            resource_utilization = 0
            if total_capacity > 0:
                resource_utilization = (total_movements / total_capacity) * 100
            
            comparison = {
                "workload_balance_improvement": float(workload_balance_improvement),
                "congestion_reduction": float(congestion_reduction),
                "resource_utilization": float(resource_utilization),
                "total_movements_diff": int(total_movements - np.sum(real_movements))
            }
        
        # Logs para debugging
        logger.info(f"block_participation: {block_participation}")
        logger.info(f"time_participation: {time_participation}")
        logger.info(f"total_movements: {total_movements}")
        
        # Retornar respuesta con TODOS los campos necesarios
        return {
            "run_id": str(run.id),
            "config": {
                "semana": run.semana,
                "dia": run.dia,
                "turno": run.turno,
                "modelo_tipo": run.modelo_tipo,
                "con_segregaciones": run.con_segregaciones,
                "disponible": True
            },
            "grue_assignment": {"data": grue_assignment},
            "reception_flow": {"data": reception_flow.tolist()},
            "delivery_flow": {"data": delivery_flow.tolist()},
            "loading_flow": {"data": loading_flow.tolist()},
            "unloading_flow": {"data": unloading_flow.tolist()},
            "total_flows": {"data": total_flows_matrix.tolist()},
            "capacity": {"data": capacity_matrix.tolist()},
            "availability": {"data": availability_matrix.tolist()},
            "recommended_quotas": {"data": recommended_quotas.tolist()},
            "block_participation": block_participation,
            "time_participation": time_participation,
            "std_dev_blocks": std_dev_blocks,
            "std_dev_time": std_dev_time,
            "workload_balance": workload_balance,
            "congestion_index": congestion_index,
            "objective_value": getattr(run, 'objective_value', 0),
            "real_data": real_data,
            "comparison": comparison
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en get_camila_metrics: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(500, f"Error interno: {str(e)}")

# NUEVOS ENDPOINTS PARA FILTRADO Y ANÁLISIS DETALLADO

@router.get("/flows/by-segregation")
async def get_flows_by_segregation(
    run_id: UUID = Query(...),
    segregaciones: List[str] = Query(None),
    db: AsyncSession = Depends(get_db)
):
    """Obtener flujos filtrados por segregaciones específicas"""
    try:
        query = select(CamilaFlujos).where(CamilaFlujos.run_id == run_id)
        
        if segregaciones:
            query = query.where(CamilaFlujos.segregacion.in_(segregaciones))
        
        result = await db.execute(query)
        flujos = result.scalars().all()
        
        # Agrupar por segregación y tipo
        segregation_data = {}
        for flujo in flujos:
            if flujo.segregacion not in segregation_data:
                segregation_data[flujo.segregacion] = {
                    'reception': 0,
                    'delivery': 0,
                    'loading': 0,
                    'unloading': 0,
                    'total': 0
                }
            
            flow_type = FLOW_TYPES.get(flujo.variable, 'other')
            if flow_type != 'other':
                segregation_data[flujo.segregacion][flow_type] += flujo.valor
                segregation_data[flujo.segregacion]['total'] += flujo.valor
        
        return segregation_data
        
    except Exception as e:
        logger.error(f"Error obteniendo flujos por segregación: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/gruas/timeline")
async def get_gruas_timeline(
    run_id: UUID = Query(...),
    gruas: List[str] = Query(None),
    start_hour: int = Query(1, ge=1, le=8),
    end_hour: int = Query(8, ge=1, le=8),
    db: AsyncSession = Depends(get_db)
):
    """Obtener timeline detallado de asignación de grúas"""
    try:
        query = select(CamilaGruas).where(
            and_(
                CamilaGruas.run_id == run_id,
                CamilaGruas.tiempo >= start_hour,
                CamilaGruas.tiempo <= end_hour,
                CamilaGruas.valor == 1
            )
        )
        
        if gruas:
            query = query.where(CamilaGruas.grua.in_(gruas))
        
        result = await db.execute(query)
        asignaciones = result.scalars().all()
        
        # Construir timeline
        timeline = {}
        for asig in asignaciones:
            if asig.grua not in timeline:
                timeline[asig.grua] = []
            
            timeline[asig.grua].append({
                'hora': asig.tiempo,
                'bloque': asig.bloque,
                'productividad': GRUA_PRODUCTIVITY
            })
        
        # Calcular estadísticas por grúa
        stats = {}
        for grua, assignments in timeline.items():
            stats[grua] = {
                'horas_trabajadas': len(assignments),
                'bloques_unicos': len(set(a['bloque'] for a in assignments)),
                'utilizacion': (len(assignments) / (end_hour - start_hour + 1)) * 100
            }
        
        return {
            'timeline': timeline,
            'stats': stats
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo timeline de grúas: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/blocks/congestion")
async def get_blocks_congestion(
    run_id: UUID = Query(...),
    threshold: float = Query(None, ge=0, le=100),
    db: AsyncSession = Depends(get_db)
):
    """Obtener bloques con nivel de congestión específico"""
    try:
        # Obtener flujos totales
        flujos_query = select(CamilaFlujos).where(CamilaFlujos.run_id == run_id)
        flujos_result = await db.execute(flujos_query)
        flujos_data = flujos_result.scalars().all()
        
        # Calcular congestión por bloque
        block_totals = {}
        for flujo in flujos_data:
            if flujo.bloque not in block_totals:
                block_totals[flujo.bloque] = 0
            block_totals[flujo.bloque] += flujo.valor
        
        # Calcular métricas de congestión
        if block_totals:
            max_total = max(block_totals.values())
            avg_total = sum(block_totals.values()) / len(block_totals)
            
            congestion_data = []
            for bloque, total in block_totals.items():
                congestion_level = (total / max_total * 100) if max_total > 0 else 0
                
                if threshold is None or congestion_level >= threshold:
                    congestion_data.append({
                        'bloque': bloque,
                        'total_movimientos': total,
                        'congestion_level': congestion_level,
                        'vs_promedio': ((total - avg_total) / avg_total * 100) if avg_total > 0 else 0
                    })
            
            # Ordenar por nivel de congestión
            congestion_data.sort(key=lambda x: x['congestion_level'], reverse=True)
            
            return congestion_data
        
        return []
        
    except Exception as e:
        logger.error(f"Error obteniendo congestión de bloques: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/comparison/minmax-vs-maxmin")
async def compare_models(
    semana: int = Query(..., ge=1, le=52),
    dia: str = Query(...),
    turno: int = Query(..., ge=1, le=3),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Comparar resultados MinMax vs MaxMin para la misma configuración"""
    try:
        service = CamilaService(db)
        
        # Obtener ambos modelos
        models_data = {}
        for model_type in ['minmax', 'maxmin']:
            try:
                # Buscar run
                query = select(CamilaRun).where(
                    and_(
                        CamilaRun.semana == semana,
                        CamilaRun.dia == dia,
                        CamilaRun.turno == turno,
                        CamilaRun.modelo_tipo == model_type,
                        CamilaRun.con_segregaciones == con_segregaciones
                    )
                )
                
                result = await db.execute(query)
                run = result.scalar_one_or_none()
                
                if run:
                    models_data[model_type] = {
                        'run_id': str(run.id),
                        'total_movimientos': run.total_movimientos,
                        'balance_workload': run.balance_workload,
                        'indice_congestion': run.indice_congestion
                    }
            except:
                pass
        
        if len(models_data) < 2:
            raise HTTPException(404, "No se encontraron ambos modelos para comparar")
        
        # Calcular diferencias
        minmax = models_data.get('minmax', {})
        maxmin = models_data.get('maxmin', {})
        
        comparison = {
            'minmax': minmax,
            'maxmin': maxmin,
            'diferencias': {
                'total_movimientos': maxmin.get('total_movimientos', 0) - minmax.get('total_movimientos', 0),
                'balance_workload': maxmin.get('balance_workload', 0) - minmax.get('balance_workload', 0),
                'indice_congestion': maxmin.get('indice_congestion', 0) - minmax.get('indice_congestion', 0)
            },
            'recomendacion': 'minmax' if minmax.get('indice_congestion', 0) < maxmin.get('indice_congestion', 0) else 'maxmin'
        }
        
        return comparison
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparando modelos: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/patterns/peak-hours")
async def get_peak_hour_patterns(
    semana_min: int = Query(None, ge=1, le=52),
    semana_max: int = Query(None, ge=1, le=52),
    db: AsyncSession = Depends(get_db)
):
    """Identificar patrones de horas pico a través de múltiples semanas"""
    try:
        # Construir query
        query = select(
            CamilaFlujos.tiempo,
            func.sum(CamilaFlujos.valor).label('total_movimientos')
        ).join(
            CamilaRun
        ).group_by(
            CamilaFlujos.tiempo
        )
        
        # Aplicar filtros de semana
        if semana_min:
            query = query.where(CamilaRun.semana >= semana_min)
        if semana_max:
            query = query.where(CamilaRun.semana <= semana_max)
        
        result = await db.execute(query)
        hour_data = result.all()
        
        # Construir respuesta
        patterns = []
        for hora in hour_data:
            patterns.append({
                'hora': hora.tiempo,
                'hora_real': hora.tiempo + 7,  # Ajustar a hora real (turno 1)
                'total_movimientos': float(hora.total_movimientos),
                'es_hora_pico': hora.total_movimientos > np.mean([h.total_movimientos for h in hour_data])
            })
        
        # Ordenar por total de movimientos
        patterns.sort(key=lambda x: x['total_movimientos'], reverse=True)
        
        return patterns
        
    except Exception as e:
        logger.error(f"Error obteniendo patrones de horas pico: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.post("/upload")
async def upload_camila_file(
    file: UploadFile = File(...),
    semana: int = Query(..., ge=1, le=52),
    dia: str = Query(...),
    turno: int = Query(..., ge=1, le=3),
    modelo_tipo: str = Query(..., regex="^(minmax|maxmin)$"),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Cargar archivo de resultados"""
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(400, "Solo se permiten archivos Excel")
    
    try:
        # Por ahora, simplemente retornar éxito
        # Aquí deberías implementar la lógica real de carga
        return {
            "message": "Archivo procesado exitosamente",
            "filename": file.filename,
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
        raise HTTPException(500, f"Error procesando archivo: {str(e)}")

@router.delete("/runs/{run_id}")
async def delete_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Eliminar un run y todos sus datos relacionados"""
    try:
        # Verificar que existe
        query = select(CamilaRun).where(CamilaRun.id == run_id)
        result = await db.execute(query)
        run = result.scalar_one_or_none()
        
        if not run:
            raise HTTPException(404, "Run no encontrado")
        
        # Eliminar en cascada
        await db.execute(delete(CamilaFlujos).where(CamilaFlujos.run_id == run_id))
        await db.execute(delete(CamilaGruas).where(CamilaGruas.run_id == run_id))
        await db.execute(delete(CamilaRealData).where(CamilaRealData.run_id == run_id))
        await db.execute(delete(CamilaRun).where(CamilaRun.id == run_id))
        
        await db.commit()
        
        return {"message": "Run eliminado exitosamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error eliminando run: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/stats/summary")
async def get_summary_stats(db: AsyncSession = Depends(get_db)):
    """Obtener estadísticas generales"""
    try:
        # Total de runs
        total_runs_result = await db.execute(
            select(func.count(CamilaRun.id))
        )
        total_runs = total_runs_result.scalar() or 0
        
        # Promedio de balance workload
        avg_balance_result = await db.execute(
            select(func.avg(CamilaRun.balance_workload))
        )
        avg_balance = avg_balance_result.scalar() or 0
        
        # Runs por modelo
        model_stats = await db.execute(
            select(
                CamilaRun.modelo_tipo,
                func.count(CamilaRun.id).label('count'),
                func.avg(CamilaRun.balance_workload).label('avg_balance'),
                func.avg(CamilaRun.indice_congestion).label('avg_congestion')
            ).group_by(CamilaRun.modelo_tipo)
        )
        
        model_data = []
        for row in model_stats:
            model_data.append({
                'modelo': row.modelo_tipo,
                'runs': row.count,
                'avg_balance': float(row.avg_balance or 0),
                'avg_congestion': float(row.avg_congestion or 0)
            })
        
        return {
            "global": {
                "total_runs": total_runs,
                "avg_balance": float(avg_balance)
            },
            "por_modelo": model_data
        }
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {str(e)}")
        raise HTTPException(500, "Error al obtener estadísticas")