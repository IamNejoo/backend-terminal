# app/api/v1/endpoints/camila.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, delete, or_
import numpy as np
import logging
import tempfile
import os
from uuid import UUID

from app.core.database import get_db
from app.models.camila import (
    CamilaRun, CamilaVariable, CamilaParametro, CamilaMetrica, CamilaSegregacion
)
from app.schemas.camila import (
    CamilaConfigInput, CamilaResults, CamilaRunSummary, HealthCheck,
    VariableInfo, VariablesSummary, MetricasBloque, MetricasGrua,
    MetricasTiempo, MetricasSegregacion, GruaTimeline, BlockDetail,
    ModelComparison, UploadResponse, CamilaFilter, PaginationParams,
    CamilaRunsResponse
)
from app.services.camila_loader import CamilaLoader

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/health", response_model=HealthCheck)
async def health_check(db: AsyncSession = Depends(get_db)):
    """Verificar estado del servicio"""
    try:
        # Contar runs
        count_result = await db.execute(select(func.count(CamilaRun.id)))
        total_runs = count_result.scalar() or 0
        
        # Última actualización
        last_update_result = await db.execute(
            select(func.max(CamilaRun.fecha_carga))
        )
        last_update = last_update_result.scalar()
        
        return HealthCheck(
            status="healthy",
            service="camila",
            version="2.0",
            tables_exist=True,
            total_runs=total_runs,
            last_update=last_update
        )
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return HealthCheck(
            status="error",
            service="camila",
            version="2.0",
            tables_exist=False,
            total_runs=0,
            last_update=None
        )

@router.get("/configurations")
async def get_available_configurations(db: AsyncSession = Depends(get_db)):
    """Obtener todas las configuraciones disponibles"""
    try:
        query = select(CamilaRun).order_by(
            CamilaRun.semana,
            CamilaRun.dia,
            CamilaRun.turno
        )
        
        result = await db.execute(query)
        runs = result.scalars().all()
        
        configurations = []
        for run in runs:
            configurations.append({
                "week": run.semana,
                "day": run.dia,
                "shift": run.turno,
                "modelType": run.modelo_tipo,
                "withSegregations": run.con_segregaciones,
                "totalMovements": run.total_movimientos,
                "workloadBalance": run.balance_workload,
                "objectiveValue": run.funcion_objetivo,
                "runId": str(run.id)
            })
        
        logger.info(f"Configuraciones encontradas: {len(configurations)}")
        return configurations
        
    except Exception as e:
        logger.error(f"Error obteniendo configuraciones: {str(e)}")
        raise HTTPException(500, f"Error al obtener configuraciones: {str(e)}")

@router.get("/results", response_model=CamilaResults)
async def get_camila_results(
    semana: int = Query(..., ge=1, le=52),
    dia: str = Query(...),
    turno: int = Query(..., ge=1, le=3),
    modelo_tipo: str = Query(..., regex="^(minmax|maxmin)$"),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Obtener resultados completos del modelo"""
    
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
        
        # Obtener métricas
        metrics_result = await db.execute(
            select(CamilaMetrica).where(CamilaMetrica.run_id == run.id)
        )
        metrics = metrics_result.scalar_one_or_none()
        
        if not metrics:
            raise HTTPException(404, "No se encontraron métricas para esta configuración")
        
        # Obtener variables para el resumen
        variables = await _get_variables_summary(db, run.id)
        
        # Obtener parámetros
        params = await _get_parameters(db, run.id)
        
        # Construir métricas por bloque
        metricas_bloques = []
        for bloque, data in metrics.metricas_bloque.items():
            metricas_bloques.append(MetricasBloque(
                bloque=bloque,
                movimientos_total=int(data.get('movimientos_total', 0)),
                recepcion=int(data.get('recepcion', 0)),
                entrega=int(data.get('entrega', 0)),
                gruas_asignadas=int(data.get('gruas_asignadas', 0)),
                periodos_activos=len(data.get('periodos_activos', [])),
                participacion=0,  # Se calcula después
                utilizacion=0  # Se calcula después
            ))
        
        # Calcular participación
        total_mov = run.total_movimientos or 1
        for mb in metricas_bloques:
            mb.participacion = (mb.movimientos_total / total_mov * 100) if total_mov > 0 else 0
        
        # Construir métricas por grúa
        metricas_gruas = []
        for grua, data in metrics.metricas_grua.items():
            metricas_gruas.append(MetricasGrua(
                grua=grua,
                periodos_activos=data.get('periodos_activos', 0),
                bloques_asignados=list(data.get('bloques', [])),
                movimientos_teoricos=data.get('periodos_activos', 0) * params.get('mu', 30),
                utilizacion=(data.get('periodos_activos', 0) / 8 * 100),
                asignaciones=data.get('asignaciones', [])
            ))
        
        # Construir métricas por tiempo
        metricas_tiempo = []
        for t in range(1, 9):
            tiempo_str = str(t)
            total_t = sum(metrics.matriz_flujos_total[b][t-1] for b in range(9))
            participacion_t = (total_t / total_mov * 100) if total_mov > 0 else 0
            
            # Calcular hora real según turno
            base_hour = (turno - 1) * 8
            hora_real = f"{(base_hour + t - 1):02d}:00"
            
            metricas_tiempo.append(MetricasTiempo(
                tiempo=t,
                hora_real=hora_real,
                movimientos_total=int(total_t),
                gruas_activas=0,  # TODO: calcular desde matriz
                bloques_activos=sum(1 for b in range(9) if metrics.matriz_flujos_total[b][t-1] > 0),
                participacion=participacion_t
            ))
        
        # Construir métricas por segregación
        segregaciones = await _get_segregaciones_info(db, run.id)
        metricas_segregaciones = []
        
        for seg_code, data in metrics.metricas_segregacion.items():
            seg_info = next((s for s in segregaciones if s['codigo'] == seg_code), None)
            
            metricas_segregaciones.append(MetricasSegregacion(
                segregacion=seg_code,
                descripcion=seg_info['descripcion'] if seg_info else '',
                tipo=seg_info['tipo'] if seg_info else 'desconocido',
                movimientos_recepcion=int(data.get('recepcion', 0)),
                movimientos_entrega=int(data.get('entrega', 0)),
                bloques=list(data.get('bloques', []))
            ))
        
        # Construir respuesta
        return CamilaResults(
            run_id=run.id,
            config=CamilaConfigInput(
                semana=run.semana,
                dia=run.dia,
                turno=run.turno,
                modelo_tipo=run.modelo_tipo,
                con_segregaciones=run.con_segregaciones
            ),
            funcion_objetivo=run.funcion_objetivo,
            total_movimientos=run.total_movimientos,
            balance_workload=run.balance_workload,
            indice_congestion=run.indice_congestion,
            utilizacion_sistema=metrics.utilizacion_promedio,
            variables_summary=variables,
            metricas_bloques=metricas_bloques,
            metricas_gruas=metricas_gruas,
            metricas_tiempo=metricas_tiempo,
            metricas_segregaciones=metricas_segregaciones,
            matriz_flujos=metrics.matriz_flujos_total,
            matriz_gruas=metrics.matriz_asignacion_gruas,
            matriz_capacidad=metrics.matriz_capacidad,
            matriz_disponibilidad=metrics.matriz_disponibilidad,
            participacion_bloques=metrics.participacion_bloques,
            participacion_tiempo=metrics.participacion_tiempo,
            parametros=params
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error obteniendo resultados: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/gruas/{grua_id}/timeline", response_model=GruaTimeline)
async def get_grua_timeline(
    grua_id: str,
    run_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Obtener timeline detallado de una grúa específica"""
    try:
        # Obtener asignaciones de la grúa
        query = select(CamilaVariable).where(
            and_(
                CamilaVariable.run_id == run_id,
                CamilaVariable.grua == grua_id,
                CamilaVariable.variable.in_(['ygbt', 'alpha_gbt']),
                CamilaVariable.valor > 0
            )
        ).order_by(CamilaVariable.tiempo)
        
        result = await db.execute(query)
        asignaciones = result.scalars().all()
        
        timeline = []
        bloques_unicos = set()
        
        for asig in asignaciones:
            timeline.append({
                'tiempo': asig.tiempo,
                'bloque': asig.bloque,
                'tipo': asig.variable
            })
            bloques_unicos.add(asig.bloque)
        
        return GruaTimeline(
            grua=grua_id,
            timeline=timeline,
            total_periodos=len(timeline),
            bloques_unicos=len(bloques_unicos),
            utilizacion=(len(timeline) / 8 * 100)
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo timeline de grúa: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/blocks/{block_id}/detail", response_model=BlockDetail)
async def get_block_detail(
    block_id: str,
    run_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Obtener detalle de un bloque específico"""
    try:
        # Obtener métricas
        metrics_result = await db.execute(
            select(CamilaMetrica).where(CamilaMetrica.run_id == run_id)
        )
        metrics = metrics_result.scalar_one_or_none()
        
        if not metrics:
            raise HTTPException(404, "No se encontraron métricas")
        
        b_idx = int(block_id[1:]) - 1  # b1 -> 0
        
        # Extraer datos del bloque
        movimientos_por_tiempo = [int(metrics.matriz_flujos_total[b_idx][t]) for t in range(8)]
        capacidad_por_tiempo = [int(metrics.matriz_capacidad[b_idx][t]) for t in range(8)]
        disponibilidad_por_tiempo = [int(metrics.matriz_disponibilidad[b_idx][t]) for t in range(8)]
        
        # Obtener grúas por tiempo
        gruas_por_tiempo = []
        for t in range(8):
            gruas_en_t = []
            for g in range(12):
                if metrics.matriz_asignacion_gruas[g][b_idx * 8 + t] == 1:
                    gruas_en_t.append(f"g{g+1}")
            gruas_por_tiempo.append(gruas_en_t)
        
        # Obtener segregaciones del bloque
        segregaciones = []
        if block_id in metrics.metricas_bloque:
            # Buscar en variables qué segregaciones tienen flujos en este bloque
            seg_query = select(CamilaVariable.segregacion).where(
                and_(
                    CamilaVariable.run_id == run_id,
                    CamilaVariable.bloque == block_id,
                    CamilaVariable.tipo_variable.in_(['flujo_recepcion', 'flujo_entrega']),
                    CamilaVariable.valor > 0
                )
            ).distinct()
            
            seg_result = await db.execute(seg_query)
            segregaciones = [s for s in seg_result.scalars().all() if s]
        
        total_movimientos = sum(movimientos_por_tiempo)
        total_capacidad = sum(capacidad_por_tiempo)
        utilizacion_promedio = (total_movimientos / total_capacidad * 100) if total_capacidad > 0 else 0
        
        return BlockDetail(
            bloque=block_id,
            movimientos_por_tiempo=movimientos_por_tiempo,
            gruas_por_tiempo=gruas_por_tiempo,
            capacidad_por_tiempo=capacidad_por_tiempo,
            disponibilidad_por_tiempo=disponibilidad_por_tiempo,
            segregaciones=segregaciones,
            total_movimientos=total_movimientos,
            utilizacion_promedio=utilizacion_promedio
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo detalle de bloque: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.post("/upload", response_model=UploadResponse)
async def upload_model_files(
    resultado_file: UploadFile = File(...),
    instancia_file: UploadFile = File(...),
    semana: int = Form(...),
    dia: str = Form(...),
    turno: int = Form(...),
    modelo_tipo: str = Form(...),
    con_segregaciones: bool = Form(True),
    db: AsyncSession = Depends(get_db)
):
    """Cargar archivos del modelo"""
    
    # Validar archivos
    for file in [resultado_file, instancia_file]:
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(400, f"El archivo {file.filename} debe ser Excel")
    
    try:
        # Guardar archivos temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_res:
            content = await resultado_file.read()
            tmp_res.write(content)
            resultado_path = tmp_res.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_inst:
            content = await instancia_file.read()
            tmp_inst.write(content)
            instancia_path = tmp_inst.name
        
        # Cargar usando el loader
        loader = CamilaLoader(db)
        run_id = await loader.load_model_results(
            resultado_path,
            instancia_path,
            semana,
            dia,
            turno,
            modelo_tipo,
            con_segregaciones
        )
        
        # Obtener estadísticas del run cargado
        run = await db.get(CamilaRun, run_id)
        
        # Limpiar archivos temporales
        os.unlink(resultado_path)
        os.unlink(instancia_path)
        
        return UploadResponse(
            message="Archivos procesados exitosamente",
            run_id=run_id,
            config=CamilaConfigInput(
                semana=semana,
                dia=dia,
                turno=turno,
                modelo_tipo=modelo_tipo,
                con_segregaciones=con_segregaciones
            ),
            stats={
                "funcion_objetivo": run.funcion_objetivo,
                "total_movimientos": run.total_movimientos,
                "balance_workload": run.balance_workload,
                "indice_congestion": run.indice_congestion
            }
        )
        
    except Exception as e:
        logger.error(f"Error cargando archivos: {str(e)}")
        # Limpiar archivos temporales en caso de error
        if 'resultado_path' in locals():
            os.unlink(resultado_path)
        if 'instancia_path' in locals():
            os.unlink(instancia_path)
        raise HTTPException(500, f"Error procesando archivos: {str(e)}")

@router.get("/comparison/models", response_model=ModelComparison)
async def compare_models(
    semana: int = Query(...),
    dia: str = Query(...),
    turno: int = Query(...),
    con_segregaciones: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Comparar modelos MinMax vs MaxMin"""
    try:
        # Obtener ambos modelos
        models_data = {}
        
        for model_type in ['minmax', 'maxmin']:
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
                # Obtener métricas
                metrics_result = await db.execute(
                    select(CamilaMetrica).where(CamilaMetrica.run_id == run.id)
                )
                metrics = metrics_result.scalar_one_or_none()
                
                if metrics:
                    models_data[model_type] = {
                        'run': run,
                        'metrics': metrics
                    }
        
        if len(models_data) < 2:
            raise HTTPException(404, "No se encontraron ambos modelos para comparar")
        
        # Preparar comparación
        config1 = CamilaConfigInput(
            semana=semana,
            dia=dia,
            turno=turno,
            modelo_tipo='minmax',
            con_segregaciones=con_segregaciones
        )
        
        config2 = CamilaConfigInput(
            semana=semana,
            dia=dia,
            turno=turno,
            modelo_tipo='maxmin',
            con_segregaciones=con_segregaciones
        )
        
        # Métricas comparadas
        metricas_comparadas = {
            'funcion_objetivo': {
                'minmax': models_data['minmax']['run'].funcion_objetivo,
                'maxmin': models_data['maxmin']['run'].funcion_objetivo
            },
            'total_movimientos': {
                'minmax': models_data['minmax']['run'].total_movimientos,
                'maxmin': models_data['maxmin']['run'].total_movimientos
            },
            'balance_workload': {
                'minmax': models_data['minmax']['run'].balance_workload,
                'maxmin': models_data['maxmin']['run'].balance_workload
            },
            'indice_congestion': {
                'minmax': models_data['minmax']['run'].indice_congestion,
                'maxmin': models_data['maxmin']['run'].indice_congestion
            },
            'utilizacion': {
                'minmax': models_data['minmax']['metrics'].utilizacion_promedio,
                'maxmin': models_data['maxmin']['metrics'].utilizacion_promedio
            }
        }
        
        # Calcular mejoras (maxmin vs minmax)
        mejoras = {}
        for metrica, valores in metricas_comparadas.items():
            if metrica in ['balance_workload', 'utilizacion']:
                # Mayor es mejor
                mejoras[metrica] = valores['maxmin'] - valores['minmax']
            elif metrica in ['indice_congestion', 'funcion_objetivo']:
                # Menor es mejor
                mejoras[metrica] = valores['minmax'] - valores['maxmin']
            else:
                mejoras[metrica] = valores['maxmin'] - valores['minmax']
        
        # Distribución por bloques
        distribucion_bloques1 = models_data['minmax']['metrics'].participacion_bloques
        distribucion_bloques2 = models_data['maxmin']['metrics'].participacion_bloques
        
        # Análisis y recomendación
        analisis = []
        
        if mejoras['balance_workload'] > 5:
            analisis.append("MaxMin mejora significativamente el balance de carga")
        elif mejoras['balance_workload'] < -5:
            analisis.append("MinMax proporciona mejor balance de carga")
        
        if mejoras['indice_congestion'] > 0.2:
            analisis.append("MinMax reduce la congestión en bloques críticos")
        elif mejoras['indice_congestion'] < -0.2:
            analisis.append("MaxMin genera mayor congestión en algunos bloques")
        
        if mejoras['utilizacion'] > 5:
            analisis.append("MaxMin aprovecha mejor los recursos disponibles")
        
        # Recomendación basada en prioridades
        puntos_minmax = 0
        puntos_maxmin = 0
        
        if mejoras['balance_workload'] > 0:
            puntos_maxmin += 1
        else:
            puntos_minmax += 1
        
        if mejoras['indice_congestion'] > 0:
            puntos_minmax += 2  # Más peso a la congestión
        else:
            puntos_maxmin += 2
        
        if mejoras['funcion_objetivo'] > 0:
            puntos_minmax += 1
        else:
            puntos_maxmin += 1
        
        recomendacion = 'minmax' if puntos_minmax > puntos_maxmin else 'maxmin'
        
        if abs(puntos_minmax - puntos_maxmin) <= 1:
            analisis.append("Ambos modelos tienen desempeño similar, la elección depende de las prioridades operacionales")
        
        return ModelComparison(
            config1=config1,
            config2=config2,
            metricas_comparadas=metricas_comparadas,
            mejoras=mejoras,
            distribucion_bloques1=distribucion_bloques1,
            distribucion_bloques2=distribucion_bloques2,
            recomendacion=recomendacion,
            analisis=analisis
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparando modelos: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

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
        
        # El cascade debería eliminar todo automáticamente
        await db.delete(run)
        await db.commit()
        
        return {"message": "Run eliminado exitosamente", "run_id": str(run_id)}
        
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        logger.error(f"Error eliminando run: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

@router.get("/runs", response_model=CamilaRunsResponse)
async def get_runs(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    semana_min: Optional[int] = Query(None, ge=1, le=52),
    semana_max: Optional[int] = Query(None, ge=1, le=52),
    modelo_tipo: Optional[str] = Query(None),
    order_by: str = Query("fecha_carga"),
    order_desc: bool = Query(True),
    db: AsyncSession = Depends(get_db)
):
    """Obtener lista de runs con paginación y filtros"""
    try:
        # Query base
        query = select(CamilaRun)
        
        # Aplicar filtros
        if semana_min:
            query = query.where(CamilaRun.semana >= semana_min)
        if semana_max:
            query = query.where(CamilaRun.semana <= semana_max)
        if modelo_tipo:
            query = query.where(CamilaRun.modelo_tipo == modelo_tipo)
        
        # Contar total
        count_query = select(func.count()).select_from(query.subquery())
        total_result = await db.execute(count_query)
        total = total_result.scalar() or 0
        
        # Ordenar
        order_column = getattr(CamilaRun, order_by, CamilaRun.fecha_carga)
        if order_desc:
            query = query.order_by(order_column.desc())
        else:
            query = query.order_by(order_column)
        
        # Paginar
        query = query.offset(skip).limit(limit)
        
        # Ejecutar
        result = await db.execute(query)
        runs = result.scalars().all()
        
        # Convertir a schema
        items = [
            CamilaRunSummary(
                id=run.id,
                semana=run.semana,
                dia=run.dia,
                turno=run.turno,
                modelo_tipo=run.modelo_tipo,
                con_segregaciones=run.con_segregaciones,
                fecha_carga=run.fecha_carga,
                funcion_objetivo=run.funcion_objetivo,
                total_movimientos=run.total_movimientos,
                balance_workload=run.balance_workload,
                indice_congestion=run.indice_congestion,
                gruas_activas=0,  # TODO: obtener de métricas
                bloques_activos=0  # TODO: obtener de métricas
            )
            for run in runs
        ]
        
        # Calcular páginas
        pages = (total + limit - 1) // limit
        page = (skip // limit) + 1
        
        return CamilaRunsResponse(
            total=total,
            items=items,
            page=page,
            pages=pages
        )
        
    except Exception as e:
        logger.error(f"Error obteniendo runs: {str(e)}")
        raise HTTPException(500, f"Error interno: {str(e)}")

# ===================== Funciones auxiliares =====================

async def _get_variables_summary(db: AsyncSession, run_id: UUID) -> VariablesSummary:
    """Obtener resumen de variables"""
    
    # Obtener todas las variables
    query = select(CamilaVariable).where(CamilaVariable.run_id == run_id)
    result = await db.execute(query)
    variables = result.scalars().all()
    
    # Clasificar por tipo
    flujos_recepcion = []
    flujos_entrega = []
    asignacion_gruas = []
    alpha_variables = []
    z_variables = []
    funcion_objetivo = 0
    
    for var in variables:
        var_info = VariableInfo(
            variable=var.variable,
            indice=var.indice,
            valor=var.valor,
            segregacion=var.segregacion,
            grua=var.grua,
            bloque=var.bloque,
            tiempo=var.tiempo,
            tipo_variable=var.tipo_variable
        )
        
        if var.tipo_variable == 'flujo_recepcion':
            flujos_recepcion.append(var_info)
        elif var.tipo_variable == 'flujo_entrega':
            flujos_entrega.append(var_info)
        elif var.variable == 'ygbt':
            asignacion_gruas.append(var_info)
        elif var.variable == 'alpha_gbt':
            alpha_variables.append(var_info)
        elif var.variable == 'Z_gb':
            z_variables.append(var_info)
        elif var.variable == 'min_diff_val':
            funcion_objetivo = var.valor
    
    return VariablesSummary(
        flujos_recepcion=flujos_recepcion,
        flujos_entrega=flujos_entrega,
        asignacion_gruas=asignacion_gruas,
        alpha_variables=alpha_variables,
        z_variables=z_variables,
        funcion_objetivo=funcion_objetivo,
        total_variables=len(variables)
    )

async def _get_parameters(db: AsyncSession, run_id: UUID) -> Dict[str, Any]:
    """Obtener parámetros del modelo"""
    
    query = select(CamilaParametro).where(CamilaParametro.run_id == run_id)
    result = await db.execute(query)
    parametros = result.scalars().all()
    
    params_dict = {}
    for param in parametros:
        if param.indices:
            # Parámetro indexado
            if param.parametro not in params_dict:
                params_dict[param.parametro] = {}
            params_dict[param.parametro][str(param.indices)] = param.valor
        else:
            # Parámetro simple
            params_dict[param.parametro] = param.valor
    
    return params_dict

async def _get_segregaciones_info(db: AsyncSession, run_id: UUID) -> List[Dict[str, Any]]:
    """Obtener información de segregaciones"""
    
    query = select(CamilaSegregacion).where(CamilaSegregacion.run_id == run_id)
    result = await db.execute(query)
    segregaciones = result.scalars().all()
    
    return [
        {
            'codigo': seg.codigo,
            'descripcion': seg.descripcion,
            'tipo': seg.tipo
        }
        for seg in segregaciones
    ]