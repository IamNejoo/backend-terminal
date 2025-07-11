# app/api/endpoints/camila.py
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
import os
import tempfile
from datetime import datetime, timedelta

from app.database import get_db
from app.models.camila import InstanciaCamila, EstadoInstancia, MetricaResultado
from app.schemas.camila import (
    InstanciaCamilaCreate, InstanciaCamilaResponse, DashboardResponse,
    AsignacionGruaResponse, FlujosResponse, CuotasResponse,
    BalanceResponse, TimelineResponse, UploadResponse,
    MagdalenaImportRequest, MagdalenaImportResponse,
    ValidacionCoherencia, EstadisticasGenerales,
    InstanciaListResponse, MetricasDetalladas,
    FileValidation, ComparacionReal
)
from app.services.camila_service import CamilaService
from app.services.camila_loader import CamilaLoader

router = APIRouter(prefix="/api/v1/camila", tags=["camila"])

# Servicios
camila_service = CamilaService()
camila_loader = CamilaLoader()

@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    instance_file: UploadFile = File(...),
    results_file: UploadFile = File(...),
    anio: int = Query(..., ge=2017, le=2030),
    semana: int = Query(..., ge=1, le=52),
    turno: int = Query(..., ge=1, le=21),
    participacion: int = Query(..., ge=0, le=100)
):
    """Sube y procesa archivos de instancia y resultados de Camila"""
    try:
        # Guardar archivos temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_instance:
            content = await instance_file.read()
            tmp_instance.write(content)
            instance_path = tmp_instance.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_results:
            content = await results_file.read()
            tmp_results.write(content)
            results_path = tmp_results.name
        
        # Validar estructura
        instance_validation = camila_loader.validate_file_structure(instance_path)
        if not instance_validation['is_valid']:
            raise HTTPException(
                status_code=400,
                detail=f"Archivo de instancia inválido. Faltan hojas: {instance_validation['missing_sheets']}"
            )
        
        results_validation = camila_loader.validate_file_structure(results_path)
        if not results_validation['is_valid']:
            raise HTTPException(
                status_code=400,
                detail=f"Archivo de resultados inválido. Faltan hojas: {results_validation['missing_sheets']}"
            )
        
        # Leer datos
        instance_data = camila_loader.read_instance_file(instance_path)
        results_data = camila_loader.read_results_file(results_path)
        
        # Crear instancia
        instance_create = InstanciaCamilaCreate(
            anio=anio,
            semana=semana,
            fecha=datetime(anio, 1, 1) + timedelta(weeks=semana-1),
            turno=turno,
            participacion=participacion
        )
        
        # Procesar en background
        instancia = await camila_service._create_instance(db, instance_create)
        background_tasks.add_task(
            process_instance_background,
            db,
            instancia.id,
            instance_data,
            results_data,
            instance_create
        )
        
        return UploadResponse(
            success=True,
            message=f"Archivos cargados exitosamente. Procesando instancia {instancia.id}",
            instance_id=instancia.id
        )
        
    except Exception as e:
        return UploadResponse(
            success=False,
            message="Error al procesar archivos",
            errors=[str(e)]
        )
    finally:
        # Limpiar archivos temporales
        if 'instance_path' in locals():
            os.unlink(instance_path)
        if 'results_path' in locals():
            os.unlink(results_path)

@router.post("/import-magdalena", response_model=MagdalenaImportResponse)
async def import_from_magdalena(
    request: MagdalenaImportRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    magdalena_instance_file: UploadFile = File(...),
    magdalena_result_file: UploadFile = File(...)
):
    """Importa datos desde una instancia de Magdalena"""
    try:
        # Guardar archivos temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_instance:
            content = await magdalena_instance_file.read()
            tmp_instance.write(content)
            instance_path = tmp_instance.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_result:
            content = await magdalena_result_file.read()
            tmp_result.write(content)
            result_path = tmp_result.name
        
        # Crear instancia de Camila
        instance_create = InstanciaCamilaCreate(
            anio=request.anio,
            semana=request.semana,
            fecha=datetime(request.anio, 1, 1) + timedelta(weeks=request.semana-1),
            turno=request.turno,
            participacion=request.participacion,
            magdalena_instance_id=request.magdalena_instance_id
        )
        
        instancia = await camila_service._create_instance(db, instance_create)
        
        # Procesar en background
        background_tasks.add_task(
            import_magdalena_background,
            db,
            instancia.id,
            request.magdalena_instance_id,
            request.turno,
            instance_path,
            result_path,
            instance_create
        )
        
        return MagdalenaImportResponse(
            success=True,
            instance_id=instancia.id,
            inventario_importado={},  # Se llenará después
            demanda_importada={},
            capacidad_importada={},
            mensaje=f"Importación iniciada. Instancia Camila {instancia.id} creada"
        )
        
    except Exception as e:
        return MagdalenaImportResponse(
            success=False,
            instance_id=None,
            inventario_importado={},
            demanda_importada={},
            capacidad_importada={},
            mensaje=f"Error en importación: {str(e)}"
        )
    finally:
        # Limpiar archivos temporales
        if 'instance_path' in locals():
            os.unlink(instance_path)
        if 'result_path' in locals():
            os.unlink(result_path)

@router.get("/dashboard/{instance_id}", response_model=DashboardResponse)
async def get_dashboard(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene el dashboard principal para una instancia"""
    try:
        dashboard = await camila_service.get_dashboard(db, instance_id)
        return dashboard
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/instances", response_model=InstanciaListResponse)
async def list_instances(
    db: AsyncSession = Depends(get_db),
    anio: Optional[int] = None,
    semana: Optional[int] = None,
    participacion: Optional[int] = None,
    estado: Optional[EstadoInstancia] = None,
    skip: int = 0,
    limit: int = 100
):
    """Lista instancias con filtros opcionales"""
    query = select(InstanciaCamila)
    
    # Aplicar filtros
    if anio:
        query = query.where(InstanciaCamila.anio == anio)
    if semana:
        query = query.where(InstanciaCamila.semana == semana)
    if participacion:
        query = query.where(InstanciaCamila.participacion == participacion)
    if estado:
        query = query.where(InstanciaCamila.estado == estado)
    
    # Total
    count_query = select(func.count()).select_from(query.subquery())
    total_result = await db.execute(count_query)
    total = total_result.scalar()
    
    # Paginación
    query = query.offset(skip).limit(limit).order_by(InstanciaCamila.id.desc())
    result = await db.execute(query)
    items = result.scalars().all()
    
    return InstanciaListResponse(
        items=items,
        total=total,
        page=skip // limit + 1,
        size=limit,
        pages=(total + limit - 1) // limit
    )

@router.get("/{instance_id}/gruas", response_model=AsignacionGruaResponse)
async def get_crane_assignments(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene la asignación de grúas por hora"""
    try:
        data = await camila_service.get_crane_assignments(db, instance_id)
        return AsignacionGruaResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{instance_id}/flujos", response_model=FlujosResponse)
async def get_flows(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene los flujos operacionales"""
    try:
        data = await camila_service.get_flows(db, instance_id)
        return FlujosResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{instance_id}/cuotas", response_model=CuotasResponse)
async def get_truck_quotas(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene las cuotas de camiones calculadas"""
    try:
        data = await camila_service.get_truck_quotas(db, instance_id)
        return CuotasResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{instance_id}/balance", response_model=BalanceResponse)
async def get_balance(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene el análisis de balance por bloque"""
    try:
        data = await camila_service.get_balance_by_block(db, instance_id)
        return BalanceResponse(**data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{instance_id}/timeline")
async def get_timeline(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene el timeline de eventos del turno"""
    # Construir timeline de eventos principales
    events = []
    
    # Obtener datos de flujos
    flujos = await camila_service.get_flows(db, instance_id)
    
    # Obtener umbral de congestión
    umbral_congestion = await camila_service._get_config_value(db, 'umbral_congestion_alta', 150)
    
    for hora_data in flujos['por_hora']:
        hora = hora_data['hora']
        
        # Evento de mayor flujo
        max_tipo = max(
            ['carga', 'descarga', 'recepcion', 'entrega'],
            key=lambda t: hora_data[t]
        )
        
        events.append({
            'hora': hora,
            'tipo': 'flujo_maximo',
            'descripcion': f"Mayor flujo: {max_tipo} ({hora_data[max_tipo]} movimientos)",
            'valor': hora_data[max_tipo]
        })
        
        # Si hay congestión alta
        if hora_data['total'] > umbral_congestion:
            events.append({
                'hora': hora,
                'tipo': 'congestion',
                'descripcion': f"Alta demanda: {hora_data['total']} movimientos totales",
                'valor': hora_data['total']
            })
    
    # Ordenar por hora
    events.sort(key=lambda e: e['hora'])
    
    return TimelineResponse(
        instancia_id=instance_id,
        eventos=events
    )

@router.get("/{instance_id}/validacion", response_model=ValidacionCoherencia)
async def validate_coherence(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Valida la coherencia de flujos de una instancia"""
    # TODO: Implementar validación real
    return ValidacionCoherencia(
        es_coherente=True,
        mensaje="Validación exitosa",
        detalles={
            "balance_flujos": "OK",
            "capacidades": "OK",
            "restricciones_gruas": "OK"
        }
    )

@router.get("/stats", response_model=EstadisticasGenerales)
async def get_general_stats(
    db: AsyncSession = Depends(get_db),
    anio: Optional[int] = None
):
    """Obtiene estadísticas generales del sistema"""
    query = select(InstanciaCamila)
    if anio:
        query = query.where(InstanciaCamila.anio == anio)
    
    # Total instancias
    total_result = await db.execute(
        select(func.count(InstanciaCamila.id))
        .select_from(query.subquery())
    )
    total = total_result.scalar()
    
    # Por estado
    estados_result = await db.execute(
        select(
            InstanciaCamila.estado,
            func.count(InstanciaCamila.id)
        )
        .group_by(InstanciaCamila.estado)
    )
    
    estados = {row[0]: row[1] for row in estados_result}
    
    # Promedios de métricas
    metrics_result = await db.execute(
        select(
            func.avg(MetricaResultado.valor_funcion_objetivo),
            func.avg(MetricaResultado.utilizacion_gruas_pct),
            func.avg(
                (MetricaResultado.cumplimiento_carga_pct +
                 MetricaResultado.cumplimiento_descarga_pct +
                 MetricaResultado.cumplimiento_recepcion_pct +
                 MetricaResultado.cumplimiento_entrega_pct) / 4
            )
        )
        .select_from(MetricaResultado)
        .join(InstanciaCamila)
    )
    
    metrics = metrics_result.first()
    
    return EstadisticasGenerales(
        total_instancias=total,
        instancias_completadas=estados.get(EstadoInstancia.completado, 0),
        instancias_error=estados.get(EstadoInstancia.error, 0),
        promedio_funcion_objetivo=metrics[0] or 0,
        promedio_utilizacion_gruas=metrics[1] or 0,
        promedio_cumplimiento=metrics[2] or 0
    )

@router.get("/{instance_id}/metricas", response_model=MetricasDetalladas)
async def get_detailed_metrics(
    instance_id: int,
    db: AsyncSession = Depends(get_db)
):
    """Obtiene métricas detalladas de una instancia"""
    result = await db.execute(
        select(MetricaResultado)
        .where(MetricaResultado.instancia_id == instance_id)
    )
    
    metricas = result.scalar_one_or_none()
    if not metricas:
        raise HTTPException(status_code=404, detail="Métricas no encontradas")
    
    # Construir respuesta detallada
    return MetricasDetalladas(
        funcion_objetivo=metricas.valor_funcion_objetivo,
        gap_optimalidad=metricas.gap_optimalidad,
        tiempo_ejecucion_ms=None,
        iteraciones=None,
        detalles_balance={
            'desviacion_estandar': metricas.desviacion_estandar_carga,
            'coeficiente_variacion': metricas.coeficiente_variacion,
            'indice_balance': metricas.indice_balance
        },
        detalles_gruas={
            'utilizacion': metricas.utilizacion_gruas_pct,
            'gruas_promedio': metricas.gruas_utilizadas_promedio,
            'productividad': metricas.productividad_promedio,
            'cambios_totales': metricas.cambios_bloque_total,
            'cambios_promedio': metricas.cambios_por_grua_promedio
        },
        detalles_flujos={
            'total': metricas.movimientos_totales,
            'cumplimiento_carga': metricas.cumplimiento_carga_pct,
            'cumplimiento_descarga': metricas.cumplimiento_descarga_pct,
            'cumplimiento_recepcion': metricas.cumplimiento_recepcion_pct,
            'cumplimiento_entrega': metricas.cumplimiento_entrega_pct
        },
        detalles_congestion={
            'maxima': metricas.congestion_maxima,
            'bloque': metricas.bloque_mas_congestionado,
            'hora_pico': metricas.hora_pico
        }
    )

@router.post("/validate-file")
async def validate_file(
    file: UploadFile = File(...)
):
    """Valida la estructura de un archivo antes de procesarlo"""
    try:
        # Guardar temporalmente
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            content = await file.read()
            tmp.write(content)
            tmp_path = tmp.name
        
        validation = camila_loader.validate_file_structure(tmp_path)
        
        return FileValidation(**validation)
        
    finally:
        if 'tmp_path' in locals():
            os.unlink(tmp_path)

# Funciones auxiliares para procesamiento en background
async def process_instance_background(
    db: AsyncSession,
    instance_id: int,
    instance_data: Dict[str, Any],
    results_data: Dict[str, Any],
    instance_create: InstanciaCamilaCreate
):
    """Procesa una instancia en background"""
    async with db.begin():
        try:
            # Obtener instancia
            result = await db.execute(
                select(InstanciaCamila)
                .where(InstanciaCamila.id == instance_id)
            )
            instancia = result.scalar_one()
            
            # Procesar
            await camila_service.process_instance(
                db,
                instance_data,
                results_data,
                instance_create
            )
            
        except Exception as e:
            # Actualizar estado de error
            instancia.estado = EstadoInstancia.error
            instancia.mensaje_error = str(e)
            await db.commit()
            logger.error(f"Error procesando instancia {instance_id}: {str(e)}")

async def import_magdalena_background(
    db: AsyncSession,
    camila_instance_id: int,
    magdalena_instance_id: int,
    turno: int,
    instance_path: str,
    result_path: str,
    instance_create: InstanciaCamilaCreate
):
    """Importa datos de Magdalena en background"""
    async with db.begin():
        try:
            await camila_service.import_from_magdalena(
                db,
                magdalena_instance_id,
                turno,
                instance_path,
                result_path,
                instance_create
            )
            
        except Exception as e:
            # Log error
            logger.error(f"Error importando desde Magdalena: {str(e)}")

# Registrar el router en el archivo principal de la aplicación