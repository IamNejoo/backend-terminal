# app/api/v1/endpoints/sai_flujos.py
from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File, Body
from fastapi import Path as FastAPIPath
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func
import tempfile
import shutil
import os
import numpy as np
import logging

from app.core.database import get_db
from app.models.sai_flujos import (
    SAIConfiguration, SAIFlujo, SAIVolumenBloque, SAIVolumenSegregacion,
    SAISegregacion, SAICapacidadBloque, SAIMapeoCriterios
)
from app.schemas.sai_flujos import (
    SAIMetrics, BlockBahiasView, BahiaCell, SegregacionInfo,
    SAIConfigurationList, SAIConfigurationResponse, LoadResult,
    SegregacionVolumen, SAIComparisonResponse
)
from app.services.sai_flujos_loader import SAIFlujosLoader

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/metrics")
async def get_sai_metrics(
    fecha: datetime = Query(..., description="Fecha a consultar"),
    turno: Optional[int] = Query(None, ge=1, le=3, description="Turno específico"),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Obtiene métricas de SAI en formato compatible con visualización"""
    
    try:
        # Buscar configuración más cercana a la fecha
        config_query = await db.execute(
            select(SAIConfiguration)
            .where(SAIConfiguration.fecha <= fecha)
            .order_by(SAIConfiguration.fecha.desc())
            .limit(1)
        )
        config = config_query.scalar_one_or_none()
        
        if not config:
            raise HTTPException(404, "No hay datos disponibles para esta fecha")
        
        # Obtener segregaciones
        seg_query = await db.execute(select(SAISegregacion))
        segregaciones = {s.id: s for s in seg_query.scalars().all()}
        
        # Obtener capacidades
        cap_query = await db.execute(select(SAICapacidadBloque))
        capacidades = {c.bloque: c for c in cap_query.scalars().all()}
        
        # Inicializar estructuras de datos
        bahias_por_bloque = {}  # {bloque-turno: {segregacion: bahias}}
        volumen_por_bloque = {} # {bloque-turno: {segregacion: volumen}}
        ocupacion_por_bloque = {}
        
        # Procesar por turno o todos los turnos
        turnos_a_procesar = [turno] if turno else [1, 2, 3]
        
        for t in turnos_a_procesar:
            # Obtener volúmenes por segregación
            vol_query = await db.execute(
                select(SAIVolumenSegregacion)
                .where(SAIVolumenSegregacion.config_id == config.id)
            )
            volumenes = vol_query.scalars().all()
            
            for vol in volumenes:
                # Determinar volumen del turno
                if t == 1:
                    volumen_teus = vol.turno_1
                elif t == 2:
                    volumen_teus = vol.turno_2
                else:
                    volumen_teus = vol.turno_3
                
                if volumen_teus > 0:
                    # IMPORTANTE: Usar formato bloque-turno
                    key = f"{vol.bloque}-{t}"
                    
                    # Inicializar diccionarios si no existen
                    if key not in bahias_por_bloque:
                        bahias_por_bloque[key] = {}
                        volumen_por_bloque[key] = {}
                    
                    # Calcular bahías necesarias
                    seg = segregaciones.get(vol.segregacion_id)
                    cap = capacidades.get(vol.bloque)
                    
                    if seg and cap:
                        contenedores = volumen_teus / seg.teus
                        bahias_necesarias = int(np.ceil(contenedores / cap.contenedores_por_bahia))
                        
                        # Guardar datos
                        bahias_por_bloque[key][vol.segregacion_id] = bahias_necesarias
                        volumen_por_bloque[key][vol.segregacion_id] = volumen_teus
        
        # Calcular ocupación por bloque
        for bloque, cap in capacidades.items():
            volumen_total = 0
            for t in turnos_a_procesar:
                key = f"{bloque}-{t}"
                if key in volumen_por_bloque:
                    volumen_total += sum(volumen_por_bloque[key].values())
            
            capacidad_total = cap.capacidad_teus * len(turnos_a_procesar)
            ocupacion_por_bloque[bloque] = (volumen_total / capacidad_total * 100) if capacidad_total > 0 else 0
        
        # Preparar información de segregaciones
        segregaciones_info = {}
        for seg_id, seg in segregaciones.items():
            segregaciones_info[seg_id] = {
                'id': seg.id,
                'nombre': seg.nombre,
                'teus': seg.teus,
                'tipo': seg.tipo,
                'categoria': seg.categoria,
                'direccion': seg.direccion,
                'color': seg.color
            }
        
        # IMPORTANTE: Formato correcto para el componente
        capacidades_por_bloque = {
            c.bloque: c.contenedores_por_bahia 
            for c in capacidades.values()
        }
        
        teus_por_segregacion = {
            s.id: s.teus for s in segregaciones.values()
        }
        
        # Calcular totales
        total_volumen = sum(
            sum(volumen_por_bloque.get(f"{b}-{t}", {}).values()) 
            for b in capacidades.keys() 
            for t in turnos_a_procesar
        )
        
        # Construir respuesta compatible con el componente
        return {
            'configId': str(config.id),
            'fecha': config.fecha.isoformat(),
            'semana': config.semana,
            'turno': turno if turno else 0,
            'totalMovimientos': await db.scalar(
                select(func.count(SAIFlujo.id))
                .where(SAIFlujo.config_id == config.id)
            ),
            'totalVolumenTeus': total_volumen,
            'bloquesActivos': len(set(key.split('-')[0] for key in volumen_por_bloque.keys())),
            'segregacionesActivas': len(segregaciones),
            'ocupacionPromedio': float(np.mean(list(ocupacion_por_bloque.values()))) if ocupacion_por_bloque else 0,
            'ocupacionPorBloque': ocupacion_por_bloque,
            'bahiasPorBloque': bahias_por_bloque,
            'volumenPorBloque': volumen_por_bloque,
            'segregacionesInfo': segregaciones_info,
            'capacidadesPorBloque': capacidades_por_bloque,  # Solo contenedores/bahía
            'teusPorSegregacion': teus_por_segregacion,      # NUEVO
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo métricas SAI: {str(e)}")
        raise HTTPException(500, f"Error procesando datos: {str(e)}")

@router.get("/configurations")
async def get_configurations(
    skip: int = Query(0, ge=0),
    limit: int = Query(10, ge=1, le=100),
    db: AsyncSession = Depends(get_db)
) -> SAIConfigurationList:
    """Obtener configuraciones disponibles"""
    
    # Contar total
    count_query = await db.execute(select(func.count(SAIConfiguration.id)))
    total = count_query.scalar()
    
    # Obtener items
    query = await db.execute(
        select(SAIConfiguration)
        .order_by(SAIConfiguration.fecha.desc())
        .offset(skip)
        .limit(limit)
    )
    items = query.scalars().all()
    
    return SAIConfigurationList(
        total=total,
        items=[SAIConfigurationResponse.from_orm(item) for item in items]
    )

@router.get("/segregaciones")
async def get_segregaciones(db: AsyncSession = Depends(get_db)) -> List[SegregacionInfo]:
    """Obtener lista de segregaciones"""
    
    query = await db.execute(
        select(SAISegregacion).order_by(SAISegregacion.id)
    )
    segregaciones = query.scalars().all()
    
    return [
        SegregacionInfo(
            id=s.id,
            nombre=s.nombre,
            teus=s.teus,
            tipo=s.tipo,
            color=s.color
        )
        for s in segregaciones
    ]

@router.post("/upload")
async def upload_sai_files(
    flujos_file: UploadFile = File(...),
    instancia_file: UploadFile = File(...),
    evolucion_file: Optional[UploadFile] = File(None),
    fecha: datetime = Body(...),
    semana: int = Body(..., ge=1, le=52),
    participacion: int = Body(68),
    con_dispersion: bool = Body(True),
    db: AsyncSession = Depends(get_db)
) -> LoadResult:
    """Cargar archivos de SAI"""
    
    loader = SAIFlujosLoader(db)
    temp_files = []
    
    try:
        # 1. Guardar archivos temporales
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_flujos:
            shutil.copyfileobj(flujos_file.file, tmp_flujos)
            flujos_path = tmp_flujos.name
            temp_files.append(flujos_path)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_instancia:
            shutil.copyfileobj(instancia_file.file, tmp_instancia)
            instancia_path = tmp_instancia.name
            temp_files.append(instancia_path)
        
        # 2. Cargar instancia primero (segregaciones y capacidades)
        instancia_stats = await loader.load_instancia_file(instancia_path)
        
        # 3. Cargar flujos
        config_id = await loader.load_flujos_file(
            flujos_path,
            fecha,
            semana,
            participacion,
            con_dispersion
        )
        
        # 4. Cargar evolución si se proporciona
        evolucion_stats = {}
        if evolucion_file:
            with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_evolucion:
                shutil.copyfileobj(evolucion_file.file, tmp_evolucion)
                evolucion_path = tmp_evolucion.name
                temp_files.append(evolucion_path)
                
            evolucion_stats = await loader.load_evolucion_file(evolucion_path, config_id)
        
        return LoadResult(
            success=True,
            message="Archivos cargados exitosamente",
            config_id=config_id,
            statistics={
                'instancia': instancia_stats,
                'evolucion': evolucion_stats,
                'fecha': fecha.isoformat(),
                'semana': semana
            }
        )
        
    except Exception as e:
        logger.error(f"Error cargando archivos SAI: {str(e)}")
        return LoadResult(
            success=False,
            message=f"Error al cargar archivos: {str(e)}",
            errors=[str(e)]
        )
    finally:
        # Limpiar archivos temporales
        for temp_file in temp_files:
            try:
                os.unlink(temp_file)
            except:
                pass

@router.get("/bahias/{bloque}/{turno}")
async def get_block_bahias_view(
    bloque: str,
    turno: int = FastAPIPath(..., ge=1, le=3),
    semana: int = Query(..., ge=1, le=52),
    fecha: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> BlockBahiasView:
    """Obtener vista de bahías para un bloque específico (matriz 7x30)"""
    
    # Buscar configuración
    query = select(SAIConfiguration).where(SAIConfiguration.semana == semana)
    if fecha:
        query = query.where(SAIConfiguration.fecha == fecha)
    
    config_result = await db.execute(query.order_by(SAIConfiguration.fecha_carga.desc()))
    config = config_result.scalars().first()
    
    if not config:
        raise HTTPException(404, f"No hay datos para semana {semana}")
    
    # Obtener distribución de bahías
    loader = SAIFlujosLoader(db)
    result = await loader.calculate_bahias_distribution(config.id, bloque, turno)
    
    # Crear matriz de ocupación 7x30
    matrix = [[None for _ in range(30)] for _ in range(7)]
    
    # Estadísticas
    bahias_ocupadas = result['total_bahias_usadas']
    total_volumen = 0
    segregaciones_stats = {}
    
    # Llenar matriz columna por columna
    current_column = 0
    
    for item in result['distribucion']:
        seg_id = item['segregacion_id']
        color = item['color']
        bahias = item['bahias']
        volumen = item['volumen_teus']
        ocupacion = item['ocupacion']
        
        total_volumen += volumen
        
        # Estadísticas por segregación
        segregaciones_stats[seg_id] = {
            'color': color,
            'count': 0,  # Se actualizará al llenar la matriz
            'bahias': bahias,
            'volumen': volumen,
            'porcentajeOcupacion': ocupacion,
            'tipo': '40' if item['teus'] == 2 else '20'
        }
        
        # Llenar bahías en la matriz
        for b in range(bahias):
            if current_column >= 30:
                break
                
            # Calcular cuántas celdas llenar basado en ocupación
            celdas_a_ocupar = int(np.ceil((ocupacion / 100) * 7))
            
            # Llenar de abajo hacia arriba
            for row in range(6, -1, -1):
                celdas_ocupadas = 6 - row + 1
                if celdas_ocupadas <= celdas_a_ocupar:
                    matrix[row][current_column] = BahiaCell(
                        segregacion=seg_id,
                        color=color,
                        percentage=100,
                        volumen_teus=volumen,
                        capacidad_teus=bahias * result['capacidad_bloque'].contenedores_por_bahia * item['teus']
                    )
                    segregaciones_stats[seg_id]['count'] += 1
            
            current_column += 1
    
    # Calcular ocupación real del bloque
    capacidad_total = result['capacidad_bloque'].capacidad_teus
    ocupacion_real = (total_volumen / capacidad_total * 100) if capacidad_total > 0 else 0
    
    # Obtener hora del turno
    hora_map = {1: "08-00", 2: "15-30", 3: "23-00"}
    
    return BlockBahiasView(
        bloque=bloque,
        turno=turno,
        hora=hora_map[turno],
        occupancy_matrix=matrix,
        bahias_ocupadas=bahias_ocupadas,
        ocupacion_real=ocupacion_real,
        segregaciones_activas=len(segregaciones_stats),
        total_volumen_teus=total_volumen,
        capacidad_total_teus=capacidad_total,
        segregaciones_stats=segregaciones_stats
    )

@router.get("/comparison/{semana}/{turno}")
async def get_sai_magdalena_comparison(
    semana: int = FastAPIPath(..., ge=1, le=52),
    turno: int = FastAPIPath(..., ge=1, le=3),
    participacion: int = Query(68),
    fecha: Optional[datetime] = Query(None),
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """Comparar datos SAI con Magdalena para el mismo período"""
    
    try:
        # Buscar configuración para la semana
        query = select(SAIConfiguration).where(SAIConfiguration.semana == semana)
        if fecha:
            query = query.where(SAIConfiguration.fecha == fecha)
        
        config_result = await db.execute(query.order_by(SAIConfiguration.fecha_carga.desc()))
        config = config_result.scalars().first()
        
        if not config:
            raise HTTPException(404, f"No hay datos SAI para semana {semana}")
        
        # Obtener métricas SAI
        sai_metrics = await get_sai_metrics(config.fecha, turno, db)
        
        # TODO: Obtener métricas Magdalena si están disponibles
        # Esto requeriría acceso al endpoint de Magdalena o a su servicio
        magdalena_metrics = None
        
        # Calcular comparación
        comparacion = {
            'sai_volumen_total': sai_metrics['totalVolumenTeus'],
            'sai_bloques_activos': sai_metrics['bloquesActivos'],
            'sai_segregaciones': sai_metrics['segregacionesActivas'],
            'sai_ocupacion_promedio': sai_metrics['ocupacionPromedio']
        }
        
        if magdalena_metrics:
            # Agregar comparación con Magdalena
            pass
        
        return {
            'fecha': sai_metrics['fecha'],
            'semana': semana,
            'turno': turno,
            'sai_metrics': sai_metrics,
            'magdalena_metrics': magdalena_metrics,
            'comparacion': comparacion
        }
        
    except Exception as e:
        logger.error(f"Error en comparación: {str(e)}")
        raise HTTPException(500, f"Error procesando comparación: {str(e)}")