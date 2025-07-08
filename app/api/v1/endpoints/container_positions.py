# app/api/v1/endpoints/container_positions.py
from typing import List, Optional, Dict, Any
from datetime import datetime, date
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
import numpy as np

from app.core.database import get_db
from app.models.container_position import ContainerPosition
from app.schemas.container_positions import (
    BlockPositionsResponse, 
    BahiaCell, 
    SegregacionStats,
    ContainerMetrics
)

router = APIRouter()

@router.get("/positions/block/{bloque}/{turno}")
async def get_block_positions(
    bloque: str = Path(..., description="ID del bloque (ej: C1, C2)"),
    turno: int = Path(..., ge=1, le=3, description="Número de turno (1-3)"),
    fecha: date = Query(..., description="Fecha a consultar"),
    db: AsyncSession = Depends(get_db)
) -> BlockPositionsResponse:
    """
    Obtener posiciones de contenedores para un bloque específico.
    Compatible con el formato del componente BloqueView.
    """
    
    # Normalizar el ID del bloque
    normalized_bloque = bloque.lstrip('C')  # Quitar 'C' si existe
    
    # Consultar posiciones
    query = select(ContainerPosition).where(
        and_(
            ContainerPosition.fecha == fecha,
            ContainerPosition.turno == turno,
            ContainerPosition.bloque == normalized_bloque,
            ContainerPosition.is_active == True
        )
    )
    
    result = await db.execute(query)
    positions = result.scalars().all()
    
    if not positions:
        # Retornar estructura vacía si no hay datos
        return BlockPositionsResponse(
            bloque=bloque,
            turno=turno,
            fecha=fecha,
            bahiasOcupadas=0,
            ocupacionReal=0.0,
            segregacionesActivas=0,
            totalVolumenTeus=0,
            capacidadTotalTeus=0,
            bahiasPorBloque={},
            volumenPorBloque={},
            segregacionesInfo={},
            segregacionesStats={},
            occupancyMatrix=[[None for _ in range(30)] for _ in range(7)]
        )
    
    # Procesar datos
    # Agrupar por categoría (usaremos category como segregación)
    segregaciones = {}
    bahias_ocupadas = set()
    
    for pos in positions:
        # Usar category como segregación (IMPRT, EXPRT, STRGE)
        seg_id = pos.category
        
        if seg_id not in segregaciones:
            segregaciones[seg_id] = {
                'positions': [],
                'teus': 0,
                'containers_20': 0,
                'containers_40': 0,
                'bahias': set(),
                'reefer': 0,
                'hazardous': 0
            }
        
        segregaciones[seg_id]['positions'].append(pos)
        segregaciones[seg_id]['bahias'].add(pos.bahia)
        
        # Contar TEUs
        teus = 2 if pos.nominal_length == 40 else 1
        segregaciones[seg_id]['teus'] += teus
        
        if pos.nominal_length == 20:
            segregaciones[seg_id]['containers_20'] += 1
        else:
            segregaciones[seg_id]['containers_40'] += 1
            
        if pos.requires_power:
            segregaciones[seg_id]['reefer'] += 1
        if pos.hazardous:
            segregaciones[seg_id]['hazardous'] += 1
            
        bahias_ocupadas.add(pos.bahia)
    
    # Construir respuesta en formato compatible
    key = f"{bloque}-{turno}"
    bahias_por_bloque = {key: {}}
    volumen_por_bloque = {key: {}}
    segregaciones_info = {}
    segregaciones_stats = {}
    
    # Mapeo de colores para categorías
    color_map = {
        'IMPRT': '#3B82F6',  # Azul
        'EXPRT': '#10B981',  # Verde
        'STRGE': '#F59E0B'   # Amarillo
    }
    
    for seg_id, data in segregaciones.items():
        num_bahias = len(data['bahias'])
        total_teus = data['teus']
        
        bahias_por_bloque[key][seg_id] = num_bahias
        volumen_por_bloque[key][seg_id] = total_teus
        
        segregaciones_info[seg_id] = {
            'id': seg_id,
            'nombre': seg_id,
            'teus': 2 if data['containers_40'] > data['containers_20'] else 1,
            'tipo': 'contenedor',
            'categoria': seg_id,
            'direccion': 'import' if seg_id == 'IMPRT' else 'export',
            'color': color_map.get(seg_id, '#6B7280')
        }
        
        # Calcular ocupación estimada (35 contenedores por bahía)
        capacidad_bahias = num_bahias * 35
        ocupacion = (total_teus / capacidad_bahias * 100) if capacidad_bahias > 0 else 0
        
        segregaciones_stats[seg_id] = {
            'color': color_map.get(seg_id, '#6B7280'),
            'count': len(data['positions']),
            'bahias': num_bahias,
            'volumen': total_teus,
            'porcentajeOcupacion': ocupacion,
            'tipo': '40' if data['containers_40'] > data['containers_20'] else '20',
            'reefer': data['reefer'],
            'hazardous': data['hazardous']
        }
    
    # Crear matriz de ocupación (7x30)
    occupancy_matrix = [[None for _ in range(30)] for _ in range(7)]
    
    # Mapeo de filas: A=0, B=1, ..., G=6
    row_map = {'A': 0, 'B': 1, 'C': 2, 'D': 3, 'E': 4, 'F': 5, 'G': 6}
    
    # Llenar matriz con las posiciones reales
    for pos in positions:
        if pos.fila in row_map and 1 <= pos.bahia <= 30:
            row_idx = row_map[pos.fila]
            col_idx = pos.bahia - 1  # Ajustar a índice 0
            
            seg_data = segregaciones_info.get(pos.category, {})
            
            occupancy_matrix[row_idx][col_idx] = {
                'segregacion': pos.category,
                'color': seg_data.get('color', '#6B7280'),
                'percentage': 100,  # Celda ocupada
                'gkey': pos.gkey,
                'nominal_length': pos.nominal_length,
                'requires_power': pos.requires_power,
                'hazardous': pos.hazardous
            }
    
    # Calcular métricas totales
    total_bahias = len(bahias_ocupadas)
    total_volumen_teus = sum(s['teus'] for s in segregaciones.values())
    capacidad_total = total_bahias * 35 * 2  # Asumiendo capacidad máxima en TEUs
    ocupacion_real = (total_volumen_teus / capacidad_total * 100) if capacidad_total > 0 else 0
    
    return BlockPositionsResponse(
        bloque=bloque,
        turno=turno,
        fecha=fecha,
        hora="08-00" if turno == 1 else "15-30" if turno == 2 else "23-00",
        bahiasOcupadas=total_bahias,
        ocupacionReal=ocupacion_real,
        segregacionesActivas=len(segregaciones),
        totalVolumenTeus=total_volumen_teus,
        capacidadTotalTeus=capacidad_total,
        bahiasPorBloque=bahias_por_bloque,
        volumenPorBloque=volumen_por_bloque,
        segregacionesInfo=segregaciones_info,
        segregacionesStats=segregaciones_stats,
        occupancyMatrix=occupancy_matrix,
        capacidadesPorBloque={bloque: 35},  # Contenedores por bahía
        teusPorSegregacion={k: v['teus'] for k, v in segregaciones_info.items()}
    )

@router.get("/positions/metrics")
async def get_container_metrics(
    fecha: date = Query(..., description="Fecha a consultar"),
    turno: Optional[int] = Query(None, ge=1, le=3, description="Turno específico"),
    db: AsyncSession = Depends(get_db)
) -> ContainerMetrics:
    """
    Obtener métricas generales de contenedores.
    Compatible con el formato usado por useSAIData hook.
    """
    
    # Si no se especifica turno, usar todos
    turnos = [turno] if turno else [1, 2, 3]
    
    # Obtener estadísticas por bloque
    bloques_data = {}
    
    for t in turnos:
        query = await db.execute(
            select(
                ContainerPosition.bloque,
                func.count(ContainerPosition.id).label('total'),
                func.count(func.distinct(ContainerPosition.bahia)).label('bahias'),
                func.sum(
                    func.case(
                        (ContainerPosition.nominal_length == 20, 1),
                        else_=2
                    )
                ).label('teus')
            )
            .where(
                and_(
                    ContainerPosition.fecha == fecha,
                    ContainerPosition.turno == t,
                    ContainerPosition.is_active == True
                )
            )
            .group_by(ContainerPosition.bloque)
        )
        
        for row in query:
            key = f"C{row.bloque}-{t}"
            bloques_data[key] = {
                'bloque': f"C{row.bloque}",
                'turno': t,
                'total_contenedores': row.total,
                'bahias_ocupadas': row.bahias,
                'teus': row.teus or 0
            }
    
    # Obtener totales
    total_query = await db.execute(
        select(
            func.count(ContainerPosition.id).label('total'),
            func.count(func.distinct(ContainerPosition.gkey)).label('unique_containers'),
            func.count(func.distinct(ContainerPosition.bloque)).label('bloques'),
            func.sum(
                func.case(
                    (ContainerPosition.nominal_length == 20, 1),
                    else_=2
                )
            ).label('total_teus')
        )
        .where(
            and_(
                ContainerPosition.fecha == fecha,
                ContainerPosition.turno.in_(turnos),
                ContainerPosition.is_active == True
            )
        )
    )
    
    totals = total_query.one()
    
    # Construir respuesta
    bahias_por_bloque = {}
    volumen_por_bloque = {}
    ocupacion_por_bloque = {}
    
    for key, data in bloques_data.items():
        bloque_id = data['bloque']
        
        # Simplificar para el componente
        bahias_por_bloque[key] = {'total': data['bahias_ocupadas']}
        volumen_por_bloque[key] = {'total': data['teus']}
        
        # Calcular ocupación (asumiendo 30 bahías por bloque, 35 contenedores por bahía)
        capacidad_bloque = 30 * 35 * 2  # En TEUs
        ocupacion = (data['teus'] / capacidad_bloque * 100) if capacidad_bloque > 0 else 0
        ocupacion_por_bloque[bloque_id] = ocupacion
    
    return ContainerMetrics(
        fecha=fecha,
        turno=turno if turno else 0,
        totalMovimientos=totals.total,
        totalVolumenTeus=totals.total_teus or 0,
        bloquesActivos=totals.bloques,
        segregacionesActivas=3,  # IMPRT, EXPRT, STRGE
        ocupacionPromedio=float(np.mean(list(ocupacion_por_bloque.values()))) if ocupacion_por_bloque else 0,
        ocupacionPorBloque=ocupacion_por_bloque,
        bahiasPorBloque=bahias_por_bloque,
        volumenPorBloque=volumen_por_bloque,
        segregacionesInfo={
            'IMPRT': {
                'id': 'IMPRT',
                'nombre': 'Importación',
                'teus': 2,
                'tipo': 'import',
                'color': '#3B82F6'
            },
            'EXPRT': {
                'id': 'EXPRT',
                'nombre': 'Exportación',
                'teus': 2,
                'tipo': 'export',
                'color': '#10B981'
            },
            'STRGE': {
                'id': 'STRGE',
                'nombre': 'Almacenaje',
                'teus': 2,
                'tipo': 'storage',
                'color': '#F59E0B'
            }
        },
        capacidadesPorBloque={f"C{i}": 35 for i in range(1, 10)},
        teusPorSegregacion={'IMPRT': 2, 'EXPRT': 2, 'STRGE': 2}
    )

@router.get("/positions/dates")
async def get_available_dates(
    db: AsyncSession = Depends(get_db)
) -> List[date]:
    """Obtener fechas disponibles con datos"""
    
    query = await db.execute(
        select(func.distinct(ContainerPosition.fecha))
        .order_by(ContainerPosition.fecha.desc())
        .limit(100)
    )
    
    return [row[0] for row in query]