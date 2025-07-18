# app/api/v1/endpoints/sai_flujos.py
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, case, extract
from sqlalchemy.orm import selectinload
from app.core.database import get_db
from app.models.movement_flow import MovementFlow
from app.models.optimization import Bloque
import logging
from sqlalchemy import distinct
import math


router = APIRouter()
logger = logging.getLogger(__name__)

def get_turno_from_hour(hour: int) -> int:
    """Determina el turno basado en la hora (1: 8-16h, 2: 16-24h, 3: 0-8h)"""
    if 8 <= hour < 16:
        return 1
    elif 16 <= hour < 24:
        return 2
    else:
        return 3

def get_periodo_from_datetime(dt: datetime) -> int:
    """
    Calcula el período (1-21) basado en el día de la semana y turno
    Lunes = 1, Domingo = 7
    """
    weekday = dt.weekday()  # 0 = Lunes, 6 = Domingo
    turno = get_turno_from_hour(dt.hour)
    return weekday * 3 + turno



@router.get("/bloques/{bloque_id}/historico")
async def get_bloque_historico(
   bloque_id: str,
   fecha: datetime = Query(..., description="Fecha específica"),
   unidad_temporal: str = Query("turno", regex="^(semana|dia|turno|hora)$"),
   turno: Optional[int] = Query(None, ge=1, le=3, description="Turno específico (1-3)"),
   hora: Optional[int] = Query(None, ge=0, le=23, description="Hora específica (0-23)"),
   db: AsyncSession = Depends(get_db)
):
   """
   Obtener datos históricos de ocupación de un bloque para BloqueView
   Soporta diferentes unidades temporales: semana, día, turno, hora
   """
   
   # Remover timezone si existe para evitar errores con PostgreSQL
   if fecha.tzinfo is not None:
       fecha = fecha.replace(tzinfo=None)
   
   # Determinar rango de fechas según la unidad temporal
   if unidad_temporal == "semana":
       # Obtener inicio de semana (lunes)
       start_date = fecha - timedelta(days=fecha.weekday())
       start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
       end_date = start_date + timedelta(days=7)
       end_date = end_date.replace(tzinfo=None)
       
   elif unidad_temporal == "dia":
       start_date = fecha.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
       end_date = start_date + timedelta(days=1)
       end_date = end_date.replace(tzinfo=None)
       
   elif unidad_temporal == "turno":
       if turno is None:
           turno = get_turno_from_hour(fecha.hour)
       
       # Determinar horas del turno
       if turno == 1:
           start_hour, end_hour = 8, 16
       elif turno == 2:
           start_hour, end_hour = 16, 24
       else:  # turno 3
           start_hour, end_hour = 0, 8
           
       start_date = fecha.replace(hour=start_hour, minute=0, second=0, microsecond=0, tzinfo=None)
       if end_hour == 24:
           end_date = (fecha + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
       else:
           end_date = fecha.replace(hour=end_hour, minute=0, second=0, microsecond=0, tzinfo=None)
           
   else:  # hora
       if hora is None:
           hora = fecha.hour
       start_date = fecha.replace(hour=hora, minute=0, second=0, microsecond=0, tzinfo=None)
       end_date = start_date + timedelta(hours=1)
       end_date = end_date.replace(tzinfo=None)
   
   logger.info(f"Consultando bloque {bloque_id} desde {start_date} hasta {end_date}")
   
   # Consultar movimientos en el rango
   query = select(
       MovementFlow.iu_category.label('segregacion'),
       MovementFlow.ret_nominal_length,
       func.count(distinct(MovementFlow.ime_ufv_gkey)).label('contenedores'),
       func.count(MovementFlow.id).label('movimientos')
   ).where(
       and_(
           MovementFlow.bloque == bloque_id,
           MovementFlow.ime_time >= start_date,
           MovementFlow.ime_time < end_date,
           MovementFlow.iu_category.isnot(None)
       )
   ).group_by(
       MovementFlow.iu_category,
       MovementFlow.ret_nominal_length
   )
   
   result = await db.execute(query)
   movimientos_data = result.all()
   
   # Obtener capacidad del bloque
   bloque_query = await db.execute(
       select(Bloque).where(Bloque.codigo == bloque_id)
   )
   bloque = bloque_query.scalar_one_or_none()
   
   capacidad_por_bahia = 35  # Default
   if bloque and bloque.capacidad_bahias > 0:
       capacidad_por_bahia = bloque.capacidad_teus // bloque.capacidad_bahias
   
   # Procesar datos para el formato esperado por BloqueView
   bahias_por_segregacion = {}
   volumen_por_segregacion = {}
   teus_por_segregacion = {}
   segregaciones_info = {}
   
   # Calcular bahías de manera más realista
   import math
   
   for mov in movimientos_data:
       segregacion = mov.segregacion or "UNKNOWN"
       
       # Determinar TEUs por tamaño
       teu_factor = 2 if mov.ret_nominal_length == "NOM40" else 1
       
       # Calcular bahías de manera más precisa
       # Un contenedor de 20' ocupa ~0.5 bahía, uno de 40' ocupa ~1 bahía
       if mov.ret_nominal_length == "NOM40":
           bahias_por_contenedor = 1.0
       else:
           bahias_por_contenedor = 0.5
       
       # Calcular bahías necesarias para esta segregación
       bahias_necesarias = math.ceil(mov.contenedores * bahias_por_contenedor)
       
       # Asegurar que no exceda las 30 bahías disponibles
       bahias_estimadas = min(30, max(1, bahias_necesarias))
       
       # Calcular volumen en TEUs
       volumen_teus = mov.contenedores * teu_factor
       
       bahias_por_segregacion[segregacion] = bahias_estimadas
       volumen_por_segregacion[segregacion] = volumen_teus
       teus_por_segregacion[segregacion] = teu_factor
       segregaciones_info[segregacion] = {
           'descripcion': f"{segregacion} - {'40ft' if teu_factor == 2 else '20ft'}",
           'movimientos': mov.movimientos,
           'contenedores': mov.contenedores
       }
   
   # Calcular estadísticas generales
   total_bahias_ocupadas = sum(bahias_por_segregacion.values())
   total_volumen_teus = sum(volumen_por_segregacion.values())
   capacidad_total_teus = 30 * capacidad_por_bahia  # 30 bahías * capacidad por bahía
   ocupacion_real = (total_volumen_teus / capacidad_total_teus * 100) if capacidad_total_teus > 0 else 0
   
   # Si es vista semanal, necesitamos datos para cada turno (1-21)
   if unidad_temporal == "semana":
       response = {
           'bahiasPorBloque': {},
           'volumenPorBloque': {},
           'capacidadesPorBloque': {bloque_id: capacidad_por_bahia},
           'teusPorSegregacion': teus_por_segregacion,
           'segregacionesInfo': segregaciones_info
       }
       
       # Iterar por cada día y turno de la semana
       for dia in range(7):
           fecha_dia = start_date + timedelta(days=dia)
           
           for turno_num in range(1, 4):
               periodo = dia * 3 + turno_num
               
               # Determinar rango de horas para este turno
               if turno_num == 1:
                   hora_inicio, hora_fin = 8, 16
               elif turno_num == 2:
                   hora_inicio, hora_fin = 16, 24
               else:
                   hora_inicio, hora_fin = 0, 8
               
               turno_start = fecha_dia.replace(hour=hora_inicio, minute=0, second=0, tzinfo=None)
               if hora_fin == 24:
                   turno_end = (fecha_dia + timedelta(days=1)).replace(hour=0, minute=0, tzinfo=None)
               else:
                   turno_end = fecha_dia.replace(hour=hora_fin, minute=0, tzinfo=None)
               
               # Consultar datos para este turno específico
               turno_query = select(
                   MovementFlow.iu_category.label('segregacion'),
                   MovementFlow.ret_nominal_length,
                   func.count(distinct(MovementFlow.ime_ufv_gkey)).label('contenedores')
               ).where(
                   and_(
                       MovementFlow.bloque == bloque_id,
                       MovementFlow.ime_time >= turno_start,
                       MovementFlow.ime_time < turno_end,
                       MovementFlow.iu_category.isnot(None)
                   )
               ).group_by(
                   MovementFlow.iu_category,
                   MovementFlow.ret_nominal_length
               )
               
               turno_result = await db.execute(turno_query)
               turno_data = turno_result.all()
               
               # Procesar datos del turno
               key = f"{bloque_id}-{periodo}"
               bahias_turno = {}
               volumen_turno = {}
               
               for td in turno_data:
                   seg = td.segregacion or "UNKNOWN"
                   
                   # Calcular bahías más precisamente
                   if td.ret_nominal_length == "NOM40":
                       bahias_por_cont = 1.0
                       teu = 2
                   else:
                       bahias_por_cont = 0.5
                       teu = 1
                   
                   bahias_turno[seg] = max(1, min(30, math.ceil(td.contenedores * bahias_por_cont)))
                   volumen_turno[seg] = td.contenedores * teu
               
               response['bahiasPorBloque'][key] = bahias_turno
               response['volumenPorBloque'][key] = volumen_turno
       
       # Agregar estadísticas generales
       response['estadisticas'] = {
           'bahiasOcupadas': total_bahias_ocupadas,
           'ocupacionReal': ocupacion_real,
           'segregacionesActivas': len(bahias_por_segregacion),
           'totalVolumenTeus': total_volumen_teus,
           'capacidadTotalTeus': capacidad_total_teus
       }
       
       return response
   
   else:
       # Para otras unidades temporales, usar periodo 1
       key = f"{bloque_id}-1"
       
       return {
           'bahiasPorBloque': {
               key: bahias_por_segregacion
           },
           'volumenPorBloque': {
               key: volumen_por_segregacion
           },
           'capacidadesPorBloque': {
               bloque_id: capacidad_por_bahia
           },
           'teusPorSegregacion': teus_por_segregacion,
           'segregacionesInfo': segregaciones_info,
           'estadisticas': {
               'bahiasOcupadas': total_bahias_ocupadas,
               'ocupacionReal': ocupacion_real,
               'segregacionesActivas': len(bahias_por_segregacion),
               'totalVolumenTeus': total_volumen_teus,
               'capacidadTotalTeus': capacidad_total_teus
           }
       }
       
       
       
@router.get("/dashboard/historico")
async def get_dashboard_historico(
    fecha: datetime = Query(...),
    unidad_temporal: str = Query("dia", regex="^(semana|dia|turno|hora)$"),
    patio: Optional[str] = Query(None, regex="^(costanera|ohiggins|tebas)$"),
    db: AsyncSession = Depends(get_db)
):
    """
    Dashboard con métricas históricas SAI
    Similar al dashboard de Magdalena pero con datos reales
    """
    
    # Determinar rango según unidad temporal
    if unidad_temporal == "semana":
        start_date = fecha - timedelta(days=fecha.weekday())
        start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=7)
    elif unidad_temporal == "dia":
        start_date = fecha.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)
    elif unidad_temporal == "turno":
        turno = get_turno_from_hour(fecha.hour)
        if turno == 1:
            start_date = fecha.replace(hour=8, minute=0, second=0, microsecond=0)
            end_date = fecha.replace(hour=16, minute=0, second=0, microsecond=0)
        elif turno == 2:
            start_date = fecha.replace(hour=16, minute=0, second=0, microsecond=0)
            end_date = (fecha + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start_date = fecha.replace(hour=0, minute=0, second=0, microsecond=0)
            end_date = fecha.replace(hour=8, minute=0, second=0, microsecond=0)
    else:  # hora
        start_date = fecha.replace(minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(hours=1)
    
    # Query base con filtros
    base_filters = [
        MovementFlow.ime_time >= start_date,
        MovementFlow.ime_time < end_date
    ]
    
    if patio:
        base_filters.append(MovementFlow.patio == patio)
    
    # Contar movimientos por tipo
    mov_query = await db.execute(
        select(
            MovementFlow.ime_move_kind,
            func.count(MovementFlow.id).label('total')
        ).where(
            and_(*base_filters)
        ).group_by(MovementFlow.ime_move_kind)
    )
    movimientos_por_tipo = {row.ime_move_kind: row.total for row in mov_query}
    
    # Total de movimientos
    total_real = sum(movimientos_por_tipo.values())
    yard_movimientos = movimientos_por_tipo.get('YARD', 0)
    
    # Ocupación por bloque
    ocupacion_query = await db.execute(
        select(
            MovementFlow.bloque,
            func.count(distinct(MovementFlow.ime_ufv_gkey)).label('contenedores'),
            MovementFlow.ret_nominal_length
        ).where(
            and_(
                *base_filters,
                MovementFlow.bloque.isnot(None)
            )
        ).group_by(
            MovementFlow.bloque,
            MovementFlow.ret_nominal_length
        )
    )
    
    # Calcular ocupación
    ocupacion_por_bloque = {}
    for row in ocupacion_query:
        if row.bloque not in ocupacion_por_bloque:
            ocupacion_por_bloque[row.bloque] = {'contenedores': 0, 'teus': 0}
        
        teu_factor = 2 if row.ret_nominal_length == "NOM40" else 1
        ocupacion_por_bloque[row.bloque]['contenedores'] += row.contenedores
        ocupacion_por_bloque[row.bloque]['teus'] += row.contenedores * teu_factor
    
    # Segregaciones activas
    seg_query = await db.execute(
        select(
            MovementFlow.iu_category,
            func.count(MovementFlow.id).label('movimientos'),
            func.count(distinct(MovementFlow.ime_ufv_gkey)).label('contenedores')
        ).where(
            and_(
                *base_filters,
                MovementFlow.iu_category.isnot(None)
            )
        ).group_by(MovementFlow.iu_category)
        .order_by(func.count(MovementFlow.id).desc())
    )
    segregaciones = seg_query.all()
    
    # Evolución temporal (si es semana o día)
    evolucion_temporal = []
    if unidad_temporal in ["semana", "dia"]:
        # Agrupar por hora
        evol_query = await db.execute(
            select(
                extract('hour', MovementFlow.ime_time).label('hora'),
                func.count(MovementFlow.id).label('movimientos'),
                func.count(case((MovementFlow.ime_move_kind == 'YARD', 1))).label('yard')
            ).where(
                and_(*base_filters)
            ).group_by('hora')
            .order_by('hora')
        )
        
        for row in evol_query:
            turno = get_turno_from_hour(int(row.hora))
            evolucion_temporal.append({
                'hora': int(row.hora),
                'turno': turno,
                'movimientos_real': row.movimientos,
                'movimientos_yard': row.yard,
                'movimientos_utiles': row.movimientos - row.yard
            })
    
    # Construir respuesta
    return {
        'metadata': {
            'fecha_inicio': start_date.isoformat(),
            'fecha_fin': end_date.isoformat(),
            'unidad_temporal': unidad_temporal,
            'patio': patio,
            'fuente': 'SAI Histórico'
        },
        'kpis_principales': {
            'movimientos': {
                'total_real': total_real,
                'yard_total': yard_movimientos,
                'utiles': total_real - yard_movimientos,
                'yard_porcentaje': (yard_movimientos / total_real * 100) if total_real > 0 else 0
            },
            'segregaciones': {
                'total': len(segregaciones),
                'mas_activa': segregaciones[0].iu_category if segregaciones else None
            },
            'ocupacion': {
                'bloques_activos': len(ocupacion_por_bloque),
                'contenedores_total': sum(b['contenedores'] for b in ocupacion_por_bloque.values()),
                'teus_total': sum(b['teus'] for b in ocupacion_por_bloque.values())
            }
        },
        'movimientos_por_tipo': movimientos_por_tipo,
        'ocupacion_por_bloque': [
            {
                'bloque': bloque,
                'contenedores': data['contenedores'],
                'teus': data['teus'],
                'ocupacion_estimada': min(100, (data['contenedores'] / 210) * 100)  # 210 = capacidad típica
            }
            for bloque, data in ocupacion_por_bloque.items()
        ],
        'segregaciones_activas': [
            {
                'codigo': seg.iu_category,
                'movimientos': seg.movimientos,
                'contenedores': seg.contenedores
            }
            for seg in segregaciones
        ],
        'evolucion_temporal': evolucion_temporal
    }

@router.get("/estadisticas/historico")
async def get_estadisticas_historico(
    year: Optional[int] = Query(None, ge=2017, le=2023),
    db: AsyncSession = Depends(get_db)
):
    """Estadísticas generales de datos históricos SAI"""
    
    # Filtros base
    filters = []
    if year:
        filters.append(extract('year', MovementFlow.ime_time) == year)
    
    # Total de registros
    total_query = await db.execute(
        select(func.count(MovementFlow.id)).where(
            and_(*filters) if filters else True
        )
    )
    total_movimientos = total_query.scalar()
    
    # Rango de fechas
    fecha_query = await db.execute(
        select(
            func.min(MovementFlow.ime_time).label('min_fecha'),
            func.max(MovementFlow.ime_time).label('max_fecha')
        ).where(
            and_(*filters) if filters else True
        )
    )
    fechas = fecha_query.one()
    
    # Estadísticas por año
    stats_year = await db.execute(
        select(
            extract('year', MovementFlow.ime_time).label('year'),
            func.count(MovementFlow.id).label('total'),
            func.count(distinct(MovementFlow.ime_ufv_gkey)).label('contenedores_unicos')
        ).group_by('year')
        .order_by('year')
    )
    
    # Estadísticas por tipo de movimiento
    stats_tipo = await db.execute(
        select(
            MovementFlow.ime_move_kind,
            func.count(MovementFlow.id).label('total')
        ).where(
            and_(*filters) if filters else True
        ).group_by(MovementFlow.ime_move_kind)
        .order_by(func.count(MovementFlow.id).desc())
        .limit(10)
    )
    
    # Estadísticas por patio
    stats_patio = await db.execute(
        select(
            MovementFlow.patio,
            func.count(MovementFlow.id).label('total'),
            func.count(distinct(MovementFlow.bloque)).label('bloques')
        ).where(
            and_(
                MovementFlow.patio.isnot(None),
                *filters
            )
        ).group_by(MovementFlow.patio)
    )
    
    return {
        'resumen': {
            'total_movimientos': total_movimientos,
            'fecha_inicio': fechas.min_fecha.isoformat() if fechas.min_fecha else None,
            'fecha_fin': fechas.max_fecha.isoformat() if fechas.max_fecha else None
        },
        'por_año': [
            {
                'año': int(row.year),
                'movimientos': row.total,
                'contenedores_unicos': row.contenedores_unicos
            }
            for row in stats_year
        ],
        'por_tipo_movimiento': [
            {
                'tipo': row.ime_move_kind,
                'total': row.total
            }
            for row in stats_tipo
        ],
        'por_patio': [
            {
                'patio': row.patio,
                'movimientos': row.total,
                'bloques_activos': row.bloques
            }
            for row in stats_patio
        ]
    }