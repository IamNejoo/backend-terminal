# app/services/sai_flujos_loader.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from uuid import UUID
import logging
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.models.sai_flujos import (
    SAIConfiguration, SAIFlujo, SAIVolumenBloque, SAIVolumenSegregacion,
    SAISegregacion, SAICapacidadBloque, SAIMapeoCriterios
)

logger = logging.getLogger(__name__)

class SAIFlujosLoader:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.segregacion_colors = self._generate_colors()
        
    def _generate_colors(self) -> List[str]:
        """Genera lista de colores para segregaciones"""
        return [
            '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
            '#EC4899', '#14B8A6', '#F97316', '#6366F1', '#84CC16',
            '#06B6D4', '#A855F7', '#DC2626', '#059669', '#7C3AED',
            '#2563EB', '#EA580C', '#0891B2', '#9333EA', '#16A34A'
        ]
    
    def _get_turno_from_hora(self, hora: int) -> Tuple[int, str]:
        """Determina el turno basado en la hora"""
        if 6 <= hora < 14:
            return 1, "08-00"
        elif 14 <= hora < 22:
            return 2, "15-30"
        else:
            return 3, "23-00"
    
    async def load_instancia_file(self, file_path: str) -> Dict[str, Any]:
        """Carga archivo de instancia con segregaciones y capacidades"""
        logger.info(f"Cargando instancia desde {file_path}")
        
        try:
            excel_data = pd.ExcelFile(file_path)
            
            # 1. Cargar segregaciones
            if 'S' in excel_data.sheet_names:
                df_segregaciones = pd.read_excel(file_path, sheet_name='S')
                
                # Cargar TEUs
                teus_map = {}
                if 'TEU_s' in excel_data.sheet_names:
                    df_teus = pd.read_excel(file_path, sheet_name='TEU_s')
                    for _, row in df_teus.iterrows():
                        teus_map[str(row['S'])] = int(row['TEU'])
                
                # Procesar segregaciones
                for idx, row in df_segregaciones.iterrows():
                    seg_id = str(row['S'])
                    seg_nombre = str(row['Segregacion'])
                    teus = teus_map.get(seg_id, 1)
                    
                    # Extraer información del nombre
                    tipo = '40' if '40' in seg_nombre else '20'
                    categoria = 'reefer' if 'reefer' in seg_nombre else 'dry'
                    direccion = 'impo' if 'impo' in seg_nombre else 'expo'
                    
                    # Verificar si ya existe
                    existing = await self.db.execute(
                        select(SAISegregacion).where(SAISegregacion.id == seg_id)
                    )
                    if not existing.scalar():
                        segregacion = SAISegregacion(
                            id=seg_id,
                            nombre=seg_nombre,
                            teus=teus,
                            tipo=tipo,
                            categoria=categoria,
                            direccion=direccion,
                            color=self.segregacion_colors[idx % len(self.segregacion_colors)]
                        )
                        self.db.add(segregacion)
                
                logger.info(f"Cargadas {len(df_segregaciones)} segregaciones")
            
            # 2. Cargar capacidades de bloques
            capacidades = {}
            if 'C_b' in excel_data.sheet_names:
                df_capacidades = pd.read_excel(file_path, sheet_name='C_b')
                for _, row in df_capacidades.iterrows():
                    bloque = str(row['B'])
                    capacidades[bloque] = int(row['C'])
            
            # Cargar VS_b (contenedores por bahía)
            vs_map = {}
            if 'VS_b' in excel_data.sheet_names:
                df_vs = pd.read_excel(file_path, sheet_name='VS_b')
                for _, row in df_vs.iterrows():
                    vs_map[str(row['B'])] = int(row['VS'])
            
            # Capacidades definidas - TODOS LOS BLOQUES
            BLOCK_CAPACITIES = {
                'C1': 1155, 'C2': 1225, 'C3': 1400, 'C4': 1400,
                'C5': 490, 'C6': 1015, 'C7': 1015, 'C8': 980, 'C9': 420,
                'H1': 800, 'H2': 800, 'H3': 800, 'H4': 800, 'H5': 900,
                'T1': 400, 'T2': 400, 'T3': 400, 'T4': 400
            }
            
            BLOCK_TOTAL_BAYS = {
                'C1': 33, 'C2': 35, 'C3': 40, 'C4': 40,
                'C5': 14, 'C6': 29, 'C7': 29, 'C8': 28, 'C9': 12,
                'H1': 23, 'H2': 23, 'H3': 23, 'H4': 23, 'H5': 26,
                'T1': 12, 'T2': 12, 'T3': 12, 'T4': 12
            }
            
            BLOCK_REEFER_BAYS = {
                'C1': 8, 'C2': 8, 'C3': 8, 'C4': 8,
                'C5': 14, 'C6': 0, 'C7': 0, 'C8': 28, 'C9': 12,
                'H1': 0, 'H2': 0, 'H3': 0, 'H4': 0, 'H5': 0,
                'T1': 0, 'T2': 0, 'T3': 0, 'T4': 0
            }
            
            # Crear registros de capacidad
            for bloque, capacidad_contenedores in BLOCK_CAPACITIES.items():
                existing = await self.db.execute(
                    select(SAICapacidadBloque).where(SAICapacidadBloque.bloque == bloque)
                )
                if not existing.scalar():
                    bahias_totales = BLOCK_TOTAL_BAYS.get(bloque, 30)
                    contenedores_por_bahia = vs_map.get(bloque, 35)
                    
                    capacidad = SAICapacidadBloque(
                        bloque=bloque,
                        capacidad_contenedores=capacidad_contenedores,
                        capacidad_teus=capacidades.get(bloque, capacidad_contenedores * 2),
                        bahias_totales=bahias_totales,
                        bahias_reefer=BLOCK_REEFER_BAYS.get(bloque, 0),
                        contenedores_por_bahia=contenedores_por_bahia
                    )
                    self.db.add(capacidad)
            
            await self.db.commit()
            
            return {
                'segregaciones': len(df_segregaciones) if 'S' in excel_data.sheet_names else 0,
                'capacidades': len(BLOCK_CAPACITIES)
            }
            
        except Exception as e:
            logger.error(f"Error cargando instancia: {str(e)}")
            await self.db.rollback()
            raise
    
    async def load_flujos_file(
        self,
        file_path: str,
        fecha: datetime,
        semana: int,
        participacion: int = 68,
        con_dispersion: bool = True
    ) -> UUID:
        """Carga archivo de flujos"""
        logger.info(f"Cargando flujos desde {file_path}")
        
        try:
            # Crear configuración
            config = SAIConfiguration(
                fecha=fecha,
                semana=semana,
                participacion=participacion,
                con_dispersion=con_dispersion
            )
            self.db.add(config)
            await self.db.flush()
            
            # Leer flujos
            df_flujos = pd.read_excel(file_path)
            
            # Obtener mapeo de segregaciones
            seg_query = await self.db.execute(select(SAISegregacion))
            segregaciones = {s.nombre: s.id for s in seg_query.scalars().all()}
            
            # Procesar flujos y crear mapeo de criterios
            criterios_map = {}
            flujos_count = 0
            
            for _, row in df_flujos.iterrows():
                # Obtener fecha/hora exacta
                ime_time = pd.to_datetime(row.get('ime_time'))
                hora_exacta = ime_time.time()  # Solo la hora
                
                # Obtener turno basado en la hora
                turno, hora_turno = self._get_turno_from_hora(ime_time.hour)
                
                # Crear flujo con hora exacta y turno
                flujo = SAIFlujo(
                    config_id=config.id,
                    ime_time=ime_time,
                    hora_exacta=hora_exacta,  # NUEVO: hora exacta
                    turno=turno,
                    hora_turno=hora_turno,    # RENOMBRADO de 'hora'
                    ime_fm=str(row.get('ime_fm', '')),
                    ime_to=str(row.get('ime_to', '')),
                    ime_move_kind=str(row.get('ime_move_kind', '')),
                    criterio_i=str(row.get('criterio_i', '')),
                    criterio_ii=str(row.get('criterio_ii', '')),
                    criterio_iii=str(row.get('criterio_iii', '')),
                    iu_category=str(row.get('iu_category', '')),
                    ig_hazardous=bool(row.get('ig_hazardous', 0)),
                    iu_requires_power=bool(row.get('iu_requires_power', 0))
                )
                self.db.add(flujo)
                flujos_count += 1
                
                # Mapear criterio_ii con segregación
                criterio = str(row.get('criterio_ii', ''))
                if criterio and criterio not in criterios_map:
                    # Buscar segregación correspondiente
                    for seg_nombre, seg_id in segregaciones.items():
                        if criterio == seg_nombre:
                            criterios_map[criterio] = seg_id
                            break
            
            # Crear mapeo de criterios
            for criterio, seg_id in criterios_map.items():
                existing = await self.db.execute(
                    select(SAIMapeoCriterios).where(SAIMapeoCriterios.criterio == criterio)
                )
                if not existing.scalar():
                    mapeo = SAIMapeoCriterios(
                        criterio=criterio,
                        segregacion_id=seg_id,
                        frecuencia_uso=1,
                        fecha_ultimo_uso=fecha
                    )
                    self.db.add(mapeo)
            
            await self.db.commit()
            logger.info(f"Cargados {flujos_count} flujos, {len(criterios_map)} mapeos de criterios")
            
            return config.id
            
        except Exception as e:
            logger.error(f"Error cargando flujos: {str(e)}")
            await self.db.rollback()
            raise
    
async def load_evolucion_file(
    self,
    file_path: str,
    config_id: UUID
) -> Dict[str, Any]:
    """Carga archivo de evolución de turnos"""
    logger.info(f"Cargando evolución desde {file_path}")
    
    try:
        excel_data = pd.ExcelFile(file_path)
        
        # Obtener configuración
        config_query = await self.db.execute(
            select(SAIConfiguration).where(SAIConfiguration.id == config_id)
        )
        config = config_query.scalar_one()
        
        stats = {'volumen_bloques': 0, 'volumen_segregaciones': 0}
        
        # 1. Cargar volumen por bloques
        if 'Volumen_Bloques' in excel_data.sheet_names:
            df_volumen = pd.read_excel(file_path, sheet_name='Volumen_Bloques')
            
            for _, row in df_volumen.iterrows():
                fecha = pd.to_datetime(row['Fecha'])
                turno_str = str(row['Turno'])
                
                # Determinar turno numérico
                if '08' in turno_str or '8' in turno_str:
                    turno = 1
                    hora_turno = "08-00"
                elif '15' in turno_str:
                    turno = 2
                    hora_turno = "15-30"
                elif '23' in turno_str:
                    turno = 3
                    hora_turno = "23-00"
                else:
                    turno = 1
                    hora_turno = "08-00"
                
                volumen_bloque = SAIVolumenBloque(
                    config_id=config_id,
                    fecha=fecha,
                    turno=turno,
                    hora_turno=hora_turno,
                    # Bloques C
                    c1=int(row.get('C1', 0)),
                    c2=int(row.get('C2', 0)),
                    c3=int(row.get('C3', 0)),
                    c4=int(row.get('C4', 0)),
                    c5=int(row.get('C5', 0)),
                    c6=int(row.get('C6', 0)),
                    c7=int(row.get('C7', 0)),
                    c8=int(row.get('C8', 0)),
                    c9=int(row.get('C9', 0)),
                    # Bloques H
                    h1=int(row.get('H1', 0)),
                    h2=int(row.get('H2', 0)),
                    h3=int(row.get('H3', 0)),
                    h4=int(row.get('H4', 0)),
                    h5=int(row.get('H5', 0)),
                    # Bloques T
                    t1=int(row.get('T1', 0)),
                    t2=int(row.get('T2', 0)),
                    t3=int(row.get('T3', 0)),
                    t4=int(row.get('T4', 0))
                )
                self.db.add(volumen_bloque)
                stats['volumen_bloques'] += 1
        
        # 2. Cargar volumen por segregación - CORREGIDO
        if 'Bloques_Seg_Volumen' in excel_data.sheet_names:
            df_seg_volumen = pd.read_excel(file_path, sheet_name='Bloques_Seg_Volumen')
            
            # Procesar cada fila directamente
            for _, row in df_seg_volumen.iterrows():
                bloque_str = str(row['Bloque'])
                
                # Solo procesar bloques válidos (C, H, T)
                if not (bloque_str.startswith('C') or bloque_str.startswith('H') or bloque_str.startswith('T')):
                    continue
                
                # IMPORTANTE: Las columnas numéricas en el Excel se llaman 1, 2, 3
                # pandas las lee como enteros, no strings
                volumen_seg = SAIVolumenSegregacion(
                    config_id=config_id,
                    bloque=bloque_str,
                    segregacion_id=str(row['S']),
                    segregacion_nombre=str(row['Segregacion']),
                    turno_1=int(row[1]) if 1 in row.index else 0,  # Columna numérica 1
                    turno_2=int(row[2]) if 2 in row.index else 0,  # Columna numérica 2
                    turno_3=int(row[3]) if 3 in row.index else 0,  # Columna numérica 3
                    total=int(row['Total']) if 'Total' in row.index else 0
                )
                
                # Solo agregar si hay algún volumen
                if volumen_seg.total > 0:
                    self.db.add(volumen_seg)
                    stats['volumen_segregaciones'] += 1
                    
                    # Log para debugging
                    if volumen_seg.total > 100:  # Solo loguear volúmenes significativos
                        logger.debug(f"Agregando volumen: {bloque_str}-{row['S']}: "
                                   f"T1={volumen_seg.turno_1}, T2={volumen_seg.turno_2}, "
                                   f"T3={volumen_seg.turno_3}, Total={volumen_seg.total}")
        
        await self.db.commit()
        logger.info(f"Cargados: {stats}")
        
        # Verificación adicional
        total_check = await self.db.execute(
            select(func.sum(SAIVolumenSegregacion.total))
            .where(SAIVolumenSegregacion.config_id == config_id)
        )
        total_volumen = total_check.scalar() or 0
        logger.info(f"Volumen total cargado para config {config_id}: {total_volumen} TEUs")
        
        return stats
        
    except Exception as e:
        logger.error(f"Error cargando evolución: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        await self.db.rollback()
        raise
    
    async def calculate_bahias_distribution(
        self,
        config_id: UUID,
        bloque: str,
        turno: int
    ) -> Dict[str, Any]:
        """Calcula la distribución de bahías para un bloque y turno"""
        
        # Obtener capacidad del bloque
        cap_query = await self.db.execute(
            select(SAICapacidadBloque).where(SAICapacidadBloque.bloque == bloque)
        )
        capacidad = cap_query.scalar_one()
        
        # Obtener volumen por segregación
        vol_query = await self.db.execute(
            select(SAIVolumenSegregacion).where(
                SAIVolumenSegregacion.config_id == config_id,
                SAIVolumenSegregacion.bloque == bloque
            )
        )
        volumenes = vol_query.scalars().all()
        
        # Obtener información de segregaciones
        seg_ids = [v.segregacion_id for v in volumenes]
        seg_query = await self.db.execute(
            select(SAISegregacion).where(SAISegregacion.id.in_(seg_ids))
        )
        segregaciones = {s.id: s for s in seg_query.scalars().all()}
        
        # Calcular distribución de bahías
        distribucion = []
        total_bahias_usadas = 0
        
        for vol in volumenes:
            # Obtener volumen del turno específico
            if turno == 1:
                volumen_teus = vol.turno_1
            elif turno == 2:
                volumen_teus = vol.turno_2
            else:
                volumen_teus = vol.turno_3
            
            if volumen_teus > 0:
                seg = segregaciones.get(vol.segregacion_id)
                if seg:
                    # Calcular bahías necesarias
                    contenedores = volumen_teus / seg.teus
                    bahias_necesarias = int(np.ceil(contenedores / capacidad.contenedores_por_bahia))
                    
                    # Calcular ocupación real
                    capacidad_asignada = bahias_necesarias * capacidad.contenedores_por_bahia * seg.teus
                    ocupacion = (volumen_teus / capacidad_asignada * 100) if capacidad_asignada > 0 else 0
                    
                    distribucion.append({
                        'segregacion_id': seg.id,
                        'segregacion_nombre': seg.nombre,
                        'color': seg.color,
                        'teus': seg.teus,
                        'volumen_teus': volumen_teus,
                        'bahias': bahias_necesarias,
                        'ocupacion': ocupacion
                    })
                    
                    total_bahias_usadas += bahias_necesarias
        
        # Ordenar por número de bahías (mayor a menor)
        distribucion.sort(key=lambda x: x['bahias'], reverse=True)
        
        return {
            'distribucion': distribucion,
            'total_bahias_usadas': total_bahias_usadas,
            'bahias_disponibles': capacidad.bahias_totales - total_bahias_usadas,
            'capacidad_bloque': capacidad
        }