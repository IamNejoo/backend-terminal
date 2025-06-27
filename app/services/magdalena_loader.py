import pandas as pd
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from uuid import UUID
import logging

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete

from app.core.database import AsyncSessionLocal
from app.models.magdalena import (
    MagdalenaRun, MagdalenaGeneral, MagdalenaOcupacion,
    MagdalenaWorkload, MagdalenaBahias, MagdalenaVolumen,
    MagdalenaInstancia, MagdalenaRealData
)

logger = logging.getLogger(__name__)

class MagdalenaLoader:
    def __init__(self, db: AsyncSession):
        self.db = db
        
    async def load_resultado_file(
        self,
        file_path: str,
        semana: int,
        participacion: int,
        con_dispersion: bool
    ) -> UUID:
        """Carga un archivo de resultados de Magdalena"""
        
        # Verificar si ya existe
        existing = await self.db.execute(
            select(MagdalenaRun).where(
                MagdalenaRun.semana == semana,
                MagdalenaRun.participacion == participacion,
                MagdalenaRun.con_dispersion == con_dispersion
            )
        )
        if existing.scalar():
            logger.info(f"Run ya existe para S{semana}_P{participacion}_{'K' if con_dispersion else 'C'}")
            return existing.scalar().id
            
        # Crear nueva corrida
        run = MagdalenaRun(
            semana=semana,
            participacion=participacion,
            con_dispersion=con_dispersion
        )
        self.db.add(run)
        await self.db.flush()
        
        # Leer archivo Excel
        excel_data = pd.ExcelFile(file_path)
        
        # Procesar hoja General
        if 'General' in excel_data.sheet_names:
            df_general = pd.read_excel(file_path, sheet_name='General')
            bloques_set = set()
            periodos_set = set()
            
            for _, row in df_general.iterrows():
                if pd.notna(row.get('Bloque')):
                    general = MagdalenaGeneral(
                        run_id=run.id,
                        bloque=str(row['Bloque']),
                        periodo=int(row.get('Periodo', 0)),
                        segregacion=str(row.get('Segregación', row.get('Segregacion', ''))),
                        recepcion=int(row.get('Recepción', row.get('Recepcion', 0))),
                        carga=int(row.get('Carga', 0)),
                        descarga=int(row.get('Descarga', 0)),
                        entrega=int(row.get('Entrega', 0))
                    )
                    self.db.add(general)
                    bloques_set.add(str(row['Bloque']))
                    periodos_set.add(int(row.get('Periodo', 0)))
            
            run.total_bloques = len(bloques_set)
            run.periodos = max(periodos_set) if periodos_set else 0
        
        # Procesar Ocupación Bloques
        if 'Ocupación Bloques' in excel_data.sheet_names:
            df_ocupacion = pd.read_excel(file_path, sheet_name='Ocupación Bloques')
            for _, row in df_ocupacion.iterrows():
                if pd.notna(row.get('Bloque')):
                    ocupacion = MagdalenaOcupacion(
                        run_id=run.id,
                        bloque=str(row['Bloque']),
                        periodo=int(row.get('Periodo', 0)),
                        volumen_teus=float(row.get('Volumen bloques (TEUs)', 0)),
                        capacidad_bloque=float(row.get('Capacidad Bloque', 1155))
                    )
                    self.db.add(ocupacion)
        
        # Procesar Workload bloques
        if 'Workload bloques' in excel_data.sheet_names:
            df_workload = pd.read_excel(file_path, sheet_name='Workload bloques')
            for _, row in df_workload.iterrows():
                if pd.notna(row.get('Bloque')):
                    workload = MagdalenaWorkload(
                        run_id=run.id,
                        bloque=str(row['Bloque']),
                        periodo=int(row.get('Periodo', 0)),
                        carga_trabajo=float(row.get('Carga de trabajo', row.get('Workload', 0)))
                    )
                    self.db.add(workload)
        
        # Procesar Bahías por bloques
        if 'Bahías por bloques' in excel_data.sheet_names:
            df_bahias = pd.read_excel(file_path, sheet_name='Bahías por bloques')
            segregaciones_set = set()
            for _, row in df_bahias.iterrows():
                if pd.notna(row.get('Bloque')) and pd.notna(row.get('Segregación', row.get('Segregacion'))):
                    bahias = MagdalenaBahias(
                        run_id=run.id,
                        bloque=str(row['Bloque']),
                        periodo=int(row.get('Periodo', 0)),
                        segregacion=str(row.get('Segregación', row.get('Segregacion', ''))),
                        bahias_ocupadas=int(row.get('Bahías ocupadas', 0))
                    )
                    self.db.add(bahias)
                    segregaciones_set.add(str(row.get('Segregación', row.get('Segregacion', ''))))
            
            run.total_segregaciones = len(segregaciones_set)
        
        # Procesar Volumen bloques (TEUs)
        if 'Volumen bloques (TEUs)' in excel_data.sheet_names:
            df_volumen = pd.read_excel(file_path, sheet_name='Volumen bloques (TEUs)')
            for _, row in df_volumen.iterrows():
                if pd.notna(row.get('Bloque')) and pd.notna(row.get('Segregación', row.get('Segregacion'))):
                    volumen = MagdalenaVolumen(
                        run_id=run.id,
                        bloque=str(row['Bloque']),
                        periodo=int(row.get('Periodo', 0)),
                        segregacion=str(row.get('Segregación', row.get('Segregacion', ''))),
                        volumen=float(row.get('Volumen', 0))
                    )
                    self.db.add(volumen)
        
        await self.db.commit()
        logger.info(f"Cargado resultado Magdalena S{semana}_P{participacion}_{'K' if con_dispersion else 'C'}")
        return run.id
    
    async def load_instancia_file(
        self,
        file_path: str,
        semana: int,
        participacion: int,
        con_dispersion: bool
    ):
        """Carga archivo de instancia de Magdalena"""
        
        # Verificar si ya existe
        existing = await self.db.execute(
            select(MagdalenaInstancia).where(
                MagdalenaInstancia.semana == semana,
                MagdalenaInstancia.participacion == participacion,
                MagdalenaInstancia.con_dispersion == con_dispersion
            )
        )
        if existing.scalar():
            logger.info(f"Instancia ya existe")
            return
            
        excel_data = pd.ExcelFile(file_path)
        
        capacidades_bloques = {}
        teus_segregaciones = {}
        info_segregaciones = {}
        
        # Capacidades por bloque
        if 'VS_b' in excel_data.sheet_names:
            df_vs = pd.read_excel(file_path, sheet_name='VS_b')
            for _, row in df_vs.iterrows():
                if pd.notna(row.get('B')):
                    capacidades_bloques[str(row['B'])] = float(row.get('VS', 35))
        
        # TEUs por segregación
        if 'TEU_s' in excel_data.sheet_names:
            df_teu = pd.read_excel(file_path, sheet_name='TEU_s')
            for _, row in df_teu.iterrows():
                if pd.notna(row.get('S')):
                    teus_segregaciones[str(row['S'])] = float(row.get('TEU', 1))
        
        # Info segregaciones
        if 'S' in excel_data.sheet_names:
            df_s = pd.read_excel(file_path, sheet_name='S')
            for _, row in df_s.iterrows():
                if pd.notna(row.get('S')):
                    seg_id = str(row['S'])
                    info_segregaciones[seg_id] = {
                        'id': seg_id,
                        'nombre': str(row.get('Segregacion', seg_id)),
                        'teu': teus_segregaciones.get(seg_id, 1)
                    }
        
        instancia = MagdalenaInstancia(
            semana=semana,
            participacion=participacion,
            con_dispersion=con_dispersion,
            capacidades_bloques=capacidades_bloques,
            teus_segregaciones=teus_segregaciones,
            info_segregaciones=info_segregaciones
        )
        self.db.add(instancia)
        await self.db.commit()
        logger.info(f"Cargada instancia Magdalena")
    
    async def load_real_data_file(self, file_path: str, semana: int):
        """Carga archivo de datos reales"""
        
        # Verificar si ya existe
        existing = await self.db.execute(
            select(MagdalenaRealData).where(
                MagdalenaRealData.semana == semana
            )
        )
        if existing.scalar():
            logger.info(f"Datos reales ya existen para semana {semana}")
            return
            
        excel_data = pd.ExcelFile(file_path)
        
        if 'FlujosAll_sbt' in excel_data.sheet_names:
            df_flujos = pd.read_excel(file_path, sheet_name='FlujosAll_sbt')
            
            total_movimientos = 0
            reubicaciones = 0
            movimientos = {'DLVR': 0, 'DSCH': 0, 'LOAD': 0, 'RECV': 0, 'OTHR': 0}
            bloques_set = set()
            turnos_set = set()
            carriers_set = set()
            
            for _, row in df_flujos.iterrows():
                for tipo in movimientos.keys():
                    valor = int(row.get(tipo, 0))
                    movimientos[tipo] += valor
                    total_movimientos += valor
                
                if pd.notna(row.get('YARD')):
                    reubicaciones += int(row['YARD'])
                
                if pd.notna(row.get('ime_to')) and str(row['ime_to']).startswith('C'):
                    bloques_set.add(str(row['ime_to']))
                
                if pd.notna(row.get('shift')):
                    turnos_set.add(int(row['shift']))
                
                if pd.notna(row.get('carrier')):
                    carriers_set.add(str(row['carrier']))
            
            real_data = MagdalenaRealData(
                semana=semana,
                total_movimientos=total_movimientos,
                reubicaciones=reubicaciones,
                movimientos_dlvr=movimientos['DLVR'],
                movimientos_dsch=movimientos['DSCH'],
                movimientos_load=movimientos['LOAD'],
                movimientos_recv=movimientos['RECV'],
                movimientos_othr=movimientos['OTHR'],
                bloques_unicos=list(bloques_set),
                turnos=list(turnos_set),
                carriers=len(carriers_set)
            )
            self.db.add(real_data)
            await self.db.commit()
            logger.info(f"Cargados datos reales semana {semana}")