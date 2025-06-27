# app/services/camila_loader.py
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, func
import logging
from uuid import UUID
import asyncio
import re

from app.models.camila import (
    CamilaRun, CamilaFlujos, CamilaGruas, CamilaAsignacion,
    CamilaResultados, CamilaRealData, CamilaCuotas
)
from app.core.constants import (
    BLOCKS_INTERNAL, BLOCKS_DISPLAY, GRUAS, FLOW_TYPES,
    get_block_index, get_grua_index, GRUA_PRODUCTIVITY,
    TIME_PERIODS
)

logger = logging.getLogger(__name__)

class CamilaLoader:
    """Clase para cargar archivos de resultados de Camila a la base de datos"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        
    async def load_camila_file(
        self,
        filepath: str,
        semana: int,
        dia: str,
        turno: int,
        modelo_tipo: str,
        con_segregaciones: bool
    ) -> UUID:
        """Carga un archivo Excel de resultados de Camila con validaciones completas"""
        
        logger.info(f"{'='*80}")
        logger.info(f"Iniciando carga de archivo: {filepath}")
        logger.info(f"Config: S{semana} {dia} T{turno} {modelo_tipo} {'CON' if con_segregaciones else 'SIN'} segregaciones")
        
        try:
            # Validar parámetros de entrada
            self._validate_input_params(semana, dia, turno, modelo_tipo)
            
            # Leer archivo Excel
            xl = pd.ExcelFile(filepath)
            logger.info(f"Archivo Excel abierto. Hojas encontradas: {xl.sheet_names}")
            
            # Verificar estructura del archivo
            self._validate_file_structure(xl)
            
            # Crear o actualizar run
            run = await self._create_or_update_run(
                semana, dia, turno, modelo_tipo, con_segregaciones
            )
            
            # Cargar datos por hoja con validaciones
            results_info = await self._load_resultados(xl, run.id)
            flujos_info = await self._load_flujos(xl, run.id)
            gruas_info = await self._load_gruas(xl, run.id)
            
            # Cargar hojas opcionales si existen
            asignacion_info = None
            real_data_info = None
            cuotas_info = None
            
            if 'Asignación' in xl.sheet_names:
                asignacion_info = await self._load_asignacion(xl, run.id)
            
            if 'Real' in xl.sheet_names:
                real_data_info = await self._load_real_data(xl, run.id)
            
            if 'Cálculo Cuotas' in xl.sheet_names:
                cuotas_info = await self._load_cuotas(xl, run.id)
            
            # Validar consistencia de datos
            self._validate_data_consistency(flujos_info, gruas_info)
            
            # Calcular y guardar resultados consolidados
            results_summary = await self._calculate_and_save_results(run.id)
            
            # Actualizar métricas del run
            await self._update_run_metrics(run.id, results_summary)
            
            # Commit final
            await self.db.commit()
            
            # Log resumen
            self._log_load_summary(
                run.id, results_info, flujos_info, gruas_info,
                asignacion_info, real_data_info, cuotas_info, results_summary
            )
            
            logger.info(f"✅ Archivo cargado exitosamente. Run ID: {run.id}")
            return run.id
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"❌ Error cargando archivo: {str(e)}")
            raise
    
    def _validate_input_params(self, semana: int, dia: str, turno: int, modelo_tipo: str):
        """Valida parámetros de entrada"""
        
        if not 1 <= semana <= 52:
            raise ValueError(f"Semana {semana} fuera de rango (1-52)")
        
        valid_days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if dia not in valid_days:
            raise ValueError(f"Día '{dia}' no válido. Debe ser uno de: {valid_days}")
        
        if turno not in [1, 2, 3]:
            raise ValueError(f"Turno {turno} no válido. Debe ser 1, 2 o 3")
        
        if modelo_tipo not in ['minmax', 'maxmin']:
            raise ValueError(f"Tipo de modelo '{modelo_tipo}' no válido. Debe ser 'minmax' o 'maxmin'")
    
    def _validate_file_structure(self, xl: pd.ExcelFile):
        """Valida estructura del archivo Excel"""
        
        required_sheets = ['Resultados', 'Flujos', 'Grúas']
        missing_sheets = [sheet for sheet in required_sheets if sheet not in xl.sheet_names]
        
        if missing_sheets:
            raise ValueError(f"Hojas requeridas no encontradas: {missing_sheets}")
        
        # Validar estructura de cada hoja requerida
        for sheet in required_sheets:
            df = pd.read_excel(xl, sheet, nrows=5)
            if df.empty:
                raise ValueError(f"Hoja '{sheet}' está vacía")
            
            # Validar columnas según el tipo de hoja
            if sheet == 'Resultados':
                required_cols = {'Variable', 'Bloques', 'Tiempo', 'Valor'}
            elif sheet == 'Flujos':
                required_cols = {'Variable', 'Segregación', 'Bloques', 'Tiempo', 'Valor'}
            else:  # Grúas
                required_cols = {'Variable', 'Grúas', 'Bloques', 'Tiempo', 'Valor'}
            
            missing_cols = required_cols - set(df.columns)
            if missing_cols:
                raise ValueError(f"Columnas faltantes en hoja '{sheet}': {missing_cols}")
    
    async def _create_or_update_run(
        self,
        semana: int,
        dia: str,
        turno: int,
        modelo_tipo: str,
        con_segregaciones: bool
    ) -> CamilaRun:
        """Crea o actualiza un run con validaciones"""
        
        # Buscar run existente
        query = select(CamilaRun).where(
            and_(
                CamilaRun.semana == semana,
                CamilaRun.dia == dia,
                CamilaRun.turno == turno,
                CamilaRun.modelo_tipo == modelo_tipo,
                CamilaRun.con_segregaciones == con_segregaciones
            )
        )
        
        result = await self.db.execute(query)
        run = result.scalar_one_or_none()
        
        if run:
            logger.info(f"Actualizando run existente: {run.id}")
            # Eliminar datos anteriores
            await self._delete_run_data(run.id)
            run.fecha_carga = datetime.utcnow()
        else:
            logger.info("Creando nuevo run")
            run = CamilaRun(
                semana=semana,
                dia=dia,
                turno=turno,
                modelo_tipo=modelo_tipo,
                con_segregaciones=con_segregaciones,
                fecha_carga=datetime.utcnow()
            )
            self.db.add(run)
            await self.db.flush()
        
        return run
    
    async def _delete_run_data(self, run_id: UUID):
        """Elimina datos anteriores de un run de forma segura"""
        
        logger.info(f"Eliminando datos anteriores del run {run_id}")
        
        # Contar registros a eliminar
        counts = {}
        for model, name in [
            (CamilaFlujos, 'flujos'),
            (CamilaGruas, 'gruas'),
            (CamilaAsignacion, 'asignacion'),
            (CamilaResultados, 'resultados'),
            (CamilaRealData, 'real_data'),
            (CamilaCuotas, 'cuotas')
        ]:
            count_result = await self.db.execute(
                select(func.count()).select_from(model).where(model.run_id == run_id)
            )
            counts[name] = count_result.scalar()
        
        logger.info(f"Registros a eliminar: {counts}")
        
        # Eliminar en orden correcto
        await self.db.execute(delete(CamilaCuotas).where(CamilaCuotas.run_id == run_id))
        await self.db.execute(delete(CamilaRealData).where(CamilaRealData.run_id == run_id))
        await self.db.execute(delete(CamilaResultados).where(CamilaResultados.run_id == run_id))
        await self.db.execute(delete(CamilaAsignacion).where(CamilaAsignacion.run_id == run_id))
        await self.db.execute(delete(CamilaGruas).where(CamilaGruas.run_id == run_id))
        await self.db.execute(delete(CamilaFlujos).where(CamilaFlujos.run_id == run_id))
        
        await self.db.flush()
    
    async def _load_resultados(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga hoja de Resultados con validaciones y métricas"""
        
        df = pd.read_excel(xl, 'Resultados')
        logger.info(f"Cargando {len(df)} filas de Resultados")
        
        # Validar datos
        self._validate_dataframe(df, 'Resultados')
        
        # Procesar en lotes
        batch_size = 1000
        flujos_batch = []
        gruas_batch = []
        
        flujos_count = 0
        gruas_count = 0
        invalid_count = 0
        
        for idx, row in df.iterrows():
            try:
                variable = str(row['Variable']).strip()
                
                if variable.startswith('y'):  # Es una grúa
                    grua = self._create_grua_from_row(row, run_id)
                    if grua:
                        gruas_batch.append(grua)
                        gruas_count += 1
                else:  # Es un flujo
                    flujo = self._create_flujo_from_row(row, run_id, True)
                    if flujo:
                        flujos_batch.append(flujo)
                        flujos_count += 1
                
                # Guardar en lotes
                if len(flujos_batch) >= batch_size:
                    self.db.add_all(flujos_batch)
                    await self.db.flush()
                    flujos_batch = []
                
                if len(gruas_batch) >= batch_size:
                    self.db.add_all(gruas_batch)
                    await self.db.flush()
                    gruas_batch = []
                    
            except Exception as e:
                invalid_count += 1
                if invalid_count < 10:  # Log solo los primeros errores
                    logger.warning(f"Error en fila {idx}: {str(e)}")
        
        # Guardar últimos registros
        if flujos_batch:
            self.db.add_all(flujos_batch)
        if gruas_batch:
            self.db.add_all(gruas_batch)
        
        await self.db.flush()
        
        return {
            'flujos_loaded': flujos_count,
            'gruas_loaded': gruas_count,
            'invalid_rows': invalid_count
        }
    
    async def _load_flujos(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga hoja de Flujos con análisis detallado"""
        
        df = pd.read_excel(xl, 'Flujos')
        logger.info(f"Cargando {len(df)} flujos")
        
        # Eliminar flujos existentes para evitar duplicados
        await self.db.execute(delete(CamilaFlujos).where(CamilaFlujos.run_id == run_id))
        
        # Analizar segregaciones
        segregaciones_found = df['Segregación'].unique()
        logger.info(f"Segregaciones encontradas: {len(segregaciones_found)}")
        
        # Analizar tipos de flujo
        flow_types_count = df['Variable'].value_counts().to_dict()
        
        batch = []
        loaded_count = 0
        invalid_count = 0
        
        for idx, row in df.iterrows():
            try:
                flujo = self._create_flujo_from_row(row, run_id, False)
                if flujo:
                    batch.append(flujo)
                    loaded_count += 1
                    
                if len(batch) >= 1000:
                    self.db.add_all(batch)
                    await self.db.flush()
                    batch = []
                    
            except Exception as e:
                invalid_count += 1
                if invalid_count < 10:
                    logger.warning(f"Error en flujo fila {idx}: {str(e)}")
        
        if batch:
            self.db.add_all(batch)
            await self.db.flush()
        
        return {
            'total_loaded': loaded_count,
            'invalid_rows': invalid_count,
            'segregaciones': len(segregaciones_found),
            'flow_types': flow_types_count
        }
    
    async def _load_gruas(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga hoja de Grúas con análisis de utilización"""
        
        df = pd.read_excel(xl, 'Grúas')
        logger.info(f"Cargando {len(df)} asignaciones de grúas")
        
        # Eliminar grúas existentes para evitar duplicados
        await self.db.execute(delete(CamilaGruas).where(CamilaGruas.run_id == run_id))
        
        # Analizar utilización
        gruas_activas = df[df['Valor'] == 1]['Grúas'].unique()
        utilizacion_por_grua = df[df['Valor'] == 1].groupby('Grúas').size().to_dict()
        
        batch = []
        loaded_count = 0
        
        for _, row in df.iterrows():
            try:
                grua = self._create_grua_from_row(row, run_id)
                if grua:
                    batch.append(grua)
                    loaded_count += 1
                    
                if len(batch) >= 1000:
                    self.db.add_all(batch)
                    await self.db.flush()
                    batch = []
                    
            except Exception as e:
                logger.warning(f"Error en grúa: {str(e)}")
        
        if batch:
            self.db.add_all(batch)
            await self.db.flush()
        
        return {
            'total_loaded': loaded_count,
            'gruas_activas': len(gruas_activas),
            'utilizacion_promedio': np.mean(list(utilizacion_por_grua.values())) if utilizacion_por_grua else 0,
            'gruas_stats': utilizacion_por_grua
        }
    
    async def _load_asignacion(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga hoja de Asignación visual con validaciones"""
        
        df = pd.read_excel(xl, 'Asignación')
        logger.info("Cargando asignación visual de grúas")
        
        # Detectar estructura de la hoja
        if 'Tiempo/Grúa' in df.iloc[0].values or 'Tiempo/Grúa' in df.columns:
            start_row = 1
        else:
            start_row = 2
        
        # Obtener nombres de grúas
        gruas_header = df.iloc[start_row - 1]
        gruas_nombres = []
        for col in gruas_header[1:]:
            if pd.notna(col) and str(col).startswith('g'):
                gruas_nombres.append(str(col))
        
        logger.info(f"Grúas encontradas en asignación: {gruas_nombres}")
        
        assignments_count = 0
        
        for idx in range(start_row, min(start_row + 8, len(df))):  # Solo 8 períodos
            row = df.iloc[idx]
            tiempo = int(row.iloc[0])
            
            for g_idx, grua in enumerate(gruas_nombres):
                if g_idx + 1 < len(row):
                    bloque_asignado = row.iloc[g_idx + 1]
                    
                    if pd.notna(bloque_asignado) and str(bloque_asignado).strip() not in ['', ' ', '0']:
                        asignacion = CamilaAsignacion(
                            run_id=run_id,
                            tiempo=tiempo,
                            grua=grua,
                            bloque_asignado=str(bloque_asignado).strip(),
                            movimientos_realizados=0
                        )
                        self.db.add(asignacion)
                        assignments_count += 1
        
        await self.db.flush()
        
        return {
            'assignments_loaded': assignments_count,
            'gruas_count': len(gruas_nombres)
        }
    
    async def _load_real_data(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga datos reales con mapeo de bloques"""
        
        df = pd.read_excel(xl, 'Real')
        logger.info("Cargando datos reales")
        
        # Detectar formato de bloques
        header_row = None
        for idx in range(min(5, len(df))):
            if 'Tiempo/Bloque' in str(df.iloc[idx, 0]):
                header_row = idx + 1
                break
        
        if header_row is None:
            header_row = 1
        
        # Obtener nombres de bloques
        bloques = []
        bloque_row = df.iloc[header_row]
        for col_val in bloque_row[1:10]:  # Máximo 9 bloques
            if pd.notna(col_val):
                bloque_str = str(col_val).strip()
                if bloque_str and bloque_str not in ['Σ', 'Total']:
                    bloques.append(bloque_str)
        
        logger.info(f"Bloques encontrados en datos reales: {bloques}")
        
        total_movements = 0
        data_points = 0
        
        for idx in range(header_row + 1, min(header_row + 9, len(df))):  # 8 períodos
            if idx >= len(df):
                break
                
            row = df.iloc[idx]
            
            try:
                tiempo = int(row.iloc[0])
                
                for b_idx, bloque in enumerate(bloques):
                    if b_idx + 1 < len(row):
                        movimientos = row.iloc[b_idx + 1]
                        
                        if pd.notna(movimientos) and isinstance(movimientos, (int, float)):
                            real_data = CamilaRealData(
                                run_id=run_id,
                                bloque=bloque,
                                tiempo=tiempo,
                                movimientos=int(movimientos)
                            )
                            self.db.add(real_data)
                            total_movements += int(movimientos)
                            data_points += 1
                            
            except Exception as e:
                logger.warning(f"Error procesando fila {idx} de datos reales: {str(e)}")
        
        await self.db.flush()
        
        return {
            'data_points': data_points,
            'total_movements': total_movements,
            'blocks_count': len(bloques)
        }
    
    async def _load_cuotas(self, xl: pd.ExcelFile, run_id: UUID) -> Dict[str, Any]:
        """Carga cálculo de cuotas con búsqueda inteligente"""
        
        df = pd.read_excel(xl, 'Cálculo Cuotas')
        logger.info("Cargando cálculo de cuotas")
        
        # Buscar sección de disponibilidad
        disponibilidad_idx = None
        for idx in range(len(df)):
            cell_val = str(df.iloc[idx, 0]).lower()
            if 'disponibilidad' in cell_val and 'movimientos' in cell_val:
                disponibilidad_idx = idx
                break
        
        if disponibilidad_idx is None:
            logger.warning("No se encontró sección de disponibilidad en hoja de cuotas")
            return {'cuotas_loaded': 0}
        
        # Cargar datos de disponibilidad
        start_idx = disponibilidad_idx + 2
        cuotas_loaded = 0
        
        for i in range(8):  # 8 períodos
            if start_idx + i >= len(df):
                break
                
            row = df.iloc[start_idx + i]
            
            try:
                tiempo = int(row.iloc[0])
                
                for b_idx in range(9):  # 9 bloques
                    if b_idx + 1 < len(row):
                        disponibilidad = row.iloc[b_idx + 1]
                        
                        if pd.notna(disponibilidad) and isinstance(disponibilidad, (int, float)):
                            cuota = CamilaCuotas(
                                run_id=run_id,
                                bloque=f'b{b_idx + 1}',
                                tiempo=tiempo,
                                disponibilidad=int(disponibilidad),
                                cuota_recomendada=0
                            )
                            self.db.add(cuota)
                            cuotas_loaded += 1
                            
            except Exception as e:
                logger.warning(f"Error procesando cuotas en fila {start_idx + i}: {str(e)}")
        
        await self.db.flush()
        
        return {'cuotas_loaded': cuotas_loaded}
    
    def _create_flujo_from_row(self, row: pd.Series, run_id: UUID, from_resultados: bool) -> Optional[CamilaFlujos]:
        """Crea un objeto CamilaFlujos desde una fila con validaciones"""
        
        try:
            variable = str(row['Variable']).strip()
            bloque = str(row['Bloques']).strip()
            tiempo = int(row['Tiempo'])
            valor = float(row['Valor'])
            
            # Validar variable
            if variable not in FLOW_TYPES:
                return None
            
            # Validar bloque
            try:
                block_idx = get_block_index(bloque)
                if not (0 <= block_idx < 9):
                    return None
            except:
                return None
            
            # Validar tiempo
            if not (1 <= tiempo <= TIME_PERIODS):
                return None
            
            # Obtener segregación
            if from_resultados:
                segregacion = row.get('Segregación', 's1')
            else:
                segregacion = row['Segregación']
            
            return CamilaFlujos(
                run_id=run_id,
                variable=variable,
                segregacion=str(segregacion).strip(),
                bloque=bloque,
                tiempo=tiempo,
                valor=valor
            )
            
        except Exception as e:
            logger.debug(f"Error creando flujo: {str(e)}")
            return None
    
    def _create_grua_from_row(self, row: pd.Series, run_id: UUID) -> Optional[CamilaGruas]:
        """Crea un objeto CamilaGruas desde una fila con validaciones"""
        
        try:
            grua = str(row['Grúas']).strip()
            bloque = str(row['Bloques']).strip()
            tiempo = int(row['Tiempo'])
            valor = int(row['Valor'])
            
            # Validar grúa
            try:
                grua_idx = get_grua_index(grua)
                if not (0 <= grua_idx < 12):
                    return None
            except:
                return None
            
            # Validar bloque
            try:
                block_idx = get_block_index(bloque)
                if not (0 <= block_idx < 9):
                    return None
            except:
                return None
            
            # Validar tiempo
            if not (1 <= tiempo <= TIME_PERIODS):
                return None
            
            # Validar valor binario
            if valor not in [0, 1]:
                return None
            
            return CamilaGruas(
                run_id=run_id,
                grua=grua,
                bloque=bloque,
                tiempo=tiempo,
                valor=valor
            )
            
        except Exception as e:
            logger.debug(f"Error creando grúa: {str(e)}")
            return None
    
    def _validate_dataframe(self, df: pd.DataFrame, sheet_name: str):
        """Valida la integridad de un DataFrame"""
        
        if df.empty:
            raise ValueError(f"Hoja '{sheet_name}' está vacía")
        
        # Verificar columnas requeridas
        required_cols = {'Variable', 'Bloques', 'Tiempo', 'Valor'}
        missing_cols = required_cols - set(df.columns)
        
        if missing_cols:
            raise ValueError(f"Columnas faltantes en '{sheet_name}': {missing_cols}")
        
        # Verificar tipos de datos
        if not pd.api.types.is_numeric_dtype(df['Tiempo']):
            logger.warning(f"Columna 'Tiempo' en '{sheet_name}' no es numérica")
        
        if not pd.api.types.is_numeric_dtype(df['Valor']):
            logger.warning(f"Columna 'Valor' en '{sheet_name}' no es numérica")
    
    def _validate_data_consistency(self, flujos_info: Dict, gruas_info: Dict):
        """Valida consistencia entre flujos y grúas"""
        
        # Verificar que hay datos cargados
        if flujos_info['total_loaded'] == 0:
            raise ValueError("No se cargaron flujos válidos")
        
        if gruas_info['total_loaded'] == 0:
            raise ValueError("No se cargaron asignaciones de grúas válidas")
        
        # Verificar proporciones razonables
        if gruas_info['gruas_activas'] == 0:
            logger.warning("⚠️ No hay grúas activas en el modelo")
        
        if gruas_info['utilizacion_promedio'] < 20:
            logger.warning(f"⚠️ Utilización promedio muy baja: {gruas_info['utilizacion_promedio']:.1f}%")
    
    async def _calculate_and_save_results(self, run_id: UUID) -> Dict[str, Any]:
        """Calcula y guarda resultados consolidados con métricas detalladas"""
        
        logger.info("Calculando resultados consolidados")
        
        # Obtener todos los flujos
        flujos_result = await self.db.execute(
            select(CamilaFlujos).where(CamilaFlujos.run_id == run_id)
        )
        flujos = flujos_result.scalars().all()
        
        # Calcular matrices por tipo de flujo
        matrices = {
            'total': np.zeros((9, 8)),
            'reception': np.zeros((9, 8)),
            'delivery': np.zeros((9, 8)),
            'loading': np.zeros((9, 8)),
            'unloading': np.zeros((9, 8))
        }
        
        segregaciones_stats = {}
        
        for flujo in flujos:
            try:
                b_idx = get_block_index(flujo.bloque)
                t_idx = flujo.tiempo - 1
                
                if 0 <= b_idx < 9 and 0 <= t_idx < 8:
                    # Matriz total
                    matrices['total'][b_idx][t_idx] += flujo.valor
                    
                    # Matrices por tipo
                    flow_type = FLOW_TYPES.get(flujo.variable)
                    if flow_type in matrices:
                        matrices[flow_type][b_idx][t_idx] = flujo.valor
                    
                    # Estadísticas por segregación
                    if flujo.segregacion not in segregaciones_stats:
                        segregaciones_stats[flujo.segregacion] = 0
                    segregaciones_stats[flujo.segregacion] += flujo.valor
                    
            except Exception as e:
                logger.debug(f"Error procesando flujo para resultados: {str(e)}")
                continue
        
        # Calcular capacidad basada en grúas
        gruas_result = await self.db.execute(
            select(CamilaGruas).where(
                and_(
                    CamilaGruas.run_id == run_id,
                    CamilaGruas.valor == 1
                )
            )
        )
        gruas = gruas_result.scalars().all()
        
        capacidad = np.zeros((9, 8))
        gruas_por_bloque_tiempo = {}
        
        for grua in gruas:
            try:
                b_idx = get_block_index(grua.bloque)
                t_idx = grua.tiempo - 1
                
                if 0 <= b_idx < 9 and 0 <= t_idx < 8:
                    capacidad[b_idx][t_idx] += GRUA_PRODUCTIVITY
                    
                    key = f"{grua.bloque}-{grua.tiempo}"
                    if key not in gruas_por_bloque_tiempo:
                        gruas_por_bloque_tiempo[key] = []
                    gruas_por_bloque_tiempo[key].append(grua.grua)
                    
            except Exception as e:
                logger.debug(f"Error procesando grúa para capacidad: {str(e)}")
                continue
        
        # Calcular disponibilidad
        disponibilidad = np.maximum(0, capacidad - matrices['total'])
        
        # Calcular cuotas recomendadas
        cuotas_recomendadas = np.round(
            matrices['reception'] + (disponibilidad * 0.8)
        )
        
        # Calcular KPIs
        block_totals = np.sum(matrices['total'], axis=1)
        time_totals = np.sum(matrices['total'], axis=0)
        total_movements = np.sum(block_totals)
        
        # Participación por bloque y tiempo
        participacion_bloques = []
        participacion_tiempo = []
        
        if total_movements > 0:
            participacion_bloques = (block_totals / total_movements * 100).tolist()
            participacion_tiempo = (time_totals / total_movements * 100).tolist()
        else:
            participacion_bloques = [0.0] * 9
            participacion_tiempo = [0.0] * 8
        
        # Estadísticas
        desviacion_std_bloques = float(np.std(block_totals))
        desviacion_std_tiempo = float(np.std(time_totals))
        
        # Balance de carga
        avg_block = np.mean(block_totals)
        balance_workload = 100.0
        if avg_block > 0:
            cv = (desviacion_std_bloques / avg_block) * 100
            balance_workload = max(0, 100 - cv)
        
        # Índice de congestión
        max_flow = float(np.max(block_totals)) if len(block_totals) > 0 else 0
        indice_congestion = 1.0
        if avg_block > 0:
            indice_congestion = max_flow / avg_block
        
        # Actualizar run con métricas
        run_result = await self.db.execute(
            select(CamilaRun).where(CamilaRun.id == run_id)
        )
        run = run_result.scalar_one()
        
        run.total_movimientos = int(total_movements)
        run.balance_workload = float(balance_workload)
        run.indice_congestion = float(indice_congestion)
        
        # Crear o actualizar registro de resultados
        results_query = await self.db.execute(
            select(CamilaResultados).where(CamilaResultados.run_id == run_id)
        )
        resultados = results_query.scalar_one_or_none()
        
        if resultados:
            # Actualizar existente
            resultados.total_flujos = matrices['total'].tolist()
            resultados.capacidad = capacidad.tolist()
            resultados.disponibilidad = disponibilidad.tolist()
            resultados.participacion_bloques = participacion_bloques
            resultados.participacion_tiempo = participacion_tiempo
            resultados.desviacion_std_bloques = desviacion_std_bloques
            resultados.desviacion_std_tiempo = desviacion_std_tiempo
            resultados.cuotas_recomendadas = cuotas_recomendadas.tolist()
        else:
            # Crear nuevo
            resultados = CamilaResultados(
                run_id=run_id,
                total_flujos=matrices['total'].tolist(),
                capacidad=capacidad.tolist(),
                disponibilidad=disponibilidad.tolist(),
                participacion_bloques=participacion_bloques,
                participacion_tiempo=participacion_tiempo,
                desviacion_std_bloques=desviacion_std_bloques,
                desviacion_std_tiempo=desviacion_std_tiempo,
                cuotas_recomendadas=cuotas_recomendadas.tolist()
            )
            self.db.add(resultados)
        
        await self.db.flush()
        
        return {
            'total_movements': int(total_movements),
            'balance_workload': float(balance_workload),
            'congestion_index': float(indice_congestion),
            'avg_utilization': float(np.mean(capacidad[capacidad > 0] / GRUA_PRODUCTIVITY * 100)) if np.any(capacidad > 0) else 0,
            'segregaciones_count': len(segregaciones_stats),
            'top_segregaciones': sorted(segregaciones_stats.items(), key=lambda x: x[1], reverse=True)[:5]
        }
    
    async def _update_run_metrics(self, run_id: UUID, results_summary: Dict[str, Any]):
        """Actualiza métricas adicionales del run"""
        
        run_result = await self.db.execute(
            select(CamilaRun).where(CamilaRun.id == run_id)
        )
        run = run_result.scalar_one()
        
        # Actualizar con métricas calculadas
        run.funcion_objetivo = results_summary.get('congestion_index', 0)  # Usar índice como proxy
        
        await self.db.flush()
    
    def _log_load_summary(
        self,
        run_id: UUID,
        results_info: Dict,
        flujos_info: Dict,
        gruas_info: Dict,
        asignacion_info: Optional[Dict],
        real_data_info: Optional[Dict],
        cuotas_info: Optional[Dict],
        results_summary: Dict
    ):
        """Log resumen detallado de la carga"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA")
        logger.info("="*80)
        logger.info(f"Run ID: {run_id}")
        
        logger.info("\n📋 Datos cargados:")
        logger.info(f"  - Flujos: {flujos_info['total_loaded']:,} registros")
        logger.info(f"  - Grúas: {gruas_info['total_loaded']:,} asignaciones")
        
        if asignacion_info:
            logger.info(f"  - Asignación visual: {asignacion_info['assignments_loaded']} registros")
        
        if real_data_info:
            logger.info(f"  - Datos reales: {real_data_info['data_points']} puntos ({real_data_info['total_movements']:,} movimientos)")
        
        if cuotas_info:
            logger.info(f"  - Cuotas: {cuotas_info['cuotas_loaded']} registros")
        
        logger.info("\n📈 Métricas calculadas:")
        logger.info(f"  - Total movimientos: {results_summary['total_movements']:,}")
        logger.info(f"  - Balance workload: {results_summary['balance_workload']:.1f}%")
        logger.info(f"  - Índice congestión: {results_summary['congestion_index']:.2f}")
        logger.info(f"  - Utilización promedio: {results_summary['avg_utilization']:.1f}%")
        
        logger.info("\n🏷️ Segregaciones:")
        logger.info(f"  - Total: {results_summary['segregaciones_count']}")
        logger.info("  - Top 5:")
        for seg, count in results_summary['top_segregaciones']:
            logger.info(f"    • {seg}: {count:.0f} movimientos")
        
        logger.info("\n🚛 Grúas:")
        logger.info(f"  - Grúas activas: {gruas_info['gruas_activas']}")
        logger.info(f"  - Utilización promedio: {gruas_info['utilizacion_promedio']:.1f} asignaciones/grúa")
        
        if self.warnings:
            logger.info("\n⚠️ Advertencias:")
            for warning in self.warnings[:5]:  # Mostrar máximo 5
                logger.info(f"  - {warning}")
            if len(self.warnings) > 5:
                logger.info(f"  ... y {len(self.warnings) - 5} advertencias más")
        
        logger.info("="*80)