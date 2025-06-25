# app/services/camila_loader.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
import logging
from datetime import datetime
import uuid

from app.models.camila import *

logger = logging.getLogger(__name__)

class CamilaLoader:
    def __init__(self, db: AsyncSession):
        self.db = db
        
    async def load_camila_file(
        self,
        filepath: str,
        semana: int,
        dia: str,
        turno: int,
        modelo_tipo: str,  # 'minmax' o 'maxmin'
        con_segregaciones: bool = True
    ) -> uuid.UUID:
        """Carga un archivo de resultados de Camila"""
        
        logger.info(f"🔄 Cargando archivo Camila: {filepath}")
        logger.info(f"   Configuración: S{semana} {dia} T{turno} {modelo_tipo}")
        
        try:
            # Leer Excel
            xls = pd.ExcelFile(filepath)
            
            # Verificar hojas requeridas
            required_sheets = ['Flujos', 'Grúas', 'Asignación', 'Real']
            for sheet in required_sheets:
                if sheet not in xls.sheet_names:
                    raise ValueError(f"Hoja '{sheet}' no encontrada en el archivo")
            
            # 1. Crear o actualizar run
            run_id = await self._create_or_update_run(
                semana, dia, turno, modelo_tipo, con_segregaciones
            )
            
            # 2. Cargar flujos
            await self._load_flujos(xls, run_id)
            
            # 3. Cargar grúas
            await self._load_gruas(xls, run_id)
            
            # 4. Cargar asignación
            await self._load_asignacion(xls, run_id)
            
            # 5. Cargar datos reales
            await self._load_real_data(xls, run_id)
            
            # 6. Calcular y cargar resultados
            await self._calculate_results(run_id)
            
            # 7. Calcular métricas generales
            await self._update_run_metrics(run_id)
            
            await self.db.commit()
            logger.info(f"✅ Archivo Camila cargado exitosamente. Run ID: {run_id}")
            
            return run_id
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"❌ Error cargando archivo Camila: {str(e)}")
            raise
    
    async def _create_or_update_run(
        self, semana: int, dia: str, turno: int, 
        modelo_tipo: str, con_segregaciones: bool
    ) -> uuid.UUID:
        """Crea o actualiza un run"""
        
        # Buscar run existente
        result = await self.db.execute(
            select(CamilaRun).where(
                CamilaRun.semana == semana,
                CamilaRun.dia == dia,
                CamilaRun.turno == turno,
                CamilaRun.modelo_tipo == modelo_tipo,
                CamilaRun.con_segregaciones == con_segregaciones
            )
        )
        existing_run = result.scalar_one_or_none()
        
        if existing_run:
            # Eliminar datos anteriores
            logger.info(f"🗑️ Eliminando datos anteriores del run {existing_run.id}")
            await self.db.delete(existing_run)
            await self.db.flush()
        
        # Crear nuevo run
        new_run = CamilaRun(
            semana=semana,
            dia=dia,
            turno=turno,
            modelo_tipo=modelo_tipo,
            con_segregaciones=con_segregaciones,
            fecha_carga=datetime.utcnow()
        )
        self.db.add(new_run)
        await self.db.flush()
        
        return new_run.id
    
    async def _load_flujos(self, xls: pd.ExcelFile, run_id: uuid.UUID):
        """Carga los datos de flujos"""
        logger.info("   📊 Cargando flujos...")
        
        df = pd.read_excel(xls, 'Flujos')
        
        # Limpiar espacios en los nombres de columnas
        df.columns = df.columns.str.strip()
        
        # Procesar cada fila
        flujos_to_insert = []
        skipped_count = 0
        
        for _, row in df.iterrows():
            try:
                # Verificar que los campos requeridos no sean NaN
                if pd.isna(row['Variable']) or pd.isna(row['Bloques']) or pd.isna(row['Tiempo']):
                    skipped_count += 1
                    continue
                    
                # Convertir tiempo a entero, manejando NaN
                tiempo = row['Tiempo']
                if pd.isna(tiempo):
                    skipped_count += 1
                    continue
                tiempo = int(tiempo)
                
                # Solo procesar si el valor no es 0 o NaN
                valor = row['Valor']
                if pd.isna(valor) or valor == 0:
                    continue
                    
                # Limpiar el campo Bloques de espacios
                bloque = str(row['Bloques']).strip()
                # Si tiene espacios intermedios, removerlos también
                if ' ' in bloque:
                    bloque = bloque.replace(' ', '')
                # Truncar a 10 caracteres si es necesario
                bloque = bloque[:10]
                    
                flujo = CamilaFlujos(
                    run_id=run_id,
                    variable=str(row['Variable']).strip(),
                    segregacion=str(row['Segregación']).strip() if pd.notna(row['Segregación']) else 'S0',
                    bloque=bloque,
                    tiempo=tiempo,
                    valor=float(valor)
                )
                flujos_to_insert.append(flujo)
                
            except Exception as e:
                logger.warning(f"      ⚠️ Error procesando fila: {e}")
                skipped_count += 1
                continue
        
        # Insertar en lotes
        if flujos_to_insert:
            self.db.add_all(flujos_to_insert)
            await self.db.flush()
            logger.info(f"      ✓ {len(flujos_to_insert)} flujos cargados, {skipped_count} omitidos")
    
    async def _load_gruas(self, xls: pd.ExcelFile, run_id: uuid.UUID):
        """Carga los datos de grúas"""
        logger.info("   🏗️ Cargando asignación de grúas...")
        
        df = pd.read_excel(xls, 'Grúas')
        
        # Limpiar espacios en los nombres de columnas
        df.columns = df.columns.str.strip()
        
        gruas_to_insert = []
        skipped_count = 0
        
        for _, row in df.iterrows():
            try:
                # Verificar campos requeridos
                if pd.isna(row['Grúas']) or pd.isna(row['Bloques']) or pd.isna(row['Tiempo']):
                    skipped_count += 1
                    continue
                    
                tiempo = int(row['Tiempo'])
                valor = row['Valor']
                
                if pd.isna(valor):
                    continue
                    
                # Solo insertar si valor es 1
                if int(valor) == 1:
                    # Limpiar el campo Bloques
                    bloque = str(row['Bloques']).strip()
                    if ' ' in bloque:
                        bloque = bloque.replace(' ', '')
                    # Truncar a 10 caracteres si es necesario
                    bloque = bloque[:10]
                        
                    grua = CamilaGruas(
                        run_id=run_id,
                        grua=str(row['Grúas']).strip(),
                        bloque=bloque,
                        tiempo=tiempo,
                        valor=1
                    )
                    gruas_to_insert.append(grua)
                    
            except Exception as e:
                logger.warning(f"      ⚠️ Error procesando fila de grúas: {e}")
                skipped_count += 1
                continue
        
        if gruas_to_insert:
            self.db.add_all(gruas_to_insert)
            await self.db.flush()
            logger.info(f"      ✓ {len(gruas_to_insert)} asignaciones de grúas cargadas, {skipped_count} omitidos")
    
    async def _load_asignacion(self, xls: pd.ExcelFile, run_id: uuid.UUID):
        """Carga la matriz de asignación visual"""
        logger.info("   📋 Cargando matriz de asignación...")
        
        df = pd.read_excel(xls, 'Asignación')
        
        asignaciones = []
        # Iterar por filas (tiempos)
        for idx, row in df.iterrows():
            # Verificar si es la fila de μ_g o está vacía
            if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() == 'μ_g' or str(row.iloc[0]).strip() == '':
                continue
                
            try:
                # Extraer el tiempo
                tiempo_str = str(row.iloc[0]).strip()
                if tiempo_str.startswith('t'):
                    tiempo = int(tiempo_str.replace('t', ''))
                else:
                    tiempo = int(tiempo_str)
                
                # Iterar por columnas (grúas)
                for col_idx in range(1, min(13, len(row))):  # g1 a g12, pero verificar límites
                    if col_idx < len(row):
                        valor = row.iloc[col_idx]
                        if pd.notna(valor) and str(valor).strip() != '':
                            valor_str = str(valor).strip()
                            # Limpiar espacios en el valor
                            if ' ' in valor_str:
                                valor_str = valor_str.replace(' ', '')
                            
                            # IMPORTANTE: Manejar valores especiales y truncar
                            # Si el valor es un texto descriptivo, convertirlo a un código corto
                            if valor_str.lower() in ['entrega', 'recepción', 'recepcion', 'carga', 'descarga']:
                                # Mapear a códigos cortos
                                mapping = {
                                    'entrega': 'ENT',
                                    'recepción': 'REC',
                                    'recepcion': 'REC',
                                    'carga': 'CAR',
                                    'descarga': 'DES'
                                }
                                valor_str = mapping.get(valor_str.lower(), valor_str[:3].upper())
                            
                            # Truncar a 10 caracteres
                            valor_str = valor_str[:10]
                                
                            asignacion = CamilaAsignacion(
                                run_id=run_id,
                                tiempo=tiempo,
                                grua=f'g{col_idx}',
                                bloque_asignado=valor_str,
                                movimientos_realizados=20  # Capacidad estándar
                            )
                            asignaciones.append(asignacion)
            except Exception as e:
                logger.warning(f"      ⚠️ Error procesando fila de asignación: {e}")
                continue
        
        if asignaciones:
            self.db.add_all(asignaciones)
            await self.db.flush()
            logger.info(f"      ✓ {len(asignaciones)} asignaciones visuales cargadas")
    
    async def _load_real_data(self, xls: pd.ExcelFile, run_id: uuid.UUID):
        """Carga los datos reales históricos"""
        logger.info("   📈 Cargando datos reales...")
        
        df = pd.read_excel(xls, 'Real')
        
        real_data_list = []
        # Primera columna son los tiempos, las demás son bloques
        bloques = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
        
        for idx, row in df.iterrows():
            # Verificar si es la fila Total o está vacía
            if pd.isna(row.iloc[0]) or 'Total' in str(row.iloc[0]):
                continue
                
            try:
                tiempo_str = str(row.iloc[0]).strip()
                if tiempo_str.startswith('t'):
                    tiempo = int(tiempo_str.replace('t', ''))
                else:
                    tiempo = int(tiempo_str)
                
                for col_idx, bloque in enumerate(bloques, 1):
                    if col_idx < len(row):
                        valor = row.iloc[col_idx]
                        movimientos = 0
                        if pd.notna(valor):
                            # Manejar posibles formatos de número
                            if isinstance(valor, (int, float)):
                                movimientos = int(valor)
                            else:
                                # Intentar convertir string a número
                                try:
                                    movimientos = int(float(str(valor)))
                                except:
                                    movimientos = 0
                                    
                        if movimientos > 0:
                            real_data = CamilaRealData(
                                run_id=run_id,
                                bloque=bloque,
                                tiempo=tiempo,
                                movimientos=movimientos
                            )
                            real_data_list.append(real_data)
            except Exception as e:
                logger.warning(f"      ⚠️ Error procesando fila de datos reales: {e}")
                continue
        
        if real_data_list:
            self.db.add_all(real_data_list)
            await self.db.flush()
            logger.info(f"      ✓ {len(real_data_list)} datos reales cargados")
    
    async def _calculate_results(self, run_id: uuid.UUID):
        """Calcula y almacena los resultados consolidados"""
        logger.info("   🧮 Calculando resultados...")
        
        # Obtener flujos
        flujos_result = await self.db.execute(
            select(CamilaFlujos).where(CamilaFlujos.run_id == run_id)
        )
        flujos = flujos_result.scalars().all()
        
        # Obtener grúas
        gruas_result = await self.db.execute(
            select(CamilaGruas).where(CamilaGruas.run_id == run_id)
        )
        gruas = gruas_result.scalars().all()
        
        # Inicializar matrices
        bloques = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
        tiempos = 8
        
        total_flujos = [[0 for _ in range(tiempos)] for _ in range(len(bloques))]
        capacidad = [[0 for _ in range(tiempos)] for _ in range(len(bloques))]
        
        # Calcular total de flujos por bloque y tiempo
        for flujo in flujos:
            # Limpiar el nombre del bloque
            bloque_name = flujo.bloque.strip().upper()
            if bloque_name.startswith('B'):
                bloque_idx = int(bloque_name.replace('B', '')) - 1
            elif bloque_name.startswith('C'):
                bloque_idx = int(bloque_name.replace('C', '')) - 1
            else:
                # Intentar convertir directamente
                try:
                    bloque_idx = int(bloque_name) - 1
                except:
                    continue
                    
            tiempo_idx = flujo.tiempo - 1
            if 0 <= bloque_idx < len(bloques) and 0 <= tiempo_idx < tiempos:
                total_flujos[bloque_idx][tiempo_idx] += flujo.valor
        
        # Calcular capacidad basada en grúas asignadas
        for grua in gruas:
            if grua.valor == 1:
                # Limpiar el nombre del bloque
                bloque_name = grua.bloque.strip().upper()
                if bloque_name.startswith('B'):
                    bloque_idx = int(bloque_name.replace('B', '')) - 1
                elif bloque_name.startswith('C'):
                    bloque_idx = int(bloque_name.replace('C', '')) - 1
                else:
                    try:
                        bloque_idx = int(bloque_name) - 1
                    except:
                        continue
                        
                tiempo_idx = grua.tiempo - 1
                if 0 <= bloque_idx < len(bloques) and 0 <= tiempo_idx < tiempos:
                    capacidad[bloque_idx][tiempo_idx] += 20  # Productividad por grúa
        
        # Calcular disponibilidad
        disponibilidad = [[max(0, capacidad[b][t] - total_flujos[b][t]) 
                          for t in range(tiempos)] 
                          for b in range(len(bloques))]
        
        # Calcular KPIs
        block_totals = [sum(total_flujos[b]) for b in range(len(bloques))]
        total_movimientos = sum(block_totals)
        
        participacion_bloques = [
            (total / total_movimientos * 100) if total_movimientos > 0 else 0 
            for total in block_totals
        ]
        
        time_totals = [sum(total_flujos[b][t] for b in range(len(bloques))) 
                      for t in range(tiempos)]
        participacion_tiempo = [
            (total / total_movimientos * 100) if total_movimientos > 0 else 0 
            for total in time_totals
        ]
        
        # Desviación estándar
        avg_block = np.mean(block_totals) if block_totals else 0
        std_bloques = np.std(block_totals) if block_totals else 0
        
        avg_time = np.mean(time_totals) if time_totals else 0
        std_tiempo = np.std(time_totals) if time_totals else 0
        
        # Cuotas recomendadas
        cuotas_recomendadas = [[int(total_flujos[b][t] + disponibilidad[b][t] * 0.8) 
                               for t in range(tiempos)] 
                               for b in range(len(bloques))]
        
        # Crear registro de resultados
        resultado = CamilaResultados(
            run_id=run_id,
            total_flujos=total_flujos,
            capacidad=capacidad,
            disponibilidad=disponibilidad,
            participacion_bloques=participacion_bloques,
            participacion_tiempo=participacion_tiempo,
            desviacion_std_bloques=float(std_bloques),
            desviacion_std_tiempo=float(std_tiempo),
            cuotas_recomendadas=cuotas_recomendadas
        )
        self.db.add(resultado)
        
        # Guardar cuotas detalladas
        cuotas_list = []
        for b_idx, bloque in enumerate(bloques):
            for t in range(tiempos):
                cuota = CamilaCuotas(
                    run_id=run_id,
                    bloque=bloque,
                    tiempo=t + 1,
                    disponibilidad=disponibilidad[b_idx][t],
                    cuota_recomendada=cuotas_recomendadas[b_idx][t]
                )
                cuotas_list.append(cuota)
        
        self.db.add_all(cuotas_list)
        await self.db.flush()
        logger.info("      ✓ Resultados calculados y almacenados")
    
    async def _update_run_metrics(self, run_id: uuid.UUID):
        """Actualiza las métricas del run"""
        
        # Obtener resultados
        result = await self.db.execute(
            select(CamilaResultados).where(CamilaResultados.run_id == run_id)
        )
        resultados = result.scalar_one_or_none()
        
        if resultados:
            # Calcular métricas
            total_movimientos = sum(sum(row) for row in resultados.total_flujos)
            
            # Balance de workload
            block_totals = [sum(row) for row in resultados.total_flujos]
            avg_block = np.mean(block_totals) if block_totals else 0
            cv = (resultados.desviacion_std_bloques / avg_block * 100) if avg_block > 0 else 0
            balance_workload = 100 - cv
            
            # Índice de congestión
            max_flow = max(block_totals) if block_totals else 0
            indice_congestion = (max_flow / avg_block) if avg_block > 0 else 0
            
            # Actualizar run
            run_result = await self.db.execute(
                select(CamilaRun).where(CamilaRun.id == run_id)
            )
            run = run_result.scalar_one()
            
            run.total_movimientos = int(total_movimientos)
            run.balance_workload = float(balance_workload)
            run.indice_congestion = float(indice_congestion)
            
            await self.db.flush()
            logger.info("      ✓ Métricas del run actualizadas")