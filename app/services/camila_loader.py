# app/services/camila_loader.py - VERSIÓN CORREGIDA
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, func
import logging
from uuid import UUID
import re
import json

from app.models.camila import (
    CamilaRun, CamilaVariable, CamilaParametro, CamilaMetrica, CamilaSegregacion
)

logger = logging.getLogger(__name__)

class CamilaLoader:
    """Clase para cargar archivos de resultados del modelo Camila"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        
    async def load_model_results(
        self,
        resultado_filepath: str,
        instancia_filepath: str,
        semana: int,
        dia: str,
        turno: int,
        modelo_tipo: str,
        con_segregaciones: bool
    ) -> UUID:
        """Carga archivos de resultados e instancia del modelo Camila"""
        
        logger.info(f"{'='*80}")
        logger.info(f"Iniciando carga de archivos del modelo")
        logger.info(f"Resultado: {resultado_filepath}")
        logger.info(f"Instancia: {instancia_filepath}")
        logger.info(f"Config: S{semana} {dia} T{turno} {modelo_tipo}")
        
        try:
            # Crear o actualizar run
            run = await self._create_or_update_run(
                semana, dia, turno, modelo_tipo, con_segregaciones,
                resultado_filepath, instancia_filepath
            )
            
            # Cargar archivo de resultados
            variables_stats = await self._load_resultado_file(resultado_filepath, run.id)
            
            # Cargar archivo de instancia
            params_stats = await self._load_instancia_file(instancia_filepath, run.id)
            
            # Calcular métricas
            metrics_stats = await self._calculate_metrics(run.id)
            
            # Actualizar run con estadísticas
            await self._update_run_stats(run.id, variables_stats, metrics_stats)
            
            # Commit final
            await self.db.commit()
            
            # Log resumen
            self._log_summary(run.id, variables_stats, params_stats, metrics_stats)
            
            return run.id
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"❌ Error cargando archivos: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def _create_or_update_run(
        self, semana: int, dia: str, turno: int, modelo_tipo: str,
        con_segregaciones: bool, resultado_file: str, instancia_file: str
    ) -> CamilaRun:
        """Crea o actualiza un run"""
        
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
            await self._delete_run_data(run.id)
            run.fecha_carga = datetime.utcnow()
            run.archivo_resultado = resultado_file
            run.archivo_instancia = instancia_file
        else:
            logger.info("Creando nuevo run")
            run = CamilaRun(
                semana=semana,
                dia=dia,
                turno=turno,
                modelo_tipo=modelo_tipo,
                con_segregaciones=con_segregaciones,
                fecha_carga=datetime.utcnow(),
                archivo_resultado=resultado_file,
                archivo_instancia=instancia_file
            )
            self.db.add(run)
            await self.db.flush()
        
        logger.info(f"Run ID: {run.id}")
        return run
    
    async def _delete_run_data(self, run_id: UUID):
        """Elimina datos anteriores de un run"""
        logger.info(f"Eliminando datos anteriores del run {run_id}")
        await self.db.execute(delete(CamilaVariable).where(CamilaVariable.run_id == run_id))
        await self.db.execute(delete(CamilaParametro).where(CamilaParametro.run_id == run_id))
        await self.db.execute(delete(CamilaMetrica).where(CamilaMetrica.run_id == run_id))
        await self.db.execute(delete(CamilaSegregacion).where(CamilaSegregacion.run_id == run_id))
        await self.db.flush()
    
    async def _load_resultado_file(self, filepath: str, run_id: UUID) -> Dict[str, Any]:
        """Carga archivo de resultados con formato índice"""
        
        logger.info("Cargando archivo de resultados...")
        
        # Leer el archivo
        df = pd.read_excel(filepath)
        logger.info(f"Archivo leído: {len(df)} filas")
        logger.info(f"Columnas: {list(df.columns)}")
        
        # Verificar columnas esperadas
        expected_cols = {'variable', 'índice', 'valor'}
        actual_cols = set(df.columns)
        
        if not expected_cols.issubset(actual_cols):
            # Intentar con nombres alternativos
            df.columns = df.columns.str.lower().str.strip()
            actual_cols = set(df.columns)
            
            if not expected_cols.issubset(actual_cols):
                raise ValueError(f"Columnas faltantes. Esperadas: {expected_cols}, Encontradas: {actual_cols}")
        
        # Estadísticas por tipo de variable
        stats = {
            'total_variables': 0,
            'fr_sbt': 0,
            'fe_sbt': 0,
            'fc_sbt': 0,
            'fd_sbt': 0,
            'ygbt': 0,
            'alpha_gbt': 0,
            'Z_gb': 0,
            'min_diff_val': 0,
            'variables_ignoradas': 0
        }
        
        batch = []
        batch_size = 100
        
        for idx, row in df.iterrows():
            try:
                variable = str(row['variable']).strip() if pd.notna(row['variable']) else ''
                indice = str(row['índice']).strip() if pd.notna(row['índice']) else ''
                valor = float(row['valor']) if pd.notna(row['valor']) else 0.0
                
                # Saltar filas vacías
                if not variable:
                    continue
                
                # Crear registro de variable
                var_record = CamilaVariable(
                    run_id=run_id,
                    variable=variable,
                    indice=indice,
                    valor=valor
                )
                
                # Parsear índice según tipo de variable
                parsed = self._parse_indice(indice, variable)
                var_record.segregacion = parsed.get('segregacion')
                var_record.grua = parsed.get('grua')
                var_record.bloque = parsed.get('bloque')
                var_record.tiempo = parsed.get('tiempo')
                var_record.tipo_variable = self._get_tipo_variable(variable)
                
                batch.append(var_record)
                stats['total_variables'] += 1
                
                if variable in stats:
                    stats[variable] += 1
                else:
                    stats['variables_ignoradas'] += 1
                
                # Guardar en lotes
                if len(batch) >= batch_size:
                    self.db.add_all(batch)
                    await self.db.flush()
                    batch = []
                    
            except Exception as e:
                logger.warning(f"Error en fila {idx}: {str(e)}")
                logger.debug(f"Datos de la fila: variable={row.get('variable')}, indice={row.get('índice')}, valor={row.get('valor')}")
        
        # Guardar últimos registros
        if batch:
            self.db.add_all(batch)
            await self.db.flush()
        
        logger.info(f"Variables cargadas: {stats}")
        
        # Obtener función objetivo
        obj_result = await self.db.execute(
            select(CamilaVariable.valor).where(
                and_(
                    CamilaVariable.run_id == run_id,
                    CamilaVariable.variable == 'min_diff_val'
                )
            )
        )
        stats['funcion_objetivo'] = obj_result.scalar() or 0
        
        return stats
    
    def _parse_indice(self, indice_str: str, variable: str) -> Dict[str, Any]:
        """Parsea string de índice según el tipo de variable"""
        
        result = {}
        
        if not indice_str or indice_str == '' or variable == 'min_diff_val':
            return result
        
        try:
            # Patrones según tipo de variable
            if variable in ['fr_sbt', 'fe_sbt', 'fc_sbt', 'fd_sbt']:
                # Formato: ('s3', 'b3', 1)
                match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", indice_str)
                if match:
                    result['segregacion'] = match.group(1)
                    result['bloque'] = match.group(2)
                    result['tiempo'] = int(match.group(3))
                    
            elif variable in ['ygbt', 'alpha_gbt']:
                # Formato: ('g1', 'b3', 3)
                match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", indice_str)
                if match:
                    result['grua'] = match.group(1)
                    result['bloque'] = match.group(2)
                    result['tiempo'] = int(match.group(3))
                    
            elif variable == 'Z_gb':
                # Formato: ('g1', 'b3')
                match = re.match(r"\('([^']+)',\s*'([^']+)'\)", indice_str)
                if match:
                    result['grua'] = match.group(1)
                    result['bloque'] = match.group(2)
                    
        except Exception as e:
            logger.debug(f"Error parseando índice '{indice_str}': {str(e)}")
        
        return result
    
    def _get_tipo_variable(self, variable: str) -> str:
        """Determina el tipo de variable"""
        
        tipo_map = {
            'fr_sbt': 'flujo_recepcion',
            'fe_sbt': 'flujo_entrega',
            'fc_sbt': 'flujo_carga',
            'fd_sbt': 'flujo_descarga',
            'ygbt': 'asignacion_grua',
            'alpha_gbt': 'alpha_variable',
            'Z_gb': 'z_variable',
            'min_diff_val': 'funcion_objetivo'
        }
        
        return tipo_map.get(variable, 'desconocido')
    
    async def _load_instancia_file(self, filepath: str, run_id: UUID) -> Dict[str, Any]:
        """Carga archivo de instancia con parámetros del modelo"""
        
        logger.info("Cargando archivo de instancia...")
        
        try:
            xl = pd.ExcelFile(filepath)
            logger.info(f"Hojas disponibles en instancia: {xl.sheet_names}")
            
            stats = {
                'hojas_procesadas': 0,
                'parametros_cargados': 0,
                'segregaciones_cargadas': 0
            }
            
            # Cargar parámetros simples
            params_simples = ['mu', 'W', 'K', 'Rmax']
            for param in params_simples:
                if param in xl.sheet_names:
                    try:
                        df = pd.read_excel(xl, param, header=None)  # Sin header
                        logger.info(f"Leyendo parámetro {param}, shape: {df.shape}")
                        
                        # El valor suele estar en la segunda fila, primera columna
                        valor = None
                        if len(df) >= 2:
                            # Intentar fila 1 (índice 1)
                            valor = df.iloc[1, 0]
                        elif len(df) >= 1:
                            # Si solo hay una fila, intentar esa
                            valor = df.iloc[0, 0]
                        
                        if pd.notna(valor) and isinstance(valor, (int, float)):
                            param_record = CamilaParametro(
                                run_id=run_id,
                                parametro=param,
                                valor=float(valor),
                                descripcion=self._get_param_description(param)
                            )
                            self.db.add(param_record)
                            stats['parametros_cargados'] += 1
                            logger.info(f"✓ Parámetro {param} = {valor}")
                        else:
                            logger.warning(f"No se pudo leer valor para {param}, df:")
                            logger.warning(df.head())
                            
                    except Exception as e:
                        logger.error(f"Error cargando parámetro {param}: {e}")
                        logger.error(f"Contenido de la hoja {param}:")
                        try:
                            df_debug = pd.read_excel(xl, param)
                            logger.error(df_debug)
                        except:
                            pass
            
            # Cargar información de segregaciones
            if 'S' in xl.sheet_names:
                try:
                    df = pd.read_excel(xl, 'S', header=None)  # Sin header
                    logger.info(f"Leyendo segregaciones, shape: {df.shape}")
                    
                    # La primera fila suele ser el header 'S' | 'Segregacion'
                    start_row = 0
                    if len(df) > 0 and str(df.iloc[0, 0]).upper() == 'S':
                        start_row = 1
                    
                    for idx in range(start_row, len(df)):
                        row = df.iloc[idx]
                        
                        if pd.notna(row[0]):
                            codigo = str(row[0]).strip()
                            
                            # Verificar que es un código válido de segregación
                            if not codigo.startswith('s') or not codigo[1:].isdigit():
                                continue
                            
                            descripcion = ''
                            if len(row) > 1 and pd.notna(row[1]):
                                descripcion = str(row[1]).strip()
                            
                            # Determinar tipo
                            tipo = 'desconocido'
                            if descripcion:
                                desc_lower = descripcion.lower()
                                if 'expo' in desc_lower:
                                    tipo = 'exportacion'
                                elif 'impo' in desc_lower:
                                    tipo = 'importacion'
                            
                            seg_record = CamilaSegregacion(
                                run_id=run_id,
                                codigo=codigo,
                                descripcion=descripcion,
                                tipo=tipo,
                                total_recepcion=0,
                                total_entrega=0,
                                bloques_asignados=[]
                            )
                            self.db.add(seg_record)
                            stats['segregaciones_cargadas'] += 1
                            
                            if stats['segregaciones_cargadas'] <= 5:  # Log primeras 5
                                logger.info(f"✓ Segregación {codigo}: {descripcion[:30]}... ({tipo})")
                            
                except Exception as e:
                    logger.error(f"Error cargando segregaciones: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
            
            await self.db.flush()
            stats['hojas_procesadas'] = len(xl.sheet_names)
            
            # Si no se cargaron parámetros, usar valores por defecto
            if stats['parametros_cargados'] == 0:
                logger.warning("No se pudieron cargar parámetros, usando valores por defecto")
                
                # Cargar mu por defecto
                param_mu = CamilaParametro(
                    run_id=run_id,
                    parametro='mu',
                    valor=30.0,
                    descripcion='Productividad de grúa (movimientos/hora) - VALOR POR DEFECTO'
                )
                self.db.add(param_mu)
                await self.db.flush()
                stats['parametros_cargados'] = 1
                logger.info("✓ Parámetro mu = 30 (por defecto)")
            
            logger.info(f"Instancia cargada: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando instancia: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Continuar con valores por defecto
            return {
                'hojas_procesadas': 0,
                'parametros_cargados': 0,
                'segregaciones_cargadas': 0
            }
    
    def _get_param_description(self, param: str) -> str:
        """Obtiene descripción de un parámetro"""
        
        descriptions = {
            'mu': 'Productividad de grúa (movimientos/hora)',
            'W': 'Límite de grúas por bloque',
            'K': 'Períodos mínimos de operación',
            'Rmax': 'Máximo número de grúas'
        }
        
        return descriptions.get(param, '')
    
    async def _calculate_metrics(self, run_id: UUID) -> Dict[str, Any]:
        """Calcula métricas agregadas"""
        
        logger.info("Calculando métricas...")
        
        try:
            # Inicializar estructuras
            metricas_bloque = {}
            metricas_grua = {}
            metricas_tiempo = {}
            metricas_segregacion = {}
            
            # Matrices
            matriz_flujos = np.zeros((9, 8))
            matriz_gruas = np.zeros((12, 72))
            
            # Obtener todas las variables
            variables_result = await self.db.execute(
                select(CamilaVariable).where(CamilaVariable.run_id == run_id)
            )
            variables = variables_result.scalars().all()
            
            logger.info(f"Procesando {len(variables)} variables para métricas...")
            
            # Procesar variables
            for var in variables:
                # Flujos
                if var.tipo_variable in ['flujo_recepcion', 'flujo_entrega', 'flujo_carga', 'flujo_descarga']:
                    if var.bloque and var.tiempo:
                        try:
                            b_idx = int(var.bloque[1:]) - 1  # b1 -> 0
                            t_idx = var.tiempo - 1
                            
                            if 0 <= b_idx < 9 and 0 <= t_idx < 8:
                                matriz_flujos[b_idx][t_idx] += var.valor
                                
                                # Actualizar métricas por bloque
                                if var.bloque not in metricas_bloque:
                                    metricas_bloque[var.bloque] = {
                                        'movimientos_total': 0,
                                        'recepcion': 0,
                                        'entrega': 0,
                                        'gruas_asignadas': 0,
                                        'periodos_activos': set()
                                    }
                                
                                metricas_bloque[var.bloque]['movimientos_total'] += var.valor
                                if var.tipo_variable == 'flujo_recepcion':
                                    metricas_bloque[var.bloque]['recepcion'] += var.valor
                                elif var.tipo_variable == 'flujo_entrega':
                                    metricas_bloque[var.bloque]['entrega'] += var.valor
                                metricas_bloque[var.bloque]['periodos_activos'].add(var.tiempo)
                                
                                # Métricas por segregación
                                if var.segregacion:
                                    if var.segregacion not in metricas_segregacion:
                                        metricas_segregacion[var.segregacion] = {
                                            'recepcion': 0,
                                            'entrega': 0,
                                            'bloques': set()
                                        }
                                    
                                    if var.tipo_variable == 'flujo_recepcion':
                                        metricas_segregacion[var.segregacion]['recepcion'] += var.valor
                                    elif var.tipo_variable == 'flujo_entrega':
                                        metricas_segregacion[var.segregacion]['entrega'] += var.valor
                                    metricas_segregacion[var.segregacion]['bloques'].add(var.bloque)
                        except Exception as e:
                            logger.debug(f"Error procesando flujo: {e}")
                
                # Asignación de grúas
                elif var.variable == 'ygbt' and var.valor == 1:
                    if var.grua and var.bloque and var.tiempo:
                        try:
                            g_idx = int(var.grua[1:]) - 1  # g1 -> 0
                            b_idx = int(var.bloque[1:]) - 1
                            t_idx = var.tiempo - 1
                            
                            if 0 <= g_idx < 12 and 0 <= b_idx < 9 and 0 <= t_idx < 8:
                                slot_idx = b_idx * 8 + t_idx
                                matriz_gruas[g_idx][slot_idx] = 1
                                
                                # Métricas por grúa
                                if var.grua not in metricas_grua:
                                    metricas_grua[var.grua] = {
                                        'periodos_activos': 0,
                                        'bloques': set(),
                                        'asignaciones': []
                                    }
                                
                                metricas_grua[var.grua]['periodos_activos'] += 1
                                metricas_grua[var.grua]['bloques'].add(var.bloque)
                                metricas_grua[var.grua]['asignaciones'].append({
                                    'tiempo': var.tiempo,
                                    'bloque': var.bloque
                                })
                                
                                # Actualizar grúas por bloque
                                if var.bloque in metricas_bloque:
                                    metricas_bloque[var.bloque]['gruas_asignadas'] += 1
                        except Exception as e:
                            logger.debug(f"Error procesando grúa: {e}")
            
            # Convertir sets a listas para JSON
            for bloque in metricas_bloque:
                metricas_bloque[bloque]['periodos_activos'] = list(metricas_bloque[bloque]['periodos_activos'])
            
            for grua in metricas_grua:
                metricas_grua[grua]['bloques'] = list(metricas_grua[grua]['bloques'])
            
            for seg in metricas_segregacion:
                metricas_segregacion[seg]['bloques'] = list(metricas_segregacion[seg]['bloques'])
            
            # Calcular totales y porcentajes
            total_movimientos = np.sum(matriz_flujos)
            logger.info(f"Total movimientos calculado: {total_movimientos}")
            
            # Participación por bloque
            participacion_bloques = []
            for i in range(9):
                total_bloque = np.sum(matriz_flujos[i, :])
                participacion = (total_bloque / total_movimientos * 100) if total_movimientos > 0 else 0
                participacion_bloques.append(float(participacion))
            
            # Participación por tiempo
            participacion_tiempo = []
            for j in range(8):
                total_tiempo = np.sum(matriz_flujos[:, j])
                participacion = (total_tiempo / total_movimientos * 100) if total_movimientos > 0 else 0
                participacion_tiempo.append(float(participacion))
            
            # Calcular capacidad basada en grúas asignadas
            mu_param = await self.db.execute(
                select(CamilaParametro.valor).where(
                    and_(
                        CamilaParametro.run_id == run_id,
                        CamilaParametro.parametro == 'mu'
                    )
                )
            )
            mu = mu_param.scalar() or 30
            logger.info(f"Productividad (mu): {mu}")
            
            matriz_capacidad = np.zeros((9, 8))
            for b in range(9):
                for t in range(8):
                    gruas_en_slot = 0
                    for g in range(12):
                        if matriz_gruas[g][b * 8 + t] == 1:
                            gruas_en_slot += 1
                    matriz_capacidad[b][t] = gruas_en_slot * mu
            
            # Disponibilidad
            matriz_disponibilidad = np.maximum(0, matriz_capacidad - matriz_flujos)
            
            # Estadísticas
            block_totals = np.sum(matriz_flujos, axis=1)
            time_totals = np.sum(matriz_flujos, axis=0)
            
            desviacion_std_bloques = float(np.std(block_totals))
            desviacion_std_tiempo = float(np.std(time_totals))
            
            # Balance de workload
            avg_block = np.mean(block_totals)
            balance_workload = 100.0
            if avg_block > 0:
                cv = (desviacion_std_bloques / avg_block) * 100
                balance_workload = max(0, 100 - cv)
            
            # Índice de congestión
            max_flow = float(np.max(block_totals)) if len(block_totals) > 0 else 0
            indice_congestion = max_flow / avg_block if avg_block > 0 else 1.0
            
            # Utilización
            total_capacidad = np.sum(matriz_capacidad)
            utilizacion_promedio = (total_movimientos / total_capacidad * 100) if total_capacidad > 0 else 0
            
            logger.info(f"Balance workload: {balance_workload:.1f}%")
            logger.info(f"Índice congestión: {indice_congestion:.2f}")
            logger.info(f"Utilización: {utilizacion_promedio:.1f}%")
            
            # Crear registro de métricas
            metrica = CamilaMetrica(
                run_id=run_id,
                metricas_bloque=metricas_bloque,
                metricas_grua=metricas_grua,
                metricas_tiempo=metricas_tiempo,
                metricas_segregacion=metricas_segregacion,
                matriz_flujos_total=matriz_flujos.tolist(),
                matriz_asignacion_gruas=matriz_gruas.tolist(),
                matriz_capacidad=matriz_capacidad.tolist(),
                matriz_disponibilidad=matriz_disponibilidad.tolist(),
                participacion_bloques=participacion_bloques,
                participacion_tiempo=participacion_tiempo,
                desviacion_std_bloques=desviacion_std_bloques,
                desviacion_std_tiempo=desviacion_std_tiempo,
                utilizacion_promedio=utilizacion_promedio,
                gruas_activas=len(metricas_grua),
                bloques_activos=len([b for b in metricas_bloque if metricas_bloque[b]['movimientos_total'] > 0])
            )
            
            self.db.add(metrica)
            await self.db.flush()
            
            return {
                'total_movimientos': int(total_movimientos),
                'balance_workload': balance_workload,
                'indice_congestion': indice_congestion,
                'utilizacion_promedio': utilizacion_promedio,
                'gruas_activas': len(metricas_grua),
                'bloques_activos': metrica.bloques_activos
            }
            
        except Exception as e:
            logger.error(f"Error calculando métricas: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def _update_run_stats(self, run_id: UUID, variables_stats: Dict, metrics_stats: Dict):
        """Actualiza estadísticas del run"""
        
        run_result = await self.db.execute(
            select(CamilaRun).where(CamilaRun.id == run_id)
        )
        run = run_result.scalar_one()
        
        run.funcion_objetivo = variables_stats.get('funcion_objetivo', 0)
        run.total_movimientos = metrics_stats.get('total_movimientos', 0)
        run.balance_workload = metrics_stats.get('balance_workload', 0)
        run.indice_congestion = metrics_stats.get('indice_congestion', 0)
        
        await self.db.flush()
    
    def _log_summary(self, run_id: UUID, variables_stats: Dict, params_stats: Dict, metrics_stats: Dict):
        """Log resumen de la carga"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA DEL MODELO")
        logger.info("="*80)
        logger.info(f"Run ID: {run_id}")
        
        logger.info("\n📋 Variables cargadas:")
        for var, count in variables_stats.items():
            if var not in ['total_variables', 'funcion_objetivo'] and count > 0:
                logger.info(f"  - {var}: {count} registros")
        logger.info(f"  Total: {variables_stats['total_variables']} variables")
        
        logger.info(f"\n🎯 Función objetivo: {variables_stats['funcion_objetivo']}")
        
        logger.info("\n📈 Métricas calculadas:")
        logger.info(f"  - Total movimientos: {metrics_stats['total_movimientos']}")
        logger.info(f"  - Balance workload: {metrics_stats['balance_workload']:.1f}%")
        logger.info(f"  - Índice congestión: {metrics_stats['indice_congestion']:.2f}")
        logger.info(f"  - Utilización: {metrics_stats['utilizacion_promedio']:.1f}%")
        logger.info(f"  - Grúas activas: {metrics_stats['gruas_activas']}/12")
        logger.info(f"  - Bloques activos: {metrics_stats['bloques_activos']}/9")
        
        logger.info("="*80)