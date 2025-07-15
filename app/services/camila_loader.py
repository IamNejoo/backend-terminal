# app/services/camila_loader.py

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, func
import logging
from uuid import UUID
import re
import json
from pathlib import Path

from app.models.camila import (
    ResultadoCamila, AsignacionGrua, CuotaCamion, MetricaGrua,
    ComparacionReal, FlujoModelo, ParametroCamila, LogProcesamientoCamila,
    EstadoProcesamiento, TipoOperacion, TipoAsignacion
)

logger = logging.getLogger(__name__)


class CamilaLoader:
    """Servicio para cargar y procesar datos del modelo Camila"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        self.parametros_cache = {}
        
    async def load_camila_results(
        self,
        resultado_filepath: str,
        instancia_filepath: Optional[str],
        flujos_real_filepath: Optional[str],
        fecha_inicio: datetime,
        semana: int,
        anio: int,
        turno: int,
        participacion: int,
        con_dispersion: bool
    ) -> UUID:
        """Carga completa de resultados de Camila para un turno específico"""
        
        logger.info(f"{'='*80}")
        logger.info(f"Iniciando carga de Camila")
        logger.info(f"Turno: {turno}, Fecha: {fecha_inicio.date()}")
        logger.info(f"Config: Año {anio}, Semana {semana}, P{participacion}, Disp={'K' if con_dispersion else 'N'}")
        
        # Crear log de procesamiento
        log_proceso = LogProcesamientoCamila(
            tipo_proceso='carga_modelo',
            archivo_procesado=resultado_filepath,
            fecha_inicio=datetime.utcnow(),
            estado=EstadoProcesamiento.PROCESANDO
        )
        self.db.add(log_proceso)
        await self.db.flush()
        
        try:
            # 1. Crear o actualizar resultado de Camila
            resultado_camila = await self._create_or_update_resultado(
                fecha_inicio, semana, anio, turno, participacion, con_dispersion,
                resultado_filepath, instancia_filepath, flujos_real_filepath
            )
            
            # Actualizar log con resultado_id
            log_proceso.resultado_id = resultado_camila.id
            
            # 2. Cargar parámetros del modelo
            if instancia_filepath and Path(instancia_filepath).exists():
                await self._load_parametros(instancia_filepath)
            
            # 3. Cargar archivo de resultado (output del modelo)
            stats_modelo = await self._load_resultado_file(resultado_filepath, resultado_camila.id)
            
            # 4. Cargar archivo de instancia para demandas y capacidades
            stats_instancia = {}
            if instancia_filepath and Path(instancia_filepath).exists():
                stats_instancia = await self._load_instancia_file(instancia_filepath, resultado_camila.id)
            
            # 5. Calcular métricas del modelo
            await self._calculate_model_metrics(resultado_camila.id, stats_modelo, stats_instancia)
            
            # 6. Si hay datos reales, comparar
            if flujos_real_filepath and Path(flujos_real_filepath).exists():
                await self._compare_with_reality(
                    resultado_camila.id,
                    flujos_real_filepath,
                    fecha_inicio,
                    turno
                )
            
            # 7. Actualizar estado
            resultado_camila.estado = EstadoProcesamiento.COMPLETADO
            resultado_camila.fecha_procesamiento = datetime.utcnow()
            
            # Actualizar log
            log_proceso.estado = EstadoProcesamiento.COMPLETADO
            log_proceso.fecha_fin = datetime.utcnow()
            log_proceso.duracion_segundos = (log_proceso.fecha_fin - log_proceso.fecha_inicio).seconds
            log_proceso.registros_procesados = stats_modelo.get('total_registros', 0)
            log_proceso.metricas = {
                'movimientos_modelo': stats_modelo.get('total_movimientos', 0),
                'gruas_utilizadas': len(stats_modelo.get('gruas_activas', [])), 
                'bloques_visitados': len(stats_modelo.get('bloques_visitados', []))
            }
            
            # Commit final
            await self.db.commit()
            
            # Log resumen
            self._log_summary(resultado_camila.id, stats_modelo, stats_instancia)
            
            return resultado_camila.id
            
        except Exception as e:
            await self.db.rollback()
            log_proceso.estado = EstadoProcesamiento.ERROR
            log_proceso.fecha_fin = datetime.utcnow()
            log_proceso.detalle_error = {'error': str(e), 'tipo': type(e).__name__}
            await self.db.commit()
            
            logger.error(f"❌ Error cargando Camila: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def _create_or_update_resultado(
        self, fecha_inicio: datetime, semana: int, anio: int,
        turno: int, participacion: int, con_dispersion: bool,
        resultado_filepath: str, instancia_filepath: Optional[str],
        flujos_real_filepath: Optional[str]
    ) -> ResultadoCamila:
        """Crea o actualiza un resultado de Camila"""
        
        # Calcular día y turno del día
        dia = ((turno - 1) // 3) + 1
        turno_del_dia = ((turno - 1) % 3) + 1
        
        # Calcular fecha/hora específica del turno
        hora_inicio = {1: 8, 2: 16, 3: 0}[turno_del_dia]
        fecha_turno = fecha_inicio + timedelta(days=dia-1, hours=hora_inicio)
        fecha_fin_turno = fecha_turno + timedelta(hours=8)
        
        # Generar código único
        fecha_str = fecha_inicio.strftime('%Y%m%d')
        dispersion_str = 'K' if con_dispersion else 'N'
        codigo = f"{fecha_str}_{participacion}_{dispersion_str}_T{turno:02d}"
        
        # Buscar resultado existente
        query = select(ResultadoCamila).where(ResultadoCamila.codigo == codigo)
        result = await self.db.execute(query)
        resultado = result.scalar_one_or_none()
        
        if resultado:
            logger.info(f"Actualizando resultado existente: {resultado.id}")
            # Limpiar datos anteriores
            await self._delete_resultado_data(resultado.id)
            resultado.fecha_procesamiento = datetime.utcnow()
            resultado.estado = EstadoProcesamiento.PROCESANDO
        else:
            logger.info("Creando nuevo resultado")
            resultado = ResultadoCamila(
                codigo=codigo,
                fecha_inicio=fecha_turno,
                fecha_fin=fecha_fin_turno,
                anio=anio,
                semana=semana,
                dia=dia,
                turno=turno,
                turno_del_dia=turno_del_dia,
                participacion=participacion,
                con_dispersion=con_dispersion,
                estado=EstadoProcesamiento.PROCESANDO,
                archivo_resultado=Path(resultado_filepath).name if resultado_filepath else None,
                archivo_instancia=Path(instancia_filepath).name if instancia_filepath else None,
                archivo_flujos_real=Path(flujos_real_filepath).name if flujos_real_filepath else None
            )
            self.db.add(resultado)
            await self.db.flush()
        
        logger.info(f"Resultado ID: {resultado.id}, Código: {codigo}")
        return resultado
    
    async def _delete_resultado_data(self, resultado_id: UUID):
        """Elimina datos anteriores de un resultado"""
        logger.info(f"Eliminando datos anteriores del resultado {resultado_id}")
        
        await self.db.execute(delete(AsignacionGrua).where(AsignacionGrua.resultado_id == resultado_id))
        await self.db.execute(delete(CuotaCamion).where(CuotaCamion.resultado_id == resultado_id))
        await self.db.execute(delete(MetricaGrua).where(MetricaGrua.resultado_id == resultado_id))
        await self.db.execute(delete(ComparacionReal).where(ComparacionReal.resultado_id == resultado_id))
        await self.db.execute(delete(FlujoModelo).where(FlujoModelo.resultado_id == resultado_id))
        await self.db.flush()
    
    async def _load_parametros(self, filepath: str):
        """Carga parámetros del modelo desde la instancia"""
        logger.info("Cargando parámetros del modelo...")
        
        try:
            xl = pd.ExcelFile(filepath)
            
            parametros_map = {
                'mu': ('Tiempo de servicio', 'minutos'),
                'W': ('Ventana de colisión', 'grúas'),
                'K': ('Duración mínima asignación', 'periodos'),
                'Rmax': ('Máximo grúas activas', 'grúas')
            }
            
            for param_code, (descripcion, unidad) in parametros_map.items():
                if param_code in xl.sheet_names:
                    df = pd.read_excel(xl, param_code, header=None)
                    if len(df) > 1:
                        valor = float(df.iloc[1, 0])
                        self.parametros_cache[param_code] = valor
                        
                        # Actualizar o crear parámetro en BD
                        query = select(ParametroCamila).where(ParametroCamila.codigo == param_code)
                        result = await self.db.execute(query)
                        param = result.scalar_one_or_none()
                        
                        if not param:
                            param = ParametroCamila(
                                codigo=param_code,
                                descripcion=descripcion,
                                valor_default=valor,
                                valor_actual=valor,
                                unidad=unidad
                            )
                            self.db.add(param)
                        else:
                            param.valor_actual = valor
                            param.fecha_actualizacion = datetime.utcnow()
            
            await self.db.flush()
            logger.info(f"Parámetros cargados: {list(self.parametros_cache.keys())}")
            
        except Exception as e:
            logger.warning(f"Error cargando parámetros: {e}")
    
    async def _load_resultado_file(self, filepath: str, resultado_id: UUID) -> Dict[str, Any]:
        """Carga archivo de resultados de Camila (output del modelo)"""
        
        logger.info("Cargando archivo de resultados del modelo...")
        
        try:
            df = pd.read_excel(filepath, header=None, names=['var', 'idx', 'val'])
            logger.info(f"Archivo con {len(df)} filas")
            
            stats = {
                'total_registros': len(df),
                'total_movimientos': 0,
                'bloques_visitados': [],  # Cambiar a lista
                'segregaciones_atendidas': [],  # Cambiar a lista
                'periodos_activos': [],  # Cambiar a lista
                'gruas_activas': [],  # Cambiar a lista
                'asignaciones_grua': {},
                'flujos_por_tipo': {'fr': 0, 'fe': 0, 'fc': 0, 'fd': 0}
            }
            
            batch_flujos = []
            batch_asignaciones = []
            asignaciones_dict = {}  # Para acumular por grúa-bloque-periodo
            
            for idx, row in df.iterrows():
                try:
                    if pd.isna(row['var']) or pd.isna(row['val']):
                        continue
                    
                    var_name = str(row['var']).strip()
                    var_index = str(row['idx']).strip()
                    var_value = float(row['val'])
                    
                    if var_value == 0:
                        continue
                    
                    # Procesar flujos (fr_sbt, fe_sbt, fc_sbt, fd_sbt)
                    if var_name in ['fr_sbt', 'fe_sbt', 'fc_sbt', 'fd_sbt']:
                        match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", var_index)
                        if match:
                            segregacion = match.group(1).upper()  # s1 -> S1
                            bloque = match.group(2).upper().replace('B', 'C')  # b1 -> C1
                            periodo = int(match.group(3))
                            
                            # Determinar tipo de operación
                            tipo_map = {
                                'fr': TipoOperacion.RECEPCION,
                                'fe': TipoOperacion.ENTREGA,
                                'fc': TipoOperacion.CARGA,
                                'fd': TipoOperacion.DESCARGA
                            }
                            tipo_flujo = var_name.split('_')[0]
                            
                            flujo = FlujoModelo(
                                resultado_id=resultado_id,
                                tipo_flujo=tipo_flujo,
                                segregacion_codigo=segregacion,
                                bloque_codigo=bloque,
                                periodo=periodo,
                                cantidad=int(var_value),
                                tipo_operacion=tipo_map[tipo_flujo]
                            )
                            batch_flujos.append(flujo)
                            
                            if bloque not in stats['bloques_visitados']:
                                stats['bloques_visitados'].append(bloque)
                            if segregacion not in stats['segregaciones_atendidas']:
                                stats['segregaciones_atendidas'].append(segregacion)
                            if periodo not in stats['periodos_activos']:
                                stats['periodos_activos'].append(periodo)
                            
                            stats['total_movimientos'] += int(var_value)
                            stats['flujos_por_tipo'][tipo_flujo] += int(var_value)
                    
                    # Procesar asignaciones de grúas (ygbt)
                    elif var_name == 'ygbt' and var_value == 1:
                        match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", var_index)
                        if match:
                            grua = match.group(1).upper()  # g1 -> G1
                            grua_id = int(grua.replace('G', ''))
                            bloque = match.group(2).upper().replace('B', 'C')  # b1 -> C1
                            periodo = int(match.group(3))
                            
                            key = (grua_id, bloque, periodo)
                            if key not in asignaciones_dict:
                                asignaciones_dict[key] = {
                                    'asignada': False,
                                    'activada': False,
                                    'movimientos': 0
                                }
                            asignaciones_dict[key]['asignada'] = True
                            
                            # Usar append con verificación en lugar de add
                            if grua_id not in stats['gruas_activas']:
                                stats['gruas_activas'].append(grua_id)
                            
                            stats['asignaciones_grua'][key] = True
                    
                    # Procesar activaciones (alpha_gbt)
                    elif var_name == 'alpha_gbt' and var_value == 1:
                        match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", var_index)
                        if match:
                            grua = match.group(1).upper()
                            grua_id = int(grua.replace('G', ''))
                            bloque = match.group(2).upper().replace('B', 'C')
                            periodo = int(match.group(3))
                            
                            key = (grua_id, bloque, periodo)
                            if key not in asignaciones_dict:
                                asignaciones_dict[key] = {
                                    'asignada': False,
                                    'activada': False,
                                    'movimientos': 0
                                }
                            asignaciones_dict[key]['activada'] = True
                    
                except Exception as e:
                    logger.warning(f"Error en fila {idx}: {str(e)}")
            
            # Guardar flujos
            if batch_flujos:
                self.db.add_all(batch_flujos)
                await self.db.flush()
            
            # Calcular movimientos por asignación basándose en los flujos
            for flujo in batch_flujos:
                # Buscar qué grúas podrían haber atendido este flujo
                for (grua_id, bloque, periodo), asig_data in asignaciones_dict.items():
                    if bloque == flujo.bloque_codigo and periodo == flujo.periodo and asig_data['asignada']:
                        asig_data['movimientos'] += flujo.cantidad
            
            # Crear asignaciones
            for (grua_id, bloque, periodo), asig_data in asignaciones_dict.items():
                asignacion = AsignacionGrua(
                    resultado_id=resultado_id,
                    grua_id=grua_id,
                    bloque_codigo=bloque,
                    periodo=periodo,
                    asignada=asig_data['asignada'],
                    activada=asig_data['activada'],
                    movimientos_asignados=asig_data['movimientos'],
                    tipo_asignacion=TipoAsignacion.REGULAR
                )
                batch_asignaciones.append(asignacion)
            
            if batch_asignaciones:
                self.db.add_all(batch_asignaciones)
                await self.db.flush()
            
            # Calcular cuotas basadas en asignaciones
            await self._calculate_truck_quotas(resultado_id, batch_asignaciones)
            
            logger.info(f"Resultados cargados: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando resultados: {e}")
            raise
    
    async def _load_instancia_file(self, filepath: str, resultado_id: UUID) -> Dict[str, Any]:
        """Carga archivo de instancia de Camila"""
        
        logger.info("Cargando archivo de instancia...")
        
        try:
            xl = pd.ExcelFile(filepath)
            stats = {
                'parametros_cargados': len(self.parametros_cache),
                'demanda_total': 0,
                'gruas_disponibles': 0,
                'bloques_disponibles': 0,
                'capacidad_teorica_turno': 0
            }
            
            # Contar recursos disponibles
            if 'G' in xl.sheet_names:
                df_g = pd.read_excel(xl, 'G', header=None)
                stats['gruas_disponibles'] = len(df_g) - 1  # Menos el header
            
            if 'B' in xl.sheet_names:
                df_b = pd.read_excel(xl, 'B', header=None)
                stats['bloques_disponibles'] = len(df_b) - 1
            
            # Cargar demanda
            if 'DMEst' in xl.sheet_names:
                df_dme = pd.read_excel(xl, 'DMEst')
                demanda_e = df_dme['DMEst'].sum() if 'DMEst' in df_dme.columns else 0
                stats['demanda_total'] += demanda_e
            
            if 'DMIst' in xl.sheet_names:
                df_dmi = pd.read_excel(xl, 'DMIst')
                demanda_i = df_dmi['DMIst'].sum() if 'DMIst' in df_dmi.columns else 0
                stats['demanda_total'] += demanda_i
            
            # Calcular capacidad teórica
            mu = self.parametros_cache.get('mu', 30)
            gruas = stats['gruas_disponibles']
            periodos = 8
            stats['capacidad_teorica_turno'] = int((60 / mu) * gruas * periodos)
            
            logger.info(f"Instancia cargada: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando instancia: {e}")
            return {}
    
    async def _calculate_truck_quotas(self, resultado_id: UUID, asignaciones: List[AsignacionGrua]):
        """Calcula cuotas de camiones basadas en las asignaciones de grúas"""
        
        logger.info("Calculando cuotas de camiones...")
        
        # Obtener flujos para calcular cuotas reales
        flujos_result = await self.db.execute(
            select(FlujoModelo).where(FlujoModelo.resultado_id == resultado_id)
        )
        flujos = flujos_result.scalars().all()
        
        # Agrupar flujos por periodo y bloque
        cuotas_por_periodo_bloque = {}
        
        for flujo in flujos:
            key = (flujo.periodo, flujo.bloque_codigo)
            if key not in cuotas_por_periodo_bloque:
                cuotas_por_periodo_bloque[key] = {
                    'cantidad_total': 0,
                    'segregaciones': set(),
                    'tipos_operacion': set()
                }
            cuotas_por_periodo_bloque[key]['cantidad_total'] += flujo.cantidad
            cuotas_por_periodo_bloque[key]['segregaciones'].add(flujo.segregacion_codigo)
            cuotas_por_periodo_bloque[key]['tipos_operacion'].add(flujo.tipo_operacion)
        
        # Contar grúas asignadas por periodo-bloque
        gruas_por_periodo_bloque = {}
        for asig in asignaciones:
            if asig.asignada:
                key = (asig.periodo, asig.bloque_codigo)
                if key not in gruas_por_periodo_bloque:
                    gruas_por_periodo_bloque[key] = 0
                gruas_por_periodo_bloque[key] += 1
        
        # Crear cuotas
        batch_cuotas = []
        mu = self.parametros_cache.get('mu', 30)
        
        for (periodo, bloque), data in cuotas_por_periodo_bloque.items():
            gruas_asignadas = gruas_por_periodo_bloque.get((periodo, bloque), 0)
            capacidad_maxima = int((60 / mu) * gruas_asignadas) if gruas_asignadas > 0 else 0
            
            # Determinar tipo de operación predominante
            if len(data['tipos_operacion']) == 1:
                tipo_op = list(data['tipos_operacion'])[0]
            else:
                tipo_op = TipoOperacion.MIXTO
            
            cuota = CuotaCamion(
                resultado_id=resultado_id,
                periodo=periodo,
                bloque_codigo=bloque,
                cuota_modelo=data['cantidad_total'],
                capacidad_maxima=capacidad_maxima,
                gruas_asignadas=gruas_asignadas,
                tipo_operacion=tipo_op,
                segregaciones_incluidas=list(data['segregaciones'])
            )
            batch_cuotas.append(cuota)
        
        # Agregar periodos/bloques sin movimientos pero con grúas asignadas
        for (periodo, bloque), gruas in gruas_por_periodo_bloque.items():
            if (periodo, bloque) not in cuotas_por_periodo_bloque and gruas > 0:
                capacidad_maxima = int((60 / mu) * gruas)
                cuota = CuotaCamion(
                    resultado_id=resultado_id,
                    periodo=periodo,
                    bloque_codigo=bloque,
                    cuota_modelo=0,
                    capacidad_maxima=capacidad_maxima,
                    gruas_asignadas=gruas,
                    tipo_operacion=TipoOperacion.MIXTO,
                    segregaciones_incluidas=[]
                )
                batch_cuotas.append(cuota)
        
        if batch_cuotas:
            self.db.add_all(batch_cuotas)
            await self.db.flush()
        
        logger.info(f"✓ Calculadas {len(batch_cuotas)} cuotas de camiones")
    
    async def _calculate_model_metrics(
        self, resultado_id: UUID, stats_modelo: Dict, stats_instancia: Dict
    ):
        """Calcula métricas del modelo"""
        
        logger.info("Calculando métricas del modelo...")
        
        # Obtener datos necesarios
        asig_result = await self.db.execute(
            select(AsignacionGrua).where(
                and_(
                    AsignacionGrua.resultado_id == resultado_id,
                    AsignacionGrua.asignada == True
                )
            )
        )
        asignaciones = asig_result.scalars().all()
        
        cuotas_result = await self.db.execute(
            select(CuotaCamion).where(CuotaCamion.resultado_id == resultado_id)
        )
        cuotas = cuotas_result.scalars().all()
        
        # Calcular métricas agregadas
        total_movimientos = stats_modelo.get('total_movimientos', 0)
        gruas_utilizadas = len(stats_modelo.get('gruas_activas', []))
        bloques_visitados = len(stats_modelo.get('bloques_visitados', []))
        
        # Calcular capacidad y utilización
        capacidad_total = sum(c.capacidad_maxima for c in cuotas)
        if capacidad_total == 0:
            # Usar capacidad teórica si no hay cuotas
            capacidad_total = stats_instancia.get('capacidad_teorica_turno', 480)
        
        utilizacion = (total_movimientos / capacidad_total * 100) if capacidad_total > 0 else 0
        
        # Calcular distribución de movimientos por grúa
        movimientos_por_grua = {}
        for asig in asignaciones:
            if asig.grua_id not in movimientos_por_grua:
                movimientos_por_grua[asig.grua_id] = 0
            movimientos_por_grua[asig.grua_id] += asig.movimientos_asignados
        
        # Calcular coeficiente de variación
        if movimientos_por_grua:
            valores = list(movimientos_por_grua.values())
            if len(valores) > 1 and sum(valores) > 0:
                promedio = np.mean(valores)
                desviacion = np.std(valores)
                cv = (desviacion / promedio * 100) if promedio > 0 else 0
            else:
                cv = 0
        else:
            cv = 0
        
        # Crear métricas por grúa
        gruas_con_datos = set(movimientos_por_grua.keys())
        
        for grua_id in range(1, 13):  # Siempre 12 grúas
            # Contar bloques y periodos para esta grúa
            asig_grua = [a for a in asignaciones if a.grua_id == grua_id]
            bloques_grua = len(set(a.bloque_codigo for a in asig_grua))
            periodos_grua = len(set(a.periodo for a in asig_grua))
            movimientos_grua = movimientos_por_grua.get(grua_id, 0)
            
            # Calcular tiempos
            mu = self.parametros_cache.get('mu', 30)
            tiempo_productivo = (movimientos_grua * mu) / 60  # horas
            tiempo_total = 8  # turno de 8 horas
            tiempo_improductivo = max(0, tiempo_total - tiempo_productivo)
            utilizacion_grua = (tiempo_productivo / tiempo_total * 100) if tiempo_total > 0 else 0
            
            metrica = MetricaGrua(
                resultado_id=resultado_id,
                grua_id=grua_id,
                movimientos_modelo=movimientos_grua,
                bloques_visitados=bloques_grua,
                periodos_activa=periodos_grua,
                cambios_bloque=max(0, bloques_grua - 1),  # Simplificado
                tiempo_productivo_hrs=round(tiempo_productivo, 2),
                tiempo_improductivo_hrs=round(tiempo_improductivo, 2),
                utilizacion_pct=round(utilizacion_grua, 2)
            )
            self.db.add(metrica)
        
        # Actualizar resultado principal
        resultado = await self.db.get(ResultadoCamila, resultado_id)
        resultado.total_movimientos_modelo = total_movimientos
        resultado.total_gruas_utilizadas = gruas_utilizadas
        resultado.total_bloques_visitados = bloques_visitados
        resultado.total_segregaciones = len(stats_modelo.get('segregaciones_atendidas', []))
        resultado.capacidad_teorica = capacidad_total
        resultado.utilizacion_modelo = round(utilizacion, 2)
        resultado.coeficiente_variacion = round(cv, 2)
        
        await self.db.flush()
        logger.info(f"✓ Métricas calculadas: Movimientos={total_movimientos}, "
                   f"Utilización={utilizacion:.1f}%, CV={cv:.1f}%")
    
    async def _compare_with_reality(
        self, resultado_id: UUID, flujos_filepath: str, 
        fecha_inicio: datetime, turno: int
    ):
        """Compara resultados del modelo con datos reales"""
        
        logger.info(f"Comparando con datos reales del turno {turno}...")
        
        try:
            # Calcular rango de tiempo del turno
            dia = ((turno - 1) // 3) + 1
            turno_del_dia = ((turno - 1) % 3) + 1
            hora_inicio = {1: 8, 2: 16, 3: 0}[turno_del_dia]
            
            fecha_turno_inicio = fecha_inicio + timedelta(days=dia-1, hours=hora_inicio)
            fecha_turno_fin = fecha_turno_inicio + timedelta(hours=8)
            
            # Cargar flujos reales
            df_flujos = pd.read_excel(flujos_filepath)
            
            # Convertir columna de tiempo a datetime
            df_flujos['ime_time'] = pd.to_datetime(df_flujos['ime_time'])
            
            # Filtrar por turno y tipos de movimiento comparables
            tipos_comparables = ['RECV', 'DLVR', 'LOAD', 'DSCH']
            df_turno = df_flujos[
                (df_flujos['ime_time'] >= fecha_turno_inicio) &
                (df_flujos['ime_time'] < fecha_turno_fin) &
                (df_flujos['ime_move_kind'].isin(tipos_comparables))
            ].copy()
            
            logger.info(f"Movimientos reales en turno: {len(df_turno)}")
            
            # Mapear hora a periodo (1-8)
            df_turno['hora'] = df_turno['ime_time'].dt.hour
            df_turno['periodo'] = df_turno['hora'].apply(
                lambda h: ((h - hora_inicio) % 24) + 1 if h >= hora_inicio or hora_inicio == 0 else ((h + 24 - hora_inicio) % 24) + 1
            )
            
            # Procesar datos reales
            stats_real = await self._process_real_data(df_turno, resultado_id)
            
            # Crear comparaciones
            await self._create_comparisons(resultado_id, stats_real)
            
            # Actualizar cuotas con datos reales
            await self._update_quotas_with_real(resultado_id, df_turno)
            
            # Actualizar resultado principal
            resultado = await self.db.get(ResultadoCamila, resultado_id)
            resultado.total_movimientos_real = stats_real['total_movimientos']
            
            # Calcular accuracy
            if resultado.total_movimientos_modelo > 0 and stats_real['total_movimientos'] > 0:
                resultado.accuracy_global = round(
                    min(resultado.total_movimientos_modelo, stats_real['total_movimientos']) /
                    max(resultado.total_movimientos_modelo, stats_real['total_movimientos']) * 100,
                    2
                )
                resultado.brecha_movimientos = stats_real['total_movimientos'] - resultado.total_movimientos_modelo
            
            await self.db.flush()
            logger.info(f"✓ Comparación completada: Modelo={resultado.total_movimientos_modelo}, "
                       f"Real={stats_real['total_movimientos']}, "
                       f"Accuracy={resultado.accuracy_global}%")
            
        except Exception as e:
            logger.error(f"Error comparando con realidad: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    async def _process_real_data(self, df_turno: pd.DataFrame, resultado_id: UUID) -> Dict[str, Any]:
        """Procesa datos reales del turno"""
        
        stats = {
            'total_movimientos': len(df_turno),
            'por_periodo': {},
            'por_bloque': {},
            'por_tipo': {},
            'por_periodo_bloque': {}
        }
        
        # Agrupar por periodo
        for periodo in range(1, 9):
            df_periodo = df_turno[df_turno['periodo'] == periodo]
            stats['por_periodo'][periodo] = len(df_periodo)
        
        # Agrupar por bloque (mapear códigos)
        df_turno['bloque'] = df_turno['ime_fm'].apply(
            lambda x: x if x in ['C1','C2','C3','C4','C5','C6','C7','C8','C9'] else 'OTROS'
        )
        stats['por_bloque'] = df_turno.groupby('bloque').size().to_dict()
        
        # Agrupar por tipo
        stats['por_tipo'] = df_turno.groupby('ime_move_kind').size().to_dict()
        
        # Agrupar por periodo-bloque
        for (periodo, bloque), group in df_turno.groupby(['periodo', 'bloque']):
            if bloque != 'OTROS':
                stats['por_periodo_bloque'][(periodo, bloque)] = len(group)
        
        return stats
    
    async def _create_comparisons(self, resultado_id: UUID, stats_real: Dict):
        """Crea registros de comparación modelo vs real"""
        
        # Obtener datos del modelo
        flujos_result = await self.db.execute(
            select(FlujoModelo).where(FlujoModelo.resultado_id == resultado_id)
        )
        flujos_modelo = flujos_result.scalars().all()
        
        # Agrupar modelo por periodo
        modelo_por_periodo = {}
        for periodo in range(1, 9):
            modelo_por_periodo[periodo] = sum(
                f.cantidad for f in flujos_modelo if f.periodo == periodo
            )
        
        # Comparación general
        total_modelo = sum(f.cantidad for f in flujos_modelo)
        total_real = stats_real['total_movimientos']
        
        comp_general = ComparacionReal(
            resultado_id=resultado_id,
            tipo_comparacion='general',
            metrica='movimientos_totales',
            valor_modelo=float(total_modelo),
            valor_real=float(total_real),
            diferencia_absoluta=float(total_real - total_modelo),
            diferencia_porcentual=((total_real - total_modelo) / total_modelo * 100) if total_modelo > 0 else 0,
            accuracy=min(total_modelo, total_real) / max(total_modelo, total_real) * 100 if max(total_modelo, total_real) > 0 else 0,
            descripcion='Comparación de movimientos totales del turno'
        )
        self.db.add(comp_general)
        
        # Comparación por periodo
        for periodo in range(1, 9):
            val_modelo = modelo_por_periodo.get(periodo, 0)
            val_real = stats_real['por_periodo'].get(periodo, 0)
            
            if val_modelo > 0 or val_real > 0:
                comp_periodo = ComparacionReal(
                    resultado_id=resultado_id,
                    tipo_comparacion='por_periodo',
                    dimension=str(periodo),
                    metrica='movimientos',
                    valor_modelo=float(val_modelo),
                    valor_real=float(val_real),
                    diferencia_absoluta=float(val_real - val_modelo),
                    diferencia_porcentual=((val_real - val_modelo) / val_modelo * 100) if val_modelo > 0 else 0,
                    accuracy=min(val_modelo, val_real) / max(val_modelo, val_real) * 100 if max(val_modelo, val_real) > 0 else 0,
                    descripcion=f'Movimientos en periodo {periodo}'
                )
                self.db.add(comp_periodo)
        
        # Comparación por bloque
        modelo_por_bloque = {}
        for flujo in flujos_modelo:
            if flujo.bloque_codigo not in modelo_por_bloque:
                modelo_por_bloque[flujo.bloque_codigo] = 0
            modelo_por_bloque[flujo.bloque_codigo] += flujo.cantidad
        
        todos_bloques = set(list(modelo_por_bloque.keys()) + list(stats_real['por_bloque'].keys()))
        
        for bloque in todos_bloques:
            if bloque != 'OTROS':
                val_modelo = modelo_por_bloque.get(bloque, 0)
                val_real = stats_real['por_bloque'].get(bloque, 0)
                
                if val_modelo > 0 or val_real > 0:
                    comp_bloque = ComparacionReal(
                        resultado_id=resultado_id,
                        tipo_comparacion='por_bloque',
                        dimension=bloque,
                        metrica='movimientos',
                        valor_modelo=float(val_modelo),
                        valor_real=float(val_real),
                        diferencia_absoluta=float(val_real - val_modelo),
                        diferencia_porcentual=((val_real - val_modelo) / val_modelo * 100) if val_modelo > 0 else 0,
                        accuracy=min(val_modelo, val_real) / max(val_modelo, val_real) * 100 if max(val_modelo, val_real) > 0 else 0,
                        descripcion=f'Movimientos en bloque {bloque}'
                    )
                    self.db.add(comp_bloque)
        
        await self.db.flush()
        logger.info(f"✓ Creadas comparaciones: general + {len(modelo_por_periodo)} periodos + {len(todos_bloques)-1} bloques")
    
    async def _update_quotas_with_real(self, resultado_id: UUID, df_turno: pd.DataFrame):
        """Actualiza cuotas con datos reales"""
        
        # Obtener cuotas existentes
        cuotas_result = await self.db.execute(
            select(CuotaCamion).where(CuotaCamion.resultado_id == resultado_id)
        )
        cuotas = cuotas_result.scalars().all()
        
        # Mapear bloques
        df_turno['bloque'] = df_turno['ime_fm'].apply(
            lambda x: x if x in ['C1','C2','C3','C4','C5','C6','C7','C8','C9'] else None
        )
        
        # Actualizar cada cuota
        for cuota in cuotas:
            # Contar movimientos reales para este periodo-bloque
            movimientos_reales = len(
                df_turno[
                    (df_turno['periodo'] == cuota.periodo) &
                    (df_turno['bloque'] == cuota.bloque_codigo)
                ]
            )
            
            cuota.movimientos_reales = movimientos_reales
            if cuota.capacidad_maxima > 0:
                cuota.utilizacion_real = round(
                    (movimientos_reales / cuota.capacidad_maxima) * 100, 2
                )
        
        await self.db.flush()
    
    def _log_summary(self, resultado_id: UUID, stats_modelo: Dict, stats_instancia: Dict):
        """Log resumen de la carga"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA DE CAMILA")
        logger.info("="*80)
        logger.info(f"Resultado ID: {resultado_id}")
        
        logger.info("\n📋 Datos del modelo:")
        logger.info(f"  - Total movimientos: {stats_modelo.get('total_movimientos', 0)}")
        logger.info(f"  - Grúas activas: {len(stats_modelo.get('gruas_activas', []))}")
        logger.info(f"  - Bloques visitados: {len(stats_modelo.get('bloques_visitados', []))}")
        logger.info(f"  - Segregaciones: {len(stats_modelo.get('segregaciones_atendidas', []))}")
        logger.info(f"  - Periodos activos: {len(stats_modelo.get('periodos_activos', []))}")
        
        logger.info("\n📊 Distribución de flujos:")
        for tipo, cantidad in stats_modelo.get('flujos_por_tipo', {}).items():
            logger.info(f"  - {tipo}: {cantidad}")
        
        if stats_instancia:
            logger.info("\n🏗️ Capacidad:")
            logger.info(f"  - Capacidad teórica: {stats_instancia.get('capacidad_teorica_turno', 0)}")
            logger.info(f"  - Demanda total: {stats_instancia.get('demanda_total', 0)}")
        
        logger.info("="*80)