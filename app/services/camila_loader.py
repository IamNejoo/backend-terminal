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

from app.models.camila import *

logger = logging.getLogger(__name__)

class CamilaLoader:
    """Servicio para cargar datos del modelo Camila"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        
    async def load_camila_results(
        self,
        resultado_filepath: str,
        instancia_filepath: Optional[str],
        magdalena_resultado_filepath: Optional[str],
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
        
        try:
            # Crear o actualizar resultado de Camila
            resultado_camila = await self._create_or_update_resultado(
                fecha_inicio, semana, anio, turno, participacion, con_dispersion
            )
            
            # Cargar archivo de resultado
            stats_resultado = await self._load_resultado_file(resultado_filepath, resultado_camila.id)
            
            # Cargar archivo de instancia si existe
            stats_instancia = {}
            if instancia_filepath and Path(instancia_filepath).exists():
                stats_instancia = await self._load_instancia_file(instancia_filepath, resultado_camila.id)
            
            # Cargar información de Magdalena si existe
            stats_magdalena = {}
            if magdalena_resultado_filepath and Path(magdalena_resultado_filepath).exists():
                stats_magdalena = await self._load_magdalena_info(
                    magdalena_resultado_filepath, resultado_camila.id, turno
                )
            
            # Calcular métricas
            await self._calculate_metrics(resultado_camila.id, stats_resultado, stats_magdalena)
            
            # Calcular comparaciones si hay datos de Magdalena
            if stats_magdalena:
                await self._calculate_comparisons(resultado_camila.id, stats_magdalena)
            
            # Actualizar estado
            resultado_camila.estado = 'completado'
            resultado_camila.fecha_procesamiento = datetime.utcnow()
            
            # Commit final
            await self.db.commit()
            
            # Log resumen
            self._log_summary(resultado_camila.id, stats_resultado, stats_instancia, stats_magdalena)
            
            return resultado_camila.id
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"❌ Error cargando Camila: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    
    async def _create_or_update_resultado(
        self, fecha_inicio: datetime, semana: int, anio: int,
        turno: int, participacion: int, con_dispersion: bool
    ) -> ResultadoCamila:
        """Crea o actualiza un resultado de Camila"""
        
        # Calcular día basado en fecha
        dia = ((turno - 1) // 3) + 1
        turno_del_dia = ((turno - 1) % 3) + 1
        
        # Calcular fecha/hora específica del turno
        hora_inicio = {1: 8, 2: 16, 3: 0}[turno_del_dia]
        fecha_turno = fecha_inicio + timedelta(days=dia-1, hours=hora_inicio)
        
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
        else:
            logger.info("Creando nuevo resultado")
            resultado = ResultadoCamila(
                codigo=codigo,
                fecha_inicio=fecha_turno,
                fecha_fin=fecha_turno + timedelta(hours=8),
                anio=anio,
                semana=semana,
                dia=dia,
                turno=turno,
                turno_del_dia=turno_del_dia,
                participacion=participacion,
                con_dispersion=con_dispersion,
                estado='procesando',
                fecha_procesamiento=datetime.utcnow()
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
        await self.db.execute(delete(ComparacionCamila).where(ComparacionCamila.resultado_id == resultado_id))
        await self.db.flush()
    
    async def _load_resultado_file(self, filepath: str, resultado_id: UUID) -> Dict[str, Any]:
        """Carga archivo de resultados de Camila"""
        
        logger.info("Cargando archivo de resultados de Camila...")
        
        try:
            df = pd.read_excel(filepath, header=None)
            logger.info(f"Archivo con {len(df)} filas")
            
            stats = {
                'total_asignaciones': 0,
                'bloques_visitados': set(),
                'segregaciones_atendidas': set(),
                'periodos_activos': set(),
                'frecuencias_totales': 0
            }
            
            batch_asignaciones = []
            batch_cuotas = []
            
            for idx, row in df.iterrows():
                try:
                    if len(row) < 3:
                        continue
                    
                    var_name = str(row[0]).strip()
                    var_index = str(row[1]).strip()
                    var_value = float(row[2]) if pd.notna(row[2]) else 0
                    
                    if var_value == 0:
                        continue
                    
                    # Procesar variable fr_sbt (frecuencia segregación-bloque-tiempo)
                    if var_name == 'fr_sbt':
                        # Parsear índice: ('s8', 'b1', 4)
                        match = re.match(r"\('([^']+)',\s*'([^']+)',\s*(\d+)\)", var_index)
                        if match:
                            segregacion = match.group(1)
                            bloque = match.group(2)
                            periodo = int(match.group(3))
                            
                            # Mapear códigos
                            bloque_codigo = bloque.upper().replace('B', 'C')  # b1 -> C1
                            segregacion_codigo = segregacion.upper()  # s8 -> S8
                            
                            asignacion = AsignacionGrua(
                                resultado_id=resultado_id,
                                segregacion_codigo=segregacion_codigo,
                                bloque_codigo=bloque_codigo,
                                periodo=periodo,
                                frecuencia=int(var_value),
                                tipo_asignacion='regular'
                            )
                            batch_asignaciones.append(asignacion)
                            
                            stats['total_asignaciones'] += 1
                            stats['bloques_visitados'].add(bloque_codigo)
                            stats['segregaciones_atendidas'].add(segregacion_codigo)
                            stats['periodos_activos'].add(periodo)
                            stats['frecuencias_totales'] += int(var_value)
                    
                    # Aquí podrías procesar otras variables si las hay
                    
                except Exception as e:
                    logger.warning(f"Error en fila {idx}: {str(e)}")
            
            # Guardar en batches
            if batch_asignaciones:
                self.db.add_all(batch_asignaciones)
                await self.db.flush()
            
            # Calcular cuotas de camiones basadas en las asignaciones
            await self._calculate_truck_quotas(resultado_id, batch_asignaciones)
            
            logger.info(f"Resultados cargados: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando resultados: {e}")
            raise
    
    async def _load_instancia_file(self, filepath: str, resultado_id: UUID) -> Dict[str, Any]:
        """Carga archivo de instancia de Camila"""
        
        logger.info("Cargando archivo de instancia de Camila...")
        
        try:
            xl = pd.ExcelFile(filepath)
            stats = {
                'parametros_cargados': 0,
                'demanda_total': 0,
                'gruas_disponibles': 0
            }
            
            # Cargar parámetros básicos
            if 'mu' in xl.sheet_names:
                df_mu = pd.read_excel(xl, 'mu', header=None)
                if len(df_mu) > 1:
                    productividad = float(df_mu.iloc[1, 0])
                    stats['productividad'] = productividad
                    stats['parametros_cargados'] += 1
            
            if 'W' in xl.sheet_names:
                df_w = pd.read_excel(xl, 'W', header=None)
                if len(df_w) > 1:
                    ventana_tiempo = float(df_w.iloc[1, 0])
                    stats['ventana_tiempo'] = ventana_tiempo
                    stats['parametros_cargados'] += 1
            
            if 'G' in xl.sheet_names:
                df_g = pd.read_excel(xl, 'G', header=None)
                stats['gruas_disponibles'] = len(df_g) - 1  # Menos el header
            
            # Cargar demanda
            if 'DMEst' in xl.sheet_names:
                df_dme = pd.read_excel(xl, 'DMEst')
                demanda_e = df_dme['DMEst'].sum() if 'DMEst' in df_dme.columns else 0
                stats['demanda_total'] += demanda_e
            
            if 'DMIst' in xl.sheet_names:
                df_dmi = pd.read_excel(xl, 'DMIst')
                demanda_i = df_dmi['DMIst'].sum() if 'DMIst' in df_dmi.columns else 0
                stats['demanda_total'] += demanda_i
            
            logger.info(f"Instancia cargada: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando instancia: {e}")
            return stats
    
    async def _load_magdalena_info(
        self, filepath: str, resultado_id: UUID, turno: int
    ) -> Dict[str, Any]:
        """Carga información relevante de Magdalena para el turno específico"""
        
        logger.info(f"Cargando información de Magdalena para turno {turno}...")
        
        try:
            xl = pd.ExcelFile(filepath)
            stats = {
                'movimientos_magdalena': 0,
                'bloques_magdalena': set(),
                'segregaciones_magdalena': set(),
                'volumen_total': 0
            }
            
            # Cargar hoja General
            if 'General' in xl.sheet_names:
                df_general = pd.read_excel(xl, 'General')
                
                # Filtrar por periodo (turno)
                df_turno = df_general[df_general['Periodo'] == turno]
                
                for idx, row in df_turno.iterrows():
                    recepcion = int(row.get('Recepción', 0))
                    carga = int(row.get('Carga', 0))
                    descarga = int(row.get('Descarga', 0))
                    entrega = int(row.get('Entrega', 0))
                    
                    total_mov = recepcion + carga + descarga + entrega
                    if total_mov > 0:
                        stats['movimientos_magdalena'] += total_mov
                        stats['bloques_magdalena'].add(str(row.get('Bloque', '')))
                        stats['segregaciones_magdalena'].add(str(row.get('Segregación', '')))
                        stats['volumen_total'] += int(row.get('Volumen (TEUs)', 0))
                
                # Guardar resumen para comparación
                stats['df_magdalena_turno'] = df_turno
            
            logger.info(f"Magdalena turno {turno}: {stats['movimientos_magdalena']} movimientos")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando Magdalena: {e}")
            return {}
    
    async def _calculate_truck_quotas(self, resultado_id: UUID, asignaciones: List[AsignacionGrua]):
        """Calcula cuotas de camiones basadas en las asignaciones de grúas"""
        
        logger.info("Calculando cuotas de camiones...")
        
        # Agrupar por periodo y bloque
        cuotas_por_periodo = {}
        
        for asig in asignaciones:
            key = (asig.periodo, asig.bloque_codigo)
            if key not in cuotas_por_periodo:
                cuotas_por_periodo[key] = {
                    'frecuencia_total': 0,
                    'segregaciones': set()
                }
            cuotas_por_periodo[key]['frecuencia_total'] += asig.frecuencia
            cuotas_por_periodo[key]['segregaciones'].add(asig.segregacion_codigo)
        
        # Crear cuotas
        batch_cuotas = []
        for (periodo, bloque), data in cuotas_por_periodo.items():
            # Asumiendo productividad de 30 mov/hora y ventana de 2 horas
            capacidad_periodo = 30 * 2  # 60 movimientos por ventana
            
            cuota = CuotaCamion(
                resultado_id=resultado_id,
                periodo=periodo,
                bloque_codigo=bloque,
                ventana_inicio=periodo,
                ventana_fin=periodo,
                cuota_camiones=min(data['frecuencia_total'], capacidad_periodo),
                capacidad_maxima=capacidad_periodo,
                tipo_operacion='mixto',
                segregaciones_incluidas=list(data['segregaciones'])
            )
            batch_cuotas.append(cuota)
        
        if batch_cuotas:
            self.db.add_all(batch_cuotas)
            await self.db.flush()
        
        logger.info(f"✓ Calculadas {len(batch_cuotas)} cuotas de camiones")
    
    async def _calculate_metrics(
        self, resultado_id: UUID, stats_resultado: Dict, stats_magdalena: Dict
    ):
        """Calcula métricas de desempeño"""
        
        logger.info("Calculando métricas de Camila...")
        
        # Obtener asignaciones
        asig_result = await self.db.execute(
            select(AsignacionGrua).where(AsignacionGrua.resultado_id == resultado_id)
        )
        asignaciones = asig_result.scalars().all()
        
        if not asignaciones:
            logger.warning("No hay asignaciones para calcular métricas")
            return
        
        # Métricas por grúa (asumiendo distribución equitativa entre 12 grúas)
        num_gruas = 12
        bloques_visitados = len(stats_resultado.get('bloques_visitados', set()))
        
        # Calcular utilización
        movimientos_totales = stats_magdalena.get('movimientos_magdalena', 0)
        capacidad_turno = num_gruas * 30 * 8  # 12 grúas * 30 mov/h * 8 horas
        utilizacion = (movimientos_totales / capacidad_turno * 100) if capacidad_turno > 0 else 0
        
        # Balance de trabajo (distribución entre bloques)
        movimientos_por_bloque = {}
        for asig in asignaciones:
            if asig.bloque_codigo not in movimientos_por_bloque:
                movimientos_por_bloque[asig.bloque_codigo] = 0
            movimientos_por_bloque[asig.bloque_codigo] += asig.frecuencia
        
        if movimientos_por_bloque:
            valores = list(movimientos_por_bloque.values())
            promedio = np.mean(valores)
            desviacion = np.std(valores)
            cv = (desviacion / promedio * 100) if promedio > 0 else 0
        else:
            cv = 0
        
        # Crear métricas agregadas
        for i in range(1, num_gruas + 1):
            metrica = MetricaGrua(
                resultado_id=resultado_id,
                grua_id=i,
                movimientos_asignados=movimientos_totales // num_gruas,
                bloques_visitados=bloques_visitados,
                tiempo_productivo_hrs=8 * (utilizacion / 100),
                tiempo_improductivo_hrs=8 * (1 - utilizacion / 100),
                utilizacion_pct=utilizacion,
                distancia_recorrida_m=0  # Calcular si tenemos datos
            )
            self.db.add(metrica)
        
        # Guardar métricas generales en el resultado
        resultado = await self.db.get(ResultadoCamila, resultado_id)
        resultado.total_gruas = num_gruas
        resultado.total_movimientos = movimientos_totales
        resultado.utilizacion_promedio = utilizacion
        resultado.coeficiente_variacion = cv
        
        await self.db.flush()
        logger.info(f"✓ Métricas calculadas: Utilización {utilizacion:.1f}%, CV {cv:.1f}%")
    
    async def _calculate_comparisons(self, resultado_id: UUID, stats_magdalena: Dict):
        """Calcula comparaciones con Magdalena"""
        
        logger.info("Calculando comparaciones con Magdalena...")
        
        if 'df_magdalena_turno' not in stats_magdalena:
            logger.warning("No hay datos de Magdalena para comparar")
            return
        
        df_magdalena = stats_magdalena['df_magdalena_turno']
        
        # Obtener asignaciones de Camila
        asig_result = await self.db.execute(
            select(AsignacionGrua).where(AsignacionGrua.resultado_id == resultado_id)
        )
        asignaciones = asig_result.scalars().all()
        
        # Crear diccionario de asignaciones Camila
        camila_por_bloque = {}
        for asig in asignaciones:
            if asig.bloque_codigo not in camila_por_bloque:
                camila_por_bloque[asig.bloque_codigo] = 0
            camila_por_bloque[asig.bloque_codigo] += asig.frecuencia
        
        # Crear diccionario de movimientos Magdalena
        magdalena_por_bloque = {}
        for idx, row in df_magdalena.iterrows():
            bloque = str(row.get('Bloque', ''))
            total_mov = sum([
                int(row.get('Recepción', 0)),
                int(row.get('Carga', 0)),
                int(row.get('Descarga', 0)),
                int(row.get('Entrega', 0))
            ])
            if bloque not in magdalena_por_bloque:
                magdalena_por_bloque[bloque] = 0
            magdalena_por_bloque[bloque] += total_mov
        
        # Crear comparaciones
        batch_comparaciones = []
        
        # Comparación general
        total_magdalena = sum(magdalena_por_bloque.values())
        total_camila = sum(camila_por_bloque.values())
        
        comp_general = ComparacionCamila(
            resultado_id=resultado_id,
            tipo_comparacion='general',
            metrica='movimientos_totales',
            valor_magdalena=float(total_magdalena),
            valor_camila=float(total_camila),
            diferencia=float(total_camila - total_magdalena),
            porcentaje_diferencia=((total_camila - total_magdalena) / total_magdalena * 100) if total_magdalena > 0 else 0,
            descripcion='Comparación de movimientos totales del turno'
        )
        batch_comparaciones.append(comp_general)
        
        # Comparación por bloque
        todos_bloques = set(list(magdalena_por_bloque.keys()) + list(camila_por_bloque.keys()))
        
        for bloque in todos_bloques:
            val_magdalena = magdalena_por_bloque.get(bloque, 0)
            val_camila = camila_por_bloque.get(bloque, 0)
            
            comp_bloque = ComparacionCamila(
                resultado_id=resultado_id,
                tipo_comparacion='por_bloque',
                metrica=f'movimientos_{bloque}',
                valor_magdalena=float(val_magdalena),
                valor_camila=float(val_camila),
                diferencia=float(val_camila - val_magdalena),
                porcentaje_diferencia=((val_camila - val_magdalena) / val_magdalena * 100) if val_magdalena > 0 else 0,
                descripcion=f'Movimientos en bloque {bloque}'
            )
            batch_comparaciones.append(comp_bloque)
        
        # Balance de carga
        cv_magdalena = np.std(list(magdalena_por_bloque.values())) / np.mean(list(magdalena_por_bloque.values())) * 100 if magdalena_por_bloque else 0
        cv_camila = np.std(list(camila_por_bloque.values())) / np.mean(list(camila_por_bloque.values())) * 100 if camila_por_bloque else 0
        
        comp_balance = ComparacionCamila(
            resultado_id=resultado_id,
            tipo_comparacion='balance',
            metrica='coeficiente_variacion',
            valor_magdalena=cv_magdalena,
            valor_camila=cv_camila,
            diferencia=cv_camila - cv_magdalena,
            porcentaje_diferencia=((cv_camila - cv_magdalena) / cv_magdalena * 100) if cv_magdalena > 0 else 0,
            descripcion='Balance de carga entre bloques (CV%)'
        )
        batch_comparaciones.append(comp_balance)
        
        if batch_comparaciones:
            self.db.add_all(batch_comparaciones)
            await self.db.flush()
        
        logger.info(f"✓ Creadas {len(batch_comparaciones)} comparaciones")
    
    def _log_summary(self, resultado_id: UUID, stats_resultado: Dict, 
                     stats_instancia: Dict, stats_magdalena: Dict):
        """Log resumen de la carga"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA DE CAMILA")
        logger.info("="*80)
        logger.info(f"Resultado ID: {resultado_id}")
        
        logger.info("\n📋 Datos cargados:")
        logger.info(f"  - Asignaciones: {stats_resultado.get('total_asignaciones', 0)}")
        logger.info(f"  - Bloques visitados: {len(stats_resultado.get('bloques_visitados', set()))}")
        logger.info(f"  - Segregaciones: {len(stats_resultado.get('segregaciones_atendidas', set()))}")
        logger.info(f"  - Períodos activos: {len(stats_resultado.get('periodos_activos', set()))}")
        
        if stats_magdalena:
            logger.info("\n🔗 Integración con Magdalena:")
            logger.info(f"  - Movimientos Magdalena: {stats_magdalena.get('movimientos_magdalena', 0)}")
            logger.info(f"  - Volumen total: {stats_magdalena.get('volumen_total', 0)} TEUs")
        
        logger.info("="*80)