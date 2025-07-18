# app/services/optimization_loader.py - VERSIÓN CORREGIDA COMPLETA
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, and_, func, update
from sqlalchemy.sql import text
import logging
from uuid import UUID
import re
import json
from pathlib import Path

from app.models.optimization import *

logger = logging.getLogger(__name__)

class OptimizationLoader:
    """Servicio para cargar datos del modelo de optimización"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        self._distancias_cache = {}
        self.distancias_modelo_filepath = None
        
    async def load_optimization_results(
        self,
        resultado_filepath: str,
        instancia_filepath: Optional[str],
        flujos_filepath: Optional[str],
        distancias_filepath: Optional[str],
        fecha_inicio: datetime,
        semana: int,
        anio: int,
        participacion: int,
        con_dispersion: bool
    ) -> UUID:
        """Carga completa de resultados de optimización"""
        
        # Guardar referencia al archivo de distancias del modelo
        self.distancias_modelo_filepath = distancias_filepath
        
        logger.info(f"{'='*80}")
        logger.info(f"Iniciando carga de optimización")
        logger.info(f"Resultado: {resultado_filepath}")
        logger.info(f"Config: Año {anio}, Semana {semana}, P{participacion}, Disp={'K' if con_dispersion else 'N'}")
        
        try:
            # Crear o actualizar instancia
            instancia = await self._create_or_update_instancia(
                fecha_inicio, semana, anio, participacion, con_dispersion
            )
            
            # Cargar bloques y segregaciones base si no existen
            await self._ensure_base_data()
            
            # Cargar archivo de resultado - MEJORADO para leer capacidades
            stats_resultado = await self._load_resultado_file(resultado_filepath, instancia.id)
            
            # Cargar archivo de instancia si existe
            stats_instancia = {}
            if instancia_filepath and Path(instancia_filepath).exists():
                stats_instancia = await self._load_instancia_file(instancia_filepath, instancia.id)
            
            # Cargar flujos reales si existen
            stats_flujos = {}
            if flujos_filepath and Path(flujos_filepath).exists():
                stats_flujos = await self._load_flujos_file(flujos_filepath, instancia.id)
            
            # Cargar distancias si existen
            if distancias_filepath and Path(distancias_filepath).exists():
                await self._load_distancias_file(distancias_filepath)
            
            # Calcular KPIs comparativos - CORREGIDO
            kpis_stats = await self._calculate_kpis(instancia.id)
            
            # Calcular métricas temporales
            await self._calculate_temporal_metrics(instancia.id)
            
            # Actualizar resultados generales - MEJORADO
            await self._update_resultados_generales(
                instancia.id, stats_resultado, stats_flujos, kpis_stats
            )
            
            # Registrar log de procesamiento
            await self._log_procesamiento(
                instancia.id, 
                resultado_filepath, 
                'resultado',
                stats_resultado.get('total_registros', 0),
                'completado'
            )
            
            # Commit final
            await self.db.commit()
            
            # Log resumen
            self._log_summary(instancia.id, stats_resultado, stats_flujos, kpis_stats)
            
            return instancia.id
            
        except Exception as e:
            await self.db.rollback()
            logger.error(f"❌ Error cargando optimización: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
    async def _debug_distancias_movimientos(self, instancia_id: UUID):
        """Método temporal para debuggear por qué no se encuentran distancias"""
        
        logger.info("\n=== DEBUGGING DE DISTANCIAS ===")
        
        # 1. Verificar qué distancias hay en la base de datos
        dist_result = await self.db.execute(
            select(DistanciaReal).limit(20)
        )
        distancias_muestra = dist_result.scalars().all()
        
        logger.info(f"\nMuestra de distancias en BD:")
        for d in distancias_muestra[:10]:
            logger.info(f"  {d.origen} → {d.destino}: {d.distancia_metros}m")
        
        # 2. Verificar qué movimientos reales hay
        movs_result = await self.db.execute(
            select(MovimientoReal)
            .where(MovimientoReal.instancia_id == instancia_id)
            .limit(20)
        )
        movimientos_muestra = movs_result.scalars().all()
        
        logger.info(f"\nMuestra de movimientos reales:")
        for m in movimientos_muestra[:10]:
            logger.info(f"  {m.tipo_movimiento}: {m.bloque_origen} → {m.bloque_destino}")
        
        # 3. Intentar encontrar coincidencias manualmente
        logger.info(f"\nIntentando encontrar coincidencias:")
        for m in movimientos_muestra[:5]:
            if m.bloque_origen and m.bloque_destino:
                origen_norm = self._normalizar_ubicacion(m.bloque_origen)
                destino_norm = self._normalizar_ubicacion(m.bloque_destino)
                
                # Buscar distancia directa
                dist_result = await self.db.execute(
                    select(DistanciaReal).where(
                        and_(
                            DistanciaReal.origen == origen_norm,
                            DistanciaReal.destino == destino_norm
                        )
                    )
                )
                dist_directa = dist_result.scalar_one_or_none()
                
                # Buscar distancia inversa
                dist_result_inv = await self.db.execute(
                    select(DistanciaReal).where(
                        and_(
                            DistanciaReal.origen == destino_norm,
                            DistanciaReal.destino == origen_norm
                        )
                    )
                )
                dist_inversa = dist_result_inv.scalar_one_or_none()
                
                logger.info(f"\n  Movimiento: {m.bloque_origen} → {m.bloque_destino}")
                logger.info(f"  Normalizado: {origen_norm} → {destino_norm}")
                logger.info(f"  Distancia directa: {dist_directa.distancia_metros if dist_directa else 'NO ENCONTRADA'}")
                logger.info(f"  Distancia inversa: {dist_inversa.distancia_metros if dist_inversa else 'NO ENCONTRADA'}")
        
        # 4. Verificar conteo total de distancias
        count_result = await self.db.execute(
            select(func.count(DistanciaReal.id))
        )
        total_distancias = count_result.scalar()
        
        logger.info(f"\nTotal de distancias en BD: {total_distancias}")
        logger.info("=== FIN DEBUGGING ===\n")   
    async def _calculate_kpis(self, instancia_id: UUID) -> Dict[str, Any]:
        """Calcula KPIs comparativos incluyendo distancias - VERSIÓN CORREGIDA"""
        
        logger.info("Calculando KPIs con distancias...")
        await self._debug_distancias_movimientos(instancia_id)

        kpis = {}
        
        try:
            # 1. KPIs de movimientos
            movs_real = await self.db.execute(
                select(
                    func.count(MovimientoReal.id).label('total'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'YARD', Integer)).label('yard'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'DLVR', Integer)).label('dlvr'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'LOAD', Integer)).label('load'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'RECV', Integer)).label('recv'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'DSCH', Integer)).label('dsch')
                ).where(MovimientoReal.instancia_id == instancia_id)
            )
            real_stats = movs_real.one()
            
            # Guardar stats detalladas
            kpis['movimientos_recv_real'] = real_stats.recv or 0
            kpis['movimientos_dsch_real'] = real_stats.dsch or 0
            
            # 2. Leer distancias del modelo desde archivo
            distancia_modelo_total = 0
            distancia_modelo_load = 0
            distancia_modelo_dlvr = 0
            movimientos_dlvr_modelo = 0
            movimientos_load_modelo = 0
            
            if self.distancias_modelo_filepath and Path(self.distancias_modelo_filepath).exists():
                logger.info(f"Leyendo distancias del modelo desde: {self.distancias_modelo_filepath}")
                xl = pd.ExcelFile(self.distancias_modelo_filepath)
                
                # Leer resumen semanal
                if 'Resumen Semanal' in xl.sheet_names:
                    df_resumen = pd.read_excel(xl, 'Resumen Semanal')
                    if len(df_resumen) > 0:
                        distancia_modelo_total = int(df_resumen.iloc[0]['Distancia Total'])
                        distancia_modelo_load = int(df_resumen.iloc[0]['Distancia LOAD'])
                        distancia_modelo_dlvr = int(df_resumen.iloc[0]['Distancia DLVR'])
                        movimientos_dlvr_modelo = int(df_resumen.iloc[0]['Movimientos_DLVR'])
                        movimientos_load_modelo = int(df_resumen.iloc[0]['Movimientos_LOAD'])
                        
                        logger.info(f"Distancias del modelo cargadas:")
                        logger.info(f"  - Total: {distancia_modelo_total:,} m")
                        logger.info(f"  - LOAD: {distancia_modelo_load:,} m")
                        logger.info(f"  - DLVR: {distancia_modelo_dlvr:,} m")
                        logger.info(f"  - Movimientos DLVR: {movimientos_dlvr_modelo}")
                        logger.info(f"  - Movimientos LOAD: {movimientos_load_modelo}")
            
            # 3. Calcular distancias reales históricas
            logger.info("Calculando distancias reales...")
            
            # Cargar todas las distancias a memoria
            dist_result = await self.db.execute(select(DistanciaReal))
            todas_distancias = dist_result.scalars().all()
            
            # Crear mapa de distancias
            mapa_distancias = {}
            for d in todas_distancias:
                mapa_distancias[f"{d.origen}_{d.destino}"] = d.distancia_metros
            
            # Obtener movimientos reales
            movs_result = await self.db.execute(
                select(MovimientoReal).where(MovimientoReal.instancia_id == instancia_id)
            )
            movimientos = movs_result.scalars().all()
            
            # Calcular distancias por tipo de movimiento
            distancias_por_tipo = {
                'YARD': 0,
                'DLVR': 0,
                'RECV': 0,
                'LOAD': 0,
                'DSCH': 0,
                'SHFT': 0,
                'OTHR': 0
            }
            
            distancia_total_real = 0
            movimientos_con_distancia = 0
            movimientos_sin_distancia = 0
            
            # Análisis de movimientos sin distancia
            movimientos_sin_distancia_detalle = {}
            
            for mov in movimientos:
                if mov.bloque_origen and mov.bloque_destino:
                    # Normalizar ubicaciones
                    origen = self._normalizar_ubicacion(mov.bloque_origen)
                    destino = self._normalizar_ubicacion(mov.bloque_destino)
                    
                    # Buscar distancia
                    key = f"{origen}_{destino}"
                    distancia = mapa_distancias.get(key, 0)
                    
                    # Si no se encuentra, intentar invertida
                    if distancia == 0:
                        key_inv = f"{destino}_{origen}"
                        distancia = mapa_distancias.get(key_inv, 0)
                    
                    if distancia > 0:
                        distancia_total_real += distancia
                        if mov.tipo_movimiento in distancias_por_tipo:
                            distancias_por_tipo[mov.tipo_movimiento] += distancia
                        movimientos_con_distancia += 1
                    else:
                        movimientos_sin_distancia += 1
                        tipo_key = f"{mov.tipo_movimiento}_{origen}_{destino}"
                        if tipo_key not in movimientos_sin_distancia_detalle:
                            movimientos_sin_distancia_detalle[tipo_key] = 0
                        movimientos_sin_distancia_detalle[tipo_key] += 1
            
            logger.info(f"Movimientos con distancia encontrada: {movimientos_con_distancia}")
            logger.info(f"Movimientos sin distancia: {movimientos_sin_distancia}")
            if movimientos_sin_distancia_detalle:
                logger.warning(f"Tipos de movimientos sin distancia:")
                for tipo, count in list(movimientos_sin_distancia_detalle.items())[:10]:
                    logger.warning(f"  - {tipo}: {count} movimientos")
            logger.info(f"Distancia total real: {distancia_total_real:,} m")
            
            # 4. Calcular métricas correctamente - CORREGIDO
            
            # Movimientos operativos (solo YARD, DLVR, LOAD)
            movimientos_operativos_real = real_stats.yard + real_stats.dlvr + real_stats.load
            movimientos_operativos_modelo = movimientos_dlvr_modelo + movimientos_load_modelo
            
            # Si no tenemos movimientos del modelo, usar estimación
            if movimientos_operativos_modelo == 0:
                movimientos_operativos_modelo = real_stats.dlvr + real_stats.load
            
            # Reducción de movimientos operativos
            reduccion_movimientos = movimientos_operativos_real - movimientos_operativos_modelo
            porcentaje_reduccion_movimientos = (reduccion_movimientos / movimientos_operativos_real * 100) if movimientos_operativos_real > 0 else 0
            
            # Si no tenemos distancias del modelo, estimarlas
            if distancia_modelo_total == 0:
                # Estimar: modelo elimina YARD completamente
                distancia_modelo_total = distancia_total_real - distancias_por_tipo['YARD']
                distancia_modelo_load = distancias_por_tipo['LOAD']
                distancia_modelo_dlvr = distancias_por_tipo['DLVR']
            
            # Distancia ahorrada
            distancia_ahorrada = distancia_total_real - distancia_modelo_total
            
            # Eficiencia basada en DISTANCIAS
            eficiencia_ganada = (distancia_ahorrada / distancia_total_real * 100) if distancia_total_real > 0 else 0
            
            # Crear KPIs de movimientos - CORREGIDO
            kpis_movimientos = [
                {
                    'categoria': 'movimientos',
                    'metrica': 'movimientos_operativos_total',
                    'valor_real': movimientos_operativos_real,
                    'valor_modelo': movimientos_operativos_modelo,
                    'diferencia': reduccion_movimientos,
                    'porcentaje_mejora': porcentaje_reduccion_movimientos,
                    'unidad': 'movimientos'
                },
                {
                    'categoria': 'movimientos',
                    'metrica': 'yard_eliminados',
                    'valor_real': real_stats.yard,
                    'valor_modelo': 0,
                    'diferencia': real_stats.yard,
                    'porcentaje_mejora': 100,
                    'unidad': 'movimientos'
                },
                {
                    'categoria': 'movimientos',
                    'metrica': 'dlvr',
                    'valor_real': real_stats.dlvr,
                    'valor_modelo': movimientos_dlvr_modelo,
                    'diferencia': real_stats.dlvr - movimientos_dlvr_modelo,
                    'porcentaje_mejora': ((real_stats.dlvr - movimientos_dlvr_modelo) / real_stats.dlvr * 100) if real_stats.dlvr > 0 else 0,
                    'unidad': 'movimientos'
                },
                {
                    'categoria': 'movimientos',
                    'metrica': 'load',
                    'valor_real': real_stats.load,
                    'valor_modelo': movimientos_load_modelo,
                    'diferencia': real_stats.load - movimientos_load_modelo,
                    'porcentaje_mejora': ((real_stats.load - movimientos_load_modelo) / real_stats.load * 100) if real_stats.load > 0 else 0,
                    'unidad': 'movimientos'
                }
            ]
            
            # KPIs de distancias
            kpis_distancias = [
                {
                    'categoria': 'distancia',
                    'metrica': 'distancia_total',
                    'valor_real': distancia_total_real,
                    'valor_modelo': distancia_modelo_total,
                    'diferencia': distancia_ahorrada,
                    'porcentaje_mejora': eficiencia_ganada,
                    'unidad': 'metros'
                },
                {
                    'categoria': 'distancia',
                    'metrica': 'distancia_yard',
                    'valor_real': distancias_por_tipo['YARD'],
                    'valor_modelo': 0,
                    'diferencia': distancias_por_tipo['YARD'],
                    'porcentaje_mejora': 100,
                    'unidad': 'metros'
                },
                {
                    'categoria': 'distancia',
                    'metrica': 'distancia_load',
                    'valor_real': distancias_por_tipo['LOAD'],
                    'valor_modelo': distancia_modelo_load,
                    'diferencia': distancias_por_tipo['LOAD'] - distancia_modelo_load,
                    'porcentaje_mejora': ((distancias_por_tipo['LOAD'] - distancia_modelo_load) / distancias_por_tipo['LOAD'] * 100) if distancias_por_tipo['LOAD'] > 0 else 0,
                    'unidad': 'metros'
                },
                {
                    'categoria': 'distancia',
                    'metrica': 'distancia_dlvr',
                    'valor_real': distancias_por_tipo['DLVR'],
                    'valor_modelo': distancia_modelo_dlvr,
                    'diferencia': distancias_por_tipo['DLVR'] - distancia_modelo_dlvr,
                    'porcentaje_mejora': ((distancias_por_tipo['DLVR'] - distancia_modelo_dlvr) / distancias_por_tipo['DLVR'] * 100) if distancias_por_tipo['DLVR'] > 0 else 0,
                    'unidad': 'metros'
                }
            ]
            
            # KPI de eficiencia
            kpis_eficiencia = [
                {
                    'categoria': 'eficiencia',
                    'metrica': 'eficiencia_distancia',
                    'valor_real': 0,
                    'valor_modelo': eficiencia_ganada,
                    'diferencia': eficiencia_ganada,
                    'porcentaje_mejora': eficiencia_ganada,
                    'unidad': 'porcentaje'
                },
                {
                    'categoria': 'eficiencia',
                    'metrica': 'reduccion_movimientos_operativos',
                    'valor_real': 0,
                    'valor_modelo': porcentaje_reduccion_movimientos,
                    'diferencia': porcentaje_reduccion_movimientos,
                    'porcentaje_mejora': porcentaje_reduccion_movimientos,
                    'unidad': 'porcentaje'
                }
            ]
            
            # Guardar todos los KPIs
            for kpi_data in kpis_movimientos + kpis_distancias + kpis_eficiencia:
                kpi = KPIComparativo(instancia_id=instancia_id, **kpi_data)
                self.db.add(kpi)
            
            # Actualizar el diccionario de retorno
            kpis.update({
                'movimientos_real': real_stats.total,
                'movimientos_operativos_real': movimientos_operativos_real,
                'movimientos_operativos_modelo': movimientos_operativos_modelo,
                'movimientos_yard': real_stats.yard,
                'movimientos_modelo': movimientos_dlvr_modelo + movimientos_load_modelo,
                'movimientos_dlvr_real': real_stats.dlvr,
                'movimientos_load_real': real_stats.load,
                'movimientos_recv_real': real_stats.recv,
                'movimientos_dsch_real': real_stats.dsch,
                'movimientos_dlvr_modelo': movimientos_dlvr_modelo,
                'movimientos_load_modelo': movimientos_load_modelo,
                'reduccion_movimientos': reduccion_movimientos,
                'porcentaje_reduccion_movimientos': porcentaje_reduccion_movimientos,
                'distancia_total_real': distancia_total_real,
                'distancia_total_modelo': distancia_modelo_total,
                'distancia_yard': distancias_por_tipo['YARD'],
                'distancia_load_real': distancias_por_tipo['LOAD'],
                'distancia_load_modelo': distancia_modelo_load,
                'distancia_dlvr_real': distancias_por_tipo['DLVR'],
                'distancia_dlvr_modelo': distancia_modelo_dlvr,
                'distancia_ahorrada': distancia_ahorrada,
                'eficiencia_ganada': eficiencia_ganada,
                'distancias_por_tipo': distancias_por_tipo
            })
            
            # 5. Otros KPIs (ocupación, segregaciones)
            ocupacion_result = await self.db.execute(
                select(
                    func.avg(OcupacionBloque.porcentaje_ocupacion).label('promedio'),
                    func.max(OcupacionBloque.porcentaje_ocupacion).label('maxima'),
                    func.min(OcupacionBloque.porcentaje_ocupacion).label('minima')
                ).where(OcupacionBloque.instancia_id == instancia_id)
            )
            ocupacion_stats = ocupacion_result.one()
            
            segs_result = await self.db.execute(
                select(func.count(func.distinct(MovimientoModelo.segregacion_id)))
                .where(MovimientoModelo.instancia_id == instancia_id)
            )
            segregaciones_optimizadas = segs_result.scalar() or 0
            
            kpis['ocupacion_promedio'] = float(ocupacion_stats.promedio or 0)
            kpis['ocupacion_maxima'] = float(ocupacion_stats.maxima or 0)
            kpis['ocupacion_minima'] = float(ocupacion_stats.minima or 0)
            kpis['segregaciones_optimizadas'] = segregaciones_optimizadas
            
            await self.db.flush()
            
            logger.info(f"KPIs calculados:")
            logger.info(f"  - Movimientos operativos real: {movimientos_operativos_real}")
            logger.info(f"  - Movimientos operativos modelo: {movimientos_operativos_modelo}")
            logger.info(f"  - Reducción movimientos: {porcentaje_reduccion_movimientos:.1f}%")
            logger.info(f"  - Distancia total real: {distancia_total_real:,} m")
            logger.info(f"  - Distancia total modelo: {distancia_modelo_total:,} m")
            logger.info(f"  - Distancia ahorrada: {distancia_ahorrada:,} m")
            logger.info(f"  - Eficiencia ganada: {eficiencia_ganada:.2f}%")
            
            return kpis
            
        except Exception as e:
            logger.error(f"Error calculando KPIs: {e}")
            raise

    async def _update_resultados_generales(
        self, instancia_id: UUID, stats_resultado: Dict,
        stats_flujos: Dict, kpis: Dict
    ):
        """Actualiza tabla de resultados generales con distancias - VERSIÓN MEJORADA"""
        
        logger.info("Actualizando resultados generales con distancias...")
        
        # Obtener totales de segregaciones
        segs_result = await self.db.execute(
            select(func.count(func.distinct(Segregacion.id)))
        )
        total_segregaciones = segs_result.scalar() or 0
        
        # Obtener carga de trabajo total
        carga_result = await self.db.execute(
            select(
                func.sum(CargaTrabajo.carga_trabajo).label('total'),
                func.max(CargaTrabajo.carga_trabajo).label('maxima'),
                func.min(CargaTrabajo.carga_trabajo).label('minima')
            )
            .where(CargaTrabajo.instancia_id == instancia_id)
        )
        carga_stats = carga_result.one()
        
        # Calcular capacidad total actualizada
        capacidad_result = await self.db.execute(
            select(func.sum(Bloque.capacidad_teus))
        )
        capacidad_total = capacidad_result.scalar() or 0
        
        resultado = ResultadoGeneral(
            instancia_id=instancia_id,
            # Movimientos
            movimientos_reales_total=stats_flujos.get('total_movimientos', 0),
            movimientos_yard_real=kpis.get('movimientos_yard', 0),
            movimientos_dlvr_real=kpis.get('movimientos_dlvr_real', 0),
            movimientos_load_real=kpis.get('movimientos_load_real', 0),
            movimientos_recv_real=kpis.get('movimientos_recv_real', 0),
            movimientos_dsch_real=kpis.get('movimientos_dsch_real', 0),
            movimientos_optimizados=kpis.get('movimientos_modelo', 0),
            movimientos_dlvr_modelo=kpis.get('movimientos_dlvr_modelo', 0),
            movimientos_load_modelo=kpis.get('movimientos_load_modelo', 0),
            movimientos_reduccion=kpis.get('reduccion_movimientos', 0),
            movimientos_reduccion_pct=kpis.get('porcentaje_reduccion_movimientos', 0),
            
            # Distancias
            distancia_real_total=kpis.get('distancia_total_real', 0),
            distancia_real_load=kpis.get('distancia_load_real', 0),
            distancia_real_dlvr=kpis.get('distancia_dlvr_real', 0),
            distancia_real_yard=kpis.get('distancia_yard', 0),
            distancia_modelo_total=kpis.get('distancia_total_modelo', 0),
            distancia_modelo_load=kpis.get('distancia_load_modelo', 0),
            distancia_modelo_dlvr=kpis.get('distancia_dlvr_modelo', 0),
            distancia_reduccion=kpis.get('distancia_ahorrada', 0),
            distancia_reduccion_pct=kpis.get('eficiencia_ganada', 0),
            
            # Eficiencia
            eficiencia_real=100 - (kpis.get('movimientos_yard', 0) / stats_flujos.get('total_movimientos', 1) * 100) if stats_flujos.get('total_movimientos', 0) > 0 else 0,
            eficiencia_modelo=100,  # Sin YARD
            eficiencia_ganancia=kpis.get('eficiencia_ganada', 0),
            
            # Segregaciones
            segregaciones_total=total_segregaciones,
            segregaciones_optimizadas=kpis.get('segregaciones_optimizadas', 0),
            
            # Carga de trabajo
            carga_trabajo_total=carga_stats.total or 0,
            carga_maxima=carga_stats.maxima or 0,
            carga_minima=carga_stats.minima or 0,
            variacion_carga=stats_resultado.get('variacion_carga', 0),
            balance_carga=stats_resultado.get('balance_carga', 0),
            
            # Ocupación
            ocupacion_promedio_pct=kpis.get('ocupacion_promedio', 0),
            ocupacion_maxima_pct=kpis.get('ocupacion_maxima', 0),
            ocupacion_minima_pct=kpis.get('ocupacion_minima', 0),
            capacidad_total_teus=capacidad_total,
            
            # Metadata
            archivo_distancias_usado=Path(self.distancias_modelo_filepath).name if self.distancias_modelo_filepath else None
        )
        
        self.db.add(resultado)
        await self.db.flush()
        
        # Actualizar estado de instancia
        instancia_result = await self.db.execute(
            select(Instancia).where(Instancia.id == instancia_id)
        )
        instancia = instancia_result.scalar_one()
        instancia.estado = 'completado'
        instancia.total_movimientos = kpis.get('movimientos_modelo', 0)
        instancia.total_bloques = len(stats_resultado.get('bloques_activos', set()))
        instancia.total_segregaciones = kpis.get('segregaciones_optimizadas', 0)
        
        await self.db.flush()
        
        logger.info(f"Resultados actualizados:")
        logger.info(f"  - Movimientos optimizados: {kpis.get('movimientos_modelo', 0)}")
        logger.info(f"  - Reducción movimientos: {kpis.get('porcentaje_reduccion_movimientos', 0):.1f}%")
        logger.info(f"  - Distancia ahorrada: {kpis.get('distancia_ahorrada', 0):,} m")
        logger.info(f"  - Eficiencia ganada: {kpis.get('eficiencia_ganada', 0):.2f}%")

    async def _load_resultado_file(self, filepath: str, instancia_id: UUID) -> Dict[str, Any]:
        """Carga archivo de resultados del modelo - VERSIÓN MEJORADA CON CAPACIDADES"""
        
        logger.info("Cargando archivo de resultados...")
        
        try:
            xl = pd.ExcelFile(filepath)
            logger.info(f"Hojas disponibles: {xl.sheet_names}")
            
            stats = {
                'total_registros': 0,
                'movimientos_modelo': 0,
                'carga_trabajo': 0,
                'ocupacion': 0,
                'segregaciones': set(),
                'bloques_activos': set(),
                'variacion_carga': 0,
                'balance_carga': 0,
                'asignaciones_bloques': 0,
                'capacidades_actualizadas': False
            }
            
            # Obtener mapeo de bloques
            bloques_map = await self._get_bloques_map()
            
            # 0. NUEVO: Actualizar capacidades de bloques desde hoja Ocupación Bloques
            if 'Ocupación Bloques' in xl.sheet_names:
                df_ocupacion = pd.read_excel(xl, 'Ocupación Bloques')
                logger.info("Actualizando capacidades de bloques desde archivo...")
                
                # Obtener capacidades únicas por bloque
                capacidades_bloques = {}
                for idx, row in df_ocupacion.iterrows():
                    bloque_codigo = str(row.get('Bloque', '')).strip()
                    capacidad = row.get('Capacidad Bloque', 0)
                    
                    if bloque_codigo and capacidad > 0 and bloque_codigo not in capacidades_bloques:
                        capacidades_bloques[bloque_codigo] = int(capacidad)
                
                # Actualizar capacidades en base de datos
                for bloque_codigo, capacidad in capacidades_bloques.items():
                    if bloque_codigo in bloques_map:
                        await self.db.execute(
                            update(Bloque)
                            .where(Bloque.id == bloques_map[bloque_codigo])
                            .values(capacidad_teus=capacidad)
                        )
                        logger.info(f"  - {bloque_codigo}: {capacidad} TEUs")
                
                await self.db.flush()
                stats['capacidades_actualizadas'] = True
            
            # 1. Cargar hoja General (movimientos del modelo)
            if 'General' in xl.sheet_names:
                df_general = pd.read_excel(xl, 'General')
                logger.info(f"Procesando {len(df_general)} registros de General")
                
                batch = []
                for idx, row in df_general.iterrows():
                    try:
                        bloque_codigo = str(row.get('Bloque', '')).strip()
                        segregacion_codigo = str(row.get('Segregación', '')).strip()
                        
                        if bloque_codigo in bloques_map:
                            # Crear o obtener segregación
                            segregacion = await self._get_or_create_segregacion(segregacion_codigo)
                            
                            mov = MovimientoModelo(
                                instancia_id=instancia_id,
                                segregacion_id=segregacion.id,
                                bloque_id=bloques_map[bloque_codigo],
                                periodo=int(row.get('Periodo', 0)),
                                recepcion=int(row.get('Recepción', 0)),
                                carga=int(row.get('Carga', 0)),
                                descarga=int(row.get('Descarga', 0)),
                                entrega=int(row.get('Entrega', 0)),
                                volumen_teus=int(row.get('Volumen (TEUs)', 0)),
                                bahias_ocupadas=int(row.get('Bahías Ocupadas', 0))
                            )
                            batch.append(mov)
                            
                            total_mov = mov.recepcion + mov.carga + mov.descarga + mov.entrega
                            if total_mov > 0:
                                stats['movimientos_modelo'] += total_mov
                                stats['bloques_activos'].add(bloque_codigo)
                                stats['segregaciones'].add(segregacion_codigo)
                        
                        if len(batch) >= 100:
                            self.db.add_all(batch)
                            await self.db.flush()
                            batch = []
                            
                    except Exception as e:
                        logger.warning(f"Error en fila {idx} de General: {str(e)}")
                
                if batch:
                    self.db.add_all(batch)
                    await self.db.flush()
                
                stats['total_registros'] += len(df_general)
            
            # 2. Cargar Total bloques (asignaciones) - MEJORADO
            if 'Total bloques' in xl.sheet_names:
                df_bloques = pd.read_excel(xl, 'Total bloques')
                logger.info(f"Procesando asignaciones de bloques")
                
                for idx, row in df_bloques.iterrows():
                    try:
                        segregacion_codigo = str(row.get('Segregación', '')).strip()
                        total_bloques = int(row.get('Total bloques asignadas', 0))
                        
                        if total_bloques > 0:
                            segregacion = await self._get_or_create_segregacion(segregacion_codigo)
                            
                            # Obtener bloques asignados a esta segregación de la hoja General
                            bloques_asignados = await self._get_bloques_asignados(
                                instancia_id, segregacion.id
                            )
                            
                            asignacion = AsignacionBloque(
                                instancia_id=instancia_id,
                                segregacion_id=segregacion.id,
                                total_bloques_asignados=total_bloques,
                                bloques_codigos=list(bloques_asignados)
                            )
                            self.db.add(asignacion)
                            stats['asignaciones_bloques'] += 1
                            
                    except Exception as e:
                        logger.warning(f"Error en fila {idx} de Total bloques: {str(e)}")
                
                await self.db.flush()
            
            # 3. Cargar Workload bloques
            if 'Workload bloques' in xl.sheet_names:
                df_workload = pd.read_excel(xl, 'Workload bloques')
                logger.info(f"Procesando {len(df_workload)} registros de Workload")
                
                batch = []
                cargas = []
                cargas_por_periodo = {}
                
                for idx, row in df_workload.iterrows():
                    try:
                        bloque_codigo = str(row.get('Bloque', '')).strip()
                        periodo = int(row.get('Periodo', 0))
                        
                        if bloque_codigo in bloques_map:
                            carga_valor = int(row.get('Carga de trabajo', 0))
                            carga = CargaTrabajo(
                                instancia_id=instancia_id,
                                bloque_id=bloques_map[bloque_codigo],
                                periodo=periodo,
                                carga_trabajo=carga_valor
                            )
                            batch.append(carga)
                            stats['carga_trabajo'] += carga_valor
                            cargas.append(carga_valor)
                            
                            # Agrupar por periodo para max/min
                            if periodo not in cargas_por_periodo:
                                cargas_por_periodo[periodo] = []
                            cargas_por_periodo[periodo].append(carga_valor)
                        
                        if len(batch) >= 100:
                            self.db.add_all(batch)
                            await self.db.flush()
                            batch = []
                            
                    except Exception as e:
                        logger.warning(f"Error en fila {idx} de Workload: {str(e)}")
                
                if batch:
                    self.db.add_all(batch)
                    await self.db.flush()
                
                # Calcular balance de carga (desviación estándar)
                if cargas:
                    stats['balance_carga'] = int(np.std(cargas))
            
            # 4. NUEVO: Cargar Carga máx-min si existe
            if 'Carga máx-min' in xl.sheet_names:
                df_carga_maxmin = pd.read_excel(xl, 'Carga máx-min')
                logger.info("Procesando cargas máximas y mínimas por periodo")
                
                for idx, row in df_carga_maxmin.iterrows():
                    try:
                        periodo = int(row.get('Periodo', 0))
                        carga_max = int(row.get('Carga máxima', 0))
                        carga_min = int(row.get('Carga mínima', 0))
                        
                        # Actualizar registros de carga trabajo con max/min
                        await self.db.execute(
                            update(CargaTrabajo)
                            .where(and_(
                                CargaTrabajo.instancia_id == instancia_id,
                                CargaTrabajo.periodo == periodo
                            ))
                            .values(
                                carga_maxima=carga_max,
                                carga_minima=carga_min
                            )
                        )
                    except Exception as e:
                        logger.warning(f"Error en fila {idx} de Carga máx-min: {str(e)}")
                
                await self.db.flush()
            
            # 5. Cargar Contenedores Turno-Bloque (ocupación) - MEJORADO
            if 'Contenedores Turno-Bloque' in xl.sheet_names:
                df_contenedores = pd.read_excel(xl, 'Contenedores Turno-Bloque')
                logger.info(f"Procesando ocupación por turno-bloque")
                
                batch = []
                columnas_bloques = [col for col in df_contenedores.columns if col != 'Turno' and col in bloques_map]
                
                for idx, row in df_contenedores.iterrows():
                    try:
                        turno = int(row.get('Turno', 0))
                        periodo = turno
                        
                        for bloque_codigo in columnas_bloques:
                            contenedores = int(row.get(bloque_codigo, 0))
                            
                            # Obtener capacidad actualizada del bloque
                            bloque_result = await self.db.execute(
                                select(Bloque).where(Bloque.codigo == bloque_codigo)
                            )
                            bloque = bloque_result.scalar_one()
                            
                            porcentaje = (contenedores / bloque.capacidad_teus * 100) if bloque.capacidad_teus > 0 else 0
                            
                            ocupacion = OcupacionBloque(
                                instancia_id=instancia_id,
                                bloque_id=bloques_map[bloque_codigo],
                                periodo=periodo,
                                turno=((periodo - 1) % 3) + 1,
                                contenedores_teus=contenedores,
                                capacidad_bloque=bloque.capacidad_teus,
                                porcentaje_ocupacion=porcentaje,
                                estado='activo' if contenedores > 0 else 'inactivo'
                            )
                            batch.append(ocupacion)
                            stats['ocupacion'] += 1
                        
                        if len(batch) >= 100:
                            self.db.add_all(batch)
                            await self.db.flush()
                            batch = []
                            
                    except Exception as e:
                        logger.warning(f"Error en fila {idx} de Contenedores: {str(e)}")
                
                if batch:
                    self.db.add_all(batch)
                    await self.db.flush()
            
            # 6. Procesar hoja de Variación Carga de trabajo
            if 'Variación Carga de trabajo' in xl.sheet_names:
                try:
                    df_var = pd.read_excel(xl, 'Variación Carga de trabajo')
                    logger.info(f"Procesando hoja Variación Carga de trabajo")
                    
                    variacion_valor = None
                    
                    if len(df_var) > 0 and len(df_var.columns) > 0:
                        if len(df_var) > 1:
                            primer_valor = df_var.iloc[0, 0]
                            if isinstance(primer_valor, str) and 'variación' in primer_valor.lower():
                                variacion_valor = df_var.iloc[1, 0]
                            else:
                                variacion_valor = primer_valor
                        else:
                            variacion_valor = df_var.iloc[0, 0]
                    
                    if variacion_valor is not None and pd.notna(variacion_valor):
                        try:
                            stats['variacion_carga'] = int(float(str(variacion_valor)))
                            logger.info(f"✓ Variación de carga: {stats['variacion_carga']}")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"No se pudo convertir variación de carga a entero: {variacion_valor}")
                            stats['variacion_carga'] = 0
                    else:
                        logger.warning("No se encontró valor de variación de carga")
                        stats['variacion_carga'] = 0
                        
                except Exception as e:
                    logger.warning(f"Error procesando hoja Variación Carga de trabajo: {str(e)}")
                    stats['variacion_carga'] = 0
            
            await self.db.flush()
            
            logger.info(f"Resultado cargado: {stats}")
            logger.info(f"  - Capacidades actualizadas: {'Sí' if stats['capacidades_actualizadas'] else 'No'}")
            logger.info(f"  - Asignaciones de bloques: {stats['asignaciones_bloques']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando resultado: {e}")
            raise

    async def _get_bloques_asignados(self, instancia_id: UUID, segregacion_id: int) -> set:
        """Obtiene los bloques asignados a una segregación"""
        result = await self.db.execute(
            select(func.distinct(Bloque.codigo))
            .join(MovimientoModelo, MovimientoModelo.bloque_id == Bloque.id)
            .where(and_(
                MovimientoModelo.instancia_id == instancia_id,
                MovimientoModelo.segregacion_id == segregacion_id
            ))
        )
        return set([r[0] for r in result.all()])

    def _normalizar_ubicacion(self, ubicacion: str) -> str:
        """Normaliza códigos de ubicación para coincidencia en distancias"""
        ubicacion = str(ubicacion).strip().upper()
        
        # Mapeos comunes
        mapeos = {
            'GATE': 'GATE',
            'PUERTA': 'GATE',
            'SITIO1': 'SITIO_SUR',
            'SITIO 1': 'SITIO_SUR',
            'SITIO_1': 'SITIO_SUR',
            'SUR': 'SITIO_SUR',
            'SITIO2': 'SITIO_NORTE',
            'SITIO 2': 'SITIO_NORTE',
            'SITIO_2': 'SITIO_NORTE',
            'NORTE': 'SITIO_NORTE'
        }
        
        # Verificar mapeos
        for key, value in mapeos.items():
            if key in ubicacion:
                return value
        
        return ubicacion
    
    async def _create_or_update_instancia(self, fecha_inicio: datetime, semana: int, anio: int,
                                         participacion: int, con_dispersion: bool) -> Instancia:
        """Crea o actualiza una instancia"""
        
        # Calcular fecha fin (7 días después)
        fecha_fin = fecha_inicio + timedelta(days=6)
        
        # Generar código único
        fecha_str = fecha_inicio.strftime('%Y%m%d')
        dispersion_str = 'K' if con_dispersion else 'N'
        codigo = f"{fecha_str}_{participacion}_{dispersion_str}"
        
        # Buscar instancia existente
        query = select(Instancia).where(Instancia.codigo == codigo)
        result = await self.db.execute(query)
        instancia = result.scalar_one_or_none()
        
        if instancia:
            logger.info(f"Actualizando instancia existente: {instancia.id}")
            # Limpiar datos anteriores
            await self._delete_instancia_data(instancia.id)
            instancia.fecha_procesamiento = datetime.utcnow()
        else:
            logger.info("Creando nueva instancia")
            instancia = Instancia(
                codigo=codigo,
                fecha_inicio=fecha_inicio,
                fecha_fin=fecha_fin,
                anio=anio,
                semana=semana,
                escenario=f"Participación {participacion}%",
                participacion=participacion,
                con_dispersion=con_dispersion,
                periodos=21,
                dias=7,
                turnos_por_dia=3,
                estado='procesando',
                fecha_procesamiento=datetime.utcnow()
            )
            self.db.add(instancia)
            await self.db.flush()
        
        logger.info(f"Instancia ID: {instancia.id}, Código: {codigo}")
        return instancia
    
    async def _delete_instancia_data(self, instancia_id: UUID):
        """Elimina datos anteriores de una instancia"""
        logger.info(f"Eliminando datos anteriores de instancia {instancia_id}")
        
        await self.db.execute(delete(MovimientoReal).where(MovimientoReal.instancia_id == instancia_id))
        await self.db.execute(delete(MovimientoModelo).where(MovimientoModelo.instancia_id == instancia_id))
        await self.db.execute(delete(OcupacionBloque).where(OcupacionBloque.instancia_id == instancia_id))
        await self.db.execute(delete(CargaTrabajo).where(CargaTrabajo.instancia_id == instancia_id))
        await self.db.execute(delete(KPIComparativo).where(KPIComparativo.instancia_id == instancia_id))
        await self.db.execute(delete(MetricaTemporal).where(MetricaTemporal.instancia_id == instancia_id))
        await self.db.execute(delete(ResultadoGeneral).where(ResultadoGeneral.instancia_id == instancia_id))
        await self.db.execute(delete(AsignacionBloque).where(AsignacionBloque.instancia_id == instancia_id))
        await self.db.flush()
    
    async def _ensure_base_data(self):
        """Asegura que existan los datos base de bloques"""
        
        # Verificar si ya existen bloques
        result = await self.db.execute(select(func.count(Bloque.id)))
        count = result.scalar()
        
        if count == 0:
            logger.info("Creando bloques base...")
            
            # Capacidades por defecto basadas en los datos
            capacidades = {
                'C1': 1155, 'C2': 1225, 'C3': 1400, 'C4': 1400,
                'C5': 490, 'C6': 1015, 'C7': 1015, 'C8': 980, 'C9': 420
            }
            
            for codigo, capacidad in capacidades.items():
                bloque = Bloque(
                    codigo=codigo,
                    capacidad_teus=capacidad,
                    capacidad_bahias=35,  # Por defecto
                    capacidad_original=capacidad  # Guardar original
                )
                self.db.add(bloque)
            
            await self.db.flush()
            logger.info(f"✓ Creados {len(capacidades)} bloques")
    
    async def _load_instancia_file(self, filepath: str, instancia_id: UUID) -> Dict[str, Any]:
        """Carga archivo de instancia con parámetros"""
        
        logger.info("Cargando archivo de instancia...")
        
        try:
            xl = pd.ExcelFile(filepath)
            stats = {'parametros': 0, 'segregaciones_info': 0}
            
            # Cargar información de segregaciones si existe
            if 'S' in xl.sheet_names:
                df_s = pd.read_excel(xl, 'S')
                for idx, row in df_s.iterrows():
                    if pd.notna(row.iloc[0]):
                        codigo = str(row.iloc[0]).strip()
                        descripcion = str(row.iloc[1]).strip() if len(row) > 1 and pd.notna(row.iloc[1]) else ''
                        
                        # Actualizar segregación con descripción
                        segregacion = await self._get_or_create_segregacion(codigo, descripcion)
                        stats['segregaciones_info'] += 1
            
            logger.info(f"Instancia cargada: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando instancia: {e}")
            return {'parametros': 0, 'segregaciones_info': 0}
    
    async def _load_flujos_file(self, filepath: str, instancia_id: UUID) -> Dict[str, Any]:
        """Carga archivo de flujos reales"""
        
        logger.info("Cargando archivo de flujos reales...")
        
        try:
            df = pd.read_excel(filepath)
            logger.info(f"Procesando {len(df)} movimientos reales")
            
            stats = {
                'total_movimientos': 0,
                'yard': 0,
                'dlvr': 0,
                'recv': 0,
                'load': 0,
                'dsch': 0,
                'shft': 0,
                'othr': 0
            }
            
            batch = []
            batch_size = 500
            
            # Obtener fecha de la instancia
            instancia_result = await self.db.execute(
                select(Instancia).where(Instancia.id == instancia_id)
            )
            instancia = instancia_result.scalar_one()
            
            for idx, row in df.iterrows():
                try:
                    # Parsear fecha/hora
                    fecha_hora = pd.to_datetime(row.get('ime_time'))
                    
                    # Calcular día y turno relativos a la instancia
                    dias_diff = (fecha_hora.date() - instancia.fecha_inicio.date()).days
                    hora = fecha_hora.hour
                    
                    # Determinar turno (1: 8-15, 2: 15-23, 3: 23-8)
                    if 8 <= hora < 16:
                        turno = 1
                    elif 16 <= hora < 24:
                        turno = 2
                    else:  # 0-8
                        turno = 3
                    
                    # Calcular periodo (1-21)
                    periodo = dias_diff * 3 + turno
                    
                    tipo_mov = str(row.get('ime_move_kind', '')).upper()
                    
                    mov = MovimientoReal(
                        instancia_id=instancia_id,
                        fecha_hora=fecha_hora,
                        bloque_origen=str(row.get('ime_fm', '')),
                        bloque_destino=str(row.get('ime_to', '')),
                        tipo_movimiento=tipo_mov,
                        segregacion=str(row.get('criterio_iii', '')),
                        categoria=str(row.get('iu_category', '')),
                        contenedor_id=str(row.get('ime_ufv_gkey', '')),
                        turno=turno,
                        dia=dias_diff + 1,
                        periodo=periodo
                    )
                    batch.append(mov)
                    
                    stats['total_movimientos'] += 1
                    if tipo_mov in stats:
                        stats[tipo_mov.lower()] += 1
                    
                    if len(batch) >= batch_size:
                        self.db.add_all(batch)
                        await self.db.flush()
                        batch = []
                        
                except Exception as e:
                    logger.warning(f"Error en fila {idx} de flujos: {str(e)}")
            
            if batch:
                self.db.add_all(batch)
                await self.db.flush()
            
            logger.info(f"Flujos cargados: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando flujos: {e}")
            raise
    
    async def _load_distancias_file(self, filepath: str):
        """Carga archivo de distancias con TODAS las hojas - VERSIÓN CORREGIDA"""
        
        logger.info("Cargando archivo de distancias completo...")
        logger.info(f"Archivo: {filepath}")
        
        try:
            # Verificar si es archivo de Costanera o del modelo
            filename = Path(filepath).name
            es_costanera = 'Costanera' in filename
            
            xl = pd.ExcelFile(filepath)
            logger.info(f"Hojas de distancias disponibles: {xl.sheet_names}")
            logger.info(f"Es archivo Costanera: {'Sí' if es_costanera else 'No'}")
            
            distancias_cargadas = 0
            
            # Si es archivo Costanera, cargar TODAS las distancias
            if es_costanera:
                # 1. Cargar distancias entre bloques (hoja "Remanejo")
                if 'Remanejo' in xl.sheet_names:
                    df_remanejo = pd.read_excel(xl, 'Remanejo')
                    logger.info("Cargando distancias entre bloques desde hoja Remanejo...")
                    
                    # La primera columna tiene los bloques origen
                    for idx in range(len(df_remanejo)):
                        origen = str(df_remanejo.iloc[idx, 0]).strip()
                        if pd.isna(origen) or origen == 'fm/to' or not origen:
                            continue
                        
                        # Iterar sobre las columnas (bloques destino)
                        for col_idx in range(1, len(df_remanejo.columns)):
                            destino = str(df_remanejo.columns[col_idx]).strip()
                            if destino == 'fm/to' or not destino:
                                continue
                            
                            distancia = df_remanejo.iloc[idx, col_idx]
                            if pd.notna(distancia) and distancia > 0:
                                await self._insert_distancia(
                                    origen, destino, int(distancia), 'bloque', 'bloque'
                                )
                                distancias_cargadas += 1
                
                # 2. Cargar distancias bloque-gate (hoja "All")
                if 'All' in xl.sheet_names:
                    df_all = pd.read_excel(xl, 'All')
                    logger.info("Cargando distancias bloque-gate y bloque-sitio desde hoja All...")
                    
                    for idx, row in df_all.iterrows():
                        bloque = str(row.get('Bloque', '')).strip()
                        if not bloque or bloque == 'Bloque' or pd.isna(bloque):
                            continue
                        
                        # Gate
                        if 'Gate' in row and pd.notna(row['Gate']) and row['Gate'] > 0:
                            await self._insert_distancia(
                                bloque, 'GATE', int(row['Gate']), 'bloque', 'gate'
                            )
                            await self._insert_distancia(
                                'GATE', bloque, int(row['Gate']), 'gate', 'bloque'
                            )
                            distancias_cargadas += 2
                        
                        # Sitio Sur
                        if 'Sitio 1 - Sur' in row and pd.notna(row['Sitio 1 - Sur']) and row['Sitio 1 - Sur'] > 0:
                            await self._insert_distancia(
                                bloque, 'SITIO_SUR', int(row['Sitio 1 - Sur']), 'bloque', 'sitio'
                            )
                            await self._insert_distancia(
                                'SITIO_SUR', bloque, int(row['Sitio 1 - Sur']), 'sitio', 'bloque'
                            )
                            distancias_cargadas += 2
                        
                        # Sitio Norte
                        if 'Sitio 2 - Norte' in row and pd.notna(row['Sitio 2 - Norte']) and row['Sitio 2 - Norte'] > 0:
                            await self._insert_distancia(
                                bloque, 'SITIO_NORTE', int(row['Sitio 2 - Norte']), 'bloque', 'sitio'
                            )
                            await self._insert_distancia(
                                'SITIO_NORTE', bloque, int(row['Sitio 2 - Norte']), 'sitio', 'bloque'
                            )
                            distancias_cargadas += 2
                
                # 3. Cargar hoja "Distancias" si existe (formato ime_fm, ime_to)
                if 'Distancias' in xl.sheet_names:
                    df_dist = pd.read_excel(xl, 'Distancias')
                    logger.info("Cargando distancias desde hoja 'Distancias'...")
                    
                    for idx, row in df_dist.iterrows():
                        origen = str(row.get('ime_fm', '')).strip()
                        destino = str(row.get('ime_to', '')).strip()
                        distancia = row.get('Distancia[m]', 0)
                        
                        if origen and destino and pd.notna(distancia) and distancia > 0:
                            # Determinar tipos
                            tipo_origen = self._get_tipo_ubicacion(origen)
                            tipo_destino = self._get_tipo_ubicacion(destino)
                            
                            await self._insert_distancia(
                                origen, destino, int(distancia), tipo_origen, tipo_destino
                            )
                            distancias_cargadas += 1
                
                # 4. Cargar distancias de carga promedio si existe
                if 'CargaAvg' in xl.sheet_names:
                    df_carga = pd.read_excel(xl, 'CargaAvg')
                    logger.info("Cargando distancias promedio de carga...")
                    
                    for idx, row in df_carga.iterrows():
                        bloque = str(row.get('Bloque', '')).strip()
                        distancia = row.get('Distancia [m]', 0)
                        
                        if bloque and pd.notna(distancia) and distancia > 0:
                            # Para carga, asumimos que es desde bloque a sitio
                            await self._insert_distancia(
                                bloque, 'SITIO_CARGA', int(distancia), 'bloque', 'sitio'
                            )
                            distancias_cargadas += 1
            
            # Si NO es Costanera, solo cargar el resumen del modelo (no las distancias reales)
            else:
                logger.info("Archivo de modelo detectado, saltando carga de distancias reales")
            
            await self.db.flush()
            logger.info(f"✓ {distancias_cargadas} distancias cargadas/actualizadas")
            
            # Verificar qué se cargó
            result = await self.db.execute(
                select(func.count(DistanciaReal.id))
            )
            total_en_db = result.scalar()
            logger.info(f"Total de distancias en base de datos: {total_en_db}")
            
        except Exception as e:
            logger.error(f"Error cargando distancias: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise

    async def _insert_distancia(self, origen: str, destino: str, distancia: int, 
                               tipo_origen: str, tipo_destino: str):
        """Inserta o actualiza una distancia en la base de datos"""
        
        # Verificar si ya existe
        result = await self.db.execute(
            select(DistanciaReal).where(
                and_(
                    DistanciaReal.origen == origen,
                    DistanciaReal.destino == destino
                )
            )
        )
        existing = result.scalar_one_or_none()
        
        if not existing:
            dist = DistanciaReal(
                origen=origen,
                destino=destino,
                distancia_metros=distancia,
                tipo_origen=tipo_origen,
                tipo_destino=tipo_destino
            )
            self.db.add(dist)
            
            # Cachear para uso inmediato
            self._distancias_cache[f"{origen}_{destino}"] = distancia
        else:
            # Actualizar si cambió
            if existing.distancia_metros != distancia:
                existing.distancia_metros = distancia
                self._distancias_cache[f"{origen}_{destino}"] = distancia

    def _get_tipo_ubicacion(self, ubicacion: str) -> str:
        """Determina el tipo de ubicación basado en el código"""
        ubicacion = ubicacion.upper()
        
        if ubicacion.startswith('C') and len(ubicacion) == 2:
            return 'bloque'
        elif 'GATE' in ubicacion:
            return 'gate'
        elif 'SITIO' in ubicacion or 'SUR' in ubicacion or 'NORTE' in ubicacion:
            return 'sitio'
        elif 'PATIO' in ubicacion:
            return 'patio'
        else:
            return 'otro'
    
    async def _calculate_temporal_metrics(self, instancia_id: UUID):
        """Calcula métricas temporales agregadas"""
        
        logger.info("Calculando métricas temporales...")
        
        try:
            # Obtener datos por periodo
            for periodo in range(1, 22):  # 21 periodos
                # Movimientos reales
                real_result = await self.db.execute(
                    select(
                        func.count(MovimientoReal.id).label('total'),
                        func.sum(func.cast(MovimientoReal.tipo_movimiento == 'YARD', Integer)).label('yard')
                    ).where(
                        and_(
                            MovimientoReal.instancia_id == instancia_id,
                            MovimientoReal.periodo == periodo
                        )
                    )
                )
                real_stats = real_result.one()
                
                # Movimientos modelo
                modelo_result = await self.db.execute(
                    select(
                        func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                                MovimientoModelo.descarga + MovimientoModelo.entrega)
                    ).where(
                        and_(
                            MovimientoModelo.instancia_id == instancia_id,
                            MovimientoModelo.periodo == periodo
                        )
                    )
                )
                movimientos_modelo = modelo_result.scalar() or 0
                
                # Carga de trabajo
                carga_result = await self.db.execute(
                    select(func.sum(CargaTrabajo.carga_trabajo))
                    .where(
                        and_(
                            CargaTrabajo.instancia_id == instancia_id,
                            CargaTrabajo.periodo == periodo
                        )
                    )
                )
                carga_trabajo = carga_result.scalar() or 0
                
                # Ocupación promedio
                ocup_result = await self.db.execute(
                    select(func.avg(OcupacionBloque.porcentaje_ocupacion))
                    .where(
                        and_(
                            OcupacionBloque.instancia_id == instancia_id,
                            OcupacionBloque.periodo == periodo
                        )
                    )
                )
                ocupacion_promedio = ocup_result.scalar() or 0
                
                # Calcular día y turno
                dia = ((periodo - 1) // 3) + 1
                turno = ((periodo - 1) % 3) + 1
                
                metrica = MetricaTemporal(
                    instancia_id=instancia_id,
                    periodo=periodo,
                    dia=dia,
                    turno=turno,
                    movimientos_real=real_stats.total,
                    movimientos_yard_real=real_stats.yard,
                    movimientos_modelo=movimientos_modelo,
                    carga_trabajo=carga_trabajo,
                    ocupacion_promedio=ocupacion_promedio
                )
                self.db.add(metrica)
            
            await self.db.flush()
            logger.info("✓ Métricas temporales calculadas")
            
        except Exception as e:
            logger.error(f"Error calculando métricas temporales: {e}")
    
    async def _get_bloques_map(self) -> Dict[str, int]:
        """Obtiene mapeo de código de bloque a ID"""
        
        result = await self.db.execute(select(Bloque))
        bloques = result.scalars().all()
        return {b.codigo: b.id for b in bloques}
    
    async def _get_or_create_segregacion(self, codigo: str, descripcion: str = '') -> Segregacion:
        """Obtiene o crea una segregación"""
        
        result = await self.db.execute(
            select(Segregacion).where(Segregacion.codigo == codigo)
        )
        segregacion = result.scalar_one_or_none()
        
        if not segregacion:
            # Parsear información de la descripción
            tipo = 'desconocido'
            categoria = 'desconocido'
            tamano = None
            
            if descripcion:
                desc_lower = descripcion.lower()
                if 'expo' in desc_lower:
                    tipo = 'expo'
                elif 'impo' in desc_lower:
                    tipo = 'impo'
                
                if 'dry' in desc_lower:
                    categoria = 'dry'
                elif 'reefer' in desc_lower:
                    categoria = 'reefer'
                
                if '-20-' in descripcion:
                    tamano = 20
                elif '-40-' in descripcion:
                    tamano = 40
            
            segregacion = Segregacion(
                codigo=codigo,
                descripcion=descripcion,
                tipo=tipo,
                categoria=categoria,
                tamano=tamano
            )
            self.db.add(segregacion)
            await self.db.flush()
        
        return segregacion
    
    async def _log_procesamiento(
        self, instancia_id: UUID, archivo: str, tipo: str,
        registros: int, estado: str, error: str = None
    ):
        """Registra log de procesamiento"""
        
        log = LogProcesamiento(
            instancia_id=instancia_id,
            archivo_nombre=Path(archivo).name,
            archivo_tipo=tipo,
            registros_procesados=registros,
            estado=estado,
            mensaje_error=error
        )
        self.db.add(log)
        await self.db.flush()

    def _log_summary(self, instancia_id: UUID, stats_resultado: Dict,
                    stats_flujos: Dict, kpis: Dict):
        """Log resumen de la carga incluyendo distancias - VERSIÓN MEJORADA"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA DE OPTIMIZACIÓN")
        logger.info("="*80)
        logger.info(f"Instancia ID: {instancia_id}")
        
        logger.info("\n📋 Datos cargados:")
        logger.info(f"  - Movimientos reales totales: {stats_flujos.get('total_movimientos', 0):,}")
        logger.info(f"  - Movimientos YARD: {kpis.get('movimientos_yard', 0):,}")
        logger.info(f"  - Movimientos operativos (YARD+DLVR+LOAD): {kpis.get('movimientos_operativos_real', 0):,}")
        logger.info(f"  - Movimientos optimizados (DLVR+LOAD): {kpis.get('movimientos_dlvr_modelo', 0) + kpis.get('movimientos_load_modelo', 0):,}")
        logger.info(f"  - Bloques activos: {len(stats_resultado.get('bloques_activos', set()))}")
        logger.info(f"  - Segregaciones: {len(stats_resultado.get('segregaciones', set()))}")
        logger.info(f"  - Capacidades actualizadas: {'Sí' if stats_resultado.get('capacidades_actualizadas', False) else 'No'}")
        
        logger.info("\n📏 DISTANCIAS:")
        logger.info(f"  - Distancia total real: {kpis.get('distancia_total_real', 0):,} metros")
        logger.info(f"  - Distancia total modelo: {kpis.get('distancia_total_modelo', 0):,} metros")
        logger.info(f"  - DISTANCIA AHORRADA: {kpis.get('distancia_ahorrada', 0):,} metros")
        
        logger.info("\n  Desglose por tipo:")
        logger.info(f"  - YARD real: {kpis.get('distancia_yard', 0):,} m → modelo: 0 m (100% reducción)")
        logger.info(f"  - LOAD real: {kpis.get('distancia_load_real', 0):,} m → modelo: {kpis.get('distancia_load_modelo', 0):,} m")
        logger.info(f"  - DLVR real: {kpis.get('distancia_dlvr_real', 0):,} m → modelo: {kpis.get('distancia_dlvr_modelo', 0):,} m")
        
        logger.info("\n🎯 EFICIENCIA:")
        logger.info(f"  - Reducción de movimientos operativos: {kpis.get('porcentaje_reduccion_movimientos', 0):.1f}%")
        logger.info(f"  - Eficiencia en distancia: {kpis.get('eficiencia_ganada', 0):.2f}%")
        logger.info(f"  - Eficiencia operacional: {100 - (kpis.get('movimientos_yard', 0) / stats_flujos.get('total_movimientos', 1) * 100):.1f}% → 100%")
        
        logger.info("\n📊 OTROS KPIs:")
        logger.info(f"  - Ocupación promedio: {kpis.get('ocupacion_promedio', 0):.1f}%")
        logger.info(f"  - Ocupación máxima: {kpis.get('ocupacion_maxima', 0):.1f}%")
        logger.info(f"  - Variación de carga: {stats_resultado.get('variacion_carga', 0)}")
        logger.info(f"  - Balance de carga: {stats_resultado.get('balance_carga', 0)}")
        
        logger.info("="*80)