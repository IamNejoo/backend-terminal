# app/services/optimization_loader.py - VERSIÓN CORREGIDA
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

from app.models.optimization import *

logger = logging.getLogger(__name__)

class OptimizationLoader:
    """Servicio para cargar datos del modelo de optimización"""
    
    def __init__(self, db: AsyncSession):
        self.db = db
        self.validation_errors = []
        self.warnings = []
        self._distancias_cache = {}
        
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
            
            # Cargar archivo de resultado
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
            
            # Calcular KPIs comparativos
            kpis_stats = await self._calculate_kpis(instancia.id)
            
            # Calcular métricas temporales
            await self._calculate_temporal_metrics(instancia.id)
            
            # Actualizar resultados generales
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
    
    async def _create_or_update_instancia(
        self, fecha_inicio: datetime, semana: int, anio: int,
        participacion: int, con_dispersion: bool
    ) -> Instancia:
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
                'C1': 1517, 'C2': 1495, 'C3': 1836, 'C4': 1822,
                'C5': 1703, 'C6': 2055, 'C7': 2037, 'C8': 1988, 'C9': 1937
            }
            
            for codigo, capacidad in capacidades.items():
                bloque = Bloque(
                    codigo=codigo,
                    capacidad_teus=capacidad,
                    capacidad_bahias=35  # Por defecto
                )
                self.db.add(bloque)
            
            await self.db.flush()
            logger.info(f"✓ Creados {len(capacidades)} bloques")
    
    async def _load_resultado_file(self, filepath: str, instancia_id: UUID) -> Dict[str, Any]:
        """Carga archivo de resultados del modelo"""
        
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
                'balance_carga': 0
            }
            
            # Obtener mapeo de bloques
            bloques_map = await self._get_bloques_map()
            
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
            
            # 2. Cargar Workload bloques
            if 'Workload bloques' in xl.sheet_names:
                df_workload = pd.read_excel(xl, 'Workload bloques')
                logger.info(f"Procesando {len(df_workload)} registros de Workload")
                
                batch = []
                cargas = []
                for idx, row in df_workload.iterrows():
                    try:
                        bloque_codigo = str(row.get('Bloque', '')).strip()
                        
                        if bloque_codigo in bloques_map:
                            carga_valor = int(row.get('Carga de trabajo', 0))
                            carga = CargaTrabajo(
                                instancia_id=instancia_id,
                                bloque_id=bloques_map[bloque_codigo],
                                periodo=int(row.get('Periodo', 0)),
                                carga_trabajo=carga_valor
                            )
                            batch.append(carga)
                            stats['carga_trabajo'] += carga_valor
                            cargas.append(carga_valor)
                        
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
            
            # 3. Cargar Contenedores Turno-Bloque (ocupación)
            if 'Contenedores Turno-Bloque' in xl.sheet_names:
                df_contenedores = pd.read_excel(xl, 'Contenedores Turno-Bloque')
                logger.info(f"Procesando ocupación por turno-bloque")
                
                # Esta hoja tiene formato especial: primera columna es Turno, resto son bloques
                batch = []
                columnas_bloques = [col for col in df_contenedores.columns if col != 'Turno' and col in bloques_map]
                
                for idx, row in df_contenedores.iterrows():
                    try:
                        turno = int(row.get('Turno', 0))
                        periodo = turno  # En este caso periodo = turno
                        
                        for bloque_codigo in columnas_bloques:
                            contenedores = int(row.get(bloque_codigo, 0))
                            
                            # Obtener capacidad del bloque
                            bloque_result = await self.db.execute(
                                select(Bloque).where(Bloque.codigo == bloque_codigo)
                            )
                            bloque = bloque_result.scalar_one()
                            
                            porcentaje = (contenedores / bloque.capacidad_teus * 100) if bloque.capacidad_teus > 0 else 0
                            
                            ocupacion = OcupacionBloque(
                                instancia_id=instancia_id,
                                bloque_id=bloques_map[bloque_codigo],
                                periodo=periodo,
                                turno=((periodo - 1) % 3) + 1,  # 1, 2, 3
                                contenedores_teus=contenedores,
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
            
            # 4. Procesar hoja de Variación Carga de trabajo - CORREGIDO
            if 'Variación Carga de trabajo' in xl.sheet_names:
                try:
                    df_var = pd.read_excel(xl, 'Variación Carga de trabajo')
                    logger.info(f"Procesando hoja Variación Carga de trabajo")
                    
                    # La hoja puede tener diferentes estructuras, intentar varias formas
                    variacion_valor = None
                    
                    # Intento 1: Buscar valor en la primera columna, segunda fila (si hay encabezado)
                    if len(df_var) > 0 and len(df_var.columns) > 0:
                        # Si la primera fila tiene el título, el valor está en la segunda
                        if len(df_var) > 1:
                            # Verificar si el primer valor es texto (encabezado)
                            primer_valor = df_var.iloc[0, 0]
                            if isinstance(primer_valor, str) and 'variación' in primer_valor.lower():
                                # El valor está en la segunda fila
                                variacion_valor = df_var.iloc[1, 0]
                            else:
                                # El valor está en la primera fila
                                variacion_valor = primer_valor
                        else:
                            # Solo hay una fila, tomar el primer valor
                            variacion_valor = df_var.iloc[0, 0]
                    
                    # Intento 2: Si no se encontró, buscar en una estructura clave-valor
                    if variacion_valor is None:
                        # Buscar si hay columnas con nombres como 'Métrica' y 'Valor'
                        for col in df_var.columns:
                            if 'valor' in str(col).lower():
                                # Buscar la fila que tiene 'Variación'
                                for idx, row in df_var.iterrows():
                                    if any('variación' in str(val).lower() for val in row.values if pd.notna(val)):
                                        variacion_valor = row[col]
                                        break
                    
                    # Convertir a entero si se encontró
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
            
            # 5. Procesar hoja de Balance de carga de trabajo si existe
            if 'Balance de carga de trabajo' in xl.sheet_names:
                try:
                    df_balance = pd.read_excel(xl, 'Balance de carga de trabajo')
                    # Similar lógica para extraer el valor
                    balance_valor = None
                    
                    if len(df_balance) > 0 and len(df_balance.columns) > 0:
                        if len(df_balance) > 1 and isinstance(df_balance.iloc[0, 0], str):
                            balance_valor = df_balance.iloc[1, 0]
                        else:
                            balance_valor = df_balance.iloc[0, 0]
                    
                    if balance_valor is not None and pd.notna(balance_valor):
                        try:
                            stats['balance_carga'] = int(float(str(balance_valor)))
                            logger.info(f"✓ Balance de carga: {stats['balance_carga']}")
                        except:
                            pass
                            
                except Exception as e:
                    logger.warning(f"Error procesando hoja Balance de carga de trabajo: {str(e)}")
            
            await self.db.flush()
            
            logger.info(f"Resultado cargado: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error cargando resultado: {e}")
            raise
    
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
        """Carga archivo de distancias reales"""
        
        logger.info("Cargando archivo de distancias...")
        
        try:
            xl = pd.ExcelFile(filepath)
            
            # Buscar hoja de distancias - puede llamarse 'Distancias' o similar
            hoja_distancias = None
            for sheet in xl.sheet_names:
                if 'distancia' in sheet.lower():
                    hoja_distancias = sheet
                    break
            
            if hoja_distancias:
                df = pd.read_excel(xl, hoja_distancias)
                
                for idx, row in df.iterrows():
                    origen = str(row.get('ime_fm', ''))
                    destino = str(row.get('ime_to', ''))
                    distancia = int(row.get('Distancia[m]', 0))
                    
                    if origen and destino and distancia > 0:
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
                                tipo_origen='bloque' if origen.startswith('C') else 'otro',
                                tipo_destino='bloque' if destino.startswith('C') else 'otro'
                            )
                            self.db.add(dist)
                            self._distancias_cache[f"{origen}_{destino}"] = distancia
                
                await self.db.flush()
                logger.info(f"✓ Distancias cargadas/actualizadas")
                
        except Exception as e:
            logger.error(f"Error cargando distancias: {e}")
    
    async def _calculate_kpis(self, instancia_id: UUID) -> Dict[str, Any]:
        """Calcula KPIs comparativos"""
        
        logger.info("Calculando KPIs...")
        
        kpis = {}
        
        try:
            # 1. KPIs de movimientos
            movs_real = await self.db.execute(
                select(
                    func.count(MovimientoReal.id).label('total'),
                    func.sum(func.cast(MovimientoReal.tipo_movimiento == 'YARD', Integer)).label('yard')
                ).where(MovimientoReal.instancia_id == instancia_id)
            )
            real_stats = movs_real.one()
            
            movs_modelo = await self.db.execute(
                select(
                    func.sum(MovimientoModelo.recepcion + MovimientoModelo.carga + 
                            MovimientoModelo.descarga + MovimientoModelo.entrega)
                ).where(MovimientoModelo.instancia_id == instancia_id)
            )
            total_modelo = movs_modelo.scalar() or 0
            
            # Eficiencia
            eficiencia_real = ((real_stats.total - real_stats.yard) / real_stats.total * 100) if real_stats.total > 0 else 0
            eficiencia_modelo = 100  # Sin YARD
            
            # Crear KPIs de movimientos
            kpis_movimientos = [
                {
                    'categoria': 'movimientos',
                    'metrica': 'total_real',
                    'valor_real': real_stats.total,
                    'valor_modelo': total_modelo,
                    'diferencia': real_stats.total - total_modelo,
                    'porcentaje_mejora': ((real_stats.total - total_modelo) / real_stats.total * 100) if real_stats.total > 0 else 0,
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
                    'categoria': 'eficiencia',
                    'metrica': 'eficiencia_operacional',
                    'valor_real': eficiencia_real,
                    'valor_modelo': eficiencia_modelo,
                    'diferencia': eficiencia_modelo - eficiencia_real,
                    'porcentaje_mejora': eficiencia_modelo - eficiencia_real,
                    'unidad': 'porcentaje'
                }
            ]
            
            for kpi_data in kpis_movimientos:
                kpi = KPIComparativo(instancia_id=instancia_id, **kpi_data)
                self.db.add(kpi)
            
            # 2. KPIs de distancias (si hay datos)
            if self._distancias_cache:
                # Calcular distancias reales
                distancia_real_total = 0
                distancia_real_yard = 0
                
                movs_result = await self.db.execute(
                    select(MovimientoReal).where(MovimientoReal.instancia_id == instancia_id)
                )
                movimientos = movs_result.scalars().all()
                
                for mov in movimientos:
                    if mov.bloque_origen and mov.bloque_destino:
                        key = f"{mov.bloque_origen}_{mov.bloque_destino}"
                        if key in self._distancias_cache:
                            dist = self._distancias_cache[key]
                            distancia_real_total += dist
                            if mov.tipo_movimiento == 'YARD':
                                distancia_real_yard += dist
                
                # Distancia modelo (sin YARD)
                distancia_modelo_total = distancia_real_total - distancia_real_yard
                
                kpi_dist = KPIComparativo(
                    instancia_id=instancia_id,
                    categoria='distancia',
                    metrica='distancia_total',
                    valor_real=distancia_real_total,
                    valor_modelo=distancia_modelo_total,
                    diferencia=distancia_real_yard,
                    porcentaje_mejora=(distancia_real_yard / distancia_real_total * 100) if distancia_real_total > 0 else 0,
                    unidad='metros'
                )
                self.db.add(kpi_dist)
                
                kpis['distancia_total'] = distancia_real_total
                kpis['distancia_yard'] = distancia_real_yard
            
            # 3. KPIs de ocupación
            ocupacion_result = await self.db.execute(
                select(
                    func.avg(OcupacionBloque.porcentaje_ocupacion)
                ).where(OcupacionBloque.instancia_id == instancia_id)
            )
            ocupacion_promedio = ocupacion_result.scalar() or 0
            
            kpis['ocupacion_promedio'] = float(ocupacion_promedio)
            
            # 4. Segregaciones optimizadas
            segs_result = await self.db.execute(
                select(func.count(func.distinct(MovimientoModelo.segregacion_id)))
                .where(MovimientoModelo.instancia_id == instancia_id)
            )
            segregaciones_optimizadas = segs_result.scalar() or 0
            
            kpis['segregaciones_optimizadas'] = segregaciones_optimizadas
            
            await self.db.flush()
            
            kpis.update({
                'movimientos_real': real_stats.total,
                'movimientos_yard': real_stats.yard,
                'movimientos_modelo': total_modelo,
                'eficiencia_real': eficiencia_real,
                'eficiencia_modelo': eficiencia_modelo
            })
            
            return kpis
            
        except Exception as e:
            logger.error(f"Error calculando KPIs: {e}")
            raise
    
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
    
    async def _update_resultados_generales(
        self, instancia_id: UUID, stats_resultado: Dict,
        stats_flujos: Dict, kpis: Dict
    ):
        """Actualiza tabla de resultados generales"""
        
        logger.info("Actualizando resultados generales...")
        
        # Obtener totales de segregaciones
        segs_result = await self.db.execute(
            select(func.count(func.distinct(Segregacion.id)))
        )
        total_segregaciones = segs_result.scalar() or 0
        
        # Obtener carga de trabajo total
        carga_result = await self.db.execute(
            select(func.sum(CargaTrabajo.carga_trabajo))
            .where(CargaTrabajo.instancia_id == instancia_id)
        )
        carga_total = carga_result.scalar() or 0
        
        resultado = ResultadoGeneral(
            instancia_id=instancia_id,
            movimientos_reales_total=stats_flujos.get('total_movimientos', 0),
            movimientos_yard_real=kpis.get('movimientos_yard', 0),
            movimientos_optimizados=stats_resultado.get('movimientos_modelo', 0),
            movimientos_reduccion=stats_flujos.get('total_movimientos', 0) - stats_resultado.get('movimientos_modelo', 0),
            movimientos_reduccion_pct=(
                (stats_flujos.get('total_movimientos', 0) - stats_resultado.get('movimientos_modelo', 0)) / 
                stats_flujos.get('total_movimientos', 1) * 100
            ) if stats_flujos.get('total_movimientos', 0) > 0 else 0,
            distancia_real_total=kpis.get('distancia_total', 0),
            distancia_real_yard=kpis.get('distancia_yard', 0),
            distancia_modelo_total=kpis.get('distancia_total', 0) - kpis.get('distancia_yard', 0),
            eficiencia_real=kpis.get('eficiencia_real', 0),
            eficiencia_modelo=kpis.get('eficiencia_modelo', 100),
            eficiencia_ganancia=kpis.get('eficiencia_modelo', 100) - kpis.get('eficiencia_real', 0),
            segregaciones_total=total_segregaciones,
            segregaciones_optimizadas=kpis.get('segregaciones_optimizadas', 0),
            carga_trabajo_total=carga_total,
            variacion_carga=stats_resultado.get('variacion_carga', 0),
            balance_carga=stats_resultado.get('balance_carga', 0),
            ocupacion_promedio_pct=kpis.get('ocupacion_promedio', 0),
            capacidad_total_teus=16390  # Suma de capacidades de bloques C1-C9
        )
        
        self.db.add(resultado)
        await self.db.flush()
        
        # Actualizar estado de instancia
        await self.db.execute(
            select(Instancia).where(Instancia.id == instancia_id)
        )
        instancia_result = await self.db.execute(
            select(Instancia).where(Instancia.id == instancia_id)
        )
        instancia = instancia_result.scalar_one()
        instancia.estado = 'completado'
        instancia.total_movimientos = stats_resultado.get('movimientos_modelo', 0)
        instancia.total_bloques = len(stats_resultado.get('bloques_activos', set()))
        instancia.total_segregaciones = kpis.get('segregaciones_optimizadas', 0)
        
        await self.db.flush()
    
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
        """Log resumen de la carga"""
        
        logger.info("="*80)
        logger.info("📊 RESUMEN DE CARGA DE OPTIMIZACIÓN")
        logger.info("="*80)
        logger.info(f"Instancia ID: {instancia_id}")
        
        logger.info("\n📋 Datos cargados:")
        logger.info(f"  - Movimientos modelo: {stats_resultado.get('movimientos_modelo', 0)}")
        logger.info(f"  - Movimientos reales: {stats_flujos.get('total_movimientos', 0)}")
        logger.info(f"  - Bloques activos: {len(stats_resultado.get('bloques_activos', set()))}")
        logger.info(f"  - Segregaciones: {len(stats_resultado.get('segregaciones', set()))}")
        
        logger.info("\n🎯 KPIs principales:")
        logger.info(f"  - Eficiencia real: {kpis.get('eficiencia_real', 0):.1f}%")
        logger.info(f"  - Eficiencia modelo: {kpis.get('eficiencia_modelo', 0):.1f}%")
        logger.info(f"  - Mejora: {kpis.get('eficiencia_modelo', 0) - kpis.get('eficiencia_real', 0):.1f}%")
        logger.info(f"  - YARD eliminados: {kpis.get('movimientos_yard', 0)}")
        logger.info(f"  - Ocupación promedio: {kpis.get('ocupacion_promedio', 0):.1f}%")
        logger.info(f"  - Variación de carga: {stats_resultado.get('variacion_carga', 0)}")
        logger.info(f"  - Balance de carga: {stats_resultado.get('balance_carga', 0)}")
        
        logger.info("="*80)