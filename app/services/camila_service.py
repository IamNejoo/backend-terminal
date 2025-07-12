# app/services/camila_service.py
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, time, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_
from sqlalchemy.orm import selectinload
import numpy as np
import asyncio

# Importar modelos de Camila
from app.models.camila import (
    InstanciaCamila, Bloque, Segregacion, Grua, PeriodoHora,
    ParametroGeneral, InventarioInicial, DemandaOperacion,
    CapacidadBloque, AsignacionGrua, FlujoOperacional,
    CuotaCamion, DisponibilidadBloque, MetricaResultado,
    ProductividadGrua, IntegracionMagdalena, DemandaHoraMagdalena,
    ConfiguracionSistema, ConfiguracionInstancia
)

# Importar modelos de Magdalena
from app.models.optimization import (
    Instancia as InstanciaMagdalena,
    MovimientoModelo,
    Bloque as BloqueMagdalena,
    Segregacion as SegregacionMagdalena,
    OcupacionBloque
)

from app.schemas.camila import (
    InstanciaCamilaCreate, DashboardResponse, KPIBalance,
    KPIGruas, KPIFlujos, KPICamiones, EstadoInstancia
)
from .camila_loader import CamilaLoader

logger = logging.getLogger(__name__)

class CamilaService:
    """Servicio principal para el modelo Camila"""
    
    def __init__(self):
        self.loader = CamilaLoader()
    
    async def process_instance(
        self,
        db: AsyncSession,
        instance_data: Dict[str, Any],
        results_data: Dict[str, Any],
        instance_create: InstanciaCamilaCreate
    ) -> InstanciaCamila:
        """Procesa una instancia completa de Camila"""
        try:
            # Crear instancia
            instancia = await self._create_instance(db, instance_create)
            
            # Actualizar estado
            instancia.estado = EstadoInstancia.ejecutando
            await db.commit()
            
            # Procesar datos en orden
            await self._ensure_master_data(db)
            await self._process_parameters(db, instancia, instance_data['parametros'])
            await self._process_periods(db, instancia)
            await self._process_initial_inventory(db, instancia, instance_data)
            await self._process_demands(db, instancia, instance_data)
            await self._process_capacities(db, instancia, instance_data)
            
            # Procesar resultados
            await self._process_crane_assignments(db, instancia, results_data['asignacion_gruas'])
            await self._process_flows(db, instancia, results_data)
            await self._process_availability(db, instancia, results_data)
            await self._process_quotas(db, instancia, results_data['cuotas'])
            
            # Calcular métricas
            await self._calculate_metrics(db, instancia, results_data['metricas'])
            
            # Actualizar estado final
            instancia.estado = EstadoInstancia.completado
            instancia.fecha_ejecucion = datetime.utcnow()
            await db.commit()
            
            logger.info(f"Instancia {instancia.id} procesada exitosamente")
            return instancia
            
        except Exception as e:
            logger.error(f"Error procesando instancia: {str(e)}")
            if instancia:
                instancia.estado = EstadoInstancia.error
                instancia.mensaje_error = str(e)
                await db.commit()
            raise
    
    async def import_from_magdalena(
        self,
        db: AsyncSession,
        magdalena_instance_id: int,
        turno: int,
        instance_filepath: str,
        result_filepath: str,
        instance_create: InstanciaCamilaCreate
    ) -> InstanciaCamila:
        """Importa datos desde Magdalena usando BD y archivos"""
        try:
            # Verificar que existe la instancia de Magdalena
            result = await db.execute(
                select(InstanciaMagdalena)
                .where(InstanciaMagdalena.id == magdalena_instance_id)
            )
            instancia_magdalena = result.scalar_one_or_none()
            
            if not instancia_magdalena:
                raise ValueError(f"Instancia Magdalena {magdalena_instance_id} no encontrada")
            
            # Crear instancia Camila
            instancia = await self._create_instance(db, instance_create)
            
            # Crear registro de integración
            integracion = IntegracionMagdalena(
                instancia_camila_id=instancia.id,
                magdalena_instance_id=magdalena_instance_id,
                estado_importacion='procesando',
                archivo_instancia_magdalena=instance_filepath,
                archivo_resultado_magdalena=result_filepath
            )
            db.add(integracion)
            await db.commit()
            
            # Leer archivos de Magdalena
            magdalena_data = self.loader.read_magdalena_files(
                instance_filepath,
                result_filepath,
                turno
            )
            
            # Procesar datos
            await self._ensure_master_data(db)
            
            # 1. Importar segregaciones
            await self._import_segregaciones(db, magdalena_data['segregaciones'])
            
            # 2. Importar inventarios desde BD
            inventarios = await self._get_inventarios_from_magdalena_db(db, magdalena_instance_id, turno)
            await self._process_inventories_magdalena(db, instancia, inventarios)
            
            # 3. Importar capacidades desde BD
            capacidades = await self._get_capacidades_from_magdalena_db(db, magdalena_instance_id, turno)
            await self._process_capacities_magdalena(db, instancia, capacidades)
            
            # 4. Importar demandas POR HORA desde archivo
            await self._process_demands_magdalena(db, instancia, magdalena_data['demandas_hora'])
            
            # 5. Guardar demandas en tabla específica
            await self._save_magdalena_hourly_demands(
                db, 
                instancia.id, 
                magdalena_instance_id,
                magdalena_data['demandas_hora']
            )
            
            # Actualizar integración
            integracion.estado_importacion = 'completado'
            integracion.datos_inventario = inventarios
            integracion.datos_capacidad = capacidades
            integracion.datos_demanda = magdalena_data['demandas_hora']
            await db.commit()
            
            logger.info(f"Datos importados desde Magdalena para instancia {instancia.id}")
            return instancia
            
        except Exception as e:
            logger.error(f"Error importando desde Magdalena: {str(e)}")
            if integracion:
                integracion.estado_importacion = 'error'
                integracion.mensaje_error = str(e)
                await db.commit()
            raise
    
    async def get_dashboard(self, db: AsyncSession, instance_id: int) -> DashboardResponse:
        """Obtiene datos del dashboard para una instancia"""
        # Obtener instancia con relaciones
        result = await db.execute(
            select(InstanciaCamila)
            .options(
                selectinload(InstanciaCamila.metricas),
selectinload(InstanciaCamila.flujos),
                selectinload(InstanciaCamila.cuotas)
            )
            .where(InstanciaCamila.id == instance_id)
        )
        instancia = result.scalar_one_or_none()
        
        if not instancia:
            raise ValueError(f"Instancia {instance_id} no encontrada")
        
        # Obtener métricas
        metricas = instancia.metricas
        if not metricas:
            raise ValueError(f"No hay métricas calculadas para instancia {instance_id}")
        
        # Obtener configuraciones
        tiempo_espera = await self._get_config_value(db, 'tiempo_espera_estimado', 25)
        
        # Construir KPIs
        balance = KPIBalance(
            funcion_objetivo=metricas.valor_funcion_objetivo,
            coeficiente_variacion=metricas.coeficiente_variacion,
            indice_balance=metricas.indice_balance,
            desviacion_estandar=metricas.desviacion_estandar_carga
        )
        
        gruas = KPIGruas(
            utilizacion_promedio=metricas.utilizacion_gruas_pct,
            gruas_activas_promedio=metricas.gruas_utilizadas_promedio,
            productividad_promedio=metricas.productividad_promedio,
            cambios_totales=metricas.cambios_bloque_total,
            eficiencia_pct=(metricas.productividad_promedio / 20) * 100  # 20 es productividad nominal
        )
        
        # Calcular distribución de flujos
        flujos_result = await db.execute(
            select(
                func.sum(FlujoOperacional.flujo_carga).label('carga'),
                func.sum(FlujoOperacional.flujo_descarga).label('descarga'),
                func.sum(FlujoOperacional.flujo_recepcion).label('recepcion'),
                func.sum(FlujoOperacional.flujo_entrega).label('entrega')
            )
            .where(FlujoOperacional.instancia_id == instance_id)
        )
        flujos_data = flujos_result.first()
        
        flujos = KPIFlujos(
            movimientos_totales=metricas.movimientos_totales,
            cumplimiento_carga=metricas.cumplimiento_carga_pct,
            cumplimiento_descarga=metricas.cumplimiento_descarga_pct,
            cumplimiento_recepcion=metricas.cumplimiento_recepcion_pct,
            cumplimiento_entrega=metricas.cumplimiento_entrega_pct,
            distribucion={
                'carga': flujos_data.carga or 0,
                'descarga': flujos_data.descarga or 0,
                'recepcion': flujos_data.recepcion or 0,
                'entrega': flujos_data.entrega or 0
            }
        )
        
        # Obtener cuotas
        cuotas_result = await db.execute(
            select(
                func.min(CuotaCamion.cuota_total).label('minima'),
                func.max(CuotaCamion.cuota_total).label('maxima')
            )
            .where(CuotaCamion.instancia_id == instance_id)
        )
        cuotas_data = cuotas_result.first()
        
        camiones = KPICamiones(
            cuota_total=metricas.cuota_total_turno,
            cuota_promedio=metricas.cuota_promedio_hora,
            cuota_maxima=cuotas_data.maxima or 0,
            cuota_minima=cuotas_data.minima or 0,
            uniformidad=metricas.uniformidad_cuotas,
            tiempo_espera_promedio=tiempo_espera
        )
        
        return DashboardResponse(
            instancia=instancia,
            balance=balance,
            gruas=gruas,
            flujos=flujos,
            camiones=camiones,
            congestion_maxima=metricas.congestion_maxima,
            bloque_mas_congestionado=metricas.bloque_mas_congestionado,
            hora_pico=metricas.hora_pico
        )
    
    async def get_crane_assignments(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Obtiene asignaciones de grúas por hora"""
        result = await db.execute(
            select(AsignacionGrua, Grua, Bloque, PeriodoHora)
            .join(Grua)
            .join(Bloque)
            .join(PeriodoHora)
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True
                )
            )
            .order_by(PeriodoHora.hora_relativa, Grua.codigo)
        )
        
        asignaciones = []
        gruas_por_hora = {}
        
        for asig, grua, bloque, periodo in result:
            asignaciones.append({
                'grua': grua.codigo,
                'bloque': bloque.codigo,
                'hora': periodo.hora_relativa,
                'productividad': asig.productividad_real,
                'movimientos': asig.movimientos_realizados
            })
            
            if periodo.hora_relativa not in gruas_por_hora:
                gruas_por_hora[periodo.hora_relativa] = 0
            gruas_por_hora[periodo.hora_relativa] += 1
        
        # Calcular cambios de bloque
        cambios_por_grua = await self._calculate_block_changes(db, instance_id)
        
        return {
            'instancia_id': instance_id,
            'asignaciones': asignaciones,
            'resumen': {
                'gruas_por_hora': gruas_por_hora,
                'cambios_por_grua': cambios_por_grua,
                'total_cambios': sum(cambios_por_grua.values())
            }
        }
    
    async def get_flows(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Obtiene flujos operacionales"""
        # Por hora
        result_hora = await db.execute(
            select(
                PeriodoHora.hora_relativa,
                func.sum(FlujoOperacional.flujo_carga).label('carga'),
                func.sum(FlujoOperacional.flujo_descarga).label('descarga'),
                func.sum(FlujoOperacional.flujo_recepcion).label('recepcion'),
                func.sum(FlujoOperacional.flujo_entrega).label('entrega')
            )
            .join(PeriodoHora)
            .where(FlujoOperacional.instancia_id == instance_id)
            .group_by(PeriodoHora.hora_relativa)
            .order_by(PeriodoHora.hora_relativa)
        )
        
        flujos_hora = []
        for row in result_hora:
            flujos_hora.append({
                'hora': row.hora_relativa,
                'carga': row.carga or 0,
                'descarga': row.descarga or 0,
                'recepcion': row.recepcion or 0,
                'entrega': row.entrega or 0,
                'total': (row.carga or 0) + (row.descarga or 0) + 
                        (row.recepcion or 0) + (row.entrega or 0)
            })
        
        # Por bloque
        result_bloque = await db.execute(
            select(
                Bloque.codigo,
                func.sum(FlujoOperacional.flujo_carga).label('carga'),
                func.sum(FlujoOperacional.flujo_descarga).label('descarga'),
                func.sum(FlujoOperacional.flujo_recepcion).label('recepcion'),
                func.sum(FlujoOperacional.flujo_entrega).label('entrega')
            )
            .join(Bloque)
            .where(FlujoOperacional.instancia_id == instance_id)
            .group_by(Bloque.codigo)
            .order_by(Bloque.codigo)
        )
        
        flujos_bloque = []
        totales = {'carga': 0, 'descarga': 0, 'recepcion': 0, 'entrega': 0}
        
        for row in result_bloque:
            total_bloque = (row.carga or 0) + (row.descarga or 0) + \
                          (row.recepcion or 0) + (row.entrega or 0)
            
            flujos_bloque.append({
                'bloque': row.codigo,
                'carga': row.carga or 0,
                'descarga': row.descarga or 0,
                'recepcion': row.recepcion or 0,
                'entrega': row.entrega or 0,
                'total': total_bloque
            })
            
            totales['carga'] += row.carga or 0
            totales['descarga'] += row.descarga or 0
            totales['recepcion'] += row.recepcion or 0
            totales['entrega'] += row.entrega or 0
        
        return {
            'instancia_id': instance_id,
            'por_hora': flujos_hora,
            'por_bloque': flujos_bloque,
            'totales': totales
        }
    
    async def get_truck_quotas(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Obtiene cuotas de camiones"""
        result = await db.execute(
            select(CuotaCamion, PeriodoHora)
            .join(PeriodoHora)
            .where(CuotaCamion.instancia_id == instance_id)
            .order_by(PeriodoHora.hora_relativa)
        )
        
        cuotas = []
        total_turno = 0
        
        for cuota, periodo in result:
            cuotas.append({
                'hora': periodo.hora_relativa,
                'hora_inicio': periodo.hora_inicio,
                'hora_fin': periodo.hora_fin,
                'cuota_recepcion': cuota.cuota_recepcion,
                'cuota_entrega': cuota.cuota_entrega,
                'cuota_total': cuota.cuota_total,
                'capacidad_disponible': cuota.capacidad_disponible,
                'utilizacion_esperada': cuota.utilizacion_esperada
            })
            total_turno += cuota.cuota_total
        
        # Calcular uniformidad
        if cuotas:
            cuotas_valores = [c['cuota_total'] for c in cuotas]
            promedio = np.mean(cuotas_valores)
            desviacion = np.std(cuotas_valores)
            uniformidad = 1 - (desviacion / promedio) if promedio > 0 else 0
        else:
            promedio = 0
            uniformidad = 0
        
        return {
            'instancia_id': instance_id,
            'cuotas': cuotas,
            'total_turno': total_turno,
            'promedio_hora': promedio,
            'uniformidad': uniformidad
        }
    
    async def get_balance_by_block(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Obtiene balance de carga por bloque"""
        # Movimientos por bloque
        result_mov = await db.execute(
            select(
                Bloque.codigo,
                func.sum(
                    FlujoOperacional.flujo_carga + 
                    FlujoOperacional.flujo_descarga +
                    FlujoOperacional.flujo_recepcion + 
                    FlujoOperacional.flujo_entrega
                ).label('movimientos_totales')
            )
            .join(Bloque)
            .where(FlujoOperacional.instancia_id == instance_id)
            .group_by(Bloque.codigo)
        )
        
        movimientos_bloque = {row.codigo: row.movimientos_totales or 0 for row in result_mov}
        
        # Utilización y congestión
        result_util = await db.execute(
            select(
                Bloque.codigo,
                func.avg(DisponibilidadBloque.utilizacion_pct).label('util_promedio'),
                func.max(DisponibilidadBloque.congestion_index).label('congestion_max')
            )
            .join(Bloque)
            .where(DisponibilidadBloque.instancia_id == instance_id)
            .group_by(Bloque.codigo)
        )
        
        utilizacion_bloque = {
            row.codigo: {
                'utilizacion': row.util_promedio or 0,
                'congestion': row.congestion_max or 0
            }
            for row in result_util
        }
        
        # Grúas asignadas
        result_gruas = await db.execute(
            select(
                Bloque.codigo,
                func.count(func.distinct(AsignacionGrua.grua_id)).label('gruas')
            )
            .join(Bloque)
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True
                )
            )
            .group_by(Bloque.codigo)
        )
        
        gruas_bloque = {row.codigo: row.gruas for row in result_gruas}
        
        # Obtener capacidad desde configuración
        capacidad_default = await self._get_config_value(db, 'capacidad_bloque_default', 1820)
        
        # Construir respuesta
        bloques = []
        movimientos_lista = []
        
        for codigo in ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']:
            movimientos = movimientos_bloque.get(codigo, 0)
            movimientos_lista.append(movimientos)
            
            bloques.append({
                'bloque': codigo,
                'movimientos_totales': movimientos,
                'utilizacion_promedio': utilizacion_bloque.get(codigo, {}).get('utilizacion', 0),
                'congestion_maxima': utilizacion_bloque.get(codigo, {}).get('congestion', 0),
                'gruas_asignadas': gruas_bloque.get(codigo, 0),
                'capacidad_total': capacidad_default
            })
        
        # Calcular coeficiente de variación
        if movimientos_lista:
            media = np.mean(movimientos_lista)
            desv = np.std(movimientos_lista)
            cv = (desv / media * 100) if media > 0 else 0
        else:
            cv = 0
        
        return {
            'instancia_id': instance_id,
            'bloques': bloques,
            'coeficiente_variacion': cv,
            'balance_score': 100 - cv  # Score inverso al CV
        }
    
    # Métodos privados de procesamiento
    async def _create_instance(
        self, 
        db: AsyncSession, 
        instance_create: InstanciaCamilaCreate
    ) -> InstanciaCamila:
        """Crea una nueva instancia"""
        instancia = InstanciaCamila(**instance_create.model_dump())
        db.add(instancia)
        await db.commit()
        await db.refresh(instancia)
        return instancia
    
    async def _ensure_master_data(self, db: AsyncSession):
        """Asegura que existan los datos maestros"""
        # Verificar bloques
        result = await db.execute(select(func.count(Bloque.id)))
        if result.scalar() == 0:
            await self._create_bloques(db)
        
        # Verificar grúas
        result = await db.execute(select(func.count(Grua.id)))
        if result.scalar() == 0:
            await self._create_gruas(db)
        
        # Verificar configuraciones
        result = await db.execute(select(func.count(ConfiguracionSistema.id)))
        if result.scalar() == 0:
            await self._create_default_configs(db)
    
    async def _create_bloques(self, db: AsyncSession):
        """Crea los bloques por defecto"""
        bloques_data = [
            {'codigo': 'C1', 'grupo_movimiento': 1, 'bloques_adyacentes': ['C3'], 'capacidad_teus': 1820},
            {'codigo': 'C2', 'grupo_movimiento': 2, 'bloques_adyacentes': ['C4'], 'capacidad_teus': 1820},
            {'codigo': 'C3', 'grupo_movimiento': 1, 'bloques_adyacentes': ['C1', 'C6'], 'capacidad_teus': 1820},
            {'codigo': 'C4', 'grupo_movimiento': 2, 'bloques_adyacentes': ['C2', 'C7'], 'capacidad_teus': 1820},
            {'codigo': 'C5', 'grupo_movimiento': 3, 'bloques_adyacentes': ['C8'], 'capacidad_teus': 1820},
            {'codigo': 'C6', 'grupo_movimiento': 1, 'bloques_adyacentes': ['C3'], 'capacidad_teus': 1820},
            {'codigo': 'C7', 'grupo_movimiento': 2, 'bloques_adyacentes': ['C4'], 'capacidad_teus': 1820},
            {'codigo': 'C8', 'grupo_movimiento': 3, 'bloques_adyacentes': ['C5', 'C9'], 'capacidad_teus': 1820},
            {'codigo': 'C9', 'grupo_movimiento': 3, 'bloques_adyacentes': ['C8'], 'capacidad_teus': 1820}
        ]
        
        for data in bloques_data:
            bloque = Bloque(**data)
            db.add(bloque)
        
        await db.commit()
    
    async def _create_gruas(self, db: AsyncSession):
        """Crea las grúas por defecto"""
        for i in range(1, 13):
            grua = Grua(codigo=f'G{i}')
            db.add(grua)
        
        await db.commit()
    
    async def _create_default_configs(self, db: AsyncSession):
        """Crea configuraciones por defecto"""
        configs = [
            {'clave': 'capacidad_bloque_default', 'valor': '1820', 'tipo': 'int', 
             'descripcion': 'Capacidad por defecto de un bloque en TEUs'},
            {'clave': 'umbral_congestion_alta', 'valor': '150', 'tipo': 'int',
             'descripcion': 'Umbral de movimientos para considerar congestión alta'},
            {'clave': 'tiempo_espera_estimado', 'valor': '25', 'tipo': 'int',
             'descripcion': 'Tiempo de espera estimado en minutos'},
            {'clave': 'productividad_nominal', 'valor': '20', 'tipo': 'int',
             'descripcion': 'Productividad nominal de grúas en mov/hora'},
            {'clave': 'teus_por_bahia', 'valor': '35', 'tipo': 'int',
             'descripcion': 'Capacidad en TEUs por bahía'},
        ]
        
        for config_data in configs:
            config = ConfiguracionSistema(**config_data)
            db.add(config)
        
        await db.commit()
    
    async def _get_config_value(self, db: AsyncSession, clave: str, default: Any) -> Any:
        """Obtiene valor de configuración"""
        result = await db.execute(
            select(ConfiguracionSistema)
            .where(ConfiguracionSistema.clave == clave)
        )
        config = result.scalar_one_or_none()
        
        if not config:
            return default
        
        # Convertir según tipo
        if config.tipo == 'int':
            return int(config.valor)
        elif config.tipo == 'float':
            return float(config.valor)
        elif config.tipo == 'boolean':
            return config.valor.lower() == 'true'
        else:
            return config.valor
    
    # Métodos para obtener datos de Magdalena desde BD
    async def _get_inventarios_from_magdalena_db(
        self, 
        db: AsyncSession, 
        magdalena_id: int, 
        turno: int
    ) -> Dict[str, Any]:
        """Obtiene inventarios desde BD de Magdalena"""
        inventarios = {'exportacion': {}, 'importacion': {}}
        
        # CARGA (exportación lista para cargar)
        result = await db.execute(
            select(
                MovimientoModelo.segregacion_id,
                BloqueMagdalena.codigo.label('bloque'),
                func.count(MovimientoModelo.id).label('cantidad')
            )
            .join(BloqueMagdalena, MovimientoModelo.bloque_origen_id == BloqueMagdalena.id)
            .where(
                and_(
                    MovimientoModelo.instancia_id == magdalena_id,
                    MovimientoModelo.periodo == turno,
                    MovimientoModelo.tipo == 'LOAD'
                )
            )
            .group_by(MovimientoModelo.segregacion_id, BloqueMagdalena.codigo)
        )
        
        for row in result:
            seg = f"S{row.segregacion_id}"
            if seg not in inventarios['exportacion']:
                inventarios['exportacion'][seg] = {}
            inventarios['exportacion'][seg][row.bloque] = row.cantidad
        
        # ENTREGA (importación lista para entregar)
        result = await db.execute(
            select(
                MovimientoModelo.segregacion_id,
                BloqueMagdalena.codigo.label('bloque'),
                func.count(MovimientoModelo.id).label('cantidad')
            )
            .join(BloqueMagdalena, MovimientoModelo.bloque_origen_id == BloqueMagdalena.id)
            .where(
                and_(
                    MovimientoModelo.instancia_id == magdalena_id,
                    MovimientoModelo.periodo == turno,
                    MovimientoModelo.tipo == 'DLVR'
                )
            )
            .group_by(MovimientoModelo.segregacion_id, BloqueMagdalena.codigo)
        )
        
        for row in result:
            seg = f"S{row.segregacion_id}"
            if seg not in inventarios['importacion']:
                inventarios['importacion'][seg] = {}
            inventarios['importacion'][seg][row.bloque] = row.cantidad
        
        return inventarios
    
    async def _get_capacidades_from_magdalena_db(
        self, 
        db: AsyncSession, 
        magdalena_id: int, 
        turno: int
    ) -> Dict[str, Any]:
        """Obtiene capacidades desde BD de Magdalena"""
        capacidades = {}
        
        # Obtener ocupación por bloque
        result = await db.execute(
            select(
                OcupacionBloque.bloque_id,
                BloqueMagdalena.codigo,
                OcupacionBloque.capacidad_total,
                OcupacionBloque.ocupacion_promedio
            )
            .join(BloqueMagdalena)
            .where(
                and_(
                    OcupacionBloque.instancia_id == magdalena_id,
                    OcupacionBloque.periodo == turno
                )
            )
        )
        
        # Por cada bloque, calcular capacidad por segregación
        for row in result:
            bloque_codigo = row.codigo
            
            # Obtener segregaciones en este bloque
            seg_result = await db.execute(
                select(
                    MovimientoModelo.segregacion_id,
                    func.count(func.distinct(MovimientoModelo.bahia_id)).label('bahias')
                )
                .where(
                    and_(
                        MovimientoModelo.instancia_id == magdalena_id,
                        MovimientoModelo.bloque_destino_id == row.bloque_id,
                        MovimientoModelo.periodo <= turno
                    )
                )
                .group_by(MovimientoModelo.segregacion_id)
            )
            
            if bloque_codigo not in capacidades:
                capacidades[bloque_codigo] = {}
            
            teus_por_bahia = await self._get_config_value(db, 'teus_por_bahia', 35)
            
            for seg_row in seg_result:
                seg = f"S{seg_row.segregacion_id}"
                capacidades[bloque_codigo][seg] = seg_row.bahias * teus_por_bahia
        
        return capacidades
    
    # Métodos para procesar datos importados
    async def _process_inventories_magdalena(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        inventarios: Dict[str, Any]
    ):
        """Procesa inventarios importados de Magdalena"""
        bloques = await self._get_bloques_dict(db)
        segregaciones = await self._get_segregaciones_dict(db)
        
        # Procesar exportación
        for seg_code, bloques_data in inventarios.get('exportacion', {}).items():
            if seg_code not in segregaciones:
                continue
                
            for bloque_code, cantidad in bloques_data.items():
                if bloque_code not in bloques:
                    continue
                
                inventario = InventarioInicial(
                    instancia_id=instancia.id,
                    bloque_id=bloques[bloque_code],
                    segregacion_id=segregaciones[seg_code],
                    contenedores_exportacion=cantidad,
                    teus_exportacion=cantidad * 2,  # Ajustar según tipo
                    fuente='magdalena'
                )
                db.add(inventario)
        
        # Procesar importación
        for seg_code, bloques_data in inventarios.get('importacion', {}).items():
            if seg_code not in segregaciones:
                continue
                
            for bloque_code, cantidad in bloques_data.items():
                if bloque_code not in bloques:
                    continue
                
                # Buscar si ya existe
                result = await db.execute(
                    select(InventarioInicial)
                    .where(
                        and_(
                            InventarioInicial.instancia_id == instancia.id,
                            InventarioInicial.bloque_id == bloques[bloque_code],
                            InventarioInicial.segregacion_id == segregaciones[seg_code]
                        )
                    )
                )
                inv = result.scalar_one_or_none()
                
                if inv:
                    inv.contenedores_importacion = cantidad
                    inv.teus_importacion = cantidad * 2
                else:
                    inventario = InventarioInicial(
                        instancia_id=instancia.id,
                        bloque_id=bloques[bloque_code],
                        segregacion_id=segregaciones[seg_code],
                        contenedores_importacion=cantidad,
                        teus_importacion=cantidad * 2,
                        fuente='magdalena'
                    )
                    db.add(inventario)
        
        await db.commit()
    
    async def _process_capacities_magdalena(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        capacidades: Dict[str, Any]
    ):
        """Procesa capacidades importadas de Magdalena"""
        bloques = await self._get_bloques_dict(db)
        segregaciones = await self._get_segregaciones_dict(db)
        
        for bloque_code, segs_data in capacidades.items():
            if bloque_code not in bloques:
                continue
            
            for seg_code, capacidad_teus in segs_data.items():
                if seg_code not in segregaciones:
                    continue
                
                # Obtener tipo de contenedor
                result = await db.execute(
                    select(Segregacion.tipo_contenedor)
                    .where(Segregacion.id == segregaciones[seg_code])
                )
                tipo = result.scalar() or '40'
                
                # Calcular contenedores
                factor = 1 if tipo == '20' else 2
                capacidad_contenedores = capacidad_teus // factor
                
                cap = CapacidadBloque(
                    instancia_id=instancia.id,
                    bloque_id=bloques[bloque_code],
                    segregacion_id=segregaciones[seg_code],
                    capacidad_contenedores=capacidad_contenedores,
                    capacidad_teus=capacidad_teus,
                    bahias_asignadas=capacidad_teus // 35
                )
                db.add(cap)
        
        await db.commit()
    
    async def _process_demands_magdalena(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        demandas_hora: Dict[str, Any]
    ):
        """Procesa demandas por hora importadas de Magdalena"""
        segregaciones = await self._get_segregaciones_dict(db)
        
        # Crear períodos primero
        await self._process_periods(db, instancia)
        
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
            .order_by(PeriodoHora.hora_relativa)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        # Procesar cada tipo de demanda
        for tipo in ['carga', 'descarga', 'recepcion', 'entrega']:
            demandas_tipo = demandas_hora.get(tipo, {})
            
            for seg_code, valores_hora in demandas_tipo.items():
                if seg_code not in segregaciones:
                    continue
                
                for i, cantidad in enumerate(valores_hora):
                    hora = i + 1
                    if hora not in periodos:
                        continue
                    
                    # Buscar si ya existe
                    result = await db.execute(
                        select(DemandaOperacion)
                        .where(
                            and_(
                                DemandaOperacion.instancia_id == instancia.id,
                                DemandaOperacion.segregacion_id == segregaciones[seg_code],
                                DemandaOperacion.periodo_hora_id == periodos[hora]
                            )
                        )
                    )
                    dem = result.scalar_one_or_none()
                    
                    if not dem:
                        dem = DemandaOperacion(
                            instancia_id=instancia.id,
                            segregacion_id=segregaciones[seg_code],
                            periodo_hora_id=periodos[hora]
                        )
                        db.add(dem)
                    
                    setattr(dem, f'demanda_{tipo}', cantidad)
        
        await db.commit()
    
    async def _save_magdalena_hourly_demands(
        self,
        db: AsyncSession,
        instancia_id: int,
        magdalena_instance_id: int,
        demandas_hora: Dict[str, Any]
    ):
        """Guarda las demandas por hora de Magdalena en tabla específica"""
        for tipo in ['carga', 'descarga', 'recepcion', 'entrega']:
            demandas_tipo = demandas_hora.get(tipo, {})
            
            for seg_code, valores_hora in demandas_tipo.items():
                for i, cantidad in enumerate(valores_hora):
                    hora_turno = i + 1
                    
                    # Buscar si ya existe
                    result = await db.execute(
                        select(DemandaHoraMagdalena)
                        .where(
                            and_(
                                DemandaHoraMagdalena.instancia_id == instancia_id,
                                DemandaHoraMagdalena.segregacion == seg_code,
                                DemandaHoraMagdalena.hora_turno == hora_turno
                            )
                        )
                    )
                    dem = result.scalar_one_or_none()
                    
                    if not dem:
                        dem = DemandaHoraMagdalena(
                            instancia_id=instancia_id,
                            magdalena_instance_id=magdalena_instance_id,
                            segregacion=seg_code,
                            hora_turno=hora_turno
                        )
                        db.add(dem)
                    
                    # Actualizar según tipo
                    if tipo == 'carga':
                        dem.dc_carga = cantidad
                    elif tipo == 'descarga':
                        dem.dd_descarga = cantidad
                    elif tipo == 'recepcion':
                        dem.dr_recepcion = cantidad
                    elif tipo == 'entrega':
                        dem.de_entrega = cantidad
        
        await db.commit()
    
# Continuación de camila_service.py desde donde quedó...

    async def _process_parameters(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila, 
        params: Dict[str, Any]
    ):
        """Procesa parámetros generales"""
        parametros = ParametroGeneral(
            instancia_id=instancia.id,
            **params
        )
        db.add(parametros)
        await db.commit()
    
    async def _process_periods(self, db: AsyncSession, instancia: InstanciaCamila):
        """Crea los períodos (horas) del turno"""
        turno = instancia.turno
        hora_base = (turno - 1) * 8
        
        for hora_rel in range(1, 9):
            hora_abs = hora_base + hora_rel
            hora_inicio_int = ((hora_abs - 1) % 24)
            
            periodo = PeriodoHora(
                instancia_id=instancia.id,
                hora_relativa=hora_rel,
                hora_absoluta=hora_abs,
                hora_inicio=time(hora_inicio_int, 0),
                hora_fin=time((hora_inicio_int + 1) % 24, 0),
                dia_semana=((hora_abs - 1) // 24) + 1
            )
            db.add(periodo)
        
        await db.commit()
    
    async def _process_initial_inventory(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        data: Dict[str, Any]
    ):
        """Procesa inventarios iniciales"""
        # Obtener IDs de bloques y segregaciones
        bloques = await self._get_bloques_dict(db)
        segregaciones = await self._get_segregaciones_dict(db, data['segregaciones'])
        
        # Procesar exportación
        for seg_code, bloques_data in data['almacenados_exp'].items():
            if seg_code not in segregaciones:
                continue
                
            for bloque_code, cantidad in bloques_data.items():
                if bloque_code not in bloques:
                    continue
                
                inventario = InventarioInicial(
                    instancia_id=instancia.id,
                    bloque_id=bloques[bloque_code],
                    segregacion_id=segregaciones[seg_code],
                    contenedores_exportacion=cantidad,
                    teus_exportacion=cantidad * 2  # Ajustar según tipo
                )
                db.add(inventario)
        
        # Procesar importación
        for seg_code, bloques_data in data['almacenados_imp'].items():
            if seg_code not in segregaciones:
                continue
                
            for bloque_code, cantidad in bloques_data.items():
                if bloque_code not in bloques:
                    continue
                
                # Buscar si ya existe
                result = await db.execute(
                    select(InventarioInicial)
                    .where(
                        and_(
                            InventarioInicial.instancia_id == instancia.id,
                            InventarioInicial.bloque_id == bloques[bloque_code],
                            InventarioInicial.segregacion_id == segregaciones[seg_code]
                        )
                    )
                )
                inv = result.scalar_one_or_none()
                
                if inv:
                    inv.contenedores_importacion = cantidad
                    inv.teus_importacion = cantidad * 2
                else:
                    inventario = InventarioInicial(
                        instancia_id=instancia.id,
                        bloque_id=bloques[bloque_code],
                        segregacion_id=segregaciones[seg_code],
                        contenedores_importacion=cantidad,
                        teus_importacion=cantidad * 2
                    )
                    db.add(inventario)
        
        await db.commit()
    
    async def _process_demands(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        data: Dict[str, Any]
    ):
        """Procesa demandas operacionales"""
        segregaciones = await self._get_segregaciones_dict(db, data['segregaciones'])
        
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
            .order_by(PeriodoHora.hora_relativa)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        # Procesar cada tipo de demanda
        for seg_code, horas_data in data['demanda_carga'].items():
            if seg_code not in segregaciones:
                continue
            
            for hora, cantidad in horas_data.items():
                if hora not in periodos:
                    continue
                
                demanda = DemandaOperacion(
                    instancia_id=instancia.id,
                    segregacion_id=segregaciones[seg_code],
                    periodo_hora_id=periodos[hora],
                    demanda_carga=cantidad
                )
                db.add(demanda)
        
        # Similar para descarga
        for seg_code, horas_data in data['demanda_descarga'].items():
            if seg_code not in segregaciones:
                continue
            
            for hora, cantidad in horas_data.items():
                if hora not in periodos:
                    continue
                
                # Buscar si ya existe
                result = await db.execute(
                    select(DemandaOperacion)
                    .where(
                        and_(
                            DemandaOperacion.instancia_id == instancia.id,
                            DemandaOperacion.segregacion_id == segregaciones[seg_code],
                            DemandaOperacion.periodo_hora_id == periodos[hora]
                        )
                    )
                )
                dem = result.scalar_one_or_none()
                
                if dem:
                    dem.demanda_descarga = cantidad
                else:
                    demanda = DemandaOperacion(
                        instancia_id=instancia.id,
                        segregacion_id=segregaciones[seg_code],
                        periodo_hora_id=periodos[hora],
                        demanda_descarga=cantidad
                    )
                    db.add(demanda)
        
        # Procesar recepción gate (distribuir en 8 horas)
        for seg_code, total in data['gate_recepcion'].items():
            if seg_code not in segregaciones:
                continue
            
            cantidad_por_hora = total // 8
            resto = total % 8
            
            for hora in range(1, 9):
                cantidad = cantidad_por_hora + (1 if hora <= resto else 0)
                
                if hora not in periodos:
                    continue
                
                # Buscar si ya existe
                result = await db.execute(
                    select(DemandaOperacion)
                    .where(
                        and_(
                            DemandaOperacion.instancia_id == instancia.id,
                            DemandaOperacion.segregacion_id == segregaciones[seg_code],
                            DemandaOperacion.periodo_hora_id == periodos[hora]
                        )
                    )
                )
                dem = result.scalar_one_or_none()
                
                if dem:
                    dem.demanda_recepcion = cantidad
                else:
                    demanda = DemandaOperacion(
                        instancia_id=instancia.id,
                        segregacion_id=segregaciones[seg_code],
                        periodo_hora_id=periodos[hora],
                        demanda_recepcion=cantidad
                    )
                    db.add(demanda)
        
        await db.commit()
    
    async def _process_capacities(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        data: Dict[str, Any]
    ):
        """Procesa capacidades por bloque"""
        bloques = await self._get_bloques_dict(db)
        segregaciones = await self._get_segregaciones_dict(db, data['segregaciones'])
        
        for bloque_code, segs_data in data['capacidad_bloques'].items():
            if bloque_code not in bloques:
                continue
            
            for seg_code, capacidad in segs_data.items():
                if seg_code not in segregaciones:
                    continue
                
                cap = CapacidadBloque(
                    instancia_id=instancia.id,
                    bloque_id=bloques[bloque_code],
                    segregacion_id=segregaciones[seg_code],
                    capacidad_contenedores=capacidad,
                    capacidad_teus=capacidad * 2,  # Ajustar según tipo
                    bahias_asignadas=capacidad // 35  # 35 contenedores por bahía
                )
                db.add(cap)
        
        await db.commit()
    
    async def _process_crane_assignments(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        assignments: List[Dict[str, Any]]
    ):
        """Procesa asignaciones de grúas"""
        bloques = await self._get_bloques_dict(db)
        gruas = await self._get_gruas_dict(db)
        
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        # Crear todas las combinaciones posibles primero
        for grua_id in gruas.values():
            for periodo_id in periodos.values():
                for bloque_id in bloques.values():
                    asig = AsignacionGrua(
                        instancia_id=instancia.id,
                        grua_id=grua_id,
                        bloque_id=bloque_id,
                        periodo_hora_id=periodo_id,
                        asignada=False
                    )
                    db.add(asig)
        
        await db.commit()
        
        # Actualizar las asignadas
        for asig_data in assignments:
            if (asig_data['grua'] not in gruas or 
                asig_data['bloque'] not in bloques or
                asig_data['hora'] not in periodos):
                continue
            
            result = await db.execute(
                select(AsignacionGrua)
                .where(
                    and_(
                        AsignacionGrua.instancia_id == instancia.id,
                        AsignacionGrua.grua_id == gruas[asig_data['grua']],
                        AsignacionGrua.periodo_hora_id == periodos[asig_data['hora']]
                    )
                )
            )
            
            for asig in result.scalars():
                if asig.bloque_id == bloques[asig_data['bloque']]:
                    asig.asignada = True
                    asig.productividad_real = asig_data.get('productividad', 20)
                    asig.movimientos_realizados = asig_data.get('movimientos', 0)
        
        await db.commit()
    
    async def _process_flows(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        flows_data: Dict[str, Any]
    ):
        """Procesa flujos operacionales"""
        bloques = await self._get_bloques_dict(db)
        segregaciones = await self._get_segregaciones_dict(db)
        
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        # Procesar cada tipo de flujo
        for tipo in ['flujo_carga', 'flujo_descarga', 'flujo_recepcion', 'flujo_entrega']:
            flujos = flows_data.get(tipo, {})
            
            for seg_code, bloques_data in flujos.items():
                if seg_code not in segregaciones:
                    continue
                
                for bloque_code, horas_data in bloques_data.items():
                    if bloque_code not in bloques:
                        continue
                    
                    for hora, cantidad in horas_data.items():
                        if hora not in periodos:
                            continue
                        
                        # Buscar si ya existe el flujo
                        result = await db.execute(
                            select(FlujoOperacional)
                            .where(
                                and_(
                                    FlujoOperacional.instancia_id == instancia.id,
                                    FlujoOperacional.segregacion_id == segregaciones[seg_code],
                                    FlujoOperacional.bloque_id == bloques[bloque_code],
                                    FlujoOperacional.periodo_hora_id == periodos[hora]
                                )
                            )
                        )
                        flujo = result.scalar_one_or_none()
                        
                        if not flujo:
                            flujo = FlujoOperacional(
                                instancia_id=instancia.id,
                                segregacion_id=segregaciones[seg_code],
                                bloque_id=bloques[bloque_code],
                                periodo_hora_id=periodos[hora]
                            )
                            db.add(flujo)
                        
                        # Actualizar el tipo específico
                        setattr(flujo, tipo, cantidad)
        
        await db.commit()
    
    async def _process_availability(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        data: Dict[str, Any]
    ):
        """Procesa disponibilidad por bloque"""
        bloques = await self._get_bloques_dict(db)
        
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        # Capacidad por bloque y hora
        capacidades = data.get('capacidad_bloques', {})
        disponibilidades = data.get('disponibilidad', {})
        
        for bloque_code, horas_data in disponibilidades.items():
            if bloque_code not in bloques:
                continue
            
            for hora, disponible in horas_data.items():
                if hora not in periodos:
                    continue
                
                capacidad_total = capacidades.get(bloque_code, {}).get(hora, 0)
                utilizada = capacidad_total - disponible
                
                disp = DisponibilidadBloque(
                    instancia_id=instancia.id,
                    bloque_id=bloques[bloque_code],
                    periodo_hora_id=periodos[hora],
                    movimientos_disponibles=disponible,
                    capacidad_total=capacidad_total,
                    capacidad_utilizada=utilizada,
                    capacidad_libre=disponible,
                    utilizacion_pct=(utilizada / capacidad_total * 100) if capacidad_total > 0 else 0,
                    congestion_index=(utilizada / capacidad_total) if capacidad_total > 0 else 0
                )
                db.add(disp)
        
        await db.commit()
    
    async def _process_quotas(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        quotas_data: List[Dict[str, Any]]
    ):
        """Procesa cuotas de camiones"""
        # Obtener períodos
        result = await db.execute(
            select(PeriodoHora)
            .where(PeriodoHora.instancia_id == instancia.id)
        )
        periodos = {p.hora_relativa: p.id for p in result.scalars()}
        
        for quota_data in quotas_data:
            hora = quota_data['hora']
            if hora not in periodos:
                continue
            
            cuota = CuotaCamion(
                instancia_id=instancia.id,
                periodo_hora_id=periodos[hora],
                cuota_recepcion=quota_data['cuota_recepcion'],
                cuota_entrega=quota_data['cuota_entrega'],
                cuota_total=quota_data['cuota_total'],
                capacidad_disponible=quota_data.get('capacidad_disponible', 0),
                utilizacion_esperada=quota_data.get('utilizacion_esperada', 0)
            )
            db.add(cuota)
        
        await db.commit()
    
    async def _calculate_metrics(
        self, 
        db: AsyncSession, 
        instancia: InstanciaCamila,
        metrics_data: Dict[str, float]
    ):
        """Calcula y guarda métricas"""
        # Obtener datos para cálculos adicionales
        
        # 1. Grúas utilizadas y cambios
        gruas_stats = await self._calculate_crane_stats(db, instancia.id)
        
        # 2. Flujos totales y cumplimiento
        flujos_stats = await self._calculate_flow_stats(db, instancia.id)
        
        # 3. Balance y congestión
        balance_stats = await self._calculate_balance_stats(db, instancia.id)
        
        # 4. Cuotas
        cuotas_stats = await self._calculate_quota_stats(db, instancia.id)
        
        # Crear registro de métricas
        metricas = MetricaResultado(
            instancia_id=instancia.id,
            valor_funcion_objetivo=metrics_data.get('funcion_objetivo', 0),
            gap_optimalidad=metrics_data.get('gap', 0),
            
            # Balance
            desviacion_estandar_carga=balance_stats['desviacion'],
            coeficiente_variacion=balance_stats['cv'],
            indice_balance=100 - balance_stats['cv'],
            
            # Grúas
            gruas_utilizadas_promedio=gruas_stats['promedio_activas'],
            utilizacion_gruas_pct=gruas_stats['utilizacion'],
            cambios_bloque_total=gruas_stats['cambios_totales'],
            cambios_por_grua_promedio=gruas_stats['cambios_promedio'],
            productividad_promedio=gruas_stats['productividad'],
            
            # Flujos
            movimientos_totales=flujos_stats['total'],
            cumplimiento_carga_pct=flujos_stats['cumplimiento_carga'],
            cumplimiento_descarga_pct=flujos_stats['cumplimiento_descarga'],
            cumplimiento_recepcion_pct=flujos_stats['cumplimiento_recepcion'],
            cumplimiento_entrega_pct=flujos_stats['cumplimiento_entrega'],
            
            # Congestión
            congestion_maxima=balance_stats['congestion_maxima'],
            bloque_mas_congestionado=balance_stats['bloque_congestionado'],
            hora_pico=balance_stats['hora_pico'],
            
            # Camiones
            cuota_total_turno=cuotas_stats['total'],
            cuota_promedio_hora=cuotas_stats['promedio'],
            uniformidad_cuotas=cuotas_stats['uniformidad']
        )
        
        db.add(metricas)
        
        # Calcular productividad por grúa
        await self._calculate_crane_productivity(db, instancia.id)
        
        await db.commit()
    
    # Métodos auxiliares
    async def _get_bloques_dict(self, db: AsyncSession) -> Dict[str, int]:
        """Obtiene diccionario codigo -> id de bloques"""
        result = await db.execute(select(Bloque))
        return {b.codigo: b.id for b in result.scalars()}
    
    async def _get_segregaciones_dict(
        self, 
        db: AsyncSession, 
        segregaciones_data: List[Dict[str, Any]] = None
    ) -> Dict[str, int]:
        """Obtiene o crea segregaciones y retorna diccionario codigo -> id"""
        # Si hay datos nuevos, crear segregaciones
        if segregaciones_data:
            for seg_data in segregaciones_data:
                # Verificar si existe
                result = await db.execute(
                    select(Segregacion)
                    .where(Segregacion.codigo == seg_data['codigo'])
                )
                seg = result.scalar_one_or_none()
                
                if not seg:
                    seg = Segregacion(
                        codigo=seg_data['codigo'],
                        tipo_contenedor=seg_data.get('tipo_contenedor', '40'),
                        descripcion=seg_data.get('descripcion', ''),
                        categoria='general'
                    )
                    db.add(seg)
            
            await db.commit()
        
        # Retornar diccionario
        result = await db.execute(select(Segregacion))
        return {s.codigo: s.id for s in result.scalars()}
    
    async def _get_gruas_dict(self, db: AsyncSession) -> Dict[str, int]:
        """Obtiene diccionario codigo -> id de grúas"""
        result = await db.execute(select(Grua))
        return {g.codigo: g.id for g in result.scalars()}
    
    async def _import_segregaciones(self, db: AsyncSession, segregaciones_data: List[Dict[str, Any]]):
        """Importa segregaciones desde Magdalena"""
        for seg_data in segregaciones_data:
            # Verificar si existe
            result = await db.execute(
                select(Segregacion)
                .where(Segregacion.codigo == seg_data['codigo'])
            )
            seg = result.scalar_one_or_none()
            
            if not seg:
                seg = Segregacion(
                    codigo=seg_data['codigo'],
                    tipo_contenedor=seg_data['tipo_contenedor'],
                    descripcion=seg_data.get('descripcion', ''),
                    categoria='general'
                )
                db.add(seg)
        
        await db.commit()
    
    async def _calculate_crane_stats(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Calcula estadísticas de grúas"""
        # Grúas activas por hora
        result = await db.execute(
            select(
                PeriodoHora.hora_relativa,
                func.count(AsignacionGrua.id).label('gruas_activas')
            )
            .join(AsignacionGrua)
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True
                )
            )
            .group_by(PeriodoHora.hora_relativa)
        )
        
        gruas_por_hora = [row.gruas_activas for row in result]
        promedio_activas = np.mean(gruas_por_hora) if gruas_por_hora else 0
        
        # Cambios de bloque
        cambios = await self._calculate_block_changes(db, instance_id)
        cambios_totales = sum(cambios.values())
        cambios_promedio = cambios_totales / 12  # 12 grúas
        
        # Productividad
        result = await db.execute(
            select(func.avg(AsignacionGrua.productividad_real))
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True,
                    AsignacionGrua.productividad_real.is_not(None)
                )
            )
        )
        productividad = result.scalar() or 18
        
        return {
            'promedio_activas': promedio_activas,
            'utilizacion': (promedio_activas / 12) * 100,
            'cambios_totales': cambios_totales,
            'cambios_promedio': cambios_promedio,
            'productividad': productividad
        }
    
    async def _calculate_flow_stats(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Calcula estadísticas de flujos"""
        # Totales por tipo
        result = await db.execute(
            select(
                func.sum(FlujoOperacional.flujo_carga).label('carga'),
                func.sum(FlujoOperacional.flujo_descarga).label('descarga'),
                func.sum(FlujoOperacional.flujo_recepcion).label('recepcion'),
                func.sum(FlujoOperacional.flujo_entrega).label('entrega')
            )
            .where(FlujoOperacional.instancia_id == instance_id)
        )
        
        flujos = result.first()
        total = (flujos.carga or 0) + (flujos.descarga or 0) + \
                (flujos.recepcion or 0) + (flujos.entrega or 0)
        
        # Demandas totales
        result = await db.execute(
            select(
                func.sum(DemandaOperacion.demanda_carga).label('carga'),
                func.sum(DemandaOperacion.demanda_descarga).label('descarga'),
                func.sum(DemandaOperacion.demanda_recepcion).label('recepcion'),
                func.sum(DemandaOperacion.demanda_entrega).label('entrega')
            )
            .where(DemandaOperacion.instancia_id == instance_id)
        )
        
        demandas = result.first()
        
        # Calcular cumplimiento
        cumplimiento_carga = (flujos.carga / demandas.carga * 100) if demandas.carga else 100
        cumplimiento_descarga = (flujos.descarga / demandas.descarga * 100) if demandas.descarga else 100
        cumplimiento_recepcion = (flujos.recepcion / demandas.recepcion * 100) if demandas.recepcion else 100
        cumplimiento_entrega = (flujos.entrega / demandas.entrega * 100) if demandas.entrega else 100
        
        return {
            'total': total,
            'cumplimiento_carga': min(cumplimiento_carga, 100),
            'cumplimiento_descarga': min(cumplimiento_descarga, 100),
            'cumplimiento_recepcion': min(cumplimiento_recepcion, 100),
            'cumplimiento_entrega': min(cumplimiento_entrega, 100)
        }
    
    async def _calculate_balance_stats(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Calcula estadísticas de balance"""
        # Movimientos por bloque
        result = await db.execute(
            select(
                Bloque.codigo,
                func.sum(
                    FlujoOperacional.flujo_carga + 
                    FlujoOperacional.flujo_descarga +
                    FlujoOperacional.flujo_recepcion + 
                    FlujoOperacional.flujo_entrega
                ).label('total')
            )
            .join(Bloque)
            .where(FlujoOperacional.instancia_id == instance_id)
            .group_by(Bloque.codigo)
        )
        
        movimientos = [row.total or 0 for row in result]
        
        if movimientos:
            media = np.mean(movimientos)
            desviacion = np.std(movimientos)
            cv = (desviacion / media * 100) if media > 0 else 0
        else:
            desviacion = 0
            cv = 0
        
        # Congestión máxima
        result = await db.execute(
            select(
                DisponibilidadBloque.congestion_index,
                Bloque.codigo,
                PeriodoHora.hora_relativa
            )
            .join(Bloque)
            .join(PeriodoHora)
            .where(DisponibilidadBloque.instancia_id == instance_id)
            .order_by(DisponibilidadBloque.congestion_index.desc())
            .limit(1)
        )
        
        congestion_row = result.first()
        
        return {
            'desviacion': desviacion,
            'cv': cv,
            'congestion_maxima': congestion_row.congestion_index if congestion_row else 0,
            'bloque_congestionado': congestion_row.codigo if congestion_row else 'N/A',
            'hora_pico': congestion_row.hora_relativa if congestion_row else 0
        }
    
    async def _calculate_quota_stats(self, db: AsyncSession, instance_id: int) -> Dict[str, Any]:
        """Calcula estadísticas de cuotas"""
        result = await db.execute(
            select(
                func.sum(CuotaCamion.cuota_total).label('total'),
                func.avg(CuotaCamion.cuota_total).label('promedio')
            )
            .where(CuotaCamion.instancia_id == instance_id)
        )
        
        row = result.first()
        total = row.total or 0
        promedio = row.promedio or 0
        
        # Uniformidad
        result = await db.execute(
            select(CuotaCamion.cuota_total)
            .where(CuotaCamion.instancia_id == instance_id)
        )
        
        cuotas = [row[0] for row in result]
        
        if cuotas and promedio > 0:
            desv = np.std(cuotas)
            uniformidad = 1 - (desv / promedio)
        else:
            uniformidad = 0
        
        return {
            'total': total,
            'promedio': promedio,
            'uniformidad': uniformidad
        }
    
    async def _calculate_block_changes(self, db: AsyncSession, instance_id: int) -> Dict[str, int]:
        """Calcula cambios de bloque por grúa"""
        result = await db.execute(
            select(
                Grua.codigo,
                AsignacionGrua.bloque_id,
                PeriodoHora.hora_relativa
            )
            .join(Grua)
            .join(PeriodoHora)
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True
                )
            )
            .order_by(Grua.codigo, PeriodoHora.hora_relativa)
        )
        
        cambios = {}
        grua_actual = None
        bloque_anterior = None
        
        for row in result:
            if row.codigo != grua_actual:
                grua_actual = row.codigo
                bloque_anterior = row.bloque_id
                cambios[grua_actual] = 0
            elif row.bloque_id != bloque_anterior:
                cambios[grua_actual] += 1
                bloque_anterior = row.bloque_id
        
        return cambios
    
    async def _calculate_crane_productivity(self, db: AsyncSession, instance_id: int):
        """Calcula productividad por grúa"""
        # Obtener movimientos por grúa
        result = await db.execute(
            select(
                AsignacionGrua.grua_id,
                func.count(AsignacionGrua.id).label('horas'),
                func.sum(AsignacionGrua.movimientos_realizados).label('movimientos')
            )
            .where(
                and_(
                    AsignacionGrua.instancia_id == instance_id,
                    AsignacionGrua.asignada == True
                )
            )
            .group_by(AsignacionGrua.grua_id)
        )
        
        for row in result:
            productividad = (row.movimientos / row.horas) if row.horas > 0 else 0
            eficiencia = (productividad / 20) * 100  # 20 es nominal
            
            prod = ProductividadGrua(
                instancia_id=instance_id,
                grua_id=row.grua_id,
                horas_trabajadas=row.horas,
                movimientos_totales=row.movimientos or 0,
                productividad_real=productividad,
                eficiencia_pct=eficiencia
            )
            db.add(prod)
        
        await db.commit()