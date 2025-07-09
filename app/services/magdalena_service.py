import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, delete

from app.models.magdalena import MagdalenaRun

class MagdalenaService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.base_path = Path("/app/optimization_files")
    
    async def cargar_archivo_completo(self, fecha: str, participacion: int, con_dispersion: bool) -> Dict[str, Any]:
        """Carga y calcula todas las métricas de los archivos"""
        
        fecha_obj = datetime.strptime(fecha, '%Y-%m-%d')
        anio = fecha_obj.year
        semana = fecha_obj.isocalendar()[1]
        
        # Construir paths
        dispersion_suffix = 'K' if con_dispersion else 'N'
        resultado_path = self.base_path / f"resultados_magdalena/{fecha}/resultado_{fecha}_{participacion}_{dispersion_suffix}.xlsx"
        flujos_path = self.base_path / f"instancias_magdalena/{fecha}/Flujos_w{fecha.replace('-', '')}.xlsx"
        instancia_path = self.base_path / f"instancias_magdalena/{fecha}/Instancia_{fecha}_{participacion}_{dispersion_suffix}.xlsx"
        
        if not resultado_path.exists():
            return {"status": "error", "message": f"No existe: {resultado_path}"}
        
        try:
            # Leer archivos
            xl_resultado = pd.ExcelFile(resultado_path)
            
            # Cargar hojas necesarias
            df_general = pd.read_excel(xl_resultado, 'General')
            df_flujos = pd.read_excel(xl_resultado, 'Flujos')
            df_carga = pd.read_excel(xl_resultado, 'Carga máx-min')
            df_ocupacion = pd.read_excel(xl_resultado, 'Ocupación Bloques')
            df_workload = pd.read_excel(xl_resultado, 'Workload bloques')
            df_bahias = pd.read_excel(xl_resultado, 'Bahías por bloques') if 'Bahías por bloques' in xl_resultado.sheet_names else None
            df_volumen = pd.read_excel(xl_resultado, 'Volumen bloques (TEUs)') if 'Volumen bloques (TEUs)' in xl_resultado.sheet_names else None
            
            # Cargar datos reales si existen
            movimientos_reales_total = 0
            yard_total = 0
            movimientos_reales_tipo = {}
            
            if flujos_path.exists():
                df_flujos_real = pd.read_excel(flujos_path)
                
                # Calcular totales reales
                for col in ['DLVR', 'DSCH', 'LOAD', 'RECV', 'YARD', 'OTHR']:
                    if col in df_flujos_real.columns:
                        valor = int(df_flujos_real[col].sum())
                        movimientos_reales_tipo[col] = valor
                        if col == 'YARD':
                            yard_total = valor
                        movimientos_reales_total += valor
            
            # Calcular movimientos optimizados totales
            movimientos_opt_total = 0
            movimientos_opt_tipo = {}
            
            for col in ['Recepción', 'Carga', 'Descarga', 'Entrega']:
                if col in df_general.columns:
                    valor = int(df_general[col].sum())
                    movimientos_opt_tipo[col.lower()] = valor
                    movimientos_opt_total += valor
            
            # Calcular eficiencias
            eficiencia_ganada = 0
            eficiencia_op_real = 0
            
            if movimientos_reales_total > 0:
                # Eficiencia operacional real
                eficiencia_op_real = ((movimientos_reales_total - yard_total) / movimientos_reales_total) * 100
                
                # Eficiencia ganada
                mov_productivos_reales = movimientos_reales_total - yard_total
                if mov_productivos_reales > 0:
                    reduccion = mov_productivos_reales - movimientos_opt_total
                    eficiencia_ganada = (reduccion / mov_productivos_reales) * 100
            
            # Procesar por período
            periodos = sorted(df_general['Periodo'].unique())
            registros_creados = 0
            
            for periodo in periodos:
                # Filtrar datos del período
                general_periodo = df_general[df_general['Periodo'] == periodo]
                carga_periodo = df_carga[df_carga['Periodo'] == periodo].iloc[0] if periodo in df_carga['Periodo'].values else None
                ocupacion_periodo = df_ocupacion[df_ocupacion['Periodo'] == periodo]
                workload_periodo = df_workload[df_workload['Periodo'] == periodo]
                
                # Métricas del período
                carga_maxima = int(carga_periodo['Carga máxima']) if carga_periodo is not None else 0
                carga_minima = int(carga_periodo['Carga mínima']) if carga_periodo is not None else 0
                
                # Ocupación por bloque
                ocupacion_por_bloque = {}
                volumen_total = 0
                capacidad_total = 0
                
                for _, row in ocupacion_periodo.iterrows():
                    bloque = row['Bloque']
                    volumen = row['Volumen bloques (TEUs)']
                    capacidad = row['Capacidad Bloque']
                    
                    volumen_total += volumen
                    capacidad_total += capacidad
                    
                    if capacidad > 0:
                        ocupacion_por_bloque[bloque] = round((volumen / capacidad) * 100, 1)
                    else:
                        ocupacion_por_bloque[bloque] = 0
                
                ocupacion_promedio = round((volumen_total / capacidad_total * 100), 1) if capacidad_total > 0 else 0
                
                # Workload por bloque
                workload_por_bloque = {}
                carga_trabajo_total = 0
                
                for _, row in workload_periodo.iterrows():
                    bloque = row['Bloque']
                    carga = row['Carga de trabajo']
                    workload_por_bloque[bloque] = round(carga, 1)
                    carga_trabajo_total += carga
                
                # Balance (desviación estándar de cargas)
                cargas = list(workload_por_bloque.values())
                balance_carga = round(float(np.std(cargas)), 1) if len(cargas) > 1 else 0
                
                # Segregaciones activas en el período
                segregaciones_activas = general_periodo['Segregación'].nunique()
                
                # Bloques activos (con movimientos)
                bloques_con_movimientos = general_periodo[
                    (general_periodo['Recepción'] > 0) | 
                    (general_periodo['Carga'] > 0) | 
                    (general_periodo['Descarga'] > 0) | 
                    (general_periodo['Entrega'] > 0)
                ]['Bloque'].nunique()
                
                # Bahías por segregación (si existe la hoja)
                bahias_por_segregacion = {}
                if df_bahias is not None:
                    bahias_periodo = df_bahias[df_bahias['Periodo'] == periodo]
                    for _, row in bahias_periodo.iterrows():
                        seg = row['Segregación']
                        if seg not in bahias_por_segregacion:
                            bahias_por_segregacion[seg] = 0
                        bahias_por_segregacion[seg] += row['Bahías ocupadas']
                
                # Verificar si ya existe
                result = await self.db.execute(
                    select(MagdalenaRun).where(
                        and_(
                            MagdalenaRun.anio == anio,
                            MagdalenaRun.semana == semana,
                            MagdalenaRun.turno == int(periodo),
                            MagdalenaRun.participacion == participacion,
                            MagdalenaRun.con_dispersion == con_dispersion
                        )
                    )
                )
                existing = result.scalar_one_or_none()
                
                if existing:
                    await self.db.execute(
                        delete(MagdalenaRun).where(
                            MagdalenaRun.id == existing.id
                        )
                    )
                
                run = MagdalenaRun(
                    anio=anio,
                    semana=semana,
                    turno=int(periodo),
                    participacion=participacion,
                    con_dispersion=con_dispersion,
                    
                    # Métricas calculadas
                    reubicaciones_eliminadas=yard_total,
                    movimientos_reales=movimientos_reales_total,
                    movimientos_optimizados=movimientos_opt_total,
                    eficiencia_ganada=round(eficiencia_ganada, 2),
                    eficiencia_operacional_real=round(eficiencia_op_real, 1),
                    eficiencia_operacional_opt=100.0,
                    
                    # Del período específico
                    carga_maxima=carga_maxima,
                    carga_minima=carga_minima,
                    balance_carga=balance_carga,
                    ocupacion_promedio=ocupacion_promedio,
                    volumen_total_teus=round(volumen_total, 1),
                    capacidad_total=round(capacidad_total, 1),
                    carga_trabajo_total=round(carga_trabajo_total, 1),
                    segregaciones_activas=segregaciones_activas,
                    bloques_activos=bloques_con_movimientos,
                    
                    # JSONs
                    movimientos_por_tipo=movimientos_opt_tipo,
                    movimientos_reales_tipo=movimientos_reales_tipo,
                    ocupacion_por_bloque=ocupacion_por_bloque,
                    workload_por_bloque=workload_por_bloque,
                    bahias_por_segregacion=bahias_por_segregacion,
                    
                    # Archivos
                    archivo_resultado=resultado_path.name,
                    archivo_flujos=flujos_path.name if flujos_path.exists() else None,
                    archivo_instancia=instancia_path.name if instancia_path.exists() else None
                )
                
                self.db.add(run)
                registros_creados += 1
            
            await self.db.commit()
            
            return {
                "status": "success",
                "message": f"Cargados {registros_creados} turnos",
                "fecha": fecha,
                "anio": anio,
                "semana": semana,
                "participacion": participacion,
                "con_dispersion": con_dispersion,
                "registros": registros_creados
            }
            
        except Exception as e:
            await self.db.rollback()
            import traceback
            traceback.print_exc()
            return {"status": "error", "message": str(e)}
    
    async def get_dashboard_data(self, anio: int, semana: int, turno: int, 
                                participacion: int, con_dispersion: bool) -> Dict[str, Any]:
        """Obtiene datos para el dashboard con todos los cálculos"""
        
        result = await self.db.execute(
            select(MagdalenaRun).where(
                and_(
                    MagdalenaRun.anio == anio,
                    MagdalenaRun.semana == semana,
                    MagdalenaRun.turno == turno,
                    MagdalenaRun.participacion == participacion,
                    MagdalenaRun.con_dispersion == con_dispersion
                )
            )
        )
        run = result.scalar_one_or_none()
        
        if not run:
            return {"dataNotAvailable": True}
        
        # Construir respuesta con datos calculados
        return {
            "magdalenaMetrics": {
                # KPIs principales
                "reubicacionesEliminadas": run.reubicaciones_eliminadas,
                "eficienciaGanada": run.eficiencia_ganada,
                "segregacionesActivas": run.segregaciones_activas,
                "balanceCarga": run.balance_carga,
                "ocupacionPromedio": run.ocupacion_promedio,
                "cargaTrabajoTotal": int(run.carga_trabajo_total),
                "movimientosOptimizados": run.movimientos_optimizados,
                "variacionCarga": run.carga_maxima - run.carga_minima,
                
                # Comparación
                "totalMovimientos": run.movimientos_reales,
                "eficienciaReal": run.eficiencia_operacional_real,
                "eficienciaOptimizada": run.eficiencia_operacional_opt,
                
                # Detalles
                "movimientosReales": run.movimientos_reales_tipo,
                "movimientosOptimizadosDetalle": run.movimientos_por_tipo,
                
                # Por bloque
                "ocupacionPorBloque": run.ocupacion_por_bloque,
                "workloadPorBloque": run.workload_por_bloque,
                
                # Bloques formateados
                "bloquesMagdalena": self._format_bloques(run),
                
                # Adicionales
                "bloquesActivos": run.bloques_activos,
                "capacidadTotal": run.capacidad_total,
                "volumenTotal": run.volumen_total_teus,
                "cargaMaxima": run.carga_maxima,
                "cargaMinima": run.carga_minima
            },
            "realMetrics": {
                "totalMovimientos": run.movimientos_reales,
                "reubicaciones": run.reubicaciones_eliminadas,
                "movimientosPorTipo": run.movimientos_reales_tipo,
                "porcentajeReubicaciones": round((run.reubicaciones_eliminadas / run.movimientos_reales * 100), 1) if run.movimientos_reales > 0 else 0
            },
            "comparison": {
                "eliminacionReubicaciones": run.reubicaciones_eliminadas,
                "mejoraPorcentual": run.eficiencia_ganada,
                "optimizacionSegregaciones": run.segregaciones_activas,
                "eficienciaTotal": 100
            },
            "lastUpdated": run.fecha_carga.isoformat(),
            "dataNotAvailable": False
        }
    
    def _format_bloques(self, run: MagdalenaRun) -> List[Dict]:
        """Formatea información de bloques para el frontend"""
        bloques = []
        
        # Usar bloques reales de los datos
        for bloque, ocupacion in run.ocupacion_por_bloque.items():
            workload = run.workload_por_bloque.get(bloque, 0)
            
            bloques.append({
                'bloqueId': bloque,
                'ocupacionPromedio': int(ocupacion),
                'ocupacionTurno': ocupacion,
                'cargaTrabajo': workload,
                'capacidad': run.capacidad_total / len(run.ocupacion_por_bloque) if len(run.ocupacion_por_bloque) > 0 else 0,
                'estado': 'activo' if workload > 0 else 'inactivo'
            })
        
        # Ordenar por ID de bloque
        bloques.sort(key=lambda x: x['bloqueId'])
        
        return bloques