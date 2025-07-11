# app/services/camila_loader.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, time
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class CamilaLoader:
    """Servicio para cargar archivos Excel del modelo Camila"""
    
    # Hojas esperadas en cada tipo de archivo
    INSTANCE_SHEETS = {
        'Parametros_generales', 'Bloques', 'Segregaciones', 
        'Capacidad_bloques', 'Demanda_carga', 'Demanda_descarga',
        'Almacenados_exp', 'Almacenados_imp', 'Gate_recepcion'
    }
    
    RESULTS_SHEETS = {
        'Asignacion_gruas', 'Flujo_carga', 'Flujo_descarga',
        'Flujo_recepcion', 'Flujo_entrega', 'Capacidad_bloques',
        'Disponibilidad', 'Metricas', 'Cuotas_calculadas'
    }
    
    MAGDALENA_SHEETS = {
        'Cargar', 'Entregar', 'Recibir', 'Volumen bloques (TEUs)',
        'Bahías por bloques', 'S', 'D_params_168h'
    }
    
    def __init__(self):
        self.errors = []
        
    def read_instance_file(self, filepath: str) -> Dict[str, Any]:
        """Lee archivo de instancia de Camila"""
        try:
            logger.info(f"Leyendo archivo de instancia: {filepath}")
            xl = pd.ExcelFile(filepath)
            
            # Validar estructura
            if not self._validate_sheets(xl.sheet_names, self.INSTANCE_SHEETS, 'instancia'):
                raise ValueError(f"Estructura inválida. Errores: {self.errors}")
            
            data = {}
            
            # 1. Parámetros generales
            params_df = pd.read_excel(xl, 'Parametros_generales')
            data['parametros'] = self._parse_parameters(params_df)
            
            # 2. Bloques
            bloques_df = pd.read_excel(xl, 'Bloques')
            data['bloques'] = bloques_df['Bloque'].tolist() if 'Bloque' in bloques_df.columns else []
            
            # 3. Segregaciones
            segregaciones_df = pd.read_excel(xl, 'Segregaciones')
            data['segregaciones'] = self._parse_segregaciones(segregaciones_df)
            
            # 4. Capacidad de bloques
            cap_bloques_df = pd.read_excel(xl, 'Capacidad_bloques')
            data['capacidad_bloques'] = self._parse_capacidad_bloques(cap_bloques_df)
            
            # 5. Demandas
            data['demanda_carga'] = self._parse_demanda(pd.read_excel(xl, 'Demanda_carga'))
            data['demanda_descarga'] = self._parse_demanda(pd.read_excel(xl, 'Demanda_descarga'))
            
            # 6. Inventarios iniciales
            data['almacenados_exp'] = self._parse_inventario(pd.read_excel(xl, 'Almacenados_exp'))
            data['almacenados_imp'] = self._parse_inventario(pd.read_excel(xl, 'Almacenados_imp'))
            
            # 7. Gate recepción
            gate_df = pd.read_excel(xl, 'Gate_recepcion')
            data['gate_recepcion'] = self._parse_gate_recepcion(gate_df)
            
            logger.info(f"Instancia cargada exitosamente: {len(data['segregaciones'])} segregaciones")
            return data
            
        except Exception as e:
            logger.error(f"Error leyendo archivo de instancia: {str(e)}")
            raise
    
    def read_results_file(self, filepath: str) -> Dict[str, Any]:
        """Lee archivo de resultados de Camila"""
        try:
            logger.info(f"Leyendo archivo de resultados: {filepath}")
            xl = pd.ExcelFile(filepath)
            
            # Validar estructura
            if not self._validate_sheets(xl.sheet_names, self.RESULTS_SHEETS, 'resultados'):
                raise ValueError(f"Estructura inválida. Errores: {self.errors}")
            
            data = {}
            
            # 1. Asignación de grúas
            asig_df = pd.read_excel(xl, 'Asignacion_gruas')
            data['asignacion_gruas'] = self._parse_asignacion_gruas(asig_df)
            
            # 2. Flujos
            data['flujo_carga'] = self._parse_flujo(pd.read_excel(xl, 'Flujo_carga'))
            data['flujo_descarga'] = self._parse_flujo(pd.read_excel(xl, 'Flujo_descarga'))
            data['flujo_recepcion'] = self._parse_flujo(pd.read_excel(xl, 'Flujo_recepcion'))
            data['flujo_entrega'] = self._parse_flujo(pd.read_excel(xl, 'Flujo_entrega'))
            
            # 3. Capacidades y disponibilidad
            data['capacidad_bloques'] = self._parse_capacidad_resultado(
                pd.read_excel(xl, 'Capacidad_bloques')
            )
            data['disponibilidad'] = self._parse_disponibilidad(
                pd.read_excel(xl, 'Disponibilidad')
            )
            
            # 4. Métricas
            metricas_df = pd.read_excel(xl, 'Metricas')
            data['metricas'] = self._parse_metricas(metricas_df)
            
            # 5. Cuotas calculadas
            cuotas_df = pd.read_excel(xl, 'Cuotas_calculadas')
            data['cuotas'] = self._parse_cuotas(cuotas_df)
            
            logger.info("Resultados cargados exitosamente")
            return data
            
        except Exception as e:
            logger.error(f"Error leyendo archivo de resultados: {str(e)}")
            raise
    
    def read_magdalena_files(self, instance_filepath: str, result_filepath: str, turno: int) -> Dict[str, Any]:
        """Lee datos de archivos de Magdalena para importar a Camila"""
        try:
            logger.info(f"Leyendo datos de Magdalena para turno {turno}")
            
            # Calcular horas del turno
            h_ini = (turno - 1) * 8 + 1
            h_fin = turno * 8
            
            data = {
                'turno': turno,
                'horas': list(range(h_ini, h_fin + 1)),
                'inventarios': {},
                'demandas': {},
                'capacidades': {},
                'segregaciones': [],
                'demandas_hora': {}  # NUEVO: demandas por hora
            }
            
            # Leer archivo de instancia (para D_params_168h)
            xl_instance = pd.ExcelFile(instance_filepath)
            
            # 1. Segregaciones
            if 'S' in xl_instance.sheet_names:
                seg_df = pd.read_excel(xl_instance, 'S')
                data['segregaciones'] = self._extract_magdalena_segregaciones(seg_df)
            
            # 2. NUEVO: Demandas por hora desde D_params_168h
            if 'D_params_168h' in xl_instance.sheet_names:
                d_params_df = pd.read_excel(xl_instance, 'D_params_168h')
                data['demandas_hora'] = self._extract_magdalena_demands_hourly(
                    d_params_df, h_ini, h_fin
                )
            
            # Leer archivo de resultados
            xl_result = pd.ExcelFile(result_filepath)
            
            # 3. Inventarios iniciales (Cargar y Entregar)
            if 'Cargar' in xl_result.sheet_names:
                cargar_df = pd.read_excel(xl_result, 'Cargar')
                data['inventarios']['exportacion'] = self._extract_magdalena_inventory(
                    cargar_df, turno, 'Cargar'
                )
            
            if 'Entregar' in xl_result.sheet_names:
                entregar_df = pd.read_excel(xl_result, 'Entregar')
                data['inventarios']['importacion'] = self._extract_magdalena_inventory(
                    entregar_df, turno, 'Entregar'
                )
            
            # 4. Capacidades (Bahías por bloques)
            if 'Bahías por bloques' in xl_result.sheet_names:
                bahias_df = pd.read_excel(xl_result, 'Bahías por bloques')
                data['capacidades'] = self._extract_magdalena_capacity(bahias_df, turno)
            
            # 5. Gate recepción (calculado desde D_params_168h)
            data['gate_recepcion'] = self._calculate_gate_reception(data['demandas_hora'])
            
            logger.info(f"Datos de Magdalena extraídos: {len(data['segregaciones'])} segregaciones")
            return data
            
        except Exception as e:
            logger.error(f"Error leyendo datos de Magdalena: {str(e)}")
            raise
    
    def validate_file_structure(self, filepath: str) -> Dict[str, Any]:
        """Valida la estructura de un archivo Excel"""
        try:
            xl = pd.ExcelFile(filepath)
            sheets = set(xl.sheet_names)
            
            # Determinar tipo de archivo
            if sheets & self.INSTANCE_SHEETS:
                file_type = 'instance'
                expected = self.INSTANCE_SHEETS
            elif sheets & self.RESULTS_SHEETS:
                file_type = 'results'
                expected = self.RESULTS_SHEETS
            elif sheets & self.MAGDALENA_SHEETS:
                file_type = 'magdalena'
                expected = self.MAGDALENA_SHEETS
            else:
                file_type = 'unknown'
                expected = set()
            
            missing = expected - sheets
            extra = sheets - expected
            
            return {
                'is_valid': len(missing) == 0,
                'file_type': file_type,
                'missing_sheets': list(missing),
                'extra_sheets': list(extra),
                'errors': self.errors
            }
            
        except Exception as e:
            return {
                'is_valid': False,
                'file_type': 'error',
                'missing_sheets': [],
                'extra_sheets': [],
                'errors': [str(e)]
            }
    
    # Métodos privados de parseo
    def _validate_sheets(self, actual: List[str], expected: set, file_type: str) -> bool:
        """Valida que las hojas necesarias estén presentes"""
        actual_set = set(actual)
        missing = expected - actual_set
        
        if missing:
            self.errors.append(f"Faltan hojas en archivo {file_type}: {missing}")
            return False
        return True
    
    def _parse_parameters(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Parsea parámetros generales"""
        params = {}
        for _, row in df.iterrows():
            if 'Parametro' in row and 'Valor' in row:
                param_name = row['Parametro'].lower().replace(' ', '_')
                params[param_name] = row['Valor']
        
        # Valores por defecto
        return {
            'r_max': params.get('r_max', 12),
            't_periodos': params.get('t_periodos', 8),
            'mu_productividad': params.get('mu', 20),
            'w_max_gruas_bloque': params.get('w', 2),
            'k_permanencia_min': params.get('k', 2)
        }
    
    def _parse_segregaciones(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parsea segregaciones"""
        segregaciones = []
        
        for _, row in df.iterrows():
            seg = {
                'codigo': row.get('Segregacion', row.get('S', '')),
                'descripcion': row.get('Descripcion', ''),
                'tipo_contenedor': '40' if '40' in str(row.get('Segregacion', '')) else '20'
            }
            segregaciones.append(seg)
        
        return segregaciones
    
    def _parse_capacidad_bloques(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """Parsea capacidad de bloques (Cbs)"""
        capacidad = {}
        
        # El formato puede variar, intentar diferentes estructuras
        if 'Bloque' in df.columns and 'Segregacion' in df.columns:
            # Formato: Bloque | Segregacion | Capacidad
            for _, row in df.iterrows():
                bloque = row['Bloque']
                seg = row['Segregacion']
                cap = row.get('Capacidad', 0)
                
                if bloque not in capacidad:
                    capacidad[bloque] = {}
                capacidad[bloque][seg] = int(cap)
        else:
            # Formato: Segregaciones en filas, Bloques en columnas
            for seg in df.index:
                for bloque in df.columns:
                    if bloque.startswith('C'):
                        if bloque not in capacidad:
                            capacidad[bloque] = {}
                        capacidad[bloque][str(seg)] = int(df.loc[seg, bloque])
        
        return capacidad
    
    def _parse_demanda(self, df: pd.DataFrame) -> Dict[str, Dict[int, int]]:
        """Parsea demanda por hora"""
        demanda = {}
        
        # Formato esperado: Segregacion en filas, Horas en columnas (1-8)
        for idx, row in df.iterrows():
            seg = str(row.get('Segregacion', idx))
            demanda[seg] = {}
            
            for hora in range(1, 9):
                col_name = f'H{hora}'
                if col_name in df.columns:
                    demanda[seg][hora] = int(row.get(col_name, 0))
                elif hora in df.columns:
                    demanda[seg][hora] = int(row.get(hora, 0))
                else:
                    demanda[seg][hora] = 0
        
        return demanda
    
    def _parse_inventario(self, df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
        """Parsea inventario inicial"""
        inventario = {}
        
        # Formato: Segregacion | Bloque | Cantidad
        if 'Segregacion' in df.columns and 'Bloque' in df.columns:
            for _, row in df_turno.iterrows():
                seg = str(row['Segregacion'])
                bloque = row['Bloque']
                cantidad = int(row.get('Cantidad', row.get('Contenedores', 0)))
                
                if seg not in inventario:
                    inventario[seg] = {}
                inventario[seg][bloque] = cantidad
        else:
            # Formato alternativo: Segregaciones en filas, Bloques en columnas
            for seg in df.index:
                inventario[str(seg)] = {}
                for bloque in df.columns:
                    if bloque.startswith('C'):
                        inventario[str(seg)][bloque] = int(df.loc[seg, bloque])
        
        return inventario
    
    def _parse_gate_recepcion(self, df: pd.DataFrame) -> Dict[str, int]:
        """Parsea recepción total por gate"""
        recepcion = {}
        
        for _, row in df.iterrows():
            seg = str(row.get('Segregacion', row.get('S', '')))
            cantidad = int(row.get('Cantidad', row.get('Total', 0)))
            recepcion[seg] = cantidad
        
        return recepcion
    
    def _parse_asignacion_gruas(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parsea matriz de asignación de grúas Y(g,b,t)"""
        asignaciones = []
        
        # Formato esperado: Gruas en filas, columnas como B1_T1, B2_T1, etc.
        for grua_idx, row in df.iterrows():
            grua = f"G{grua_idx + 1}" if isinstance(grua_idx, int) else str(grua_idx)
            
            for col in df.columns:
                if '_' in col and row[col] == 1:
                    parts = col.split('_')
                    if len(parts) == 2:
                        bloque = parts[0]
                        hora = int(parts[1].replace('T', ''))
                        
                        asignaciones.append({
                            'grua': grua,
                            'bloque': bloque,
                            'hora': hora,
                            'asignada': True
                        })
        
        return asignaciones
    
    def _parse_flujo(self, df: pd.DataFrame) -> Dict[str, Dict[str, Dict[int, int]]]:
        """Parsea flujos operacionales"""
        flujos = {}
        
        # Formato: Segregacion | Bloque | H1 | H2 | ... | H8
        for _, row in df.iterrows():
            seg = str(row.get('Segregacion', row.get('S', '')))
            bloque = row.get('Bloque', '')
            
            if seg not in flujos:
                flujos[seg] = {}
            if bloque not in flujos[seg]:
                flujos[seg][bloque] = {}
            
            for hora in range(1, 9):
                col_name = f'H{hora}'
                if col_name in df.columns:
                    flujos[seg][bloque][hora] = int(row.get(col_name, 0))
        
        return flujos
    
    def _parse_capacidad_resultado(self, df: pd.DataFrame) -> Dict[str, Dict[int, int]]:
        """Parsea capacidad por bloque y hora"""
        capacidad = {}
        
        # Formato: Bloque | H1 | H2 | ... | H8
        for _, row in df.iterrows():
            bloque = row.get('Bloque', '')
            capacidad[bloque] = {}
            
            for hora in range(1, 9):
                col_name = f'H{hora}'
                if col_name in df.columns:
                    capacidad[bloque][hora] = int(row.get(col_name, 0))
        
        return capacidad
    
    def _parse_disponibilidad(self, df: pd.DataFrame) -> Dict[str, Dict[int, int]]:
        """Parsea disponibilidad (Msbt)"""
        return self._parse_capacidad_resultado(df)  # Mismo formato
    
    def _parse_metricas(self, df: pd.DataFrame) -> Dict[str, float]:
        """Parsea métricas del modelo"""
        metricas = {}
        
        # Si es formato clave-valor
        if 'Metrica' in df.columns and 'Valor' in df.columns:
            for _, row in df.iterrows():
                metrica = row['Metrica'].lower().replace(' ', '_')
                metricas[metrica] = float(row['Valor'])
        else:
            # Si es una sola fila con columnas
            row = df.iloc[0]
            for col in df.columns:
                metricas[col.lower().replace(' ', '_')] = float(row[col])
        
        return metricas
    
    def _parse_cuotas(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Parsea cuotas de camiones"""
        cuotas = []
        
        # Formato: Hora | Cuota_Recepcion | Cuota_Entrega | Cuota_Total
        for _, row in df.iterrows():
            cuota = {
                'hora': int(row.get('Hora', 0)),
                'cuota_recepcion': int(row.get('Cuota_Recepcion', 0)),
                'cuota_entrega': int(row.get('Cuota_Entrega', 0)),
                'cuota_total': int(row.get('Cuota_Total', 0))
            }
            cuotas.append(cuota)
        
        return cuotas
    
    # Métodos específicos para Magdalena
    def _extract_magdalena_inventory(self, df: pd.DataFrame, turno: int, tipo: str) -> Dict[str, Dict[str, int]]:
        """Extrae inventario de Magdalena para un turno específico"""
        inventario = {}
        
        # Filtrar por periodo/turno
        if 'Periodo' in df.columns:
            df_turno = df[df['Periodo'] == turno].copy()
        else:
            df_turno = df.copy()
        
        for _, row in df_turno.iterrows():
            seg = str(row.get('Segregacion', ''))
            bloque = row.get('Bloque', '')
            cantidad = int(row.get(tipo, 0))
            
            if seg not in inventario:
                inventario[seg] = {}
            inventario[seg][bloque] = cantidad
        
        return inventario
    
    def _extract_magdalena_capacity(self, df: pd.DataFrame, turno: int) -> Dict[str, Dict[str, int]]:
        """Extrae capacidades de Magdalena"""
        capacidad = {}
        
        # Tomar máximo entre turno actual y anterior
        turnos = [turno - 1, turno] if turno > 1 else [turno]
        
        for t in turnos:
            if 'Periodo' in df.columns:
                df_turno = df[df['Periodo'] == t].copy()
            else:
                continue
            
            for _, row in df_turno.iterrows():
                seg = str(row.get('Segregacion', ''))
                bloque = row.get('Bloque', '')
                bahias = int(row.get('Bahías Ocupadas', 0))
                
                # Convertir bahías a capacidad TEUs
                cap_teus = bahias * 35  # 35 TEUs por bahía
                
                if bloque not in capacidad:
                    capacidad[bloque] = {}
                
                # Tomar el máximo si ya existe
                if seg in capacidad[bloque]:
                    capacidad[bloque][seg] = max(capacidad[bloque][seg], cap_teus)
                else:
                    capacidad[bloque][seg] = cap_teus
        
        return capacidad
    
    def _extract_magdalena_segregaciones(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Extrae segregaciones de Magdalena"""
        segregaciones = []
        
        for _, row in df.iterrows():
            codigo = str(row.get('S', row.get('Segregacion', '')))
            # Determinar tipo de contenedor
            tipo = '40' if '40' in codigo else '20'
            
            seg = {
                'codigo': codigo,
                'tipo_contenedor': tipo,
                'descripcion': row.get('Descripcion', '')
            }
            segregaciones.append(seg)
        
        return segregaciones
    
    def _extract_magdalena_demands_hourly(self, df: pd.DataFrame, h_ini: int, h_fin: int) -> Dict[str, Dict[str, List[int]]]:
        """Extrae demandas POR HORA de Magdalena (D_params_168h)"""
        demandas = {
            'carga': {},      # DC
            'descarga': {},   # DD
            'recepcion': {},  # DR
            'entrega': {}     # DE
        }
        
        # Filtrar por horas del turno
        if 'T' in df.columns:
            df_turno = df[(df['T'] >= h_ini) & (df['T'] <= h_fin)].copy()
            df_turno['hora_relativa'] = df_turno['T'] - h_ini + 1
        else:
            logger.error("No se encontró columna 'T' en D_params_168h")
            return demandas
        
        # Agrupar por segregación y ordenar por hora
        for seg in df_turno['S'].unique():
            seg_data = df_turno[df_turno['S'] == seg].sort_values('hora_relativa')
            
            # Asegurar que tenemos 8 horas
            demandas['carga'][seg] = []
            demandas['descarga'][seg] = []
            demandas['recepcion'][seg] = []
            demandas['entrega'][seg] = []
            
            for hora_rel in range(1, 9):
                hora_data = seg_data[seg_data['hora_relativa'] == hora_rel]
                
                if not hora_data.empty:
                    demandas['carga'][seg].append(int(hora_data['DC'].iloc[0]) if 'DC' in hora_data.columns else 0)
                    demandas['descarga'][seg].append(int(hora_data['DD'].iloc[0]) if 'DD' in hora_data.columns else 0)
                    demandas['recepcion'][seg].append(int(hora_data['DR'].iloc[0]) if 'DR' in hora_data.columns else 0)
                    demandas['entrega'][seg].append(int(hora_data['DE'].iloc[0]) if 'DE' in hora_data.columns else 0)
                else:
                    # Si no hay datos para esta hora, poner 0
                    demandas['carga'][seg].append(0)
                    demandas['descarga'][seg].append(0)
                    demandas['recepcion'][seg].append(0)
                    demandas['entrega'][seg].append(0)
        
        return demandas
    
    def _calculate_gate_reception(self, demandas_hora: Dict[str, Dict[str, List[int]]]) -> Dict[str, int]:
        """Calcula gate recepción total desde las demandas por hora"""
        gate_recepcion = {}
        
        # Sumar DR (recepción) de las 8 horas
        for seg, horas in demandas_hora.get('recepcion', {}).items():
            gate_recepcion[seg] = sum(horas)
        
        return gate_recepcion