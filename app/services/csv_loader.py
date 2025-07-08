# app/services/csv_loader.py
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, date
import logging
import re
from sqlalchemy import select, and_, text
from app.models.historical_movements import HistoricalMovement
from app.models.container_dwell_time import ContainerDwellTime
from app.models.truck_turnaround_time import TruckTurnaroundTime
from app.models.container_position import ContainerPosition
from pathlib import Path
import glob
import numpy as np
logger = logging.getLogger(__name__)
def clean_numeric_value(value, field_name=None, stats=None):
    """Limpia valores numéricos y cuenta los cambios"""
    if pd.isna(value):
        return None
    
    value_str = str(value)
    original = value_str
    
    # Inicializar stats si no existe
    if stats is not None and field_name:
        if field_name not in stats:
            stats[field_name] = {'total': 0, 'cleaned': 0, 'nulls': 0, 'examples': []}
        stats[field_name]['total'] += 1
    
    # Extraer números de strings como 'NOM20', 'NOM40'
    if 'NOM' in value_str:
        numbers = re.findall(r'\d+', value_str)
        if numbers:
            if stats is not None and field_name:
                stats[field_name]['cleaned'] += 1
                if len(stats[field_name]['examples']) < 5:
                    stats[field_name]['examples'].append(f"{original} → {numbers[0]}")
            return int(numbers[0])
        else:
            if stats is not None and field_name:
                stats[field_name]['nulls'] += 1
            return None
    
    # Manejar valores numéricos normales
    try:
        value_str = value_str.replace(',', '.')
        return int(float(value_str))
    except ValueError:
        if stats is not None and field_name:
            stats[field_name]['nulls'] += 1
            if len(stats[field_name]['examples']) < 5:
                stats[field_name]['examples'].append(f"{original} → NULL")
        return None

def clean_float_value(value, field_name=None, stats=None):
    """Limpia valores float, manejando fechas incorrectas"""
    if pd.isna(value):
        return None
    
    value_str = str(value)
    
    # Si parece una fecha (contiene - o :), retornar None
    if '-' in value_str or ':' in value_str:
        if stats is not None and field_name:
            if field_name not in stats:
                stats[field_name] = {'total': 0, 'cleaned': 0, 'nulls': 0, 'examples': []}
            stats[field_name]['nulls'] += 1
            if len(stats[field_name]['examples']) < 5:
                stats[field_name]['examples'].append(f"{value_str} → NULL (fecha)")
        return None
    
    try:
        value_str = value_str.replace(',', '.')
        return float(value_str)
    except ValueError:
        return None

class CSVLoaderService:
    def __init__(self, db: AsyncSession):
        self.db = db
    async def load_container_positions_csv(self, file_path: str, fecha: date, turno: int, semana_iso: str):
        """
        Cargar CSV de posiciones de contenedores - VERSIÓN FINAL FUNCIONANDO
        """
        try:
            filename = Path(file_path).name
            
            # Leer CSV
            df = pd.read_csv(file_path, sep=';', dtype={
                'gkey': str,
                'Posicion': str,
                'category': str,
                'tiempo': str,
                'requires_power': str,
                'nominal_length': str,
                'hazardous': str
            })
            
            if len(df) == 0:
                return 0
            
            # Filtrar posiciones válidas
            df = df[df['Posicion'].notna() & (df['Posicion'].str.len() >= 6)].copy()
            
            # Procesar datos
            df['patio'] = df['Posicion'].str[0]
            df['bloque'] = df['Posicion'].str[1] 
            df['bahia'] = pd.to_numeric(df['Posicion'].str[2:4], errors='coerce')
            df['fila'] = df['Posicion'].str[4]
            df['tier'] = pd.to_numeric(df['Posicion'].str[5], errors='coerce')
            
            df = df.dropna(subset=['bahia', 'tier'])
            df['bahia'] = df['bahia'].astype(int)
            df['tier'] = df['tier'].astype(int)
            
            df['gkey'] = df['gkey'].astype(str).str.strip()
            df = df[df['gkey'] != '']
            
            df['category'] = df['category'].fillna('UNKNOWN').astype(str).str[:10]
            df['nominal_length'] = df['nominal_length'].astype(str).str.extract('(\d+)').fillna(20).astype(int)
            df['requires_power'] = df['requires_power'].fillna('0').astype(str).str.strip() == '1'
            df['hazardous'] = df['hazardous'].fillna('0').astype(str).str.strip() == '1'
            
            # Manejar tiempo_permanencia
            df['tiempo_clean'] = pd.to_numeric(df['tiempo'], errors='coerce')
            df['tiempo_permanencia'] = df['tiempo_clean'].where(df['tiempo_clean'] > 0, None)
            
            # IMPORTANTE: Convertir timestamps a datetime de Python
            now = datetime.utcnow()
            
            df['fecha'] = fecha
            df['turno'] = turno
            df['semana_iso'] = semana_iso
            df['posicion'] = df['Posicion']
            df['created_at'] = now  # Usar datetime de Python
            df['updated_at'] = now  # Usar datetime de Python
            df['is_active'] = True
            
            columns = [
                'fecha', 'turno', 'semana_iso', 'gkey', 'posicion',
                'patio', 'bloque', 'bahia', 'fila', 'tier',
                'category', 'tiempo_permanencia', 'requires_power',
                'nominal_length', 'hazardous', 'created_at', 'updated_at', 'is_active'
            ]
            
            df_final = df[columns]
            
            # Convertir a diccionarios y limpiar
            records = []
            for _, row in df_final.iterrows():
                record = {}
                for col in columns:
                    val = row[col]
                    if col == 'tiempo_permanencia':
                        if pd.isna(val) or val is None or val == 0:
                            record[col] = None
                        else:
                            record[col] = int(val)
                    elif pd.isna(val):
                        record[col] = None
                    else:
                        record[col] = val
                records.append(record)
            
            if not records:
                return 0
            
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            
            # Insertar en chunks
            chunk_size = 1000
            total_inserted = 0
            
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i+chunk_size]
                
                try:
                    stmt = pg_insert(ContainerPosition).values(chunk)
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=['fecha', 'turno', 'gkey']
                    )
                    
                    result = await self.db.execute(stmt)
                    await self.db.commit()
                    total_inserted += result.rowcount
                    
                except Exception as e:
                    await self.db.rollback()
                    logger.debug(f"Error en chunk: {str(e)[:50]}")
                    continue
            
            if total_inserted > 0:
                logger.info(f"✅ {filename}: {total_inserted} registros")
            else:
                logger.info(f"⚠️ {filename}: 0 registros (ya existen)")
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"Error en {Path(file_path).name}: {str(e)}")
            return 0

    async def load_container_positions_year(self, year: int = 2022):
        """
        Cargar todos los archivos de posiciones - VERSIÓN SECUENCIAL OPTIMIZADA
        """
        from datetime import datetime
        
        base_path = Path(f"/app/data/{year}")
        if not base_path.exists():
            logger.error(f"No existe el directorio: {base_path}")
            return 0
        
        start_time = datetime.now()
        total_processed = 0
        total_files = 0
        files_error = 0
        
        turno_map = {"08-00": 1, "15-30": 2, "23-00": 3}
        
        # Contar archivos totales primero
        total_expected = sum(len(list(d.glob("*.csv"))) for d in base_path.iterdir() if d.is_dir())
        logger.info(f"Total de archivos esperados: {total_expected}")
        
        # Procesar secuencialmente
        for semana_num, semana_dir in enumerate(sorted(base_path.iterdir()), 1):
            if not semana_dir.is_dir():
                continue
                
            semana_iso = semana_dir.name
            logger.info(f"\n[{semana_num}/52] Procesando semana: {semana_iso}")
            
            csv_files = sorted(semana_dir.glob("*.csv"))
            
            for csv_file in csv_files:
                try:
                    filename = csv_file.stem
                    parts = filename.split('_')
                    
                    if len(parts) != 2:
                        logger.warning(f"Formato no reconocido: {filename}")
                        continue
                    
                    fecha_str, turno_str = parts
                    turno = turno_map.get(turno_str)
                    
                    if not turno:
                        logger.warning(f"Turno no válido: {turno_str}")
                        continue
                    
                    # Parsear fecha
                    fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                    
                    # Cargar archivo
                    processed = await self.load_container_positions_csv(
                        str(csv_file),
                        fecha,
                        turno,
                        semana_iso
                    )
                    
                    total_processed += processed
                    total_files += 1
                    
                    # Mostrar progreso cada 10 archivos
                    if total_files % 10 == 0:
                        elapsed = (datetime.now() - start_time).total_seconds()
                        rate = total_files / elapsed
                        eta = (total_expected - total_files) / rate if rate > 0 else 0
                        
                        logger.info(
                            f"Progreso: {total_files}/{total_expected} "
                            f"({total_files/total_expected*100:.1f}%) - "
                            f"{total_processed:,} registros - "
                            f"{rate:.1f} archivos/seg - "
                            f"ETA: {int(eta//60)}m {int(eta%60)}s"
                        )
                    
                except Exception as e:
                    logger.error(f"Error procesando {csv_file.name}: {str(e)}")
                    files_error += 1
                    continue
        
        # Resumen final
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info("\n=== RESUMEN DE CARGA DE POSICIONES ===")
        logger.info(f"Año: {year}")
        logger.info(f"Archivos procesados: {total_files}")
        logger.info(f"Archivos con error: {files_error}")
        logger.info(f"Total registros cargados: {total_processed:,}")
        logger.info(f"Tiempo total: {int(duration//60)}m {int(duration%60)}s")
        logger.info(f"Velocidad promedio: {total_files/duration:.1f} archivos/seg")
        logger.info(f"Registros/segundo: {total_processed/duration:.0f}")
        
        return total_processed


    async def load_historical_csv(self, file_path: str):
        """  
        Cargar CSV de movimientos históricos (congestión)
        """  
        logger.info(f"Cargando archivo de movimientos: {file_path}")
        
        # Leer CSV
        df = pd.read_csv(file_path, sep=';', parse_dates=['Hora'])
        
        # Procesar en lotes de 100 registros
        batch_size = 100
        total_records = len(df)
        logger.info(f"Total de registros a procesar: {total_records}")
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch_df.iterrows():
                record = {
                    'bloque': row['Bloque'],
                    'hora': row['Hora'],
                    'gate_entrada_contenedores': int(row['Gate-Entrada-Contenedores'] or 0),
                    'gate_entrada_teus': int(row['Gate-Entrada-Teus'] or 0),
                    'gate_salida_contenedores': int(row['Gate-Salida-Contenedores'] or 0),
                    'gate_salida_teus': int(row['Gate-Salida-Teus'] or 0),
                    'muelle_entrada_contenedores': int(row['Muelle-Entrada-Contenedores'] or 0),
                    'muelle_entrada_teus': int(row['Muelle-Entrada-Teus'] or 0),
                    'muelle_salida_contenedores': int(row['Muelle-Salida-Contenedores'] or 0),
                    'muelle_salida_teus': int(row['Muelle-Salida-Teus'] or 0),
                    'remanejos_contenedores': int(row['Remanejos-Contenedores'] or 0),
                    'remanejos_teus': int(row['Remanejos-Teus'] or 0),
                    'patio_entrada_contenedores': int(row['Patio-Entrada-Contenedores'] or 0),
                    'patio_entrada_teus': int(row['Patio-Entrada-Teus'] or 0),
                    'patio_salida_contenedores': int(row['Patio-Salida-Contenedores'] or 0),
                    'patio_salida_teus': int(row['Patio-Salida-Teus'] or 0),
                    'terminal_entrada_contenedores': int(row['Terminal-Entrada-Contenedores'] or 0),
                    'terminal_entrada_teus': int(row['Terminal-Entrada-Teus'] or 0),
                    'terminal_salida_contenedores': int(row['Terminal-Salida-Contenedores'] or 0),
                    'terminal_salida_teus': int(row['Terminal-Salida-Teus'] or 0),
                    'minimo_contenedores': int(row['Mínimo-Contenedores'] or 0),
                    'minimo_teus': int(row['Mínimo-Teus'] or 0),
                    'maximo_contenedores': int(row['Máximo-Contenedores'] or 0),
                    'maximos_teus': int(row['Máximos-Teus'] or 0),
                    'promedio_contenedores': int(row['Promedio-Contenedores'] or 0),
                    'promedio_teus': int(row['Promedio-Teus'] or 0),
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow(),
                    'is_active': True
                }
                records.append(record)
            
            # Insertar este lote con ON CONFLICT para manejar duplicados
            if records:
                try:
                    stmt = insert(HistoricalMovement).values(records)
                    stmt = stmt.on_conflict_do_update(
                        constraint='_bloque_hora_uc',
                        set_={
                            'gate_entrada_contenedores': stmt.excluded.gate_entrada_contenedores,
                            'gate_entrada_teus': stmt.excluded.gate_entrada_teus,
                            'gate_salida_contenedores': stmt.excluded.gate_salida_contenedores,
                            'gate_salida_teus': stmt.excluded.gate_salida_teus,
                            'muelle_entrada_contenedores': stmt.excluded.muelle_entrada_contenedores,
                            'muelle_entrada_teus': stmt.excluded.muelle_entrada_teus,
                            'muelle_salida_contenedores': stmt.excluded.muelle_salida_contenedores,
                            'muelle_salida_teus': stmt.excluded.muelle_salida_teus,
                            'remanejos_contenedores': stmt.excluded.remanejos_contenedores,
                            'remanejos_teus': stmt.excluded.remanejos_teus,
                            'patio_entrada_contenedores': stmt.excluded.patio_entrada_contenedores,
                            'patio_entrada_teus': stmt.excluded.patio_entrada_teus,
                            'patio_salida_contenedores': stmt.excluded.patio_salida_contenedores,
                            'patio_salida_teus': stmt.excluded.patio_salida_teus,
                            'terminal_entrada_contenedores': stmt.excluded.terminal_entrada_contenedores,
                            'terminal_entrada_teus': stmt.excluded.terminal_entrada_teus,
                            'terminal_salida_contenedores': stmt.excluded.terminal_salida_contenedores,
                            'terminal_salida_teus': stmt.excluded.terminal_salida_teus,
                            'minimo_contenedores': stmt.excluded.minimo_contenedores,
                            'minimo_teus': stmt.excluded.minimo_teus,
                            'maximo_contenedores': stmt.excluded.maximo_contenedores,
                            'maximos_teus': stmt.excluded.maximos_teus,
                            'promedio_contenedores': stmt.excluded.promedio_contenedores,
                            'promedio_teus': stmt.excluded.promedio_teus,
                            'updated_at': datetime.utcnow()
                        }
                    )
                    
                    await self.db.execute(stmt)
                    await self.db.commit()
                except Exception as e:
                    await self.db.rollback()
                    logger.error(f"Error insertando batch de movimientos: {e}")
                    continue
            
            # Log progreso cada 1000 registros
            if i % 1000 == 0:
                logger.info(f"Procesados {i}/{total_records} registros...")
        
        logger.info(f"✅ Cargados {total_records} registros de movimientos exitosamente")
        return total_records

    async def load_cdt_csv(self, file_path: str, operation_type: str = 'import'):
        """
        Cargar CSV de Container Dwell Time (CDT)
        operation_type: 'import' o 'export'
        """
        logger.info(f"Cargando archivo CDT {operation_type}: {file_path}")
        
        # Leer CSV sin tipos específicos, con manejo de decimales
        df = pd.read_csv(file_path, sep=';', decimal=',', low_memory=False)
        
        # Convertir columnas de fecha
        date_columns = ['iufv_it', 'iufv_ot', 'iufv_dt']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        batch_size = 100
        total_records = len(df)
        processed = 0
        cleaning_stats = {}
        
        logger.info(f"Total de registros CDT a procesar: {total_records}")
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch_df.iterrows():
                try:
                    # Calcular CDT en horas si tenemos ambas fechas
                    cdt_hours = None
                    if pd.notna(row.get('iufv_it')) and pd.notna(row.get('iufv_ot')):
                        time_diff = row['iufv_ot'] - row['iufv_it']
                        cdt_hours = time_diff.total_seconds() / 3600
                    
                    record = {
                        'iufv_gkey': clean_numeric_value(row.get('iufv_gkey'), 'iufv_gkey', cleaning_stats),
                        'operation_type': operation_type,
                        'iufv_it': row.get('iufv_it') if pd.notna(row.get('iufv_it')) else None,
                        'iufv_ot': row.get('iufv_ot') if pd.notna(row.get('iufv_ot')) else None,
                        'iufv_dt': row.get('iufv_dt') if pd.notna(row.get('iufv_dt')) else None,
                        'cdt_hours': cdt_hours,
                        'iufv_arrive_pos_name': str(row.get('iufv_arrive_pos_name', ''))[:255] if pd.notna(row.get('iufv_arrive_pos_name')) else None,
                        'iufv_last_pos_name': str(row.get('iufv_last_pos_name', ''))[:255] if pd.notna(row.get('iufv_last_pos_name')) else None,
                        'ret_nominal_length': clean_numeric_value(row.get('ret_nominal_length'), 'ret_nominal_length', cleaning_stats),
                        'ret_nominal_height': str(row.get('ret_nominal_height', ''))[:50] if pd.notna(row.get('ret_nominal_height')) else None,
                        'ret_iso_group': str(row.get('ret_iso_group', ''))[:50] if pd.notna(row.get('ret_iso_group')) else None,
                        'iu_freight_kind': str(row.get('iu_freight_kind', ''))[:50] if pd.notna(row.get('iu_freight_kind')) else None,
                        'ig_hazardous': str(row.get('ig_hazardous', '')).upper() in ['Y', 'YES', 'TRUE', '1'] if pd.notna(row.get('ig_hazardous')) else False,
                        'iu_requires_power': str(row.get('iu_requires_power', '')).upper() in ['Y', 'YES', 'TRUE', '1'] if pd.notna(row.get('iu_requires_power')) else False,
                        'iu_goods_and_ctr_wt_kg': float(str(row.get('iu_goods_and_ctr_wt_kg', 0)).replace(',', '.')) if pd.notna(row.get('iu_goods_and_ctr_wt_kg')) else None,
                        'ib_cv_id': str(row.get('ib_cv_id', ''))[:100] if pd.notna(row.get('ib_cv_id')) else None,
                        'ib_company': str(row.get('ib_company', ''))[:100] if pd.notna(row.get('ib_company')) else None,
                        'ob_cv_id': str(row.get('ob_cv_id', ''))[:100] if pd.notna(row.get('ob_cv_id')) else None,
                        'ob_company': str(row.get('ob_company', ''))[:100] if pd.notna(row.get('ob_company')) else None,
                        'ig_bl_nbr': str(row.get('ig_bl_nbr', ''))[:100] if pd.notna(row.get('ig_bl_nbr')) else None,
                        'ig_origin': str(row.get('ig_origin', ''))[:100] if pd.notna(row.get('ig_origin')) else None,
                        'ig_destination': str(row.get('ig_destination', ''))[:100] if pd.notna(row.get('ig_destination')) else None,
                        'iu_category': str(row.get('iu_category', ''))[:100] if pd.notna(row.get('iu_category')) else None,
                        'rc_name': str(row.get('rc_name', ''))[:255] if pd.notna(row.get('rc_name')) else None,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow(),
                        'is_active': True
                    }
                    
                    # Solo agregar si tiene iufv_gkey válido
                    if record['iufv_gkey'] is not None:
                        records.append(record)
                        processed += 1
                except Exception as e:
                    logger.debug(f"Error procesando registro CDT: {e}")
                    continue
            
            # Insertar lote
            if records:
                try:
                    stmt = insert(ContainerDwellTime).values(records)
                    stmt = stmt.on_conflict_do_update(
                        constraint='_cdt_gkey_type_uc',
                        set_={
                            'iufv_it': stmt.excluded.iufv_it,
                            'iufv_ot': stmt.excluded.iufv_ot,
                            'iufv_dt': stmt.excluded.iufv_dt,
                            'cdt_hours': stmt.excluded.cdt_hours,
                            'iufv_arrive_pos_name': stmt.excluded.iufv_arrive_pos_name,
                            'iufv_last_pos_name': stmt.excluded.iufv_last_pos_name,
                            'updated_at': datetime.utcnow()
                        }
                    )
                    
                    await self.db.execute(stmt)
                    await self.db.commit()
                except Exception as e:
                    await self.db.rollback()
                    logger.error(f"Error insertando batch CDT: {e}")
                    continue
            
            if i % 1000 == 0:
                logger.info(f"Procesados {i}/{total_records} registros CDT...")
        
        # Mostrar estadísticas de limpieza
        logger.info("\n=== ESTADÍSTICAS DE LIMPIEZA CDT ===")
        for field, stats in cleaning_stats.items():
            if stats['cleaned'] > 0 or stats['nulls'] > 0:
                logger.info(f"{field}:")
                logger.info(f"  - Total: {stats['total']}")
                logger.info(f"  - Limpiados: {stats['cleaned']}")
                logger.info(f"  - Convertidos a NULL: {stats['nulls']}")
                if stats['examples']:
                    logger.info(f"  - Ejemplos: {stats['examples']}")
        
        logger.info(f"✅ Cargados {processed} registros CDT exitosamente")
        return processed

    async def load_ttt_csv(self, file_path: str, operation_type: str = 'import'):
        """
        Cargar CSV de Truck Turnaround Time (TTT)
        operation_type: 'import' o 'export'
        """
        logger.info(f"Cargando archivo TTT {operation_type}: {file_path}")
        
        # Leer CSV sin tipos específicos
        df = pd.read_csv(file_path, sep=';', decimal=',', low_memory=False)
        
        # Convertir columnas de fecha
        date_columns = ['pregate_ss', 'pregate_se', 'ingate_ss', 'ingate_se', 'outgate_ss', 'outgate_se']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        batch_size = 100
        total_records = len(df)
        processed = 0
        cleaning_stats = {}
        
        logger.info(f"Total de registros TTT a procesar: {total_records}")
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch_df.iterrows():
                try:
                    # Calcular tiempos por etapa en minutos
                    pregate_time = None
                    if pd.notna(row.get('pregate_ss')) and pd.notna(row.get('pregate_se')):
                        pregate_time = (row['pregate_se'] - row['pregate_ss']).total_seconds() / 60
                    
                    ingate_time = None
                    if pd.notna(row.get('ingate_ss')) and pd.notna(row.get('ingate_se')):
                        ingate_time = (row['ingate_se'] - row['ingate_ss']).total_seconds() / 60
                    
                    outgate_time = None
                    if pd.notna(row.get('outgate_ss')) and pd.notna(row.get('outgate_se')):
                        outgate_time = (row['outgate_se'] - row['outgate_ss']).total_seconds() / 60
                    
                    # Extraer hora y día de la semana del inicio
                    hora_inicio = None
                    dia_semana = None
                    if pd.notna(row.get('pregate_ss')):
                        hora_inicio = row['pregate_ss'].hour
                        dia_semana = row['pregate_ss'].weekday()
                    
                    record = {
                        'iufv_gkey': clean_numeric_value(row.get('iufv_gkey'), 'iufv_gkey', cleaning_stats),
                        'gate_gkey': clean_numeric_value(row.get('gate_gkey'), 'gate_gkey', cleaning_stats),
                        'operation_type': operation_type,
                        'ttt': clean_float_value(row.get('ttt'), 'ttt', cleaning_stats),
                        'turn_time': clean_float_value(row.get('turn_time'), 'turn_time', cleaning_stats),
                        'pregate_ss': row.get('pregate_ss') if pd.notna(row.get('pregate_ss')) else None,
                        'pregate_se': row.get('pregate_se') if pd.notna(row.get('pregate_se')) else None,
                        'ingate_ss': row.get('ingate_ss') if pd.notna(row.get('ingate_ss')) else None,
                        'ingate_se': row.get('ingate_se') if pd.notna(row.get('ingate_se')) else None,
                        'outgate_ss': row.get('outgate_ss') if pd.notna(row.get('outgate_ss')) else None,
                        'outgate_se': row.get('outgate_se') if pd.notna(row.get('outgate_se')) else None,
                        'pregate_time': pregate_time,
                        'ingate_time': ingate_time,
                        'outgate_time': outgate_time,
                        'raw_t_dispatch': clean_float_value(row.get('raw_t_dispatch'), 'raw_t_dispatch', cleaning_stats),
                        'raw_t_fetch': clean_float_value(row.get('raw_t_fetch'), 'raw_t_fetch', cleaning_stats),
                        'raw_t_put': clean_float_value(row.get('raw_t_put'), 'raw_t_put', cleaning_stats),
                        'truck_license_nbr': str(row.get('truck_license_nbr', ''))[:50] if pd.notna(row.get('truck_license_nbr')) else None,
                        'driver_card_id': str(row.get('driver_card_id', ''))[:50] if pd.notna(row.get('driver_card_id')) else None,
                        'driver_name': str(row.get('driver_name', ''))[:100] if pd.notna(row.get('driver_name')) else None,
                        'trucking_co_id': str(row.get('trucking_co_id', ''))[:100] if pd.notna(row.get('trucking_co_id')) else None,
                        'pos_yard_gate': str(row.get('pos_yard_gate', ''))[:50] if pd.notna(row.get('pos_yard_gate')) else None,
                        'ret_nominal_length': clean_numeric_value(row.get('ret_nominal_length'), 'ret_nominal_length', cleaning_stats),
                        'iu_freight_kind': str(row.get('iu_freight_kind', ''))[:50] if pd.notna(row.get('iu_freight_kind')) else None,
                        'ig_hazardous': str(row.get('ig_hazardous', '')).upper() in ['Y', 'YES', 'TRUE', '1'] if pd.notna(row.get('ig_hazardous')) else False,
                        'iu_requires_power': str(row.get('iu_requires_power', '')).upper() in ['Y', 'YES', 'TRUE', '1'] if pd.notna(row.get('iu_requires_power')) else False,
                        'hora_inicio': hora_inicio,
                        'dia_semana': dia_semana,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow(),
                        'is_active': True
                    }
                    
                    # Solo agregar si tiene iufv_gkey válido
                    if record['iufv_gkey'] is not None:
                        records.append(record)
                        processed += 1
                except Exception as e:
                    logger.debug(f"Error procesando registro TTT: {e}")
                    continue
            
            # Insertar lote
            if records:
                try:
                    stmt = insert(TruckTurnaroundTime).values(records)
                    stmt = stmt.on_conflict_do_update(
                        constraint='_ttt_gkey_gate_type_uc',
                        set_={
                            'ttt': stmt.excluded.ttt,
                            'turn_time': stmt.excluded.turn_time,
                            'pregate_ss': stmt.excluded.pregate_ss,
                            'pregate_se': stmt.excluded.pregate_se,
                            'pregate_time': stmt.excluded.pregate_time,
                            'updated_at': datetime.utcnow()
                        }
                    )
                    
                    await self.db.execute(stmt)
                    await self.db.commit()
                except Exception as e:
                    await self.db.rollback()
                    logger.error(f"Error insertando batch TTT: {e}")
                    continue
            
            if i % 1000 == 0:
                logger.info(f"Procesados {i}/{total_records} registros TTT...")
        
        # Mostrar estadísticas de limpieza
        logger.info("\n=== ESTADÍSTICAS DE LIMPIEZA TTT ===")
        for field, stats in cleaning_stats.items():
            if stats['cleaned'] > 0 or stats['nulls'] > 0:
                logger.info(f"{field}:")
                logger.info(f"  - Total: {stats['total']}")
                logger.info(f"  - Limpiados: {stats['cleaned']}")
                logger.info(f"  - Convertidos a NULL: {stats['nulls']}")
                if stats['examples']:
                    logger.info(f"  - Ejemplos: {stats['examples']}")
        
        logger.info(f"✅ Cargados {processed} registros TTT exitosamente")
        return processed

    async def load_all_data(self, year: int = 2022):
        """
        Cargar todos los tipos de datos para un año específico
        """
        logger.info(f"Iniciando carga completa de datos para el año {year}")
        
        results = {
            'movements': 0,
            'cdt_import': 0,
            'cdt_export': 0,
            'ttt_import': 0,
            'ttt_export': 0
        }
        
        try:
            # Cargar movimientos históricos
            movements_file = f"data/resultados_congestion_SAI_{year}.csv"
            results['movements'] = await self.load_historical_csv(movements_file)
        except Exception as e:
            logger.error(f"Error cargando movimientos: {e}")
        
        try:
            # Cargar CDT importación
            cdt_import_file = f"data/resultados_CDT_impo_anio_SAI_{year}.csv"
            results['cdt_import'] = await self.load_cdt_csv(cdt_import_file, 'import')
        except Exception as e:
            logger.error(f"Error cargando CDT importación: {e}")
        
        try:
            # Cargar CDT exportación
            cdt_export_file = f"data/resultados_CDT_expo_anio_SAI_{year}.csv"
            results['cdt_export'] = await self.load_cdt_csv(cdt_export_file, 'export')
        except Exception as e:
            logger.error(f"Error cargando CDT exportación: {e}")
        
        try:
            # Cargar TTT importación
            ttt_import_file = f"data/resultados_TTT_impo_anio_SAI_{year}.csv"
            results['ttt_import'] = await self.load_ttt_csv(ttt_import_file, 'import')
        except Exception as e:
            logger.error(f"Error cargando TTT importación: {e}")
        
        try:
            # Cargar TTT exportación
            ttt_export_file = f"data/resultados_TTT_expo_anio_SAI_{year}.csv"
            results['ttt_export'] = await self.load_ttt_csv(ttt_export_file, 'export')
        except Exception as e:
            logger.error(f"Error cargando TTT exportación: {e}")
        
        logger.info(f"✅ Carga completa finalizada: {results}")
        return results