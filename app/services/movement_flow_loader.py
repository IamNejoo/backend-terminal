# app/services/movement_flow_loader.py
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import logging
import re
from app.models.movement_flow import MovementFlow

logger = logging.getLogger(__name__)

class MovementFlowLoaderService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    def extract_patio_bloque(self, position: str) -> tuple[str, str]:
        """
        Extrae patio y bloque de una posición
        Maneja formatos como C3, H5, T2
        """
        if not position or pd.isna(position):
            return None, None
        
        position = str(position).strip().upper()
        
        # Ignorar posiciones especiales
        if position in ['GATE', 'Y-SAI-RAMP', 'Y-SAI-M10', 'VESSEL']:
            return None, None
        
        # Formato simple: C3, H5, T2
        if len(position) == 2 and position[0] in ['C', 'H', 'T'] and position[1].isdigit():
            letra = position[0]
            if letra == 'C':
                return 'costanera', position
            elif letra == 'H':
                return 'ohiggins', position
            elif letra == 'T':
                return 'tebas', position
        
        # Formato Y-SAI-XXX
        if position.startswith('Y-SAI-') and len(position) > 6:
            codigo = position[6:]
            if len(codigo) >= 2 and codigo[0] in ['C', 'H', 'T'] and codigo[1].isdigit():
                letra = codigo[0]
                digito = codigo[1]
                bloque = f"{letra}{digito}"
                
                if letra == 'C':
                    return 'costanera', bloque
                elif letra == 'H':
                    return 'ohiggins', bloque
                elif letra == 'T':
                    return 'tebas', bloque
        
        return None, None
    
    async def load_movement_flows_csv(self, file_path: str, year_from: int = 2017, year_to: int = None):
        """
        Cargar CSV de flujos de movimiento filtrando por años
        
        Args:
            file_path: Ruta del archivo CSV
            year_from: Año desde el cual cargar datos (default: 2017)
            year_to: Año hasta el cual cargar datos (default: None = hasta el último año)
        """
        logger.info(f"Cargando archivo de flujos: {file_path}")
        logger.info(f"Filtrando datos desde {year_from} hasta {year_to or 'el último año'}")
        
        # Leer CSV con manejo de errores
        df = pd.read_csv(
            file_path, 
            sep=';', 
            dtype=str,  # Leer todo como string inicialmente
            engine='python',
            on_bad_lines='skip'
        )
        
        # Limpiar nombres de columnas
        df.columns = [col.strip() for col in df.columns]
        
        # Eliminar columnas vacías
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df = df.dropna(how='all', axis=1)
        
        # Convertir columnas
        logger.info("Convirtiendo tipos de datos...")
        
        # Fecha/hora
        df['ime_time'] = pd.to_datetime(df['ime_time'], errors='coerce')
        
        # FILTRAR POR AÑO
        df_before_filter = len(df)
        df = df[df['ime_time'].notna()]  # Eliminar fechas inválidas
        
        # Aplicar filtro de año
        df = df[df['ime_time'].dt.year >= year_from]
        if year_to:
            df = df[df['ime_time'].dt.year <= year_to]
        
        df_after_filter = len(df)
        logger.info(f"Registros antes del filtro: {df_before_filter}")
        logger.info(f"Registros después del filtro: {df_after_filter}")
        logger.info(f"Registros eliminados: {df_before_filter - df_after_filter}")
        
        # Mostrar rango de fechas
        if len(df) > 0:
            min_date = df['ime_time'].min()
            max_date = df['ime_time'].max()
            logger.info(f"Rango de fechas: {min_date} hasta {max_date}")
            
            # Mostrar distribución por año
            year_counts = df['ime_time'].dt.year.value_counts().sort_index()
            logger.info("\nDistribución por año:")
            for year, count in year_counts.items():
                logger.info(f"  {year}: {count:,} registros")
        
        # Numéricos
        if 'ime_ufv_gkey' in df.columns:
            df['ime_ufv_gkey'] = pd.to_numeric(df['ime_ufv_gkey'], errors='coerce')
        
        # Booleanos
        df['ig_hazardous'] = df['ig_hazardous'].fillna('0').astype(str).str.strip() == '1'
        df['iu_requires_power'] = df['iu_requires_power'].fillna('0').astype(str).str.strip() == '1'
        
        # Filtrar registros válidos
        df = df.dropna(subset=['ime_time', 'ime_ufv_gkey'])
        
        # Procesar en lotes
        batch_size = 1000
        total_records = len(df)
        processed = 0
        
        logger.info(f"Total de registros a procesar: {total_records}")
        
        for i in range(0, total_records, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            records = []
            
            for _, row in batch_df.iterrows():
                try:
                    # Extraer patio y bloque del ime_fm
                    patio, bloque = self.extract_patio_bloque(row.get('ime_fm'))
                    
                    record = {
                        'ime_time': row['ime_time'],
                        'ime_fm': str(row.get('ime_fm', ''))[:50] if pd.notna(row.get('ime_fm')) else None,
                        'ime_to': str(row.get('ime_to', ''))[:50] if pd.notna(row.get('ime_to')) else None,
                        'ime_ufv_gkey': int(row['ime_ufv_gkey']),
                        'ime_move_kind': str(row.get('ime_move_kind', ''))[:50] if pd.notna(row.get('ime_move_kind')) else None,
                        'criterio_i': str(row.get('criterio_i', ''))[:100] if pd.notna(row.get('criterio_i')) else None,
                        'criterio_ii': str(row.get('criterio_ii', ''))[:100] if pd.notna(row.get('criterio_ii')) else None,
                        'criterio_iii': str(row.get('criterio_iii', ''))[:100] if pd.notna(row.get('criterio_iii')) else None,
                        'iu_category': str(row.get('iu_category', ''))[:10] if pd.notna(row.get('iu_category')) else None,
                        'ig_hazardous': row.get('ig_hazardous', False),
                        'iu_requires_power': row.get('iu_requires_power', False),
                        'iu_freight_kind': str(row.get('iu_freight_kind', ''))[:10] if pd.notna(row.get('iu_freight_kind')) else None,
                        'ret_nominal_length': str(row.get('ret_nominal_length', ''))[:10] if pd.notna(row.get('ret_nominal_length')) else None,
                        'ibcv_id': str(row.get('ibcv_id', ''))[:50] if pd.notna(row.get('ibcv_id')) else None,
                        'ibcv_intend_id': str(row.get('ibcv_intend_id', ''))[:50] if pd.notna(row.get('ibcv_intend_id')) else None,
                        'obcv_id': str(row.get('obcv_id', ''))[:50] if pd.notna(row.get('obcv_id')) else None,
                        'obcv_intend_id': str(row.get('obcv_intend_id', ''))[:50] if pd.notna(row.get('obcv_intend_id')) else None,
                        'pod1_id': str(row.get('pod1_id', ''))[:10] if pd.notna(row.get('pod1_id')) else None,
                        'iufv_flex_string01': str(row.get('iufv_flex_string01', ''))[:255] if pd.notna(row.get('iufv_flex_string01')) else None,
                        'iufv_stow_factor': str(row.get('iufv_stow_factor', ''))[:100] if pd.notna(row.get('iufv_stow_factor')) else None,
                        'iufv_stacking_factor': str(row.get('iufv_stacking_factor', ''))[:100] if pd.notna(row.get('iufv_stacking_factor')) else None,
                        'patio': patio,
                        'bloque': bloque,
                        'created_at': datetime.utcnow(),
                        'updated_at': datetime.utcnow(),
                        'is_active': True
                    }
                    
                    records.append(record)
                    processed += 1
                    
                except Exception as e:
                    logger.debug(f"Error procesando registro: {e}")
                    continue
            
            # Insertar lote
            if records:
                try:
                    stmt = insert(MovementFlow).values(records)
                    await self.db.execute(stmt)
                    await self.db.commit()
                except Exception as e:
                    await self.db.rollback()
                    logger.error(f"Error insertando batch: {e}")
                    # Intentar insertar uno por uno
                    for record in records:
                        try:
                            stmt = insert(MovementFlow).values(record)
                            await self.db.execute(stmt)
                            await self.db.commit()
                        except:
                            continue
            
            if i % 10000 == 0:
                logger.info(f"Procesados {i}/{total_records} registros...")
        
        logger.info(f"✅ Cargados {processed} registros de flujos exitosamente")
        
        # Mostrar estadísticas
        await self.show_statistics()
        
        return processed
    
    async def show_statistics(self):
        """Mostrar estadísticas de los datos cargados"""
        from sqlalchemy import select, func, extract
        
        # Total por año
        result = await self.db.execute(
            select(
                extract('year', MovementFlow.ime_time).label('year'),
                func.count(MovementFlow.id).label('total')
            ).group_by('year')
            .order_by('year')
        )
        
        logger.info("\n=== ESTADÍSTICAS POR AÑO ===")
        for row in result:
            logger.info(f"{int(row.year)}: {row.total:,} movimientos")
        
        # Total por tipo de movimiento
        result = await self.db.execute(
            select(
                MovementFlow.ime_move_kind,
                func.count(MovementFlow.id).label('total')
            ).group_by(MovementFlow.ime_move_kind)
            .order_by(func.count(MovementFlow.id).desc())
            .limit(10)
        )
        
        logger.info("\n=== TOP 10 TIPOS DE MOVIMIENTO ===")
        for row in result:
            logger.info(f"{row.ime_move_kind}: {row.total:,} movimientos")
        
        # Total por patio/bloque
        result = await self.db.execute(
            select(
                MovementFlow.patio,
                func.count(MovementFlow.bloque).label('bloques'),
                func.count(MovementFlow.id).label('total')
            ).where(MovementFlow.patio.isnot(None))
            .group_by(MovementFlow.patio)
            .order_by(MovementFlow.patio)
        )
        
        logger.info("\n=== MOVIMIENTOS POR PATIO ===")
        for row in result:
            logger.info(f"{row.patio}: {row.total:,} movimientos")