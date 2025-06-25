# app/services/csv_loader.py
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime
import logging

from app.models.historical_movements import HistoricalMovement

logger = logging.getLogger(__name__)

class CSVLoaderService:
    def __init__(self, db: AsyncSession):
        self.db = db
        
    async def load_historical_csv(self, file_path: str):
        """
        Cargar CSV a la base de datos en lotes pequeños
        """
        logger.info(f"Cargando archivo: {file_path}")
        
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
            
            # Log progreso cada 1000 registros
            if i % 1000 == 0:
                logger.info(f"Procesados {i}/{total_records} registros...")
        
        logger.info(f"✅ Cargados {total_records} registros exitosamente")
        return total_records