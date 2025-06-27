# scripts/load_historical_data.py
import asyncio
import sys
from pathlib import Path
import argparse
import logging

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.services.csv_loader import CSVLoaderService
from app.models.base import Base

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()

async def main(year: int = 2022, load_all: bool = False):
    """
    Función principal para cargar datos históricos
    
    Args:
        year: Año de los datos a cargar
        load_all: Si True, carga todos los tipos de datos (movimientos, CDT, TTT)
    """
    # Crear engine
    engine = create_async_engine(settings.DATABASE_URL)
    
    # Crear tablas si no existen
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear sesión
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        service = CSVLoaderService(db)
        
        if load_all:
            # Cargar todos los tipos de datos
            logger.info(f"Iniciando carga completa de datos para el año {year}")
            
            results = {
                'movements': 0,
                'cdt_import': 0,
                'cdt_export': 0,
                'ttt_import': 0,
                'ttt_export': 0
            }
            
            # Cargar movimientos históricos
            try:
                csv_path = f"data/resultados_congestion_SAI_{year}.csv"
                if Path(csv_path).exists():
                    results['movements'] = await service.load_historical_csv(csv_path)
                    logger.info(f"✅ Movimientos: {results['movements']} registros")
                else:
                    logger.warning(f"❌ No se encontró archivo de movimientos: {csv_path}")
            except Exception as e:
                logger.error(f"Error cargando movimientos: {e}")
            
            # Cargar CDT importación
            try:
                cdt_import_path = f"data/resultados_CDT_impo_anio_SAI_{year}.csv"
                if Path(cdt_import_path).exists():
                    results['cdt_import'] = await service.load_cdt_csv(cdt_import_path, 'import')
                    logger.info(f"✅ CDT Import: {results['cdt_import']} registros")
                else:
                    logger.warning(f"❌ No se encontró archivo CDT import: {cdt_import_path}")
            except Exception as e:
                logger.error(f"Error cargando CDT importación: {e}")
            
            # Cargar CDT exportación
            try:
                cdt_export_path = f"data/resultados_CDT_expo_anio_SAI_{year}.csv"
                if Path(cdt_export_path).exists():
                    results['cdt_export'] = await service.load_cdt_csv(cdt_export_path, 'export')
                    logger.info(f"✅ CDT Export: {results['cdt_export']} registros")
                else:
                    logger.warning(f"❌ No se encontró archivo CDT export: {cdt_export_path}")
            except Exception as e:
                logger.error(f"Error cargando CDT exportación: {e}")
            
            # Cargar TTT importación
            try:
                ttt_import_path = f"data/resultados_TTT_impo_anio_SAI_{year}.csv"
                if Path(ttt_import_path).exists():
                    results['ttt_import'] = await service.load_ttt_csv(ttt_import_path, 'import')
                    logger.info(f"✅ TTT Import: {results['ttt_import']} registros")
                else:
                    logger.warning(f"❌ No se encontró archivo TTT import: {ttt_import_path}")
            except Exception as e:
                logger.error(f"Error cargando TTT importación: {e}")
            
            # Cargar TTT exportación
            try:
                ttt_export_path = f"data/resultados_TTT_expo_anio_SAI_{year}.csv"
                if Path(ttt_export_path).exists():
                    results['ttt_export'] = await service.load_ttt_csv(ttt_export_path, 'export')
                    logger.info(f"✅ TTT Export: {results['ttt_export']} registros")
                else:
                    logger.warning(f"❌ No se encontró archivo TTT export: {ttt_export_path}")
            except Exception as e:
                logger.error(f"Error cargando TTT exportación: {e}")
            
            # Resumen final
            logger.info("\n=== RESUMEN DE CARGA ===")
            logger.info(f"Movimientos históricos: {results['movements']} registros")
            logger.info(f"CDT Importación: {results['cdt_import']} registros")
            logger.info(f"CDT Exportación: {results['cdt_export']} registros")
            logger.info(f"TTT Importación: {results['ttt_import']} registros")
            logger.info(f"TTT Exportación: {results['ttt_export']} registros")
            logger.info(f"TOTAL: {sum(results.values())} registros cargados")
            
        else:
            # Solo cargar movimientos históricos (comportamiento original)
            csv_path = f"data/resultados_congestion_SAI_{year}.csv"
            if Path(csv_path).exists():
                records = await service.load_historical_csv(csv_path)
                print(f"✅ Cargados {records} registros de movimientos en la base de datos")
            else:
                print(f"❌ No se encontró el archivo: {csv_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cargar datos históricos del terminal')
    parser.add_argument('--year', type=int, default=2022, help='Año de datos a cargar (default: 2022)')
    parser.add_argument('--all', action='store_true', help='Cargar todos los tipos de datos (movimientos, CDT, TTT)')
    
    args = parser.parse_args()
    
    # Ejecutar carga
    asyncio.run(main(year=args.year, load_all=args.all))