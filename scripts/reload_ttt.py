# scripts/reload_ttt.py - VERSIÓN CORREGIDA
import asyncio
import sys
from pathlib import Path
import logging

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.core.config import get_settings
from app.services.csv_loader import CSVLoaderService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()

async def reload_ttt_data(year: int = 2022):
    """Recargar solo datos TTT"""
    
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        # Limpiar datos TTT existentes
        logger.info("Limpiando datos TTT existentes...")
        await db.execute(text("TRUNCATE TABLE truck_turnaround_times RESTART IDENTITY CASCADE"))
        await db.commit()
        
        service = CSVLoaderService(db)
        
        results = {
            'ttt_import': 0,
            'ttt_export': 0
        }
        
        # Buscar archivos con diferentes patrones
        possible_paths = [
            f"data/resultados_TTT_impo_anio_SAI_{year}.csv",
            f"data/resultados_TTT_impo_anio_SAI_{year} 1.csv",  # Con espacio
            f"../data/resultados_TTT_impo_anio_SAI_{year}.csv",
            f"../data/resultados_TTT_impo_anio_SAI_{year} 1.csv"
        ]
        
        # Cargar TTT importación
        ttt_import_loaded = False
        for path in possible_paths:
            try:
                if Path(path).exists():
                    logger.info(f"Encontrado archivo TTT import en: {path}")
                    results['ttt_import'] = await service.load_ttt_csv(path, 'import')
                    logger.info(f"✅ TTT Import: {results['ttt_import']} registros")
                    ttt_import_loaded = True
                    break
            except Exception as e:
                logger.error(f"Error cargando TTT importación desde {path}: {e}")
        
        if not ttt_import_loaded:
            logger.warning("❌ No se encontró archivo TTT importación")
            logger.info("Archivos en data/:")
            data_dir = Path("data")
            if data_dir.exists():
                for f in data_dir.glob("*TTT*"):
                    logger.info(f"  - {f}")
        
        # Cargar TTT exportación
        possible_paths_export = [
            f"data/resultados_TTT_expo_anio_SAI_{year}.csv",
            f"data/resultados_TTT_expo_anio_SAI_{year} 1.csv",
            f"../data/resultados_TTT_expo_anio_SAI_{year}.csv",
            f"../data/resultados_TTT_expo_anio_SAI_{year} 1.csv"
        ]
        
        ttt_export_loaded = False
        for path in possible_paths_export:
            try:
                if Path(path).exists():
                    logger.info(f"Encontrado archivo TTT export en: {path}")
                    results['ttt_export'] = await service.load_ttt_csv(path, 'export')
                    logger.info(f"✅ TTT Export: {results['ttt_export']} registros")
                    ttt_export_loaded = True
                    break
            except Exception as e:
                logger.error(f"Error cargando TTT exportación desde {path}: {e}")
        
        if not ttt_export_loaded:
            logger.warning("❌ No se encontró archivo TTT exportación")
        
        # Verificar datos cargados
        logger.info("\n=== VERIFICACIÓN TTT ===")
        
        # Total registros
        total_result = await db.execute(text("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT iufv_gkey) as gkeys_unicos,
                   COUNT(ttt) as con_ttt_valido
            FROM truck_turnaround_times
        """))
        total = total_result.first()
        logger.info(f"Total registros: {total.total}")
        logger.info(f"GKeys únicos: {total.gkeys_unicos}")
        logger.info(f"Con TTT válido: {total.con_ttt_valido}")
        
        if total.total > 0:
            # Estadísticas TTT
            stats_result = await db.execute(text("""
                SELECT 
                    operation_type,
                    COUNT(*) as total,
                    COUNT(ttt) as con_ttt,
                    AVG(CASE WHEN ttt > 0 AND ttt < 480 THEN ttt END) as promedio,
                    MIN(CASE WHEN ttt > 0 THEN ttt END) as minimo,
                    MAX(CASE WHEN ttt < 480 THEN ttt END) as maximo
                FROM truck_turnaround_times
                GROUP BY operation_type
            """))
            
            logger.info("\nEstadísticas por tipo de operación:")
            for row in stats_result:
                logger.info(f"\n{row.operation_type}:")
                logger.info(f"  Total: {row.total}")
                logger.info(f"  Con TTT: {row.con_ttt}")
                logger.info(f"  Promedio: {row.promedio:.2f} min" if row.promedio else "  Promedio: N/A")
                logger.info(f"  Mínimo: {row.minimo} min" if row.minimo else "  Mínimo: N/A")
                logger.info(f"  Máximo: {row.maximo} min" if row.maximo else "  Máximo: N/A")
        
        # Mostrar resumen final
        logger.info(f"\n=== RESUMEN FINAL ===")
        logger.info(f"TTT Importación cargados: {results['ttt_import']}")
        logger.info(f"TTT Exportación cargados: {results['ttt_export']}")
        logger.info(f"Total TTT cargados: {results['ttt_import'] + results['ttt_export']}")

if __name__ == "__main__":
    asyncio.run(reload_ttt_data())