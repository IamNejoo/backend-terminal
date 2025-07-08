# scripts/load_container_positions.py
import asyncio
import sys
from pathlib import Path
import argparse
import logging
from datetime import datetime
from sqlalchemy import text

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from app.core.config import get_settings
from app.services.csv_loader import CSVLoaderService
from app.models.base import Base
from app.models.container_position import ContainerPosition

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()

async def main(year: int = 2022, week: str = None, specific_date: str = None):
    """
    Función principal para cargar posiciones de contenedores
    
    Args:
        year: Año de los datos a cargar (default: 2022)
        week: Semana específica en formato ISO (ej: 2022-01-03)
        specific_date: Fecha específica (ej: 2022-01-03)
    """
    start_time = datetime.now()
    
    # Crear engine
    engine = create_async_engine(settings.DATABASE_URL)
    
    # Crear tablas si no existen
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear sesión
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        service = CSVLoaderService(db)
        
        logger.info("=" * 60)
        logger.info("INICIANDO CARGA DE POSICIONES DE CONTENEDORES")
        logger.info("=" * 60)
        
        if specific_date:
            # Cargar solo una fecha específica
            logger.info(f"Modo: Carga de fecha específica - {specific_date}")
            
            try:
                fecha = datetime.strptime(specific_date, "%Y-%m-%d").date()
                semana_iso = fecha.strftime("%Y-%V")  # Obtener semana ISO
                
                # Buscar archivos para esa fecha
                base_path = Path(f"/app/data/{year}")
                files_found = 0
                
                for semana_dir in base_path.iterdir():
                    if semana_dir.is_dir():
                        pattern = f"{specific_date}_*.csv"
                        csv_files = list(semana_dir.glob(pattern))
                        
                        if csv_files:
                            logger.info(f"Encontrados {len(csv_files)} archivos para {specific_date}")
                            files_found += len(csv_files)
                            
                            for csv_file in csv_files:
                                # Procesar cada archivo
                                filename = csv_file.stem
                                turno_str = filename.split('_')[1]
                                turno_map = {"08-00": 1, "15-30": 2, "23-00": 3}
                                turno = turno_map.get(turno_str, 0)
                                
                                if turno:
                                    await service.load_container_positions_csv(
                                        str(csv_file),
                                        fecha,
                                        turno,
                                        semana_dir.name
                                    )
                
                if files_found == 0:
                    logger.warning(f"No se encontraron archivos para la fecha {specific_date}")
                    
            except Exception as e:
                logger.error(f"Error procesando fecha específica: {e}")
                
        elif week:
            # Cargar solo una semana específica
            logger.info(f"Modo: Carga de semana específica - {week}")
            
            week_path = Path(f"/app/data/{year}/{week}")
            if week_path.exists():
                csv_files = list(week_path.glob("*.csv"))
                logger.info(f"Encontrados {len(csv_files)} archivos en semana {week}")
                
                for csv_file in csv_files:
                    try:
                        filename = csv_file.stem
                        parts = filename.split('_')
                        fecha_str = parts[0]
                        turno_str = parts[1]
                        
                        fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()
                        turno_map = {"08-00": 1, "15-30": 2, "23-00": 3}
                        turno = turno_map.get(turno_str, 0)
                        
                        if turno:
                            await service.load_container_positions_csv(
                                str(csv_file),
                                fecha,
                                turno,
                                week
                            )
                    except Exception as e:
                        logger.error(f"Error procesando {csv_file.name}: {e}")
            else:
                logger.error(f"No existe el directorio para la semana {week}")
                
        else:
            # Cargar todo el año
            logger.info(f"Modo: Carga completa del año {year}")
            
            total_records = await service.load_container_positions_year(year)
            
            # Estadísticas finales
            count_result = await db.execute(
                text("SELECT COUNT(*) FROM container_positions WHERE EXTRACT(YEAR FROM fecha) = :year"),
                {"year": year}
            )
            total_in_db = count_result.scalar()
            
            # Estadísticas por categoría
            stats_result = await db.execute(text("""
                SELECT 
                    category,
                    COUNT(*) as total,
                    COUNT(DISTINCT gkey) as unique_containers,
                    SUM(CASE WHEN nominal_length = 20 THEN 1 ELSE 0 END) as containers_20,
                    SUM(CASE WHEN nominal_length = 40 THEN 1 ELSE 0 END) as containers_40,
                    SUM(CASE WHEN requires_power THEN 1 ELSE 0 END) as reefer,
                    SUM(CASE WHEN hazardous THEN 1 ELSE 0 END) as hazardous
                FROM container_positions
                WHERE EXTRACT(YEAR FROM fecha) = :year
                GROUP BY category
            """), {"year": year})
            
            logger.info("\n=== ESTADÍSTICAS POR CATEGORÍA ===")
            for row in stats_result:
                logger.info(f"\nCategoría: {row.category}")
                logger.info(f"  Total posiciones: {row.total:,}")
                logger.info(f"  Contenedores únicos: {row.unique_containers:,}")
                logger.info(f"  Contenedores 20': {row.containers_20:,}")
                logger.info(f"  Contenedores 40': {row.containers_40:,}")
                logger.info(f"  Reefer: {row.reefer:,}")
                logger.info(f"  Peligrosos: {row.hazardous:,}")
            
            # Estadísticas por bloque
            block_result = await db.execute(text("""
                SELECT 
                    bloque,
                    COUNT(*) as total,
                    COUNT(DISTINCT fecha) as dias,
                    COUNT(DISTINCT CONCAT(fecha, '-', turno)) as turnos
                FROM container_positions
                WHERE EXTRACT(YEAR FROM fecha) = :year
                GROUP BY bloque
                ORDER BY bloque
            """), {"year": year})
            
            logger.info("\n=== ESTADÍSTICAS POR BLOQUE ===")
            for row in block_result:
                logger.info(f"Bloque {row.bloque}: {row.total:,} posiciones, {row.dias} días, {row.turnos} turnos")
    
    # Tiempo total
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"\n⏱️  Tiempo total de ejecución: {duration}")
    logger.info("✅ Proceso completado exitosamente")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cargar posiciones de contenedores')
    parser.add_argument('--year', type=int, default=2022, help='Año de datos a cargar (default: 2022)')
    parser.add_argument('--week', type=str, help='Semana específica en formato ISO (ej: 2022-01-03)')
    parser.add_argument('--date', type=str, help='Fecha específica (ej: 2022-01-03)')
    
    args = parser.parse_args()
    
    # Ejecutar carga
    asyncio.run(main(year=args.year, week=args.week, specific_date=args.date))