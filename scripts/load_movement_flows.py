# scripts/load_movement_flows.py
import asyncio
import sys
from pathlib import Path
import argparse
import logging
from datetime import datetime

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.core.config import get_settings
from app.services.movement_flow_loader import MovementFlowLoaderService
from app.models.base import Base
from app.models.movement_flow import MovementFlow

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

settings = get_settings()

async def update_blocks_only(year_from: int = 2017):
    """
    Solo actualizar bloques en CDT y TTT sin cargar datos nuevos
    """
    logger.info(f"=== ACTUALIZANDO BLOQUES EN CDT Y TTT ===")
    logger.info(f"Usando movimientos desde año: {year_from}")
    
    # Crear engine
    engine = create_async_engine(settings.DATABASE_URL)
    
    # Crear sesión
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        # Convertir year_from a datetime para evitar error de tipo
        year_from_date = datetime(year_from, 1, 1)
        
        # Primero, verificar cuántos movement flows hay
        count_result = await db.execute(text("""
            SELECT COUNT(*) as total,
                   COUNT(CASE WHEN patio IS NOT NULL THEN 1 END) as con_patio,
                   COUNT(CASE WHEN bloque IS NOT NULL THEN 1 END) as con_bloque
            FROM movement_flows
            WHERE ime_time >= :year_from
        """), {"year_from": year_from_date})
        
        count_row = count_result.first()
        logger.info(f"Movement Flows disponibles: {count_row.total:,} total, {count_row.con_patio:,} con patio, {count_row.con_bloque:,} con bloque")
        
        # Actualizar CDT con el último bloque conocido por contenedor
        logger.info("\nActualizando CDT...")
        result = await db.execute(text("""
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque,
                    ime_time
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                  AND ime_time >= :year_from
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE container_dwell_times cdt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE cdt.iufv_gkey = ub.ime_ufv_gkey
              AND (cdt.patio IS NULL OR cdt.bloque IS NULL)
            RETURNING cdt.iufv_gkey;
        """), {"year_from": year_from_date})
        
        cdt_updated = result.rowcount
        await db.commit()
        
        # Actualizar TTT con el último bloque conocido por contenedor
        logger.info("Actualizando TTT...")
        result = await db.execute(text("""
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque,
                    ime_time
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                  AND ime_time >= :year_from
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE truck_turnaround_times ttt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE ttt.iufv_gkey = ub.ime_ufv_gkey
              AND (ttt.patio IS NULL OR ttt.bloque IS NULL)
            RETURNING ttt.iufv_gkey;
        """), {"year_from": year_from_date})
        
        ttt_updated = result.rowcount
        await db.commit()
        
        logger.info(f"\n✅ CDT actualizados: {cdt_updated:,} registros")
        logger.info(f"✅ TTT actualizados: {ttt_updated:,} registros")
        
        # Mostrar estadísticas finales
        result = await db.execute(text("""
            SELECT 
                'CDT' as tabla,
                COUNT(*) as total,
                COUNT(patio) as con_patio,
                COUNT(bloque) as con_bloque,
                ROUND(COUNT(patio)::numeric * 100.0 / COUNT(*), 2) as pct_con_patio,
                ROUND(COUNT(bloque)::numeric * 100.0 / COUNT(*), 2) as pct_con_bloque
            FROM container_dwell_times
            WHERE iufv_it >= :year_from
            UNION ALL
            SELECT 
                'TTT' as tabla,
                COUNT(*) as total,
                COUNT(patio) as con_patio,
                COUNT(bloque) as con_bloque,
                ROUND(COUNT(patio)::numeric * 100.0 / COUNT(*), 2) as pct_con_patio,
                ROUND(COUNT(bloque)::numeric * 100.0 / COUNT(*), 2) as pct_con_bloque
            FROM truck_turnaround_times
            WHERE cv_ata >= :year_from
        """), {"year_from": year_from_date})
        
        logger.info("\n=== ESTADÍSTICAS FINALES ===")
        logger.info(f"{'Tabla':<5} {'Total':<10} {'Con Patio':<10} {'%':<6} {'Con Bloque':<10} {'%':<6}")
        logger.info("-" * 60)
        for row in result:
            logger.info(
                f"{row.tabla:<5} {row.total:<10,} {row.con_patio:<10,} {row.pct_con_patio:<6}% "
                f"{row.con_bloque:<10,} {row.pct_con_bloque:<6}%"
            )
    
    await engine.dispose()

async def main(
    file_path: str = "/data/Flujos.csv", 
    clear_existing: bool = False,
    year_from: int = 2017,
    year_to: int = None,
    update_only: bool = False
):
    """
    Cargar archivo de flujos de movimiento con filtro de años
    """
    # Si solo es actualización, ejecutar función específica
    if update_only:
        await update_blocks_only(year_from)
        return
    
    logger.info(f"=== INICIANDO CARGA DE FLUJOS ===")
    logger.info(f"Archivo: {file_path}")
    logger.info(f"Filtro de años: {year_from} - {year_to or 'actual'}")
    logger.info(f"Limpiar datos existentes: {clear_existing}")
    
    # Crear engine
    engine = create_async_engine(settings.DATABASE_URL)
    
    # Crear tablas si no existen
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear sesión
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        if clear_existing:
            logger.info("Limpiando datos existentes...")
            await db.execute(text("TRUNCATE TABLE movement_flows RESTART IDENTITY CASCADE"))
            await db.commit()
        
        service = MovementFlowLoaderService(db)
        
        # Cargar archivo con filtro de años
        if Path(file_path).exists():
            start_time = datetime.now()
            records = await service.load_movement_flows_csv(
                file_path, 
                year_from=year_from,
                year_to=year_to
            )
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"✅ Proceso completado en {elapsed_time:.2f} segundos")
            logger.info(f"✅ {records:,} registros cargados")
            logger.info(f"✅ Velocidad: {records/elapsed_time:.0f} registros/segundo")
        else:
            logger.error(f"❌ No se encontró el archivo: {file_path}")
            return
        
        # Actualizar bloques en CDT y TTT
        logger.info("\n=== ACTUALIZANDO BLOQUES EN CDT Y TTT ===")
        
        # Convertir year_from a datetime para evitar error de tipo
        year_from_date = datetime(year_from, 1, 1)
        
        # Actualizar CDT con el último bloque conocido por contenedor
        result = await db.execute(text("""
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque,
                    ime_time
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                  AND ime_time >= :year_from
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE container_dwell_times cdt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE cdt.iufv_gkey = ub.ime_ufv_gkey
              AND cdt.patio IS NULL
            RETURNING cdt.iufv_gkey;
        """), {"year_from": year_from_date})
        
        cdt_updated = result.rowcount
        await db.commit()
        
        # Actualizar TTT con el último bloque conocido por contenedor
        result = await db.execute(text("""
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque,
                    ime_time
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                  AND ime_time >= :year_from
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE truck_turnaround_times ttt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE ttt.iufv_gkey = ub.ime_ufv_gkey
              AND ttt.patio IS NULL
            RETURNING ttt.iufv_gkey;
        """), {"year_from": year_from_date})
        
        ttt_updated = result.rowcount
        await db.commit()
        
        logger.info(f"CDT actualizados: {cdt_updated:,} registros")
        logger.info(f"TTT actualizados: {ttt_updated:,} registros")
        
        # Mostrar resultados finales
        result = await db.execute(text("""
            SELECT 
                'Movement Flows' as tabla,
                COUNT(*) as total,
                COUNT(patio) as con_patio,
                COUNT(bloque) as con_bloque,
                MIN(ime_time) as fecha_min,
                MAX(ime_time) as fecha_max
            FROM movement_flows
            UNION ALL
            SELECT 
                'CDT' as tabla,
                COUNT(*) as total,
                COUNT(patio) as con_patio,
                COUNT(bloque) as con_bloque,
                MIN(iufv_it) as fecha_min,
                MAX(iufv_it) as fecha_max
            FROM container_dwell_times
            UNION ALL
            SELECT 
                'TTT' as tabla,
                COUNT(*) as total,
                COUNT(patio) as con_patio,
                COUNT(bloque) as con_bloque,
                MIN(cv_ata) as fecha_min,
                MAX(cv_ata) as fecha_max
            FROM truck_turnaround_times
        """))
        
        logger.info("\n=== RESUMEN FINAL ===")
        logger.info(f"{'Tabla':<20} {'Total':<10} {'Con Patio':<10} {'Con Bloque':<10} {'Fecha Min':<20} {'Fecha Max':<20}")
        logger.info("-" * 90)
        for row in result:
            logger.info(
                f"{row.tabla:<20} {row.total:<10,} {row.con_patio:<10,} {row.con_bloque:<10,} "
                f"{str(row.fecha_min)[:19]:<20} {str(row.fecha_max)[:19]:<20}"
            )
    
    await engine.dispose()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cargar flujos de movimiento con filtro de años')
    parser.add_argument('--file', type=str, default="data/data_2022.csv", 
                       help='Ruta del archivo CSV')
    parser.add_argument('--clear', action='store_true', 
                       help='Limpiar datos existentes antes de cargar')
    parser.add_argument('--year-from', type=int, default=2017, 
                       help='Año desde el cual cargar datos (default: 2017)')
    parser.add_argument('--year-to', type=int, default=None, 
                       help='Año hasta el cual cargar datos (default: todos)')
    parser.add_argument('--update-only', action='store_true',
                       help='Solo actualizar bloques en CDT y TTT sin cargar datos nuevos')
    
    args = parser.parse_args()
    
    asyncio.run(main(
        file_path=args.file, 
        clear_existing=args.clear,
        year_from=args.year_from,
        year_to=args.year_to,
        update_only=args.update_only
    ))