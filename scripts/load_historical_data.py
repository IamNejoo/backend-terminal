# scripts/load_historical_data.py
import asyncio
import sys
from pathlib import Path
import argparse
import logging
from datetime import datetime
import subprocess
import threading
import queue

sys.path.append(str(Path(__file__).parent.parent))

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

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

def read_output(pipe, q):
    """Leer salida del subprocess en tiempo real"""
    for line in iter(pipe.readline, ''):
        q.put(line)
    pipe.close()

def load_movement_flows(clear_existing: bool = False):
    """
    Llamar al script load_movement_flows.py con salida en tiempo real
    """
    logger.info("\n📥 CARGANDO MOVEMENT FLOWS...")
    logger.info("⚠️  Este proceso puede tomar varios minutos debido al tamaño del archivo...")
    
    cmd = ["python", "-u", "scripts/load_movement_flows.py", "--file", "data/Flujos.csv", "--year-from", "2017"]
    
    if clear_existing:
        cmd.append("--clear")
    
    try:
        # Crear el proceso
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        # Cola para capturar salida
        q = queue.Queue()
        
        # Thread para leer stdout
        stdout_thread = threading.Thread(target=read_output, args=(process.stdout, q))
        stdout_thread.daemon = True
        stdout_thread.start()
        
        # Leer y mostrar salida en tiempo real
        last_progress_time = datetime.now()
        while True:
            try:
                line = q.get(timeout=1)
                if line:
                    # Mostrar líneas importantes
                    if any(keyword in line for keyword in ['✅', '❌', 'registros', 'ERROR', 'Procesados', 'Total']):
                        logger.info(f"  Movement Flows: {line.strip()}")
            except queue.Empty:
                # Verificar si el proceso terminó
                if process.poll() is not None:
                    break
                
                # Mostrar mensaje de progreso cada 30 segundos
                if (datetime.now() - last_progress_time).seconds > 30:
                    logger.info("  ⏳ Procesando Movement Flows...")
                    last_progress_time = datetime.now()
        
        # Esperar a que termine
        stdout_thread.join(timeout=1)
        return_code = process.wait()
        
        if return_code == 0:
            logger.info("✅ Movement Flows cargados exitosamente")
            return True
        else:
            stderr = process.stderr.read()
            logger.error(f"❌ Error al cargar Movement Flows: {stderr}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error ejecutando load_movement_flows.py: {e}")
        return False

async def update_blocks_from_flows(db: AsyncSession, year_from: int = 2017):
    """
    Actualizar bloques en CDT y TTT desde Movement Flows
    """
    logger.info("\n=== ACTUALIZANDO BLOQUES EN CDT Y TTT DESDE MOVEMENT FLOWS ===")
    
    # Convertir year_from a datetime
    year_from_date = datetime(year_from, 1, 1)
    
    # Verificar cuántos movement flows hay
    count_result = await db.execute(text("""
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN patio IS NOT NULL THEN 1 END) as con_patio,
               COUNT(CASE WHEN bloque IS NOT NULL THEN 1 END) as con_bloque
        FROM movement_flows
        WHERE ime_time >= :year_from
    """), {"year_from": year_from_date})
    
    count_row = count_result.first()
    if count_row:
        logger.info(f"Movement Flows disponibles: {count_row.total:,} total, {count_row.con_patio:,} con patio, {count_row.con_bloque:,} con bloque")
    
    if not count_row or count_row.total == 0:
        logger.warning("No hay Movement Flows disponibles para actualizar bloques")
        return 0, 0
    
    # Actualizar CDT
    logger.info("Actualizando bloques en CDT...")
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
    
    # Actualizar TTT
    logger.info("Actualizando bloques en TTT...")
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
    
    logger.info(f"✅ CDT actualizados: {cdt_updated:,} registros")
    logger.info(f"✅ TTT actualizados: {ttt_updated:,} registros")
    
    return cdt_updated, ttt_updated

async def main(year: int = 2022, load_all: bool = False, clear_existing: bool = False, skip_flows: bool = False):
    """
    Función principal para cargar datos históricos
    
    Args:
        year: Año de los datos a cargar
        load_all: Si True, carga todos los tipos de datos (movimientos, CDT, TTT, flows)
        clear_existing: Si True, limpia datos existentes antes de cargar
        skip_flows: Si True, omite la carga de Movement Flows
    """
    # Crear engine
    engine = create_async_engine(settings.DATABASE_URL)
    
    # Crear tablas si no existen
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear sesión
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as db:
        # Limpiar datos si se especifica (excepto movement_flows que se limpia en su propio script)
        if clear_existing:
            logger.info("🧹 Limpiando datos existentes...")
            await db.execute(text("TRUNCATE TABLE historical_movements RESTART IDENTITY CASCADE"))
            await db.execute(text("TRUNCATE TABLE container_dwell_times RESTART IDENTITY CASCADE"))
            await db.execute(text("TRUNCATE TABLE truck_turnaround_times RESTART IDENTITY CASCADE"))
            await db.commit()
            logger.info("✅ Datos existentes eliminados")
        
        service = CSVLoaderService(db)
        
        if load_all:
            # Cargar todos los tipos de datos
            logger.info(f"Iniciando carga completa de datos para el año {year}")
            
            results = {
                'flows': 0,
                'movements': 0,
                'cdt_import': 0,
                'cdt_export': 0,
                'ttt_import': 0,
                'ttt_export': 0
            }
            
            total_start = datetime.now()
            
            # 1. PRIMERO: Cargar Movement Flows usando el script externo (si no se omite)
            if not skip_flows:
                flows_path = "data/Flujos.csv"
                if Path(flows_path).exists():
                    flow_success = load_movement_flows(clear_existing)
                    if flow_success:
                        # Verificar cuántos se cargaron
                        count_result = await db.execute(text("SELECT COUNT(*) FROM movement_flows"))
                        results['flows'] = count_result.scalar() or 0
                else:
                    logger.warning(f"❌ No se encontró archivo de flujos: {flows_path}")
            else:
                logger.info("⏭️  Omitiendo carga de Movement Flows (--skip-flows)")
                # Verificar si ya hay datos
                count_result = await db.execute(text("SELECT COUNT(*) FROM movement_flows"))
                existing_flows = count_result.scalar() or 0
                if existing_flows > 0:
                    logger.info(f"   Movement Flows existentes: {existing_flows:,} registros")
                    results['flows'] = existing_flows
            
            # 2. Cargar movimientos históricos
            try:
                csv_path = f"data/resultados_congestion_SAI_{year}.csv"
                if Path(csv_path).exists():
                    logger.info("\n📥 CARGANDO MOVIMIENTOS HISTÓRICOS...")
                    hist_start = datetime.now()
                    results['movements'] = await service.load_historical_csv(csv_path)
                    hist_elapsed = (datetime.now() - hist_start).total_seconds()
                    logger.info(f"✅ Movimientos: {results['movements']:,} registros en {hist_elapsed:.2f} segundos")
                else:
                    logger.warning(f"❌ No se encontró archivo de movimientos: {csv_path}")
            except Exception as e:
                logger.error(f"Error cargando movimientos: {e}")
            
            # 3-6. Cargar CDT y TTT (igual que antes)
            # [El resto del código permanece igual...]
            
            # 3. Cargar CDT importación
            try:
                cdt_import_path = f"data/resultados_CDT_impo_anio_SAI_{year}.csv"
                if Path(cdt_import_path).exists():
                    logger.info("\n📥 CARGANDO CDT IMPORTACIÓN...")
                    cdt_start = datetime.now()
                    results['cdt_import'] = await service.load_cdt_csv(cdt_import_path, 'import')
                    cdt_elapsed = (datetime.now() - cdt_start).total_seconds()
                    logger.info(f"✅ CDT Import: {results['cdt_import']:,} registros en {cdt_elapsed:.2f} segundos")
                else:
                    logger.warning(f"❌ No se encontró archivo CDT import: {cdt_import_path}")
            except Exception as e:
                logger.error(f"Error cargando CDT importación: {e}")
            
            # 4. Cargar CDT exportación
            try:
                cdt_export_path = f"data/resultados_CDT_expo_anio_SAI_{year}.csv"
                if Path(cdt_export_path).exists():
                    logger.info("\n📥 CARGANDO CDT EXPORTACIÓN...")
                    cdt_start = datetime.now()
                    results['cdt_export'] = await service.load_cdt_csv(cdt_export_path, 'export')
                    cdt_elapsed = (datetime.now() - cdt_start).total_seconds()
                    logger.info(f"✅ CDT Export: {results['cdt_export']:,} registros en {cdt_elapsed:.2f} segundos")
                else:
                    logger.warning(f"❌ No se encontró archivo CDT export: {cdt_export_path}")
            except Exception as e:
                logger.error(f"Error cargando CDT exportación: {e}")
            
            # 5. Cargar TTT importación
            try:
                ttt_import_path = f"data/resultados_TTT_impo_anio_SAI_{year}.csv"
                if Path(ttt_import_path).exists():
                    logger.info("\n📥 CARGANDO TTT IMPORTACIÓN...")
                    ttt_start = datetime.now()
                    results['ttt_import'] = await service.load_ttt_csv(ttt_import_path, 'import')
                    ttt_elapsed = (datetime.now() - ttt_start).total_seconds()
                    logger.info(f"✅ TTT Import: {results['ttt_import']:,} registros en {ttt_elapsed:.2f} segundos")
                else:
                    logger.warning(f"❌ No se encontró archivo TTT import: {ttt_import_path}")
            except Exception as e:
                logger.error(f"Error cargando TTT importación: {e}")
            
            # 6. Cargar TTT exportación
            try:
                ttt_export_path = f"data/resultados_TTT_expo_anio_SAI_{year}.csv"
                if Path(ttt_export_path).exists():
                    logger.info("\n📥 CARGANDO TTT EXPORTACIÓN...")
                    ttt_start = datetime.now()
                    results['ttt_export'] = await service.load_ttt_csv(ttt_export_path, 'export')
                    ttt_elapsed = (datetime.now() - ttt_start).total_seconds()
                    logger.info(f"✅ TTT Export: {results['ttt_export']:,} registros en {ttt_elapsed:.2f} segundos")
                else:
                    logger.warning(f"❌ No se encontró archivo TTT export: {ttt_export_path}")
            except Exception as e:
                logger.error(f"Error cargando TTT exportación: {e}")
            
            # 7. ACTUALIZAR BLOQUES en CDT y TTT
            if results['flows'] > 0 and (results['cdt_import'] > 0 or results['cdt_export'] > 0 or 
                                       results['ttt_import'] > 0 or results['ttt_export'] > 0):
                cdt_blocks, ttt_blocks = await update_blocks_from_flows(db)
            
            total_elapsed = (datetime.now() - total_start).total_seconds()
            
            # Resumen final
            logger.info("\n" + "="*80)
            logger.info("📊 RESUMEN DE CARGA")
            logger.info("="*80)
            logger.info(f"Movement Flows: {results['flows']:,} registros")
            logger.info(f"Movimientos históricos: {results['movements']:,} registros")
            logger.info(f"CDT Importación: {results['cdt_import']:,} registros")
            logger.info(f"CDT Exportación: {results['cdt_export']:,} registros")
            logger.info(f"TTT Importación: {results['ttt_import']:,} registros")
            logger.info(f"TTT Exportación: {results['ttt_export']:,} registros")
            logger.info("-"*80)
            logger.info(f"TOTAL: {sum(results.values()):,} registros cargados")
            logger.info(f"Tiempo total: {total_elapsed:.2f} segundos ({total_elapsed/60:.1f} minutos)")
            
        else:
            # Solo cargar movimientos históricos (comportamiento original)
            csv_path = f"data/resultados_congestion_SAI_{year}.csv"
            if Path(csv_path).exists():
                records = await service.load_historical_csv(csv_path)
                print(f"✅ Cargados {records} registros de movimientos en la base de datos")
            else:
                print(f"❌ No se encontró el archivo: {csv_path}")
        
        if load_all:
            logger.info("\n=== VERIFICACIÓN DE DATOS ===")
            
            # Verificar estadísticas generales
            stats = await db.execute(text("""
                SELECT 
                    'Movement Flows' as tabla,
                    COUNT(*) as total,
                    COUNT(patio) as con_patio,
                    COUNT(bloque) as con_bloque,
                    ROUND(COUNT(patio)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_patio,
                    ROUND(COUNT(bloque)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_bloque
                FROM movement_flows
                UNION ALL
                SELECT 
                    'CDT' as tabla,
                    COUNT(*) as total,
                    COUNT(patio) as con_patio,
                    COUNT(bloque) as con_bloque,
                    ROUND(COUNT(patio)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_patio,
                    ROUND(COUNT(bloque)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_bloque
                FROM container_dwell_times
                UNION ALL
                SELECT 
                    'TTT' as tabla,
                    COUNT(*) as total,
                    COUNT(patio) as con_patio,
                    COUNT(bloque) as con_bloque,
                    ROUND(COUNT(patio)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_patio,
                    ROUND(COUNT(bloque)::numeric * 100.0 / NULLIF(COUNT(*), 0), 2) as pct_bloque
                FROM truck_turnaround_times
            """))
            
            logger.info(f"\n{'Tabla':<20} {'Total':<15} {'Con Patio':<15} {'%':<8} {'Con Bloque':<15} {'%':<8}")
            logger.info("-"*90)
            for row in stats:
                # Manejar valores None
                total = row.total or 0
                con_patio = row.con_patio or 0
                con_bloque = row.con_bloque or 0
                pct_patio = row.pct_patio or 0.0
                pct_bloque = row.pct_bloque or 0.0
                
                logger.info(
                    f"{row.tabla:<20} {total:<15,} {con_patio:<15,} "
                    f"{pct_patio:>7.1f}% {con_bloque:<15,} {pct_bloque:>7.1f}%"
                )
            
            # Verificar CDT con patio/bloque
# Verificar CDT con patio/bloque
            cdt_check = await db.execute(
                text("""
                SELECT 
                    patio,
                    bloque,
                    COUNT(*) as total,
                    ROUND(AVG(cdt_hours)::numeric, 2) as avg_cdt_hours,
                    ROUND((AVG(cdt_hours) / 24)::numeric, 2) as avg_cdt_days
                FROM container_dwell_times
                WHERE patio IS NOT NULL
                  AND bloque IS NOT NULL
                  AND cdt_hours > 0
                GROUP BY patio, bloque
                ORDER BY patio, bloque
                LIMIT 15
                """)
            )
            
            rows = cdt_check.fetchall()
            if rows:
                logger.info("\n📊 Muestra de datos CDT por patio/bloque:")
                logger.info(f"{'Patio':<12} {'Bloque':<8} {'Total':<10} {'CDT Prom (h)':<15} {'CDT Prom (d)':<12}")
                logger.info("-"*60)
                for row in rows:
                    total = row.total or 0
                    avg_hours = row.avg_cdt_hours or 0.0
                    avg_days = row.avg_cdt_days or 0.0
                    
                    logger.info(
                        f"{row.patio:<12} {row.bloque:<8} {total:<10,} "
                        f"{avg_hours:<15.2f} {avg_days:<12.2f}"
                    )
            
            logger.info("\n✅ PROCESO COMPLETADO EXITOSAMENTE!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cargar datos históricos del terminal')
    parser.add_argument('--year', type=int, default=2022, help='Año de datos a cargar (default: 2022)')
    parser.add_argument('--all', action='store_true', help='Cargar todos los tipos de datos (movimientos, CDT, TTT, flows)')
    parser.add_argument('--clear', action='store_true', help='Limpiar datos existentes antes de cargar')
    parser.add_argument('--skip-flows', action='store_true', help='Omitir la carga de Movement Flows')
    
    args = parser.parse_args()
    
    # Ejecutar carga
    asyncio.run(main(year=args.year, load_all=args.all, clear_existing=args.clear, skip_flows=args.skip_flows))