# scripts/load_optimization_data_enhanced.py - VERSIÓN CON DISTANCIAS HARDCODEADAS
import asyncio
import os
from pathlib import Path
import sys
import traceback
from datetime import datetime
import argparse
import logging

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.optimization_loader import OptimizationLoader
from sqlalchemy import text, delete, select
from app.models.optimization import *

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# DISTANCIAS HARDCODEADAS - Basadas en Distancias_Costanera.xlsx
DISTANCIAS_COSTANERA = {
    # Distancias bloque -> gate
    ('C1', 'GATE'): 1517, ('GATE', 'C1'): 1517,
    ('C2', 'GATE'): 1495, ('GATE', 'C2'): 1495,
    ('C3', 'GATE'): 1836, ('GATE', 'C3'): 1836,
    ('C4', 'GATE'): 1822, ('GATE', 'C4'): 1822,
    ('C5', 'GATE'): 1703, ('GATE', 'C5'): 1703,
    ('C6', 'GATE'): 2055, ('GATE', 'C6'): 2055,
    ('C7', 'GATE'): 2037, ('GATE', 'C7'): 2037,
    ('C8', 'GATE'): 1988, ('GATE', 'C8'): 1988,
    ('C9', 'GATE'): 1937, ('GATE', 'C9'): 1937,
    
    # Distancias bloque -> sitio sur
    ('C1', 'SITIO_SUR'): 780, ('SITIO_SUR', 'C1'): 780,
    ('C2', 'SITIO_SUR'): 787, ('SITIO_SUR', 'C2'): 787,
    ('C3', 'SITIO_SUR'): 486, ('SITIO_SUR', 'C3'): 486,
    ('C4', 'SITIO_SUR'): 492, ('SITIO_SUR', 'C4'): 492,
    ('C5', 'SITIO_SUR'): 470, ('SITIO_SUR', 'C5'): 470,
    ('C6', 'SITIO_SUR'): 656, ('SITIO_SUR', 'C6'): 656,
    ('C7', 'SITIO_SUR'): 700, ('SITIO_SUR', 'C7'): 700,
    ('C8', 'SITIO_SUR'): 725, ('SITIO_SUR', 'C8'): 725,
    ('C9', 'SITIO_SUR'): 718, ('SITIO_SUR', 'C9'): 718,
    
    # Distancias bloque -> sitio norte
    ('C1', 'SITIO_NORTE'): 892, ('SITIO_NORTE', 'C1'): 892,
    ('C2', 'SITIO_NORTE'): 900, ('SITIO_NORTE', 'C2'): 900,
    ('C3', 'SITIO_NORTE'): 597, ('SITIO_NORTE', 'C3'): 597,
    ('C4', 'SITIO_NORTE'): 603, ('SITIO_NORTE', 'C4'): 603,
    ('C5', 'SITIO_NORTE'): 590, ('SITIO_NORTE', 'C5'): 590,
    ('C6', 'SITIO_NORTE'): 336, ('SITIO_NORTE', 'C6'): 336,
    ('C7', 'SITIO_NORTE'): 345, ('SITIO_NORTE', 'C7'): 345,
    ('C8', 'SITIO_NORTE'): 400, ('SITIO_NORTE', 'C8'): 400,
    ('C9', 'SITIO_NORTE'): 393, ('SITIO_NORTE', 'C9'): 393,
    
    # Distancias entre bloques (matriz de remanejo)
    ('C1', 'C1'): 0, ('C1', 'C2'): 572, ('C1', 'C3'): 290, ('C1', 'C4'): 296,
    ('C1', 'C5'): 403, ('C1', 'C6'): 530, ('C1', 'C7'): 536, ('C1', 'C8'): 580, ('C1', 'C9'): 390,
    
    ('C2', 'C1'): 580, ('C2', 'C2'): 0, ('C2', 'C3'): 300, ('C2', 'C4'): 280,
    ('C2', 'C5'): 388, ('C2', 'C6'): 550, ('C2', 'C7'): 530, ('C2', 'C8'): 552, ('C2', 'C9'): 373,
    
    ('C3', 'C1'): 934, ('C3', 'C2'): 920, ('C3', 'C3'): 0, ('C3', 'C4'): 690,
    ('C3', 'C5'): 260, ('C3', 'C6'): 260, ('C3', 'C7'): 266, ('C3', 'C8'): 315, ('C3', 'C9'): 323,
    
    ('C4', 'C1'): 904, ('C4', 'C2'): 888, ('C4', 'C3'): 676, ('C4', 'C4'): 0,
    ('C4', 'C5'): 243, ('C4', 'C6'): 270, ('C4', 'C7'): 257, ('C4', 'C8'): 298, ('C4', 'C9'): 310,
    
    ('C5', 'C1'): 782, ('C5', 'C2'): 771, ('C5', 'C3'): 553, ('C5', 'C4'): 536,
    ('C5', 'C5'): 0, ('C5', 'C6'): 240, ('C5', 'C7'): 224, ('C5', 'C8'): 183, ('C5', 'C9'): 226,
    
    ('C6', 'C1'): 1150, ('C6', 'C2'): 1132, ('C6', 'C3'): 905, ('C6', 'C4'): 894,
    ('C6', 'C5'): 510, ('C6', 'C6'): 0, ('C6', 'C7'): 610, ('C6', 'C8'): 560, ('C6', 'C9'): 500,
    
    ('C7', 'C1'): 1120, ('C7', 'C2'): 1010, ('C7', 'C3'): 886, ('C7', 'C4'): 874,
    ('C7', 'C5'): 497, ('C7', 'C6'): 607, ('C7', 'C7'): 0, ('C7', 'C8'): 540, ('C7', 'C9'): 488,
    
    ('C8', 'C1'): 1062, ('C8', 'C2'): 1048, ('C8', 'C3'): 824, ('C8', 'C4'): 811,
    ('C8', 'C5'): 430, ('C8', 'C6'): 544, ('C8', 'C7'): 532, ('C8', 'C8'): 0, ('C8', 'C9'): 313,
    
    ('C9', 'C1'): 882, ('C9', 'C2'): 870, ('C9', 'C3'): 636, ('C9', 'C4'): 626,
    ('C9', 'C5'): 252, ('C9', 'C6'): 370, ('C9', 'C7'): 358, ('C9', 'C8'): 307, ('C9', 'C9'): 0,
    
    # Distancias desde/hacia PATIO
    ('PATIO', 'C1'): 760, ('C1', 'PATIO'): 760,
    ('PATIO', 'C2'): 748, ('C2', 'PATIO'): 748,
    ('PATIO', 'C3'): 918, ('C3', 'PATIO'): 918,
    ('PATIO', 'C4'): 911, ('C4', 'PATIO'): 911,
    ('PATIO', 'C5'): 852, ('C5', 'PATIO'): 852,
    ('PATIO', 'C6'): 1028, ('C6', 'PATIO'): 1028,
    ('PATIO', 'C7'): 1019, ('C7', 'PATIO'): 1019,
    ('PATIO', 'C8'): 994, ('C8', 'PATIO'): 994,
    ('PATIO', 'C9'): 910, ('C9', 'PATIO'): 910,
    
    # Distancias promedio de carga
    ('C1', 'SITIO_CARGA'): 622, ('C2', 'SITIO_CARGA'): 644,
    ('C3', 'SITIO_CARGA'): 323, ('C4', 'SITIO_CARGA'): 336,
    ('C5', 'SITIO_CARGA'): 322, ('C6', 'SITIO_CARGA'): 500,
    ('C7', 'SITIO_CARGA'): 517, ('C8', 'SITIO_CARGA'): 567,
    ('C9', 'SITIO_CARGA'): 540,
}

def get_week_from_date(date_str):
    """Obtiene el número de semana ISO desde una fecha YYYY-MM-DD"""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.isocalendar()[1]

async def load_distancias_hardcoded():
    """Carga las distancias hardcodeadas en la base de datos"""
    logger.info("📏 Cargando distancias hardcodeadas...")
    
    async with AsyncSessionLocal() as db:
        try:
            # Verificar si ya hay distancias
            result = await db.execute(text("SELECT COUNT(*) FROM distancias_reales"))
            count_before = result.scalar()
            
            if count_before > 0:
                logger.info(f"Ya existen {count_before} distancias en la BD, saltando carga")
                return True
            
            # Cargar distancias hardcodeadas
            for (origen, destino), distancia in DISTANCIAS_COSTANERA.items():
                # Determinar tipos
                tipo_origen = 'bloque' if origen.startswith('C') and len(origen) == 2 else \
                             'gate' if 'GATE' in origen else \
                             'sitio' if 'SITIO' in origen else \
                             'patio' if 'PATIO' in origen else 'otro'
                
                tipo_destino = 'bloque' if destino.startswith('C') and len(destino) == 2 else \
                              'gate' if 'GATE' in destino else \
                              'sitio' if 'SITIO' in destino else \
                              'patio' if 'PATIO' in destino else 'otro'
                
                # Verificar si ya existe
                existing = await db.execute(
                    select(DistanciaReal).where(
                        (DistanciaReal.origen == origen) &
                        (DistanciaReal.destino == destino)
                    )
                )
                if not existing.scalar():
                    dist = DistanciaReal(
                        origen=origen,
                        destino=destino,
                        distancia_metros=distancia,
                        tipo_origen=tipo_origen,
                        tipo_destino=tipo_destino
                    )
                    db.add(dist)
            
            await db.commit()
            
            # Verificar cuántas se cargaron
            result = await db.execute(text("SELECT COUNT(*) FROM distancias_reales"))
            count_after = result.scalar()
            
            logger.info(f"✅ Distancias cargadas: {count_after - count_before} nuevas")
            logger.info(f"📊 Total distancias en BD: {count_after}")
            return True
            
        except Exception as e:
            await db.rollback()
            logger.error(f"❌ Error cargando distancias: {str(e)}")
            return False

async def clean_all_data():
    """Elimina TODOS los datos de optimización de la base de datos"""
    logger.warning("⚠️  ELIMINANDO TODOS LOS DATOS DE OPTIMIZACIÓN...")
    
    async with AsyncSessionLocal() as db:
        try:
            # Orden importante por las foreign keys
            tables_to_clean = [
                'logs_procesamiento',
                'metricas_temporales',
                'kpis_comparativos',
                'resultados_generales',
                'carga_trabajo',
                'ocupacion_bloques',
                'movimientos_modelo',
                'movimientos_reales',
                'asignaciones_bloques',
                'instancias',
                'distancias_reales',
                'segregaciones',
                'bloques'
            ]
            
            for table in tables_to_clean:
                result = await db.execute(text(f"DELETE FROM {table}"))
                count = result.rowcount
                logger.info(f"  - Eliminados {count:,} registros de {table}")
            
            await db.commit()
            logger.info("✅ Base de datos limpiada completamente")
            
        except Exception as e:
            await db.rollback()
            logger.error(f"❌ Error limpiando base de datos: {str(e)}")
            raise

async def clean_instance_data(fecha_str: str = None, participacion: int = None):
    """Elimina datos de instancias específicas"""
    logger.info("🧹 Limpiando datos de instancias específicas...")
    
    async with AsyncSessionLocal() as db:
        try:
            # Construir query para buscar instancias
            query = select(Instancia)
            conditions = []
            
            if fecha_str:
                fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
                conditions.append(Instancia.fecha_inicio == fecha)
            
            if participacion:
                conditions.append(Instancia.participacion == participacion)
            
            if conditions:
                query = query.where(*conditions)
            
            result = await db.execute(query)
            instancias = result.scalars().all()
            
            logger.info(f"Encontradas {len(instancias)} instancias para eliminar")
            
            for instancia in instancias:
                logger.info(f"  - Eliminando: {instancia.codigo}")
                await db.delete(instancia)
            
            await db.commit()
            logger.info("✅ Instancias eliminadas")
            
        except Exception as e:
            await db.rollback()
            logger.error(f"❌ Error eliminando instancias: {str(e)}")
            raise

async def verify_database():
    """Verifica el estado de la base de datos"""
    logger.info("🔍 Verificando estado de la base de datos...")
    
    async with AsyncSessionLocal() as db:
        try:
            queries = {
                'instancias': "SELECT COUNT(*), COUNT(DISTINCT participacion), COUNT(DISTINCT semana) FROM instancias",
                'movimientos_reales': "SELECT COUNT(*) FROM movimientos_reales",
                'movimientos_modelo': "SELECT COUNT(*) FROM movimientos_modelo",
                'kpis_comparativos': "SELECT COUNT(*) FROM kpis_comparativos",
                'resultados_generales': "SELECT COUNT(*) FROM resultados_generales",
                'bloques': "SELECT COUNT(*) FROM bloques",
                'segregaciones': "SELECT COUNT(*) FROM segregaciones",
                'distancias_reales': "SELECT COUNT(*) FROM distancias_reales"
            }
            
            for tabla, query in queries.items():
                result = await db.execute(text(query))
                row = result.fetchone()
                if tabla == 'instancias':
                    logger.info(f"  - {tabla}: {row[0]:,} registros, {row[1]} participaciones, {row[2]} semanas")
                else:
                    logger.info(f"  - {tabla}: {row[0]:,} registros")
                    
        except Exception as e:
            logger.error(f"❌ Error verificando base de datos: {str(e)}")

async def load_optimization_data(
    fecha_especifica: str = None,
    participacion_especifica: int = None,
    limite: int = None,
    skip_existing: bool = False
):
    """Carga datos de optimización con filtros opcionales"""
    
    # PRIMERO: Asegurar que las distancias estén cargadas
    await load_distancias_hardcoded()
    
    optimization_path = os.environ.get('OPTIMIZATION_DATA_PATH', '/app/optimization_data')
    base_path = Path(optimization_path)
    
    # Fallback a ruta local si no existe en Docker
    if not base_path.exists():
        local_path = Path('/home/nejoo/gurobi/resultados_generados')
        if local_path.exists():
            base_path = local_path
        else:
            logger.error(f"❌ No se encontró la ruta de datos en: {optimization_path}")
            return
        
    resultados_path = base_path / 'resultados_magdalena'
    instancias_path = base_path / 'instancias_magdalena'
    
    logger.info(f"🔍 Buscando datos en:")
    logger.info(f"   - Resultados: {resultados_path}")
    logger.info(f"   - Instancias: {instancias_path}")
    
    # Contadores
    total_instancias = 0
    instancias_exitosas = 0
    instancias_fallidas = 0
    instancias_omitidas = 0
    
    # Validar formato de fecha ISO
    def is_valid_iso_date(dirname):
        if len(dirname) != 10:
            return False
        try:
            datetime.strptime(dirname, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    # Obtener directorios de fechas
    all_dirs = [d for d in resultados_path.iterdir() if d.is_dir()]
    fechas_dirs = sorted([d for d in all_dirs if is_valid_iso_date(d.name)])
    
    # Aplicar filtro de fecha si se especifica
    if fecha_especifica:
        fechas_dirs = [d for d in fechas_dirs if d.name == fecha_especifica]
        if not fechas_dirs:
            logger.warning(f"⚠️  No se encontró la fecha especificada: {fecha_especifica}")
            return
    
    # Aplicar límite si se especifica
    if limite:
        fechas_dirs = fechas_dirs[:limite]
    
    logger.info(f"📅 Procesando {len(fechas_dirs)} fechas\n")
    
    # Obtener instancias existentes si skip_existing está activo
    existing_codes = set()
    if skip_existing:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Instancia.codigo))
            existing_codes = {row[0] for row in result.all()}
            logger.info(f"📌 Encontradas {len(existing_codes)} instancias existentes que se omitirán")
    
    for fecha_dir in fechas_dirs:
        fecha_str = fecha_dir.name
        
        try:
            fecha_inicio = datetime.strptime(fecha_str, '%Y-%m-%d')
            semana = get_week_from_date(fecha_str)
            anio = fecha_inicio.year
            
            logger.info(f"\n📁 Procesando {fecha_str} (Año {anio}, Semana {semana})")
            logger.info(f"{'-'*60}")
            
            # Buscar archivos
            resultado_files = list(fecha_dir.glob('resultado_*.xlsx'))
            distancia_files = list(fecha_dir.glob('Distancias_*.xlsx'))
            
            # Buscar archivos de instancia
            instancia_dir = instancias_path / fecha_str
            flujos_files = []
            instancia_files = []
            
            if instancia_dir.exists():
                flujos_files = list(instancia_dir.glob('Flujos_*.xlsx'))
                instancia_files = list(instancia_dir.glob('Instancia_*.xlsx'))
            
            logger.info(f"   Encontrados:")
            logger.info(f"   - {len(resultado_files)} archivos de resultado")
            logger.info(f"   - {len(distancia_files)} archivos de distancia")
            logger.info(f"   - {len(instancia_files)} archivos de instancia")
            logger.info(f"   - {len(flujos_files)} archivos de flujos")
            
            # Procesar cada archivo de resultado
            for resultado_file in resultado_files:
                # Extraer participación
                parts = resultado_file.stem.split('_')
                participacion = None
                con_dispersion = None
                
                for i, part in enumerate(parts):
                    if part.isdigit() and 60 <= int(part) <= 80:
                        participacion = int(part)
                        if i + 1 < len(parts):
                            if parts[i + 1] == 'K':
                                con_dispersion = True
                            elif parts[i + 1] == 'N':
                                con_dispersion = False
                        break
                
                if participacion is None:
                    continue
                
                # Aplicar filtro de participación si se especifica
                if participacion_especifica and participacion != participacion_especifica:
                    continue
                
                # Verificar si ya existe
                dispersion_str = 'K' if con_dispersion else 'N' if con_dispersion is not None else '?'
                codigo = f"{fecha_str.replace('-', '')}_{participacion}_{dispersion_str}"
                
                if skip_existing and codigo in existing_codes:
                    logger.info(f"   ⏭️  Omitiendo P{participacion}_{dispersion_str} (ya existe)")
                    instancias_omitidas += 1
                    continue
                
                total_instancias += 1
                
                logger.info(f"\n   📊 Procesando P{participacion}_{dispersion_str}")
                
                # Buscar archivos relacionados
                flujos_file = flujos_files[0] if flujos_files else None
                distancia_file = None
                instancia_file = None
                
                # Buscar archivo de distancia específico del modelo (NO Costanera)
                for dist in distancia_files:
                    if 'Costanera' in dist.name:
                        continue
                    if f"_{participacion}" in dist.name or f"_{participacion}_" in dist.name:
                        distancia_file = dist
                        break
                
                # Buscar instancia específica
                for inst in instancia_files:
                    if f"_{participacion}_" in inst.name:
                        if con_dispersion is not None:
                            if f"_{participacion}_{'K' if con_dispersion else 'N'}" in inst.name:
                                instancia_file = inst
                                break
                        else:
                            instancia_file = inst
                            break
                
                logger.info(f"      - Resultado: {resultado_file.name}")
                logger.info(f"      - Instancia: {instancia_file.name if instancia_file else 'No encontrada'}")
                logger.info(f"      - Flujos: {flujos_file.name if flujos_file else 'No encontrado'}")
                logger.info(f"      - Distancias modelo: {distancia_file.name if distancia_file else 'No encontrado'}")
                
                try:
                    async with AsyncSessionLocal() as db:
                        loader = OptimizationLoader(db)
                        
                        instancia_id = await loader.load_optimization_results(
                            resultado_filepath=str(resultado_file),
                            instancia_filepath=str(instancia_file) if instancia_file else None,
                            flujos_filepath=str(flujos_file) if flujos_file else None,
                            distancias_filepath=str(distancia_file) if distancia_file else None,
                            fecha_inicio=fecha_inicio,
                            semana=semana,
                            anio=anio,
                            participacion=participacion,
                            con_dispersion=con_dispersion
                        )
                        
                        await db.commit()
                        logger.info(f"   ✅ Cargado exitosamente (ID: {instancia_id})")
                        instancias_exitosas += 1
                        
                except Exception as e:
                    logger.error(f"   ❌ Error: {str(e)}")
                    if os.environ.get("DEBUG"):
                        traceback.print_exc()
                    instancias_fallidas += 1
                    
        except Exception as e:
            logger.error(f"⚠️ Error procesando {fecha_str}: {str(e)}")
            continue
    
    # Resumen final
    logger.info(f"\n{'='*80}")
    logger.info(f"✅ CARGA COMPLETA - {datetime.now()}")
    logger.info(f"{'='*80}")
    logger.info(f"📊 RESUMEN FINAL:")
    logger.info(f"   - Total instancias procesadas: {total_instancias}")
    logger.info(f"   - Exitosas: {instancias_exitosas}")
    logger.info(f"   - Fallidas: {instancias_fallidas}")
    logger.info(f"   - Omitidas: {instancias_omitidas}")
    if total_instancias > 0:
        logger.info(f"   - Tasa de éxito: {(instancias_exitosas/total_instancias*100):.1f}%")

def main():
    """Función principal con argumentos CLI"""
    parser = argparse.ArgumentParser(
        description='Carga datos de optimización de Magdalena',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Cargar todos los datos
  python %(prog)s
  
  # Limpiar toda la base de datos antes de cargar
  python %(prog)s --clean-all
  
  # Cargar solo una fecha específica
  python %(prog)s --fecha 2022-01-03
  
  # Cargar solo una participación específica
  python %(prog)s --participacion 68
  
  # Cargar primeras 5 fechas
  python %(prog)s --limite 5
  
  # Omitir instancias ya cargadas
  python %(prog)s --skip-existing
  
  # Ver estado de la base de datos
  python %(prog)s --verify-only
        """
    )
    
    # Acciones
    parser.add_argument('--clean-all', action='store_true',
                        help='Elimina TODOS los datos antes de cargar')
    parser.add_argument('--clean-instance', action='store_true',
                        help='Elimina instancias específicas (usar con --fecha o --participacion)')
    parser.add_argument('--verify-only', action='store_true',
                        help='Solo verifica el estado de la base de datos')
    
    # Filtros
    parser.add_argument('--fecha', type=str,
                        help='Cargar solo esta fecha (formato: YYYY-MM-DD)')
    parser.add_argument('--participacion', type=int,
                        help='Cargar solo esta participación (60-80)')
    parser.add_argument('--limite', type=int,
                        help='Limitar cantidad de fechas a procesar')
    parser.add_argument('--skip-existing', action='store_true',
                        help='Omitir instancias que ya existen en la base de datos')
    
    # Opciones
    parser.add_argument('--debug', action='store_true',
                        help='Mostrar información de debug')
    
    args = parser.parse_args()
    
    # Configurar debug
    if args.debug:
        os.environ['DEBUG'] = '1'
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Ejecutar acciones
    async def run_actions():
        if args.verify_only:
            await verify_database()
            return
            
        if args.clean_all:
            confirm = input("⚠️  ¿Estás seguro de que quieres ELIMINAR TODOS los datos? (escribir 'SI' para confirmar): ")
            if confirm == 'SI':
                await clean_all_data()
                # Después de limpiar, cargar las distancias hardcodeadas
                await load_distancias_hardcoded()
            else:
                logger.info("Operación cancelada")
                return
        
        if args.clean_instance:
            if not args.fecha and not args.participacion:
                logger.warning("⚠️  Especifica --fecha o --participacion para limpiar instancias específicas")
                return
            await clean_instance_data(args.fecha, args.participacion)
        
        # Cargar datos si no es solo verificación o limpieza
        if not args.verify_only and not (args.clean_instance and not args.clean_all):
            await load_optimization_data(
                fecha_especifica=args.fecha,
                participacion_especifica=args.participacion,
                limite=args.limite,
                skip_existing=args.skip_existing
            )
            
            # Verificar después de cargar
            await verify_database()
    
    # Ejecutar
    logger.info(f"🚀 Iniciando - {datetime.now()}")
    logger.info(f"="*80)
    
    asyncio.run(run_actions())

if __name__ == "__main__":
    main()