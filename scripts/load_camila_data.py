# scripts/load_camila_data.py - Script actualizado para el nuevo modelo
import asyncio
import os
from pathlib import Path
import sys
import re
import logging
from datetime import datetime, date, timedelta
import traceback
import json
import pandas as pd
import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.camila_loader import CamilaLoader

# Configurar logging
log_filename = f'camila_load_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def parse_filename_components(filename):
    """
    Parsea componentes del nombre de archivo
    Ejemplo: resultados_2022-01-03_68_T01.xlsx
    Retorna: (fecha, participacion, turno_num)
    """
    pattern = r'(?:resultados|Instancia)_(\d{4}-\d{2}-\d{2})_(\d+)_T(\d+)\.xlsx'
    match = re.match(pattern, filename)
    
    if match:
        fecha_str = match.group(1)
        participacion = int(match.group(2))
        turno_num = int(match.group(3))
        return fecha_str, participacion, turno_num
    
    return None, None, None

def get_week_and_day_from_date(fecha_str):
    """
    Calcula semana del año y día de la semana desde fecha ISO
    """
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()
    semana = fecha.isocalendar()[1]
    
    dias_semana = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dia = dias_semana[fecha.weekday()]
    
    return semana, dia

def map_turno_to_shift(turno_num):
    """
    Mapea número de turno (1-21) a turno del día (1-3)
    21 turnos por semana = 3 turnos x 7 días
    """
    turno_dia = ((turno_num - 1) % 3) + 1
    dia_offset = (turno_num - 1) // 3
    
    return turno_dia, dia_offset

def adjust_date_for_turno(fecha_str, turno_num):
    """
    Ajusta la fecha según el número de turno
    """
    fecha = datetime.strptime(fecha_str, '%Y-%m-%d').date()
    turno_dia, dia_offset = map_turno_to_shift(turno_num)
    
    fecha_ajustada = fecha + timedelta(days=dia_offset)
    
    return fecha_ajustada, turno_dia

def detect_segregaciones(archivo_path):
    """
    Detecta si el archivo tiene segregaciones analizando los datos
    """
    try:
        # Leer primeras filas
        df = pd.read_excel(archivo_path, nrows=50)
        
        # Buscar en la columna 'índice' si existe
        if 'índice' in df.columns:
            # Buscar flujos con segregaciones
            flujo_vars = ['fr_sbt', 'fe_sbt']
            flujos = df[df['variable'].isin(flujo_vars)]
            
            if flujos.empty:
                logger.info(f"No se encontraron flujos en {archivo_path.name}")
                return False
            
            # Verificar si hay segregaciones diferentes a s1
            for idx in flujos['índice']:
                if pd.notna(idx) and isinstance(idx, str):
                    # Buscar patrones como ('s3', 'b1', 1)
                    match = re.search(r"'s(\d+)'", idx)
                    if match:
                        seg_num = int(match.group(1))
                        if seg_num > 1:  # Si hay segregaciones > s1
                            return True
            
            # Si solo tiene s1 o ninguna, asumir sin segregaciones
            return False
            
        # Si no tiene formato esperado, asumir sin segregaciones
        return False
        
    except Exception as e:
        logger.warning(f"Error detectando segregaciones: {e}")
        return False

def find_matching_file_pairs(base_path):
    """
    Encuentra todos los pares de archivos instancia-resultado
    """
    pairs = []
    missing = []
    
    year_path = Path(base_path)
    if not year_path.exists():
        logger.error(f"No existe el directorio: {year_path}")
        return pairs, missing
    
    # Buscar archivos
    resultados_base = year_path / "resultados_camila" / "mu30k"
    instancias_base = year_path / "instancias_camila" / "mu30k"
    
    if not resultados_base.exists():
        logger.error(f"No existe directorio de resultados: {resultados_base}")
        return pairs, missing
    
    logger.info(f"Escaneando directorio: {resultados_base}")
    
    # Buscar por subdirectorios
    for resultado_dir in sorted(resultados_base.glob("resultados_turno_*")):
        fecha_dir = resultado_dir.name.replace("resultados_turno_", "")
        
        instancia_dir = instancias_base / f"instancias_turno_{fecha_dir}"
        
        if not instancia_dir.exists():
            logger.warning(f"No existe directorio de instancias para {fecha_dir}")
            continue
        
        # Buscar archivos de resultados
        for resultado_file in sorted(resultado_dir.glob("resultados_*.xlsx")):
            fecha_str, participacion, turno_num = parse_filename_components(resultado_file.name)
            
            if fecha_str is None:
                continue
            
            # Buscar instancia correspondiente
            instancia_filename = f"Instancia_{fecha_str}_{participacion}_T{turno_num:02d}.xlsx"
            instancia_path = instancia_dir / instancia_filename
            
            if instancia_path.exists():
                pairs.append({
                    'resultado': resultado_file,
                    'instancia': instancia_path,
                    'fecha': fecha_str,
                    'participacion': participacion,
                    'turno': turno_num
                })
            else:
                missing.append({
                    'resultado': resultado_file.name,
                    'instancia_esperada': instancia_filename
                })
    
    return pairs, missing

async def load_file_pair(pair_info, db):
    """
    Carga un par de archivos usando el nuevo loader
    """
    try:
        resultado_path = pair_info['resultado']
        instancia_path = pair_info['instancia']
        fecha_str = pair_info['fecha']
        participacion = pair_info['participacion']
        turno_original = pair_info['turno']
        
        # Ajustar fecha y turno
        fecha_ajustada, turno_dia = adjust_date_for_turno(fecha_str, turno_original)
        
        # Calcular semana y día
        semana = fecha_ajustada.isocalendar()[1]
        dias_semana = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        dia = dias_semana[fecha_ajustada.weekday()]
        
        # Detectar segregaciones
        con_segregaciones = detect_segregaciones(resultado_path)
        
        logger.info(f"\nCargando: {resultado_path.name}")
        logger.info(f"  - Fecha: {fecha_ajustada} (S{semana} {dia})")
        logger.info(f"  - Turno: {turno_dia}")
        logger.info(f"  - Segregaciones: {'Sí' if con_segregaciones else 'No'}")
        
        # Usar el nuevo loader
        loader = CamilaLoader(db)
        
        run_id = await loader.load_model_results(
            resultado_filepath=str(resultado_path),
            instancia_filepath=str(instancia_path),
            semana=semana,
            dia=dia,
            turno=turno_dia,
            modelo_tipo='maxmin',  # Por defecto
            con_segregaciones=con_segregaciones
        )
        
        return True, run_id
        
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        logger.error(f"Error cargando {pair_info['resultado'].name}: {error_msg}")
        logger.error(traceback.format_exc())
        return False, error_msg

async def load_all_data():
    """
    Carga masiva de todos los archivos
    """
    # Ajustar path según tu estructura
    base_path = Path('/app/data/camila/2022')
    
    logger.info("="*80)
    logger.info("CARGA MASIVA DE DATOS - MODELO CAMILA V2.0")
    logger.info("="*80)
    logger.info(f"Directorio base: {base_path}")
    
    # Buscar archivos
    logger.info("\n🔍 Buscando archivos...")
    pairs, missing = find_matching_file_pairs(base_path)
    
    logger.info(f"\n📊 Resumen:")
    logger.info(f"  - Pares encontrados: {len(pairs)}")
    logger.info(f"  - Archivos sin pareja: {len(missing)}")
    
    if not pairs:
        logger.error("No se encontraron archivos para cargar")
        return
    
    # Estadísticas
    fechas_unicas = set(p['fecha'] for p in pairs)
    logger.info(f"\n📅 Fechas: {len(fechas_unicas)}")
    logger.info(f"  - Desde: {min(fechas_unicas)}")
    logger.info(f"  - Hasta: {max(fechas_unicas)}")
    
    # Cargar archivos
    logger.info(f"\n🚀 Iniciando carga de {len(pairs)} archivos...")
    
    total_ok = 0
    total_failed = 0
    failed_details = []
    
    async with AsyncSessionLocal() as db:
        for i, pair in enumerate(pairs):
            logger.info(f"\n[{i+1}/{len(pairs)}] Procesando...")
            
            success, result = await load_file_pair(pair, db)
            
            if success:
                total_ok += 1
                logger.info(f"  ✅ OK - Run ID: {result}")
            else:
                total_failed += 1
                failed_details.append({
                    'archivo': pair['resultado'].name,
                    'error': result
                })
                logger.error(f"  ❌ FALLO")
            
            # Commit cada 10 archivos
            if (i + 1) % 10 == 0:
                await db.commit()
                logger.info(f"  💾 Commit realizado ({i+1} archivos)")
        
        # Commit final
        await db.commit()
    
    # Resumen final
    logger.info("\n" + "="*80)
    logger.info("RESUMEN FINAL")
    logger.info("="*80)
    logger.info(f"Total procesados: {len(pairs)}")
    logger.info(f"  ✅ Exitosos: {total_ok}")
    logger.info(f"  ❌ Fallidos: {total_failed}")
    
    if failed_details:
        error_file = f'errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(error_file, 'w') as f:
            json.dump(failed_details, f, indent=2)
        logger.info(f"\n💾 Errores guardados en: {error_file}")
    
    logger.info(f"\n📝 Log completo: {log_filename}")
    logger.info("\n✅ Carga completada")

async def load_single_file(resultado_path: str, instancia_path: str):
    """
    Carga un único par de archivos (útil para testing)
    """
    logger.info("Cargando archivo individual...")
    
    # Parsear información del nombre
    resultado_file = Path(resultado_path)
    fecha_str, participacion, turno_num = parse_filename_components(resultado_file.name)
    
    if not fecha_str:
        raise ValueError(f"No se pudo parsear el nombre del archivo: {resultado_file.name}")
    
    pair_info = {
        'resultado': resultado_file,
        'instancia': Path(instancia_path),
        'fecha': fecha_str,
        'participacion': participacion,
        'turno': turno_num
    }
    
    async with AsyncSessionLocal() as db:
        success, result = await load_file_pair(pair_info, db)
        await db.commit()
        
        if success:
            logger.info(f"✅ Archivo cargado exitosamente. Run ID: {result}")
        else:
            logger.error(f"❌ Error al cargar archivo: {result}")
        
        return success, result

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Cargar datos del modelo Camila')
    parser.add_argument('--all', action='store_true', help='Cargar todos los archivos')
    parser.add_argument('--resultado', type=str, help='Path al archivo de resultados')
    parser.add_argument('--instancia', type=str, help='Path al archivo de instancia')
    
    args = parser.parse_args()
    
    if args.all:
        asyncio.run(load_all_data())
    elif args.resultado and args.instancia:
        asyncio.run(load_single_file(args.resultado, args.instancia))
    else:
        parser.print_help()
        sys.exit(1)