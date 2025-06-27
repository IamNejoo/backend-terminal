# scripts/load_camila_data.py
import asyncio
import os
from pathlib import Path
import sys
import re
import logging
from datetime import datetime
import traceback
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.camila_loader import CamilaLoader

# Configurar logging detallado
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

def parse_filename(filename):
    """Parsea el nombre del archivo para extraer parámetros clave."""
    name = filename.replace('.xlsx', '').lower()
    semana = None
    modelo_tipo = None
    
    # Semana
    semana_match = re.search(r'semana[_-]?(\d+)', name)
    if semana_match:
        semana = int(semana_match.group(1))
    
    # Modelo tipo
    if 'min_max' in name or 'minmax' in name:
        modelo_tipo = 'minmax'
    elif 'max_min' in name or 'maxmin' in name:
        modelo_tipo = 'maxmin'
    
    # Validar
    if semana is None or modelo_tipo is None:
        return None
    
    # Siempre asumimos los mismos valores para estos campos (ajusta si lo necesitas)
    return {
        'semana': semana,
        'dia': 'Friday',
        'turno': 1,
        'modelo_tipo': modelo_tipo,
        'con_segregaciones': True
    }

async def load_initial_data():
    """Carga todos los archivos .xlsx en data/camila"""
    data_path = Path('data/camila')
    
    if not data_path.exists():
        logger.error(f"❌ No existe el directorio {data_path}")
        return
    
    total = 0
    ok = 0
    failed = []
    unparsed = []
    error_details = {}
    
    logger.info(f"🔍 Buscando archivos en: {data_path}")
    
    async with AsyncSessionLocal() as db:
        loader = CamilaLoader(db)
        
        for filepath in sorted(data_path.glob('*.xlsx')):
            if filepath.name.startswith('~'):  # Ignorar archivos temporales
                continue
                
            total += 1
            logger.info(f"\n{'='*80}")
            logger.info(f"📄 Archivo {total}: {filepath.name}")
            
            config = parse_filename(filepath.name)
            if not config:
                logger.warning(f"⚠️ No se pudo parsear {filepath.name}")
                unparsed.append(filepath.name)
                continue
            
            try:
                logger.info(f"🔄 Procesando {filepath.name}...")
                logger.info(f"   Config: S{config['semana']} {config['dia']} T{config['turno']} {config['modelo_tipo']}")
                
                await loader.load_camila_file(
                    str(filepath),
                    config['semana'],
                    config['dia'],
                    config['turno'],
                    config['modelo_tipo'],
                    config['con_segregaciones']
                )
                
                logger.info(f"✅ {filepath.name} cargado exitosamente")
                ok += 1
                
            except Exception as e:
                error_msg = f"Error procesando {filepath.name}"
                error_type = type(e).__name__
                error_str = str(e)
                error_tb = traceback.format_exc()
                
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Tipo de error: {error_type}")
                logger.error(f"   Mensaje: {error_str}")
                logger.error(f"   Traceback completo:\n{error_tb}")
                
                failed.append(filepath.name)
                error_details[filepath.name] = {
                    'tipo': error_type,
                    'mensaje': error_str,
                    'traceback': error_tb,
                    'config': config
                }
        
        logger.info(f"\n{'='*80}")
        logger.info("📊 RESUMEN DE CARGA")
        logger.info(f"{'='*80}")
        logger.info(f"Archivos procesados: {total}")
        logger.info(f"  ✅ Exitosos: {ok}")
        logger.info(f"  ❌ Errores: {len(failed)}")
        logger.info(f"  ⚠️ Sin parsear: {len(unparsed)}")
        
        if failed:
            logger.info("\n❌ ARCHIVOS CON ERRORES:")
            for idx, f in enumerate(failed, 1):
                logger.info(f"  {idx}. {f}")
                if f in error_details:
                    logger.info(f"     - Tipo: {error_details[f]['tipo']}")
                    logger.info(f"     - Error: {error_details[f]['mensaje'][:200]}...")
        
        if unparsed:
            logger.info("\n⚠️ ARCHIVOS SIN PARSEAR:")
            for idx, u in enumerate(unparsed, 1):
                logger.info(f"  {idx}. {u}")
        
        # Guardar detalles de errores en JSON
        if error_details:
            error_file = f'camila_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            with open(error_file, 'w', encoding='utf-8') as f:
                json.dump(error_details, f, indent=2, ensure_ascii=False)
            logger.info(f"\n💾 Detalles de errores guardados en: {error_file}")
        
        logger.info(f"\n📝 Log completo guardado en: {log_filename}")
        logger.info("\n✅ Carga finalizada")

if __name__ == "__main__":
    asyncio.run(load_initial_data())