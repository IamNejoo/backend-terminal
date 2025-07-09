import os
import sys
from pathlib import Path
from datetime import datetime
import logging
import asyncio

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.magdalena_service import MagdalenaService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def validar_estructura_archivo(archivo_path: Path) -> tuple[bool, str]:
    """Valida que el archivo tenga las hojas necesarias"""
    try:
        import pandas as pd
        xl = pd.ExcelFile(archivo_path)
        
        hojas_requeridas = ['General', 'Flujos', 'Carga máx-min', 'Ocupación Bloques', 'Workload bloques']
        hojas_existentes = xl.sheet_names
        
        faltantes = [h for h in hojas_requeridas if h not in hojas_existentes]
        
        if faltantes:
            return False, f"Faltan hojas: {', '.join(faltantes)}"
        
        return True, "OK"
    except Exception as e:
        return False, f"Error leyendo archivo: {str(e)}"

async def main():
    """Carga masiva de archivos de Magdalena"""
    
    base_path = Path("/app/optimization_files/resultados_magdalena")
    
    if not base_path.exists():
        logger.error(f"No existe el directorio: {base_path}")
        return
    
    # Estadísticas
    stats = {
        'total_archivos': 0,
        'archivos_validos': 0,
        'archivos_cargados': 0,
        'total_registros': 0,
        'errores': []
    }
    
    # Obtener todas las carpetas de fechas
    fechas = sorted([d for d in base_path.iterdir() if d.is_dir()])
    
    logger.info(f"{'='*60}")
    logger.info(f"Iniciando carga de {len(fechas)} fechas")
    logger.info(f"{'='*60}")
    
    async with AsyncSessionLocal() as db:
        service = MagdalenaService(db)
        
        for fecha_dir in fechas:
            fecha_str = fecha_dir.name
            
            # Validar formato de fecha
            try:
                datetime.strptime(fecha_str, '%Y-%m-%d')
            except ValueError:
                logger.warning(f"Saltando directorio con formato inválido: {fecha_str}")
                continue
            
            logger.info(f"\n📅 Procesando fecha: {fecha_str}")
            
            # Buscar archivos de resultado
            archivos = list(fecha_dir.glob("resultado_*.xlsx"))
            stats['total_archivos'] += len(archivos)
            
            for archivo in archivos:
                # Validar estructura
                valido, mensaje = validar_estructura_archivo(archivo)
                
                if not valido:
                    logger.error(f"❌ {archivo.name}: {mensaje}")
                    stats['errores'].append({
                        'archivo': archivo.name,
                        'error': mensaje
                    })
                    continue
                
                stats['archivos_validos'] += 1
                
                # Extraer parámetros del nombre
                # resultado_2022-01-03_68_K.xlsx
                parts = archivo.stem.split('_')
                
                try:
                    participacion = int(parts[-2])
                    con_dispersion = parts[-1] == 'K'
                    
                    logger.info(f"📄 Cargando: {archivo.name} (P{participacion}, {'CON' if con_dispersion else 'SIN'} dispersión)")
                    
                    result = await service.cargar_archivo_completo(fecha_str, participacion, con_dispersion)
                    
                    if result["status"] == "success":
                        stats['archivos_cargados'] += 1
                        stats['total_registros'] += result.get("registros", 0)
                        logger.info(f"✅ Cargados {result['registros']} registros")
                    else:
                        stats['errores'].append({
                            'archivo': archivo.name,
                            'error': result.get("message", "Error desconocido")
                        })
                        logger.error(f"❌ Error: {result['message']}")
                        
                except Exception as e:
                    stats['errores'].append({
                        'archivo': archivo.name,
                        'error': str(e)
                    })
                    logger.error(f"❌ Error procesando {archivo.name}: {e}")
    
    # Resumen final
    logger.info(f"\n{'='*60}")
    logger.info(f"RESUMEN DE CARGA")
    logger.info(f"{'='*60}")
    logger.info(f"Total archivos encontrados: {stats['total_archivos']}")
    logger.info(f"Archivos con estructura válida: {stats['archivos_validos']}")
    logger.info(f"Archivos cargados exitosamente: {stats['archivos_cargados']}")
    logger.info(f"Total registros creados: {stats['total_registros']}")
    logger.info(f"Errores encontrados: {len(stats['errores'])}")
    
    if stats['errores']:
        logger.info(f"\n❌ DETALLE DE ERRORES:")
        for i, err in enumerate(stats['errores'][:10], 1):
            logger.info(f"{i}. {err['archivo']}: {err['error']}")
        
        # Guardar log completo
        with open('magdalena_carga_errores.log', 'w') as f:
            for err in stats['errores']:
                f.write(f"{err['archivo']}: {err['error']}\n")
        
        logger.info(f"\nLog completo guardado en: magdalena_carga_errores.log")

if __name__ == "__main__":
    logger.info(f"🚀 Iniciando carga masiva de Magdalena - {datetime.now()}")
    asyncio.run(main())