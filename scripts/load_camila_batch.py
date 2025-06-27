# scripts/load_camila_batch.py
"""
Script mejorado para carga batch de archivos Camila con mejor parseo de nombres
"""
import asyncio
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
import re
import json
from typing import Dict, Optional, List

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.camila_loader import CamilaLoader
from app.core.constants import DAYS_ES

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'camila_batch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class CamilaBatchLoader:
    def __init__(self):
        self.results = {
            'exitosos': [],
            'errores': [],
            'sin_parsear': []
        }
        
    def parse_filename(self, filename: str) -> Optional[Dict]:
        """Parsea el nombre del archivo con patrones mejorados"""
        
        # Remover extensión y convertir a minúsculas
        name = filename.replace('.xlsx', '').replace('.xls', '').lower()
        
        # Patrones para diferentes formatos de nombre
        patterns = [
            # resultados_Semana_31_Monday_T2_min_max_Modelo1.xlsx
            {
                'pattern': r'semana[_\s]?(\d+)[_\s]+(\w+)[_\s]+t(\d+)[_\s]+(min[_\s]?max|max[_\s]?min)',
                'groups': ['semana', 'dia', 'turno', 'modelo']
            },
            # resultados_Semana_31_Modelo1_min_max.xlsx
            {
                'pattern': r'semana[_\s]?(\d+)[_\s]+modelo\d+[_\s]+(min[_\s]?max|max[_\s]?min)',
                'groups': ['semana', None, None, 'modelo']
            },
            # resultados_S31_Friday_T1_minmax.xlsx
            {
                'pattern': r's(\d+)[_\s]+(\w+)[_\s]+t(\d+)[_\s]+(minmax|maxmin)',
                'groups': ['semana', 'dia', 'turno', 'modelo']
            },
            # Semana_42_min_max_Modelo1.xlsx
            {
                'pattern': r'semana[_\s]?(\d+)[_\s]+(min[_\s]?max|max[_\s]?min)',
                'groups': ['semana', None, None, 'modelo']
            },
            # resultados_Semana_42_2_min_max_Modelo1.xlsx (con número adicional)
            {
                'pattern': r'semana[_\s]?(\d+)[_\s]+\d+[_\s]+(min[_\s]?max|max[_\s]?min)',
                'groups': ['semana', None, None, 'modelo']
            }
        ]
        
        # Mapeo de días
        dias_map = {
            'monday': 'Monday', 'lunes': 'Monday', 'mon': 'Monday',
            'tuesday': 'Tuesday', 'martes': 'Tuesday', 'tue': 'Tuesday',
            'wednesday': 'Wednesday', 'miercoles': 'Wednesday', 'wed': 'Wednesday',
            'thursday': 'Thursday', 'jueves': 'Thursday', 'thu': 'Thursday',
            'friday': 'Friday', 'viernes': 'Friday', 'fri': 'Friday',
            'saturday': 'Saturday', 'sabado': 'Saturday', 'sat': 'Saturday',
            'sunday': 'Sunday', 'domingo': 'Sunday', 'sun': 'Sunday'
        }
        
        # Mapeo de modelos
        modelo_map = {
            'min_max': 'minmax', 'min max': 'minmax', 'minmax': 'minmax',
            'max_min': 'maxmin', 'max min': 'maxmin', 'maxmin': 'maxmin'
        }
        
        # Intentar con cada patrón
        for pattern_info in patterns:
            match = re.search(pattern_info['pattern'], name)
            if match:
                groups = match.groups()
                result = {}
                
                # Extraer semana
                idx = pattern_info['groups'].index('semana')
                if idx >= 0 and idx < len(groups):
                    result['semana'] = int(groups[idx])
                
                # Extraer día
                idx = pattern_info['groups'].index('dia') if 'dia' in pattern_info['groups'] else -1
                if idx >= 0 and idx < len(groups) and groups[idx]:
                    dia_lower = groups[idx].lower()
                    result['dia'] = dias_map.get(dia_lower, 'Friday')  # Default: Friday
                else:
                    result['dia'] = 'Friday'  # Default
                
                # Extraer turno
                idx = pattern_info['groups'].index('turno') if 'turno' in pattern_info['groups'] else -1
                if idx >= 0 and idx < len(groups) and groups[idx]:
                    result['turno'] = int(groups[idx])
                else:
                    result['turno'] = 1  # Default
                
                # Extraer modelo
                idx = pattern_info['groups'].index('modelo')
                if idx >= 0 and idx < len(groups):
                    modelo_str = groups[idx].replace('_', ' ').replace('-', ' ').strip()
                    result['modelo_tipo'] = modelo_map.get(modelo_str, 'minmax')
                
                # Determinar si tiene segregaciones
                if '_ss' in name or 'sin_segreg' in name:
                    result['con_segregaciones'] = False
                else:
                    result['con_segregaciones'] = True
                
                # Validar que tenemos lo mínimo necesario
                if 'semana' in result and 'modelo_tipo' in result:
                    logger.info(f"   📄 Parseado exitoso: {filename}")
                    logger.info(f"      Config: {result}")
                    return result
        
        # Si no coincide con ningún patrón
        logger.warning(f"   ⚠️ No se pudo parsear: {filename}")
        return None
    
    async def process_directory(self, directory_path: str):
        """Procesa todos los archivos Excel en un directorio"""
        
        logger.info(f"🔍 Buscando archivos en: {directory_path}")
        
        # Encontrar todos los archivos Excel
        excel_files = []
        for file in sorted(os.listdir(directory_path)):
            if file.endswith(('.xlsx', '.xls')) and not file.startswith('~'):
                excel_files.append(os.path.join(directory_path, file))
        
        logger.info(f"📁 Encontrados {len(excel_files)} archivos Excel")
        
        # Procesar cada archivo
        async with AsyncSessionLocal() as db:
            loader = CamilaLoader(db)
            
            for idx, filepath in enumerate(excel_files, 1):
                filename = os.path.basename(filepath)
                logger.info(f"\n{'='*80}")
                logger.info(f"📄 [{idx}/{len(excel_files)}] Procesando: {filename}")
                
                # Parsear nombre
                config = self.parse_filename(filename)
                if not config:
                    self.results['sin_parsear'].append({
                        'archivo': filename,
                        'razon': 'Formato de nombre no reconocido'
                    })
                    continue
                
                # Cargar archivo
                try:
                    run_id = await loader.load_camila_file(
                        filepath,
                        config['semana'],
                        config['dia'],
                        config['turno'],
                        config['modelo_tipo'],
                        config['con_segregaciones']
                    )
                    
                    self.results['exitosos'].append({
                        'archivo': filename,
                        'run_id': str(run_id),
                        'config': config
                    })
                    
                    logger.info(f"✅ Archivo procesado exitosamente")
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    self.results['errores'].append({
                        'archivo': filename,
                        'error': error_msg,
                        'tipo_error': type(e).__name__,
                        'config_parseada': config
                    })
                    
                    logger.error(f"❌ Error procesando {filename}: {error_msg}")
        
        # Mostrar resumen
        self._print_summary()
        
        # Guardar resultados
        self._save_results()
    
    def _print_summary(self):
        """Imprime resumen de los resultados"""
        
        total = len(self.results['exitosos']) + len(self.results['errores']) + len(self.results['sin_parsear'])
        
        logger.info(f"\n{'='*80}")
        logger.info("📊 RESUMEN DE CARGA")
        logger.info(f"{'='*80}")
        logger.info(f"Total de archivos procesados: {total}")
        logger.info(f"✅ Exitosos: {len(self.results['exitosos'])}")
        logger.info(f"❌ Con errores: {len(self.results['errores'])}")
        logger.info(f"⚠️ Sin parsear: {len(self.results['sin_parsear'])}")
        
        if self.results['errores']:
            logger.info(f"\n❌ ARCHIVOS CON ERRORES:")
            for error in self.results['errores']:
                logger.info(f"   - {error['archivo']}: {error['error']}")
        
        if self.results['sin_parsear']:
            logger.info(f"\n⚠️ ARCHIVOS SIN PARSEAR:")
            for item in self.results['sin_parsear']:
                logger.info(f"   - {item['archivo']}: {item['razon']}")
        
        # Estadísticas por configuración
        if self.results['exitosos']:
            logger.info(f"\n📊 CONFIGURACIONES CARGADAS:")
            config_counts = {}
            for item in self.results['exitosos']:
                config = item['config']
                key = f"S{config['semana']} {config['dia']} T{config['turno']} {config['modelo_tipo']}"
                config_counts[key] = config_counts.get(key, 0) + 1
            
            for config, count in sorted(config_counts.items()):
                logger.info(f"   - {config}: {count} archivo(s)")
    
    def _save_results(self):
        """Guarda los resultados en un archivo JSON"""
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'camila_batch_results_{timestamp}.json'
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"\n💾 Resultados guardados en: {filename}")

async def main():
    """Función principal"""
    
    if len(sys.argv) < 2:
        print("Uso: python load_camila_batch.py <directorio_con_archivos>")
        sys.exit(1)
    
    directory = sys.argv[1]
    if not os.path.exists(directory):
        print(f"Error: El directorio '{directory}' no existe")
        sys.exit(1)
    
    loader = CamilaBatchLoader()
    await loader.process_directory(directory)

if __name__ == "__main__":
    asyncio.run(main())