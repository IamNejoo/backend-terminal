# scripts/load_camila_data.py

import asyncio
import os
from pathlib import Path
import sys
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.camila_loader import CamilaLoader

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
        print(f"❌ No existe el directorio {data_path}")
        return

    total = 0
    ok = 0
    failed = []
    unparsed = []

    async with AsyncSessionLocal() as db:
        loader = CamilaLoader(db)
        for filepath in sorted(data_path.glob('*.xlsx')):
            total += 1
            config = parse_filename(filepath.name)
            if not config:
                print(f"⚠️ No se pudo parsear {filepath.name}")
                unparsed.append(filepath.name)
                continue
            try:
                print(f"\n🔄 Procesando {filepath.name}...")
                print(f"   Config: S{config['semana']} {config['dia']} T{config['turno']} {config['modelo_tipo']}")
                await loader.load_camila_file(
                    str(filepath),
                    config['semana'],
                    config['dia'],
                    config['turno'],
                    config['modelo_tipo'],
                    config['con_segregaciones']
                )
                print(f"✅ {filepath.name} cargado exitosamente")
                ok += 1
            except Exception as e:
                print(f"❌ Error procesando {filepath.name}: {str(e)}")
                import traceback
                traceback.print_exc()
                failed.append(filepath.name)
        print("\n==== RESUMEN ====")
        print(f"Archivos procesados: {total}")
        print(f"  - Exitosos: {ok}")
        print(f"  - Errores: {len(failed)}")
        print(f"  - Sin parsear: {len(unparsed)}")
        if failed:
            print("  Errores en:")
            for f in failed:
                print("    -", f)
        if unparsed:
            print("  Sin parsear:")
            for u in unparsed:
                print("    -", u)
        print("✅ Carga finalizada")

if __name__ == "__main__":
    asyncio.run(load_initial_data())
