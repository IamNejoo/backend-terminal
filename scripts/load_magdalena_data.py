# scripts/load_magdalena_data.py
import asyncio
import os
from pathlib import Path
import sys
import traceback

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.magdalena_loader import MagdalenaLoader

async def load_initial_data():
    data_path = Path('data/magdalena')
    
    for resultado_file in data_path.glob('resultado_*.xlsx'):
        parts = resultado_file.stem.split('_')
        if len(parts) == 4:
            semana = int(parts[1])
            participacion = int(parts[2])
            con_dispersion = parts[3] == 'K'

            print(f"\n🔄 Procesando configuración S{semana}_P{participacion}_{parts[3]}")

            # 1. Cargar resultado
            try:
                async with AsyncSessionLocal() as db:
                    loader = MagdalenaLoader(db)
                    print(f"  📊 Cargando resultado: {resultado_file.name}")
                    await loader.load_resultado_file(
                        str(resultado_file),
                        semana,
                        participacion,
                        con_dispersion
                    )
                    print("  ✅ Resultado cargado OK")
            except Exception as e:
                print(f"❌ Error procesando resultado {resultado_file.name}: {str(e)}")
                traceback.print_exc()
                try:
                    await db.rollback()
                except Exception as e2:
                    print(f"[ADVERTENCIA] Falló rollback resultado: {e2}")

            # 2. Cargar instancia
            instancia_file = data_path / 'semanas' / f'Semana {semana}' / f'Instancia_{semana}_{participacion}_{parts[3]}.xlsx'
            if instancia_file.exists():
                try:
                    async with AsyncSessionLocal() as db:
                        loader = MagdalenaLoader(db)
                        print(f"  📋 Cargando instancia: {instancia_file.name}")
                        await loader.load_instancia_file(
                            str(instancia_file),
                            semana,
                            participacion,
                            con_dispersion
                        )
                        print("  ✅ Instancia cargada OK")
                except Exception as e:
                    print(f"❌ Error procesando instancia {instancia_file.name}: {str(e)}")
                    traceback.print_exc()
                    try:
                        await db.rollback()
                    except Exception as e2:
                        print(f"[ADVERTENCIA] Falló rollback instancia: {e2}")
            else:
                print(f"  ⚠️  No se encontró instancia: {instancia_file}")

            # 3. Cargar datos reales
            real_file = data_path / 'semanas' / f'Semana {semana}' / f'analisis_flujos_w{semana}_ci.xlsx'
            if real_file.exists():
                try:
                    async with AsyncSessionLocal() as db:
                        loader = MagdalenaLoader(db)
                        print(f"  📈 Cargando datos reales: {real_file.name}")
                        await loader.load_real_data_file(str(real_file), semana)
                        print("  ✅ Datos reales cargados OK")
                except Exception as e:
                    print(f"❌ Error procesando datos reales {real_file.name}: {str(e)}")
                    traceback.print_exc()
                    try:
                        await db.rollback()
                    except Exception as e2:
                        print(f"[ADVERTENCIA] Falló rollback datos reales: {e2}")
            else:
                print(f"  ⚠️  No se encontraron datos reales: {real_file}")

    print(f"\n✅ Carga completa")

if __name__ == "__main__":
    asyncio.run(load_initial_data())
