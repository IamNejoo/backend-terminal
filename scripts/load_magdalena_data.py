# scripts/load_magdalena_data.py
import asyncio
import os
from pathlib import Path
import sys
import traceback
from datetime import datetime

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.magdalena_loader import MagdalenaLoader

def get_week_from_date(date_str):
    """Obtiene el número de semana ISO desde una fecha YYYY-MM-DD"""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.isocalendar()[1]

async def load_initial_data():
    # Rutas absolutas
    base_path = Path('/app/data/magdalena/2022')
    resultados_path = base_path / 'resultados_magdalena'
    instancias_path = base_path / 'instancias_magdalena'
    
    print(f"🔍 DEBUG: Rutas configuradas:")
    print(f"  - Resultados: {resultados_path}")
    print(f"  - Instancias: {instancias_path}")
    print(f"  - ¿Existe resultados? {resultados_path.exists()}")
    print(f"  - ¿Existe instancias? {instancias_path.exists()}")
    
    # Contar archivos procesados
    total_resultados = 0
    total_instancias = 0
    total_reales = 0
    carpetas_procesadas = 0
    
    # Obtener todas las fechas únicas de ambos directorios
    fechas_resultados = set()
    fechas_instancias = set()
    
    if resultados_path.exists():
        fechas_resultados = {d.name for d in resultados_path.iterdir() if d.is_dir() and d.name != 'resultados_magdalena'}
    
    if instancias_path.exists():
        fechas_instancias = {d.name for d in instancias_path.iterdir() if d.is_dir()}
    
    # Combinar todas las fechas únicas
    todas_fechas = sorted(fechas_resultados | fechas_instancias)
    
    print(f"\n📊 RESUMEN DE FECHAS:")
    print(f"  - Fechas en resultados: {len(fechas_resultados)}")
    print(f"  - Fechas en instancias: {len(fechas_instancias)}")
    print(f"  - Total fechas únicas: {len(todas_fechas)}")
    
    # Procesar cada fecha
    for fecha_str in todas_fechas:
        # Validar formato de fecha
        try:
            semana = get_week_from_date(fecha_str)
        except ValueError:
            print(f"⚠️ Saltando directorio con formato inválido: {fecha_str}")
            continue
        
        print(f"\n{'='*60}")
        print(f"📅 Procesando fecha {fecha_str} (Semana {semana})")
        carpetas_procesadas += 1
        
        # Directorios para esta fecha
        resultado_dir = resultados_path / fecha_str
        instancia_dir = instancias_path / fecha_str
        
        # 1. Buscar y procesar archivos de resultado
        resultado_files = []
        if resultado_dir.exists():
            resultado_files = list(resultado_dir.glob('resultado_*.xlsx'))
            print(f"🔍 Resultados encontrados: {len(resultado_files)}")
            for f in resultado_files:
                print(f"  - {f.name}")
        else:
            print(f"⚠️ No existe directorio de resultados para {fecha_str}")
        
        # 2. Buscar archivos de instancia y flujos
        instancia_files = []
        flujos_files = []
        if instancia_dir.exists():
            instancia_files = list(instancia_dir.glob('[iI]nstancia*.xlsx'))
            flujos_files = list(instancia_dir.glob('*flujos*.xlsx'))
            print(f"🔍 Instancias encontradas: {len(instancia_files)}")
            print(f"🔍 Flujos encontrados: {len(flujos_files)}")
        else:
            print(f"⚠️ No existe directorio de instancias para {fecha_str}")
        
        # Procesar cada archivo de resultado
        for resultado_file in resultado_files:
            print(f"\n  🔄 Procesando resultado: {resultado_file.name}")
            
            # Extraer participación y dispersión del nombre
            parts = resultado_file.stem.split('_')
            
            # Buscar participación y dispersión
            participacion = None
            con_dispersion = None
            
            for i, part in enumerate(parts):
                if part.isdigit() and len(part) <= 3:
                    participacion = int(part)
                    if i + 1 < len(parts) and parts[i + 1] in ['K', 'N']:
                        con_dispersion = parts[i + 1] == 'K'
                        break
            
            if participacion is None:
                print(f"  ⚠️ No se pudo extraer participación de: {resultado_file.name}")
                continue
            
            print(f"  📊 Config: Semana={semana}, Participación={participacion}, Dispersión={'K' if con_dispersion else 'N'}")
            
            # 1. Cargar resultado
            try:
                async with AsyncSessionLocal() as db:
                    loader = MagdalenaLoader(db)
                    print(f"  Cargando resultado...")
                    
                    await loader.load_resultado_file(
                        str(resultado_file),
                        semana,
                        participacion,
                        con_dispersion
                    )
                    
                    await db.commit()
                    print("  ✅ Resultado cargado OK")
                    total_resultados += 1
                    
            except Exception as e:
                print(f"  ❌ ERROR resultado: {str(e)}")
                if "DEBUG" in os.environ:
                    traceback.print_exc()

            # 2. Buscar y cargar instancia correspondiente
            instancia_encontrada = None
            patron_dispersion = 'K' if con_dispersion else 'N'
            
            for inst_file in instancia_files:
                # Buscar instancia que coincida
                if f"_{participacion}_{patron_dispersion}" in inst_file.name or f"{participacion}_{patron_dispersion}" in inst_file.name:
                    instancia_encontrada = inst_file
                    break
            
            if instancia_encontrada:
                try:
                    async with AsyncSessionLocal() as db:
                        loader = MagdalenaLoader(db)
                        print(f"  Cargando instancia: {instancia_encontrada.name}")
                        
                        await loader.load_instancia_file(
                            str(instancia_encontrada),
                            semana,
                            participacion,
                            con_dispersion
                        )
                        
                        await db.commit()
                        print("  ✅ Instancia cargada OK")
                        total_instancias += 1
                        
                except Exception as e:
                    print(f"  ❌ ERROR instancia: {str(e)}")
                    if "DEBUG" in os.environ:
                        traceback.print_exc()
            else:
                print(f"  ⚠️ No se encontró instancia para P{participacion}_{patron_dispersion}")
        
        # 3. Cargar datos reales (flujos) - una vez por fecha
        if flujos_files and len(flujos_files) > 0:
            # Solo cargar si no se ha cargado ya para esta fecha
            flujos_ya_cargados = False
            for f in flujos_files:
                if 'analisis_flujos' in f.name:
                    try:
                        async with AsyncSessionLocal() as db:
                            loader = MagdalenaLoader(db)
                            print(f"\n  Cargando flujos: {f.name}")
                            
                            await loader.load_real_data_file(str(f), semana)
                            
                            await db.commit()
                            print("  ✅ Flujos cargados OK")
                            total_reales += 1
                            flujos_ya_cargados = True
                            break  # Solo cargar un archivo de flujos por fecha
                            
                    except Exception as e:
                        print(f"  ❌ ERROR flujos: {str(e)}")
                        if "DEBUG" in os.environ:
                            traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"✅ CARGA COMPLETA - {datetime.now()}")
    print(f"{'='*60}")
    print(f"📊 RESUMEN FINAL:")
    print(f"   - Carpetas procesadas: {carpetas_procesadas}")
    print(f"   - Resultados cargados: {total_resultados}")
    print(f"   - Instancias cargadas: {total_instancias}")
    print(f"   - Datos reales cargados: {total_reales}")
    print(f"{'='*60}")
    
    # Verificación adicional en base de datos
    try:
        async with AsyncSessionLocal() as db:
            # Contar registros en cada tabla
            from sqlalchemy import text
            
            # Verificar tabla de configuraciones
            result = await db.execute(text("SELECT COUNT(*) FROM magdalena_configuracion"))
            config_count = result.scalar()
            
            # Verificar tabla de resultados
            result = await db.execute(text("SELECT COUNT(*) FROM magdalena_resultado"))
            resultado_count = result.scalar()
            
            # Verificar tabla de datos reales
            result = await db.execute(text("SELECT COUNT(*) FROM magdalena_real_data"))
            real_count = result.scalar()
            
            print(f"\n📊 VERIFICACIÓN EN BASE DE DATOS:")
            print(f"   - Configuraciones: {config_count}")
            print(f"   - Resultados: {resultado_count}")
            print(f"   - Datos reales: {real_count}")
            
    except Exception as e:
        print(f"\n⚠️ No se pudo verificar la base de datos: {str(e)}")

if __name__ == "__main__":
    print(f"🚀 Iniciando carga de datos - {datetime.now()}")
    print(f"="*60)
    asyncio.run(load_initial_data())