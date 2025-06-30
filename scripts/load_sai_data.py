#!/usr/bin/env python
# scripts/load_sai_data.py

import asyncio
import os
from pathlib import Path
import sys
import traceback
from datetime import datetime
import re

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.sai_flujos_loader import SAIFlujosLoader

async def load_sai_data():
    """Carga inicial de datos SAI"""
    
    # Usar la variable de entorno o la ruta por defecto
    base_path = os.environ.get('SAI_DATA_PATH', '/app/data/magdalena/2022/instancias_magdalena')
    data_path = Path(base_path)
    
    # Patrones de archivos - ajustados para el formato con guiones
    flujos_pattern = re.compile(r'.*Flujos_w(\d{4}-\d{2}-\d{2}).*\.xlsx')
    instancia_pattern = re.compile(r'.*Instancia_(\d{4}-\d{2}-\d{2})_(\d+).*\.xlsx')
    evolucion_pattern = re.compile(r'.*evolucion_turnos_w(\d{4}-\d{2}-\d{2}).*\.xlsx')
    
    print("🚀 Iniciando carga de datos SAI")
    print(f"📁 Buscando archivos en: {data_path}")
    
    # Verificar que el directorio existe
    if not data_path.exists():
        print(f"❌ Error: No existe el directorio {data_path}")
        return
    
    # Buscar archivos
    flujos_files = {}
    instancia_files = {}
    evolucion_files = {}
    
    # Buscar en todos los subdirectorios de fechas
    for date_dir in sorted(data_path.glob('2022-*')):
        if not date_dir.is_dir():
            continue
        
        print(f"🔍 Revisando directorio: {date_dir.name}")
        
        # Buscar archivos xlsx en cada subdirectorio
        for file in date_dir.glob('*.xlsx'):
            filename = file.name
            
            # Limpiar nombre si empieza con $
            clean_filename = filename.lstrip('$')
            
            # Verificar si es archivo de flujos
            flujos_match = flujos_pattern.match(clean_filename)
            if flujos_match:
                fecha_str = flujos_match.group(1)
                flujos_files[fecha_str] = file
                print(f"   ✓ Encontrado archivo de flujos: {filename}")
                continue
            
            # Verificar si es archivo de instancia
            instancia_match = instancia_pattern.match(clean_filename)
            if instancia_match:
                fecha_str = instancia_match.group(1)
                participacion = int(instancia_match.group(2))
                # Detectar si tiene _K basándose en el nombre
                con_dispersion = '_K' in filename
                
                key = f"{fecha_str}_{participacion}_{'K' if con_dispersion else 'C'}"
                instancia_files[key] = file
                print(f"   ✓ Encontrado archivo de instancia: {filename}")
                continue
            
            # Verificar si es archivo de evolución
            evolucion_match = evolucion_pattern.match(clean_filename)
            if evolucion_match:
                fecha_str = evolucion_match.group(1)
                evolucion_files[fecha_str] = file
                print(f"   ✓ Encontrado archivo de evolución: {filename}")
    
    print(f"\n📊 Archivos encontrados:")
    print(f"   - Flujos: {len(flujos_files)}")
    print(f"   - Instancias: {len(instancia_files)}")
    print(f"   - Evolución: {len(evolucion_files)}")
    
    # Mostrar primeros archivos encontrados para verificar
    if flujos_files:
        print("\n📋 Primeros archivos de flujos:")
        for fecha, file in list(flujos_files.items())[:3]:
            print(f"   - {fecha}: {file.name}")
    
    # Procesar cada conjunto de archivos
    processed_count = 0
    error_count = 0
    
    for fecha_str, flujos_file in flujos_files.items():
        try:
            # Parsear fecha con el formato correcto
            fecha = datetime.strptime(fecha_str, '%Y-%m-%d')
            semana = fecha.isocalendar()[1]
            
            print(f"\n🔄 Procesando fecha {fecha.date()} (Semana {semana})")
            
            # Buscar instancia correspondiente (por defecto 68_K)
            instancia_key = f"{fecha_str}_68_K"
            if instancia_key not in instancia_files:
                # Intentar sin K
                instancia_key = f"{fecha_str}_68_C"
                if instancia_key not in instancia_files:
                    # Buscar cualquier instancia con la misma fecha
                    for key in instancia_files:
                        if key.startswith(fecha_str):
                            instancia_key = key
                            break
                    else:
                        print(f"   ⚠️  No se encontró instancia para {fecha_str}")
                        continue
            
            instancia_file = instancia_files[instancia_key]
            evolucion_file = evolucion_files.get(fecha_str)
            
            # Determinar parámetros desde la key
            parts = instancia_key.split('_')
            participacion = int(parts[1]) if len(parts) > 1 else 68
            con_dispersion = parts[2] == 'K' if len(parts) > 2 else True
            
            print(f"   📄 Flujos: {flujos_file.name}")
            print(f"   📋 Instancia: {instancia_file.name}")
            if evolucion_file:
                print(f"   📈 Evolución: {evolucion_file.name}")
            
            # Cargar datos
            async with AsyncSessionLocal() as db:
                loader = SAIFlujosLoader(db)
                
                try:
                    # 1. Cargar instancia (segregaciones y capacidades)
                    print("   1️⃣  Cargando instancia...")
                    instancia_stats = await loader.load_instancia_file(str(instancia_file))
                    print(f"      ✅ Segregaciones: {instancia_stats['segregaciones']}")
                    print(f"      ✅ Capacidades: {instancia_stats['capacidades']}")
                    
                    # 2. Cargar flujos
                    print("   2️⃣  Cargando flujos...")
                    config_id = await loader.load_flujos_file(
                        str(flujos_file),
                        fecha,
                        semana,
                        participacion,
                        con_dispersion
                    )
                    print(f"      ✅ Config ID: {config_id}")
                    
                    # 3. Cargar evolución si existe
                    if evolucion_file:
                        print("   3️⃣  Cargando evolución...")
                        evol_stats = await loader.load_evolucion_file(
                            str(evolucion_file),
                            config_id
                        )
                        print(f"      ✅ Volumen bloques: {evol_stats['volumen_bloques']}")
                        print(f"      ✅ Volumen segregaciones: {evol_stats['volumen_segregaciones']}")
                    
                    print(f"   ✅ Carga completa para {fecha.date()}")
                    processed_count += 1
                    
                except Exception as e:
                    print(f"   ❌ Error: {str(e)}")
                    traceback.print_exc()
                    error_count += 1
                    try:
                        await db.rollback()
                    except:
                        pass
                    
        except Exception as e:
            print(f"❌ Error procesando {fecha_str}: {str(e)}")
            traceback.print_exc()
            error_count += 1
    
    print(f"\n✅ Proceso de carga completado")
    print(f"   - Procesados exitosamente: {processed_count}")
    print(f"   - Errores: {error_count}")
    
    # Retornar estadísticas para usar en main
    return processed_count, error_count

async def verify_loaded_data_simple():
    """Versión simplificada de verificación para evitar problemas de loop"""
    
    print("\n🔍 Verificando datos cargados...")
    
    try:
        # Crear nueva sesión independiente
        async with AsyncSessionLocal() as db:
            from sqlalchemy import select, func
            from app.models.sai_flujos import SAIConfiguration
            
            # Solo contar configuraciones
            config_count = await db.scalar(select(func.count(SAIConfiguration.id)))
            print(f"\n📊 Total de configuraciones cargadas: {config_count}")
            
            # Listar algunas configuraciones
            configs = await db.execute(
                select(SAIConfiguration)
                .order_by(SAIConfiguration.fecha)
                .limit(5)
            )
            
            print(f"\n📅 Primeras configuraciones:")
            for config in configs.scalars():
                print(f"   - Semana {config.semana}: {config.fecha.date()}")
                
    except Exception as e:
        print(f"⚠️  No se pudo verificar (pero los datos probablemente se cargaron): {str(e)}")

async def main():
    """Función principal que ejecuta todo en un solo loop"""
    # Verificar directorio
    base_path = os.environ.get('SAI_DATA_PATH', '/app/data/magdalena/2022/instancias_magdalena')
    data_path = Path(base_path)
    
    if not data_path.exists():
        print(f"❌ Error: No existe el directorio {data_path}")
        print("   Verifica que el volumen esté montado correctamente en Docker")
        print("   o que la variable SAI_DATA_PATH apunte al directorio correcto")
        return
    
    print(f"✅ Directorio de datos encontrado: {data_path}")
    
    # Ejecutar carga
    processed, errors = await load_sai_data()
    
    # Solo verificar si hubo éxito
    if processed > 0:
        await verify_loaded_data_simple()
            
            
            
if __name__ == "__main__":
    # Verificar que la ruta existe
    base_path = os.environ.get('SAI_DATA_PATH', '/app/data/magdalena/2022/instancias_magdalena')
    data_path = Path(base_path)
    
    if not data_path.exists():
        print(f"❌ Error: No existe el directorio {data_path}")
        print("   Verifica que el volumen esté montado correctamente en Docker")
        print("   o que la variable SAI_DATA_PATH apunte al directorio correcto")
        sys.exit(1)
    
    print(f"✅ Directorio de datos encontrado: {data_path}")
    
    # Ejecutar carga
    asyncio.run(load_sai_data())
    