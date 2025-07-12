# scripts/load_camila_data_complete.py
import asyncio
import os
from pathlib import Path
import sys
import traceback
from datetime import datetime
import re
import pandas as pd

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import get_db
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.core.config import get_settings
from app.services.camila_service import CamilaService
from app.services.camila_loader import CamilaLoader
from app.schemas.camila import InstanciaCamilaCreate
from sqlalchemy import text

# Crear el AsyncSessionLocal
settings = get_settings()
engine = create_async_engine(settings.DATABASE_URL)
AsyncSessionLocal = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)

def parse_camila_filename(filename):
    """
    Parsea nombre de archivo de Camila
    Formato: Instancia_YYYYMMDD_PP_T##.xlsx o Resultado_YYYYMMDD_PP_T##.xlsx
    """
    pattern = r'(Instancia|Resultado)_(\d{8})_(\d{2,3})_T(\d{1,2})\.xlsx'
    match = re.match(pattern, filename)
    
    if match:
        tipo = match.group(1)
        fecha_str = match.group(2)
        participacion = int(match.group(3))
        turno = int(match.group(4))
        
        fecha = datetime.strptime(fecha_str, '%Y%m%d')
        
        return {
            'tipo': tipo,
            'fecha': fecha.date(),
            'fecha_str': fecha_str,
            'fecha_magdalena': fecha.strftime('%Y-%m-%d'),
            'anio': fecha.year,
            'semana': fecha.isocalendar()[1],
            'participacion': participacion,
            'turno': turno
        }
    return None

def find_matching_files(camila_instance_path, camila_results_path, magdalena_instance_path, info):
    """
    Encuentra los 3 archivos necesarios para un turno
    """
    files = {
        'camila_instance': None,
        'camila_result': None,
        'magdalena_instance': None
    }
    
    # 1. Archivo de instancia Camila
    pattern = f"Instancia_{info['fecha_str']}_{info['participacion']}_T{info['turno']}.xlsx"
    file_path = camila_instance_path / pattern
    if file_path.exists():
        files['camila_instance'] = file_path
    else:
        # Probar con formato T##
        pattern = f"Instancia_{info['fecha_str']}_{info['participacion']}_T{info['turno']:02d}.xlsx"
        file_path = camila_instance_path / pattern
        if file_path.exists():
            files['camila_instance'] = file_path
    
    # 2. Archivo de resultado Camila
    pattern = f"Resultado_{info['fecha_str']}_{info['participacion']}_T{info['turno']}.xlsx"
    file_path = camila_results_path / pattern
    if file_path.exists():
        files['camila_result'] = file_path
    else:
        # Probar con formato T##
        pattern = f"Resultado_{info['fecha_str']}_{info['participacion']}_T{info['turno']:02d}.xlsx"
        file_path = camila_results_path / pattern
        if file_path.exists():
            files['camila_result'] = file_path
    
    # 3. Archivo de instancia Magdalena (puede estar con K o N)
    for dispersion in ['K', 'N']:
        pattern = f"Instancia_{info['fecha_magdalena']}_{info['participacion']}_{dispersion}.xlsx"
        # Buscar en subdirectorio por fecha
        file_path = magdalena_instance_path / info['fecha_magdalena'] / pattern
        if file_path.exists():
            files['magdalena_instance'] = file_path
            break
        # Buscar en raíz
        file_path = magdalena_instance_path / pattern
        if file_path.exists():
            files['magdalena_instance'] = file_path
            break
    
    return files

async def process_camila_instance(db, service, loader, files, info):
    """
    Procesa una instancia de Camila con los 3 archivos
    """
    try:
        print(f"\n📊 Procesando: {info['fecha']} P{info['participacion']} T{info['turno']:02d}")
        
        # Verificar archivos
        if not files['camila_instance']:
            print(f"   ❌ Falta archivo de instancia Camila")
            return False
        if not files['camila_result']:
            print(f"   ❌ Falta archivo de resultado Camila")
            return False
        if not files['magdalena_instance']:
            print(f"   ❌ Falta archivo de instancia Magdalena")
            return False
        
        print(f"   📁 Archivos encontrados:")
        print(f"      - Instancia Camila: {files['camila_instance'].name}")
        print(f"      - Resultado Camila: {files['camila_result'].name}")
        print(f"      - Instancia Magdalena: {files['magdalena_instance'].name}")
        
        # 1. Leer archivo de instancia de Camila
        print(f"   📖 Leyendo instancia Camila...")
        instance_data = loader.read_instance_file(str(files['camila_instance']))
        
        # 2. Leer archivo de resultado de Camila
        print(f"   📖 Leyendo resultados Camila...")
        results_data = loader.read_results_file(str(files['camila_result']))
        
        # 3. Leer demandas por hora desde Magdalena
        print(f"   📖 Leyendo demandas por hora de Magdalena...")
        magdalena_data = loader.read_magdalena_files(
            str(files['magdalena_instance']),
            '',  # No necesitamos resultado de Magdalena
            info['turno']
        )
        
        # 4. Combinar datos: usar instancia de Camila pero reemplazar demandas con las de Magdalena
        if 'demandas_hora' in magdalena_data:
            print(f"   🔄 Reemplazando demandas con datos horarios de Magdalena...")
            
            # Convertir formato de demandas de Magdalena al formato de Camila
            instance_data['demanda_carga'] = {}
            instance_data['demanda_descarga'] = {}
            instance_data['demanda_recepcion'] = {}
            instance_data['demanda_entrega'] = {}
            
            for seg, horas in magdalena_data['demandas_hora'].get('carga', {}).items():
                instance_data['demanda_carga'][seg] = {i+1: val for i, val in enumerate(horas)}
            
            for seg, horas in magdalena_data['demandas_hora'].get('descarga', {}).items():
                instance_data['demanda_descarga'][seg] = {i+1: val for i, val in enumerate(horas)}
            
            for seg, horas in magdalena_data['demandas_hora'].get('recepcion', {}).items():
                if seg not in instance_data['demanda_recepcion']:
                    instance_data['demanda_recepcion'] = {}
                instance_data['demanda_recepcion'][seg] = {i+1: val for i, val in enumerate(horas)}
            
            for seg, horas in magdalena_data['demandas_hora'].get('entrega', {}).items():
                if seg not in instance_data['demanda_entrega']:
                    instance_data['demanda_entrega'] = {}
                instance_data['demanda_entrega'][seg] = {i+1: val for i, val in enumerate(horas)}
        
        # 5. Crear instancia
        instance_create = InstanciaCamilaCreate(
            anio=info['anio'],
            semana=info['semana'],
            fecha=info['fecha'],
            turno=info['turno'],
            participacion=info['participacion']
        )
        
        # 6. Procesar todo
        print(f"   💾 Guardando en base de datos...")
        instancia = await service.process_instance(
            db=db,
            instance_data=instance_data,
            results_data=results_data,
            instance_create=instance_create
        )
        
        # 7. Guardar referencia a demandas de Magdalena
        if 'demandas_hora' in magdalena_data:
            await service._save_magdalena_hourly_demands(
                db=db,
                instancia_id=instancia.id,
                magdalena_instance_id=0,  # No tenemos el ID real
                demandas_hora=magdalena_data['demandas_hora']
            )
        
        await db.commit()
        print(f"   ✅ Cargado exitosamente (ID: {instancia.id})")
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        if os.environ.get("DEBUG"):
            traceback.print_exc()
        await db.rollback()
        return False

async def load_camila_complete():
    """
    Carga completa de Camila usando los 3 archivos
    """
    # Configurar rutas
    base_path = Path(os.environ.get('DATA_PATH', '/app/data'))
    
    # Rutas de Camila
    camila_path = base_path / 'camila'
    camila_instance_path = camila_path / 'instancias'
    camila_results_path = camila_path / 'resultados'
    
    # Rutas de Magdalena
    magdalena_path = base_path / 'magdalena'
    magdalena_instance_path = magdalena_path / 'instancias'
    
    print(f"📁 Rutas configuradas:")
    print(f"   - Instancias Camila: {camila_instance_path}")
    print(f"   - Resultados Camila: {camila_results_path}")
    print(f"   - Instancias Magdalena: {magdalena_instance_path}")
    print(f"{'='*80}")
    
    # Verificar que existan las rutas
    for path, nombre in [
        (camila_instance_path, "Instancias Camila"),
        (camila_results_path, "Resultados Camila"),
        (magdalena_instance_path, "Instancias Magdalena")
    ]:
        if not path.exists():
            print(f"❌ No existe el directorio de {nombre}: {path}")
            return
    
    # Buscar archivos de Camila
    camila_files = list(camila_instance_path.glob('Instancia_*.xlsx'))
    print(f"\n📊 Encontrados {len(camila_files)} archivos de instancia Camila")
    
    # Agrupar por fecha-participación-turno
    instances_to_process = {}
    
    for file in camila_files:
        info = parse_camila_filename(file.name)
        if info:
            key = (info['fecha'], info['participacion'], info['turno'])
            instances_to_process[key] = info
    
    print(f"📊 {len(instances_to_process)} instancias únicas para procesar")
    
    # Procesar cada instancia
    exitosas = 0
    fallidas = 0
    
    async with AsyncSessionLocal() as db:
        service = CamilaService()
        loader = CamilaLoader()
        
        # Ordenar por fecha, participación, turno
        for key in sorted(instances_to_process.keys()):
            info = instances_to_process[key]
            
            # Buscar los 3 archivos
            files = find_matching_files(
                camila_instance_path,
                camila_results_path,
                magdalena_instance_path,
                info
            )
            
            # Procesar
            success = await process_camila_instance(db, service, loader, files, info)
            
            if success:
                exitosas += 1
            else:
                fallidas += 1
    
    # Resumen
    print(f"\n{'='*80}")
    print(f"✅ CARGA COMPLETA - {datetime.now()}")
    print(f"{'='*80}")
    print(f"📊 RESUMEN:")
    print(f"   - Instancias procesadas: {exitosas + fallidas}")
    print(f"   - Exitosas: {exitosas}")
    print(f"   - Fallidas: {fallidas}")
    print(f"   - Tasa de éxito: {(exitosas/(exitosas+fallidas)*100):.1f}%" if (exitosas+fallidas) > 0 else "N/A")

async def verify_database():
    """Verifica los datos cargados"""
    async with AsyncSessionLocal() as db:
        print(f"\n📊 VERIFICACIÓN EN BASE DE DATOS:")
        
        # Consultas básicas
        queries = {
            'Total instancias': "SELECT COUNT(*) FROM instancia_camila",
            'Instancias completadas': "SELECT COUNT(*) FROM instancia_camila WHERE estado = 'completado'",
            'Con demandas Magdalena': "SELECT COUNT(DISTINCT instancia_id) FROM demanda_hora_magdalena",
            'Total métricas': "SELECT COUNT(*) FROM metrica_resultado",
            'Total flujos': "SELECT COUNT(*) FROM flujo_operacional WHERE flujo_carga + flujo_descarga + flujo_recepcion + flujo_entrega > 0"
        }
        
        for nombre, query in queries.items():
            result = await db.execute(text(query))
            count = result.scalar()
            print(f"   - {nombre}: {count:,}")
        
        # Estadísticas detalladas
        print(f"\n📈 ESTADÍSTICAS POR AÑO-SEMANA:")
        stats_query = """
            SELECT 
                anio,
                semana,
                COUNT(*) as instancias,
                COUNT(DISTINCT turno) as turnos,
                COUNT(DISTINCT participacion) as participaciones,
                AVG(CAST(m.valor_funcion_objetivo AS FLOAT)) as fo_promedio
            FROM instancia_camila i
            LEFT JOIN metrica_resultado m ON i.id = m.instancia_id
            WHERE i.estado = 'completado'
            GROUP BY anio, semana
            ORDER BY anio DESC, semana DESC
            LIMIT 10
        """
        result = await db.execute(text(stats_query))
        for row in result:
            print(f"   {row.anio}-S{row.semana:02d}: "
                  f"{row.instancias} inst, {row.turnos} turnos, "
                  f"{row.participaciones} part, FO={row.fo_promedio:.2f}" 
                  if row.fo_promedio else "")

async def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Cargar datos de Camila usando los 3 archivos (Instancia Camila, Resultado Camila, Instancia Magdalena)'
    )
    parser.add_argument('--debug', action='store_true', help='Mostrar errores detallados')
    parser.add_argument('--verify-only', action='store_true', help='Solo verificar BD sin cargar')
    parser.add_argument('--data-path', help='Ruta base de los datos', default='/app/data')
    
    args = parser.parse_args()
    
    if args.debug:
        os.environ['DEBUG'] = '1'
    
    if args.data_path:
        os.environ['DATA_PATH'] = args.data_path
    
    print(f"🚀 CARGA DE DATOS DE CAMILA (3 ARCHIVOS) - {datetime.now()}")
    print(f"{'='*80}")
    
    if args.verify_only:
        await verify_database()
    else:
        await load_camila_complete()
        await verify_database()

if __name__ == "__main__":
    asyncio.run(main())