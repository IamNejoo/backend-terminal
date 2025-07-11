# scripts/load_magdalena_data_modified.py
import asyncio
import os
from pathlib import Path
import sys
import traceback
from datetime import datetime

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.services.optimization_loader import OptimizationLoader

def get_week_from_date(date_str):
    """Obtiene el número de semana ISO desde una fecha YYYY-MM-DD"""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.isocalendar()[1]

async def load_optimization_data():
    """Carga datos de optimización desde la estructura de directorios actual"""
    import os
    
    # Usar variable de entorno específica para datos de optimización
    optimization_path = os.environ.get('OPTIMIZATION_DATA_PATH', '/app/optimization_data')
    base_path = Path(optimization_path)
    
    # Fallback a ruta local si no existe en Docker
    if not base_path.exists():
        local_path = Path('/home/nejoo/gurobi/resultados_generados')
        if local_path.exists():
            base_path = local_path
        else:
            print(f"❌ No se encontró la ruta de datos en: {optimization_path}")
            print(f"   Tampoco en ruta local: {local_path}")
            return
        
    resultados_path = base_path / 'resultados_magdalena'
    instancias_path = base_path / 'instancias_magdalena'
    
    print(f"🔍 Buscando datos en:")
    print(f"   - Resultados: {resultados_path}")
    print(f"   - Instancias: {instancias_path}")
    print(f"{'='*80}")
    
    # Contadores
    total_instancias = 0
    instancias_exitosas = 0
    instancias_fallidas = 0
    
    # Obtener todas las fechas disponibles en resultados
    if not resultados_path.exists():
        print(f"❌ No existe el directorio de resultados: {resultados_path}")
        return
    
    # Función para validar formato de fecha ISO
    def is_valid_iso_date(dirname):
        """Valida si el nombre del directorio es una fecha ISO válida (YYYY-MM-DD)"""
        if len(dirname) != 10:
            return False
        try:
            datetime.strptime(dirname, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    # Obtener solo directorios con fechas ISO válidas
    all_dirs = [d for d in resultados_path.iterdir() if d.is_dir()]
    fechas_dirs = sorted([d for d in all_dirs if is_valid_iso_date(d.name)])
    
    # Mostrar directorios ignorados si los hay
    ignored_dirs = [d.name for d in all_dirs if not is_valid_iso_date(d.name)]
    if ignored_dirs:
        print(f"⚠️  Directorios ignorados (no son fechas ISO): {', '.join(ignored_dirs)}")
    
    print(f"📅 Encontradas {len(fechas_dirs)} fechas con resultados válidas\n")
    
    for fecha_dir in fechas_dirs:
        fecha_str = fecha_dir.name
        
        try:
            fecha_inicio = datetime.strptime(fecha_str, '%Y-%m-%d')
            semana = get_week_from_date(fecha_str)
            anio = fecha_inicio.year
            
            print(f"\n📁 Procesando {fecha_str} (Año {anio}, Semana {semana})")
            print(f"{'-'*60}")
            
            # Buscar archivos de resultado en resultados_magdalena
            resultado_files = list(fecha_dir.glob('resultado_*.xlsx'))
            distancia_files = list(fecha_dir.glob('Distancias_*.xlsx'))
            
            # Buscar archivos de instancia en instancias_magdalena
            instancia_dir = instancias_path / fecha_str
            flujos_files = []
            instancia_files = []
            
            if instancia_dir.exists():
                flujos_files = list(instancia_dir.glob('Flujos_*.xlsx'))
                instancia_files = list(instancia_dir.glob('Instancia_*.xlsx'))
                
            print(f"   Encontrados:")
            print(f"   - {len(resultado_files)} archivos de resultado")
            print(f"   - {len(distancia_files)} archivos de distancia")
            print(f"   - {len(instancia_files)} archivos de instancia")
            print(f"   - {len(flujos_files)} archivos de flujos")
            
            # Procesar cada archivo de resultado
            for resultado_file in resultado_files:
                total_instancias += 1
                
                # Extraer participación y dispersión del nombre
                # Ejemplo: resultado_2022-01-03_68_K.xlsx
                parts = resultado_file.stem.split('_')
                participacion = None
                con_dispersion = None
                
                # Buscar el número de participación (después de la fecha)
                for i, part in enumerate(parts):
                    if part.isdigit() and 60 <= int(part) <= 80:
                        participacion = int(part)
                        # Verificar si hay K o N después
                        if i + 1 < len(parts):
                            if parts[i + 1] == 'K':
                                con_dispersion = True
                            elif parts[i + 1] == 'N':
                                con_dispersion = False
                        break
                
                if participacion is None:
                    print(f"   ⚠️ No se pudo extraer participación de: {resultado_file.name}")
                    continue
                
                dispersion_str = 'K' if con_dispersion else 'N' if con_dispersion is not None else '?'
                print(f"\n   📊 Procesando P{participacion}_{dispersion_str}")
                
                # Buscar archivos relacionados
                flujos_file = flujos_files[0] if flujos_files else None
                distancia_file = None
                instancia_file = None
                
                # Buscar archivo de distancia específico
                for dist in distancia_files:
                    if f"_{participacion}" in dist.name:
                        distancia_file = dist
                        break
                
                # Buscar instancia específica
                for inst in instancia_files:
                    # Buscar por participación y dispersión
                    if f"_{participacion}_" in inst.name:
                        if con_dispersion is not None:
                            if f"_{participacion}_{'K' if con_dispersion else 'N'}" in inst.name:
                                instancia_file = inst
                                break
                        else:
                            instancia_file = inst
                            break
                    # Si no hay dispersión en el nombre, buscar solo por participación
                    elif f"_{participacion}.xlsx" in inst.name:
                        instancia_file = inst
                        break
                
                print(f"      - Resultado: {resultado_file.name}")
                print(f"      - Instancia: {instancia_file.name if instancia_file else 'No encontrada'}")
                print(f"      - Flujos: {flujos_file.name if flujos_file else 'No encontrado'}")
                print(f"      - Distancias: {distancia_file.name if distancia_file else 'No encontrado'}")
                
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
                        print(f"   ✅ Cargado exitosamente (ID: {instancia_id})")
                        instancias_exitosas += 1
                        
                except Exception as e:
                    print(f"   ❌ Error: {str(e)}")
                    if "DEBUG" in os.environ:
                        traceback.print_exc()
                    instancias_fallidas += 1
                    
        except Exception as e:
            print(f"⚠️ Error procesando {fecha_str}: {str(e)}")
            continue
    
    # Resumen final
    print(f"\n{'='*80}")
    print(f"✅ CARGA COMPLETA - {datetime.now()}")
    print(f"{'='*80}")
    print(f"📊 RESUMEN FINAL:")
    print(f"   - Total instancias procesadas: {total_instancias}")
    print(f"   - Exitosas: {instancias_exitosas}")
    print(f"   - Fallidas: {instancias_fallidas}")
    print(f"   - Tasa de éxito: {(instancias_exitosas/total_instancias*100):.1f}%" if total_instancias > 0 else "N/A")
    
    # Verificación en base de datos
    try:
        async with AsyncSessionLocal() as db:
            from sqlalchemy import text
            
            # Contar registros en tablas principales
            queries = {
                'instancias': "SELECT COUNT(*) FROM instancias WHERE estado = 'completado'",
                'movimientos_reales': "SELECT COUNT(*) FROM movimientos_reales",
                'movimientos_modelo': "SELECT COUNT(*) FROM movimientos_modelo",
                'resultados_generales': "SELECT COUNT(*) FROM resultados_generales",
                'kpis_comparativos': "SELECT COUNT(*) FROM kpis_comparativos"
            }
            
            print(f"\n📊 VERIFICACIÓN EN BASE DE DATOS:")
            for tabla, query in queries.items():
                result = await db.execute(text(query))
                count = result.scalar()
                print(f"   - {tabla}: {count:,}")
            
            # Estadísticas por año
            print(f"\n📅 INSTANCIAS POR AÑO:")
            year_query = """
                SELECT anio, COUNT(*) as total, 
                       COUNT(DISTINCT semana) as semanas,
                       COUNT(DISTINCT participacion) as participaciones
                FROM instancias 
                WHERE estado = 'completado'
                GROUP BY anio 
                ORDER BY anio
            """
            result = await db.execute(text(year_query))
            for row in result:
                print(f"   - {row.anio}: {row.total} instancias, {row.semanas} semanas, {row.participaciones} participaciones")
                
    except Exception as e:
        print(f"\n⚠️ No se pudo verificar la base de datos: {str(e)}")

if __name__ == "__main__":
    print(f"🚀 Iniciando carga de datos de optimización - {datetime.now()}")
    print(f"="*80)
    asyncio.run(load_optimization_data())