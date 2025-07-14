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
from app.services.camila_loader import CamilaLoader

def get_week_from_date(date_str):
    """Obtiene el número de semana ISO desde una fecha YYYY-MM-DD"""
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    return date_obj.isocalendar()[1]

def parse_turno_from_filename(filename):
    """Extrae el número de turno del nombre del archivo"""
    # Ejemplo: resultado_20220103_68_T01.xlsx -> 1
    match = re.search(r'_T(\d+)\.xlsx', filename)
    if match:
        return int(match.group(1))
    return None

async def load_camila_data():
    """Carga datos de Camila desde la estructura de directorios"""
    
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
            return
    
    # Usar las mismas rutas que el script de Magdalena
    resultados_camila_path = base_path / 'resultados_camila'
    instancias_camila_path = base_path / 'instancias_camila'
    resultados_magdalena_path = base_path / 'resultados_magdalena'
    instancias_magdalena_path = base_path / 'instancias_magdalena'
    
    print(f"🔍 Buscando datos de Camila en:")
    print(f"   - Resultados Camila: {resultados_camila_path}")
    print(f"   - Instancias Camila: {instancias_camila_path}")
    print(f"   - Resultados Magdalena: {resultados_magdalena_path}")
    print(f"   - Instancias Magdalena: {instancias_magdalena_path}")
    print(f"{'='*80}")
    
    # Contadores
    total_archivos = 0
    archivos_exitosos = 0
    archivos_fallidos = 0
    
    # Validar formato de fecha ISO
    def is_valid_iso_date(dirname):
        """Valida si el nombre del directorio es una fecha ISO válida (YYYY-MM-DD)"""
        if len(dirname) != 10:
            return False
        try:
            datetime.strptime(dirname, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    # Obtener directorios de resultados
    if not resultados_camila_path.exists():
        print(f"❌ No existe el directorio de resultados: {resultados_camila_path}")
        return
    
    # Buscar directorios con formato resultados_turno_YYYY-MM-DD
    all_dirs = [d for d in resultados_camila_path.iterdir() if d.is_dir()]
    turno_dirs = []
    
    for d in all_dirs:
        # Extraer fecha del nombre del directorio
        if d.name.startswith('resultados_turno_'):
            fecha_part = d.name.replace('resultados_turno_', '')
            if is_valid_iso_date(fecha_part):
                turno_dirs.append((d, fecha_part))
    
    # Ordenar por fecha
    turno_dirs = sorted(turno_dirs, key=lambda x: x[1])
    
    print(f"📅 Encontradas {len(turno_dirs)} fechas con resultados de Camila\n")
    
    for fecha_dir, fecha_str in turno_dirs:
        
        try:
            fecha_inicio = datetime.strptime(fecha_str, '%Y-%m-%d')
            semana = get_week_from_date(fecha_str)
            anio = fecha_inicio.year
            
            print(f"\n📁 Procesando {fecha_str} (Año {anio}, Semana {semana})")
            print(f"{'-'*60}")
            
            # Buscar archivos de resultado por turno en Camila
            # Los archivos están con formato resultados_YYYY-MM-DD_PP_T##.xlsx
            resultado_files = sorted(list(fecha_dir.glob('resultados_*_T*.xlsx')))
            
            # Buscar archivos de instancia en Camila
            instancia_dir = instancias_camila_path / fecha_str
            instancia_files = []
            
            if instancia_dir.exists():
                instancia_files = sorted(list(instancia_dir.glob('Instancia_*_T*.xlsx')))
            
            # Buscar archivos de Magdalena
            magdalena_resultado_dir = resultados_magdalena_path / fecha_str
            magdalena_instancia_dir = instancias_magdalena_path / fecha_str
            
            print(f"   Encontrados:")
            print(f"   - {len(resultado_files)} archivos de resultado Camila")
            print(f"   - {len(instancia_files)} archivos de instancia Camila")
            
            # Procesar cada turno
            for resultado_file in resultado_files:
                total_archivos += 1
                
                # Extraer información del archivo
                turno = parse_turno_from_filename(resultado_file.name)
                if turno is None:
                    print(f"   ⚠️ No se pudo extraer turno de: {resultado_file.name}")
                    continue
                
                # Extraer participación del nombre
                # Formato: resultado_20220103_68_T01.xlsx
                parts = resultado_file.stem.split('_')
                participacion = None
                
                for part in parts:
                    if part.isdigit() and 60 <= int(part) <= 80 and len(part) <= 3:
                        participacion = int(part)
                        break
                
                if participacion is None:
                    print(f"   ⚠️ No se pudo extraer participación de: {resultado_file.name}")
                    continue
                
                print(f"\n   📊 Procesando Turno {turno:02d} - P{participacion}")
                
                # Buscar instancia correspondiente de Camila
                instancia_file = None
                for inst in instancia_files:
                    # Buscar coincidencia por turno y participación
                    if (f"_T{turno:02d}" in inst.name or f"_T{turno}" in inst.name) and f"_{participacion}_" in inst.name:
                        instancia_file = inst
                        break
                
                # Buscar archivo de resultado de Magdalena correspondiente
                magdalena_file = None
                con_dispersion = None
                
                if magdalena_resultado_dir.exists():
                    # Buscar archivo de Magdalena con misma participación
                    # Intentar con K primero
                    magdalena_pattern_k = f"resultado_*_{participacion}_K.xlsx"
                    magdalena_files_k = list(magdalena_resultado_dir.glob(magdalena_pattern_k))
                    
                    # Luego con N
                    magdalena_pattern_n = f"resultado_*_{participacion}_N.xlsx"
                    magdalena_files_n = list(magdalena_resultado_dir.glob(magdalena_pattern_n))
                    
                    if magdalena_files_k:
                        magdalena_file = magdalena_files_k[0]
                        con_dispersion = True
                    elif magdalena_files_n:
                        magdalena_file = magdalena_files_n[0]
                        con_dispersion = False
                
                # Buscar instancia de Magdalena
                magdalena_instancia_file = None
                if magdalena_instancia_dir.exists() and con_dispersion is not None:
                    dispersion_char = 'K' if con_dispersion else 'N'
                    magdalena_inst_pattern = f"Instancia_*_{participacion}_{dispersion_char}.xlsx"
                    magdalena_inst_files = list(magdalena_instancia_dir.glob(magdalena_inst_pattern))
                    if magdalena_inst_files:
                        magdalena_instancia_file = magdalena_inst_files[0]
                
                print(f"      - Resultado Camila: {resultado_file.name}")
                print(f"      - Instancia Camila: {instancia_file.name if instancia_file else 'No encontrada'}")
                print(f"      - Resultado Magdalena: {magdalena_file.name if magdalena_file else 'No encontrado'}")
                print(f"      - Instancia Magdalena: {magdalena_instancia_file.name if magdalena_instancia_file else 'No encontrada'}")
                print(f"      - Dispersión: {'K' if con_dispersion else 'N' if con_dispersion is not None else 'No determinada'}")
                
                # Si no se pudo determinar la dispersión, asumir K por defecto
                if con_dispersion is None:
                    con_dispersion = True
                    print(f"      - Asumiendo dispersión K por defecto")
                
                try:
                    async with AsyncSessionLocal() as db:
                        # IMPORTANTE: Pasar la sesión de base de datos al constructor
                        loader = CamilaLoader(db)
                        
                        resultado_id = await loader.load_camila_results(
                            resultado_filepath=str(resultado_file),
                            instancia_filepath=str(instancia_file) if instancia_file else None,
                            magdalena_resultado_filepath=str(magdalena_file) if magdalena_file else None,
                            fecha_inicio=fecha_inicio,
                            semana=semana,
                            anio=anio,
                            turno=turno,
                            participacion=participacion,
                            con_dispersion=con_dispersion
                        )
                        
                        await db.commit()
                        print(f"   ✅ Cargado exitosamente (ID: {resultado_id})")
                        archivos_exitosos += 1
                        
                except Exception as e:
                    print(f"   ❌ Error: {str(e)}")
                    if os.environ.get("DEBUG"):
                        traceback.print_exc()
                    archivos_fallidos += 1
                    
        except Exception as e:
            print(f"⚠️ Error procesando {fecha_str}: {str(e)}")
            continue
    
    # Resumen final
    print(f"\n{'='*80}")
    print(f"✅ CARGA COMPLETA DE CAMILA - {datetime.now()}")
    print(f"{'='*80}")
    print(f"📊 RESUMEN FINAL:")
    print(f"   - Total archivos procesados: {total_archivos}")
    print(f"   - Exitosos: {archivos_exitosos}")
    print(f"   - Fallidos: {archivos_fallidos}")
    print(f"   - Tasa de éxito: {(archivos_exitosos/total_archivos*100):.1f}%" if total_archivos > 0 else "N/A")
    
    # Verificación en base de datos
    try:
        async with AsyncSessionLocal() as db:
            from sqlalchemy import text
            
            # Contar registros en tablas principales
            queries = {
                'resultados_camila': "SELECT COUNT(*) FROM resultados_camila WHERE estado = 'completado'",
                'asignaciones_gruas': "SELECT COUNT(*) FROM asignaciones_gruas",
                'cuotas_camiones': "SELECT COUNT(*) FROM cuotas_camiones",
                'metricas_gruas': "SELECT COUNT(*) FROM metricas_gruas",
                'comparaciones_camila': "SELECT COUNT(*) FROM comparaciones_camila"
            }
            
            print(f"\n📊 VERIFICACIÓN EN BASE DE DATOS:")
            for tabla, query in queries.items():
                result = await db.execute(text(query))
                count = result.scalar()
                print(f"   - {tabla}: {count:,}")
            
            # Estadísticas por año
            print(f"\n📅 RESULTADOS POR AÑO:")
            year_query = """
                SELECT anio, COUNT(*) as total, 
                       COUNT(DISTINCT semana) as semanas,
                       COUNT(DISTINCT turno) as turnos,
                       COUNT(DISTINCT participacion) as participaciones
                FROM resultados_camila 
                WHERE estado = 'completado'
                GROUP BY anio 
                ORDER BY anio
            """
            result = await db.execute(text(year_query))
            for row in result:
                print(f"   - {row.anio}: {row.total} resultados, {row.semanas} semanas, "
                      f"{row.turnos} turnos, {row.participaciones} participaciones")
                
    except Exception as e:
        print(f"\n⚠️ No se pudo verificar la base de datos: {str(e)}")

if __name__ == "__main__":
    print(f"🚀 Iniciando carga de datos de Camila - {datetime.now()}")
    print(f"="*80)
    asyncio.run(load_camila_data())