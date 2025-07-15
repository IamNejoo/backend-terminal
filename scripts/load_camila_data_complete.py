# app/scripts/load_camila_data_complete.py

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
    # Ejemplo: resultados_20220103_68_T01.xlsx -> 1
    match = re.search(r'_T(\d+)\.xlsx', filename)
    if match:
        return int(match.group(1))
    return None

def get_flujos_filepath(base_path, fecha_str):
    """Construye la ruta al archivo de flujos reales para una fecha dada"""
    # Los flujos están en: instancias_magdalena/YYYY-MM-DD/Flujos_wYYYY-MM-DD.xlsx
    flujos_path = base_path / 'instancias_magdalena' / fecha_str / f'Flujos_w{fecha_str}.xlsx'
    if flujos_path.exists():
        return str(flujos_path)
    
    # Intentar sin guión en el nombre del archivo
    flujos_path_alt = base_path / 'instancias_magdalena' / fecha_str / f'Flujos_w{fecha_str.replace("-", "")}.xlsx'
    if flujos_path_alt.exists():
        return str(flujos_path_alt)
    
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
    
    # Definir rutas
    resultados_camila_path = base_path / 'resultados_camila'
    instancias_camila_path = base_path / 'instancias_camila'
    instancias_magdalena_path = base_path / 'instancias_magdalena'
    
    print(f"🔍 Buscando datos de Camila en:")
    print(f"   - Resultados Camila: {resultados_camila_path}")
    print(f"   - Instancias Camila: {instancias_camila_path}")
    print(f"   - Instancias Magdalena (flujos): {instancias_magdalena_path}")
    print(f"{'='*80}")
    
    # Verificar existencia de directorios
    if not resultados_camila_path.exists():
        print(f"❌ No existe el directorio de resultados: {resultados_camila_path}")
        return
    
    if not instancias_camila_path.exists():
        print(f"⚠️  No existe el directorio de instancias Camila: {instancias_camila_path}")
    
    if not instancias_magdalena_path.exists():
        print(f"⚠️  No existe el directorio de instancias Magdalena: {instancias_magdalena_path}")
    
    # Listar contenido de resultados_camila para debug
    print(f"\n📂 Contenido de {resultados_camila_path}:")
    try:
        items = list(resultados_camila_path.iterdir())
        for item in items[:10]:  # Mostrar primeros 10 items
            print(f"   - {item.name} {'[DIR]' if item.is_dir() else ''}")
        if len(items) > 10:
            print(f"   ... y {len(items) - 10} más")
    except Exception as e:
        print(f"   ❌ Error listando directorio: {e}")
    
    # Contadores
    total_archivos = 0
    archivos_exitosos = 0
    archivos_fallidos = 0
    archivos_sin_flujos = 0
    
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
    
    # Buscar directorios con formato resultados_turno_YYYY-MM-DD
    all_dirs = [d for d in resultados_camila_path.iterdir() if d.is_dir()]
    turno_dirs = []
    
    for d in all_dirs:
        # Extraer fecha del nombre del directorio
        if d.name.startswith('resultados_turno_'):
            fecha_part = d.name.replace('resultados_turno_', '')
            if is_valid_iso_date(fecha_part):
                turno_dirs.append((d, fecha_part))
        # También buscar directorios que sean solo fechas
        elif is_valid_iso_date(d.name):
            turno_dirs.append((d, d.name))
    
    # Ordenar por fecha
    turno_dirs = sorted(turno_dirs, key=lambda x: x[1])
    
    print(f"\n📅 Encontradas {len(turno_dirs)} fechas con resultados de Camila\n")
    
    if len(turno_dirs) == 0:
        print("⚠️  No se encontraron directorios con formato de fecha válido")
        print("    Esperado: resultados_turno_YYYY-MM-DD o YYYY-MM-DD")
        return
    
    for fecha_dir, fecha_str in turno_dirs:
        
        try:
            fecha_inicio = datetime.strptime(fecha_str, '%Y-%m-%d')
            semana = get_week_from_date(fecha_str)
            anio = fecha_inicio.year
            
            print(f"\n📁 Procesando {fecha_str} (Año {anio}, Semana {semana})")
            print(f"{'-'*60}")
            
            # Buscar archivo de flujos reales para esta semana
            flujos_real_filepath = get_flujos_filepath(base_path, fecha_str)
            if flujos_real_filepath:
                print(f"   ✓ Archivo de flujos reales encontrado: {Path(flujos_real_filepath).name}")
            else:
                print(f"   ⚠️ No se encontró archivo de flujos reales para {fecha_str}")
                archivos_sin_flujos += 1
            
            # Buscar archivos de resultado por turno en Camila
            resultado_files = sorted(list(fecha_dir.glob('resultados_*_T*.xlsx')))
            
            if len(resultado_files) == 0:
                # Intentar con otro patrón
                resultado_files = sorted(list(fecha_dir.glob('resultado_*_T*.xlsx')))
            
            # Buscar archivos de instancia en Camila
# Buscar archivos de instancia en Camila
            # Primero intentar con el formato instancias_turno_YYYY-MM-DD
            instancia_dir = instancias_camila_path / f"instancias_turno_{fecha_str}"
            instancia_files = []
            
            if instancia_dir.exists():
                instancia_files = sorted(list(instancia_dir.glob('Instancia_*_T*.xlsx')))
            else:
                # Si no existe, intentar con el formato directo YYYY-MM-DD
                instancia_dir = instancias_camila_path / fecha_str
                if instancia_dir.exists():
                    instancia_files = sorted(list(instancia_dir.glob('Instancia_*_T*.xlsx')))
            
            print(f"   Encontrados:")
            print(f"   - {len(resultado_files)} archivos de resultado Camila")
            print(f"   - {len(instancia_files)} archivos de instancia Camila en {instancia_dir.name}")
            
            if len(resultado_files) == 0:
                print(f"   ⚠️ No se encontraron archivos de resultado en {fecha_dir}")
                continue
            
            # Procesar cada turno
            turnos_procesados = set()
            
            for resultado_file in resultado_files:
                total_archivos += 1
                
                # Extraer información del archivo
                turno = parse_turno_from_filename(resultado_file.name)
                if turno is None:
                    print(f"   ⚠️ No se pudo extraer turno de: {resultado_file.name}")
                    archivos_fallidos += 1
                    continue
                
                # Evitar procesar el mismo turno múltiples veces
                if turno in turnos_procesados:
                    continue
                turnos_procesados.add(turno)
                
                # Extraer participación del nombre
                # Formato: resultados_20220103_68_T01.xlsx
                parts = resultado_file.stem.split('_')
                participacion = None
                
                for part in parts:
                    if part.isdigit() and 60 <= int(part) <= 80 and len(part) <= 3:
                        participacion = int(part)
                        break
                
                if participacion is None:
                    print(f"   ⚠️ No se pudo extraer participación de: {resultado_file.name}")
                    archivos_fallidos += 1
                    continue
                
                # Calcular hora del turno para logging
                turno_del_dia = ((turno - 1) % 3) + 1
                hora_inicio = {1: "08:00", 2: "16:00", 3: "00:00"}[turno_del_dia]
                
                print(f"\n   📊 Procesando Turno {turno:02d} - P{participacion} (Hora: {hora_inicio})")
                
                # Buscar instancia correspondiente de Camila
                instancia_file = None
                for inst in instancia_files:
                    # Buscar coincidencia por turno y participación
                    if (f"_T{turno:02d}" in inst.name or f"_T{turno}" in inst.name) and f"_{participacion}_" in inst.name:
                        instancia_file = inst
                        break
                
                # Determinar si es con dispersión (K) o sin dispersión (N)
                # Por defecto asumimos K si no se puede determinar
                con_dispersion = True
                if '_N_' in resultado_file.name or (instancia_file and '_N_' in instancia_file.name):
                    con_dispersion = False
                elif '_K_' in resultado_file.name or (instancia_file and '_K_' in instancia_file.name):
                    con_dispersion = True
                
                print(f"      - Resultado Camila: {resultado_file.name}")
                print(f"      - Instancia Camila: {instancia_file.name if instancia_file else 'No encontrada'}")
                print(f"      - Flujos reales: {Path(flujos_real_filepath).name if flujos_real_filepath else 'No disponible'}")
                print(f"      - Dispersión: {'K' if con_dispersion else 'N'}")
                
                try:
                    async with AsyncSessionLocal() as db:
                        # Crear el loader con la sesión de base de datos
                        loader = CamilaLoader(db)
                        
                        # Cargar resultados de Camila con comparación contra datos reales
                        resultado_id = await loader.load_camila_results(
                            resultado_filepath=str(resultado_file),
                            instancia_filepath=str(instancia_file) if instancia_file else None,
                            flujos_real_filepath=flujos_real_filepath,  # Ahora incluimos los flujos reales
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
    print(f"   - Sin flujos reales: {archivos_sin_flujos}")
    print(f"   - Tasa de éxito: {(archivos_exitosos/total_archivos*100):.1f}%" if total_archivos > 0 else "N/A")
    
    # Verificación en base de datos
    print(f"\n📊 VERIFICACIÓN EN BASE DE DATOS:")
    try:
        async with AsyncSessionLocal() as db:
            from sqlalchemy import text
            
            # Contar registros en tablas principales
            queries = {
                'resultados_camila (TOTAL)': "SELECT COUNT(*) FROM resultados_camila",
                'resultados_camila (COMPLETADO)': "SELECT COUNT(*) FROM resultados_camila WHERE estado = 'COMPLETADO'",
                'asignaciones_gruas': "SELECT COUNT(*) FROM asignaciones_gruas",
                'cuotas_camiones': "SELECT COUNT(*) FROM cuotas_camiones",
                'metricas_gruas': "SELECT COUNT(*) FROM metricas_gruas",
                'comparaciones_real': "SELECT COUNT(*) FROM comparaciones_real",
                'flujos_modelo': "SELECT COUNT(*) FROM flujos_modelo"
            }
            
            for tabla, query in queries.items():
                try:
                    result = await db.execute(text(query))
                    count = result.scalar()
                    print(f"   - {tabla}: {count:,}")
                except Exception as e:
                    print(f"   - {tabla}: Error - {str(e)}")
            
            # Estadísticas por año
            print(f"\n📅 RESULTADOS POR AÑO:")
            try:
                year_query = """
                    SELECT anio, COUNT(*) as total, 
                           COUNT(DISTINCT semana) as semanas,
                           COUNT(DISTINCT turno) as turnos,
                           COUNT(DISTINCT participacion) as participaciones,
                           AVG(accuracy_global) as accuracy_promedio
                    FROM resultados_camila 
                    WHERE estado = 'COMPLETADO'
                    GROUP BY anio 
                    ORDER BY anio
                """
                result = await db.execute(text(year_query))
                rows = result.fetchall()
                if rows:
                    for row in rows:
                        accuracy_str = f"{row.accuracy_promedio:.1f}%" if row.accuracy_promedio else "N/A"
                        print(f"   - {row.anio}: {row.total} resultados, {row.semanas} semanas, "
                              f"{row.turnos} turnos únicos, {row.participaciones} participaciones, "
                              f"Accuracy promedio: {accuracy_str}")
                else:
                    print("   No hay datos completados por año")
            except Exception as e:
                print(f"   Error obteniendo estadísticas por año: {e}")
            
            # Comparaciones con datos reales
            print(f"\n📊 COMPARACIONES CON DATOS REALES:")
            try:
                comp_query = """
                    SELECT tipo_comparacion, 
                           COUNT(*) as total,
                           AVG(accuracy) as accuracy_promedio,
                           AVG(diferencia_porcentual) as diferencia_promedio
                    FROM comparaciones_real
                    GROUP BY tipo_comparacion
                """
                result = await db.execute(text(comp_query))
                rows = result.fetchall()
                if rows:
                    for row in rows:
                        print(f"   - {row.tipo_comparacion}: {row.total} comparaciones, "
                              f"Accuracy: {row.accuracy_promedio:.1f}%, "
                              f"Diferencia: {row.diferencia_promedio:+.1f}%")
                else:
                    print("   No hay datos de comparaciones")
            except Exception as e:
                print(f"   Error obteniendo comparaciones: {e}")
                
    except Exception as e:
        print(f"\n⚠️ Error en verificación de base de datos: {str(e)}")

if __name__ == "__main__":
    print(f"🚀 Iniciando carga de datos de Camila con comparación real - {datetime.now()}")
    print(f"="*80)
    print("NOTA: Este script ahora carga automáticamente los flujos reales")
    print("      desde instancias_magdalena/YYYY-MM-DD/Flujos_wYYYY-MM-DD.xlsx")
    print(f"="*80)
    asyncio.run(load_camila_data())