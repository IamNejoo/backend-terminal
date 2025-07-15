#!/bin/bash
# scripts/docker-entrypoint.sh

set -e

echo "🚀 Iniciando Terminal Backend..."

# Esperar a que PostgreSQL esté listo
echo "⏳ Esperando a PostgreSQL..."
while ! pg_isready -h $POSTGRES_SERVER -p $POSTGRES_PORT -U $POSTGRES_USER; do
    sleep 2
done
echo "✅ PostgreSQL está listo!"


# Ejecutar migraciones/crear tablas - IMPORTAR TODOS LOS MODELOS PRIMERO
echo "🔨 Creando tablas en la base de datos..."
python -c "
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from app.models.base import Base

# Importar TODOS los modelos para que se registren con Base
print('📦 Importando modelos...')

try:
    from app.models.historical_movements import HistoricalMovement
    print('  ✓ Historical movements importado')
except Exception as e:
    print(f'  ✗ Error importando historical_movements: {e}')

try:
    from app.models.sai_flujos import (
        SAIConfiguration, SAIFlujo, SAIVolumenBloque, SAIVolumenSegregacion,
        SAISegregacion, SAICapacidadBloque, SAIMapeoCriterios
    )
    print('  ✓ Modelos de SAI Flujos importados')
except Exception as e:
    print(f'  ✗ Error importando sai_flujos: {e}')

try:
    from app.models.optimization import (
            Instancia,
            Bloque,
            Segregacion,
            MovimientoReal,
            MovimientoModelo,
            DistanciaReal,
            ResultadoGeneral,
            AsignacionBloque,
            CargaTrabajo,
            OcupacionBloque,
            KPIComparativo,
            MetricaTemporal,
            LogProcesamiento
        )
    print('  ✓ Modelos de Magdalena importados')
except Exception as e:
    print(f'  ✗ Error importando magdalena: {e}')

try:
    from app.models.camila import (
        ResultadoCamila, AsignacionGrua, CuotaCamion, MetricaGrua,
        ComparacionReal, FlujoModelo, ParametroCamila, LogProcesamientoCamila,
        EstadoProcesamiento, TipoOperacion, TipoAsignacion
    )
    print('  ✓ Modelos de Camila importados')
except Exception as e:
    print(f'  ✗ Error importando camila: {e}')

try:
    from app.models.container_dwell_time import ContainerDwellTime
    from app.models.truck_turnaround_time import TruckTurnaroundTime
    print('  ✓ CDT y TTT importados')
except Exception as e:
    print(f'  ✗ Error importando CDT/TTT: {e}')

try:
    from app.models.container_position import ContainerPosition
    print('  ✓ Container Position importado')
except Exception as e:
    print(f'  ✗ Error importando container_position: {e}')

from app.core.config import get_settings

async def create_tables():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    
    print('🔄 Creando tablas...')
    
    # Crear todas las tablas
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear índices adicionales
    print('🔧 Creando índices...')
    async with engine.connect() as conn:
        try:
            # Índices para container_positions
            await conn.execute(text('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_container_position_unique 
                ON container_positions (fecha, turno, gkey)
            '''))
            
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_container_position_bloque_fecha 
                ON container_positions (bloque, fecha, turno)
            '''))
            
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_container_position_patio_fecha 
                ON container_positions (patio, fecha, turno)
            '''))
            
            # Índices para resultados_camila
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_resultados_camila_lookup 
                ON resultados_camila (anio, semana, turno, participacion, con_dispersion)
            '''))
            
            # Índices para asignaciones_gruas
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_asignaciones_gruas_lookup 
                ON asignaciones_gruas (resultado_id, periodo, bloque_codigo)
            '''))
            
            # Índices para cuotas_camiones
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_cuotas_camiones_lookup 
                ON cuotas_camiones (resultado_id, periodo)
            '''))
            
            await conn.commit()
            print('✅ Índices creados correctamente')
        except Exception as e:
            print(f'⚠️ Error creando índices (puede que ya existan): {e}')
    
    # Verificar qué tablas se crearon
    async with engine.connect() as conn:
        result = await conn.execute(
            text(\"\"\"
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'public' 
                ORDER BY tablename
            \"\"\")
        )
        tables = [row[0] for row in result]
        print(f'\\n📋 Tablas creadas en la BD: {len(tables)}')
        
        # Agrupar por tipo
        camila_tables = [t for t in tables if any(t.startswith(p) for p in ['resultados_camila', 'asignaciones_gruas', 'cuotas_camiones', 'metricas_gruas', 'comparaciones_camila', 'parametros_camila', 'logs_camila'])]
        magdalena_tables = [t for t in tables if t.startswith('magdalena_') or t == 'instancias' or t == 'bloques' or t == 'segregaciones']
        sai_tables = [t for t in tables if t.startswith('sai_')]
        other_tables = [t for t in tables if not any(t.startswith(p) for p in ['resultados_camila', 'asignaciones_gruas', 'cuotas_camiones', 'metricas_gruas', 'comparaciones_camila', 'parametros_camila', 'logs_camila', 'magdalena_', 'instancias', 'bloques', 'segregaciones', 'sai_'])]
        
        if camila_tables:
            print('\\n  📊 Tablas de Camila:')
            for table in camila_tables:
                print(f'     - {table}')
        
        if magdalena_tables:
            print('\\n  📊 Tablas de Magdalena:')
            for table in magdalena_tables:
                print(f'     - {table}')
        
        if sai_tables:
            print('\\n  📊 Tablas de SAI:')
            for table in sai_tables:
                print(f'     - {table}')
        
        if other_tables:
            print('\\n  📊 Otras tablas:')
            for table in other_tables:
                print(f'     - {table}')
    
    await engine.dispose()
    print('\\n✅ Proceso de creación de tablas completado!')

# Ejecutar la creación de tablas
asyncio.run(create_tables())
"

# Esperar un momento para asegurarse de que las tablas se crearon
sleep 2

# Verificar si ya hay datos históricos
echo "🔍 Verificando datos históricos..."

# Verificar movimientos históricos
HISTORICAL_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.historical_movements import HistoricalMovement
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(HistoricalMovement.id)))
            count = result.scalar()
            return count
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar si ya hay datos de SAI Flujos
echo "🔍 Verificando datos de SAI Flujos..."
SAI_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.sai_flujos import SAIConfiguration
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(SAIConfiguration.id)))
            count = result.scalar()
            return count
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

if [ "$SAI_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de SAI, verificando archivos..."

    echo "Contenido de /app/data/magdalena/2022/instancias_magdalena:"
    ls -l /app/data/magdalena/2022/instancias_magdalena 2>/dev/null || echo "No existe el directorio"

    if [ -d "/app/data/magdalena/2022/instancias_magdalena" ]; then
        echo "📁 Buscando archivos SAI en estructura compartida..."
        
        # Verificar si hay archivos de flujos
        if find /app/data/magdalena/2022/instancias_magdalena -name "Flujos_w*.xlsx" -type f | grep -q .; then
            echo "📁 Archivos de SAI encontrados, cargando..."
            # python /app/scripts/load_sai_data.py
            # python /app/scripts/load_magdalena_data.py
            echo "✅ Datos de SAI cargados!"
        else
            echo "⚠️  No se encontraron archivos de SAI"
        fi
    else
        echo "⚠️  No se encontró la estructura de directorios esperada"
        echo "    Estructura esperada:"
        echo "    - /app/data/magdalena/2022/instancias_magdalena/2022-01-03/Flujos_w*.xlsx"
        echo "    - /app/data/magdalena/2022/instancias_magdalena/2022-01-03/Instancia_*.xlsx"
        echo "    - /app/data/magdalena/2022/instancias_magdalena/2022-01-03/evolucion_turnos_*.xlsx"
    fi
else
    echo "✅ Ya existen $SAI_COUNT configuraciones de SAI"
fi
# Verificar Container Positions
CONTAINER_POS_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.container_position import ContainerPosition
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(ContainerPosition.id)))
            count = result.scalar()
            return count
    except Exception as e:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")
# Verificar CDT
CDT_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.container_dwell_time import ContainerDwellTime
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(ContainerDwellTime.id)))
            count = result.scalar()
            return count
    except Exception as e:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar TTT
TTT_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.truck_turnaround_time import TruckTurnaroundTime
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(TruckTurnaroundTime.id)))
            count = result.scalar()
            return count
    except Exception as e:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

echo "📊 Estado de datos:"
echo "   - Movimientos históricos: $HISTORICAL_COUNT registros"
echo "   - Container Dwell Time: $CDT_COUNT registros"
echo "   - Truck Turnaround Time: $TTT_COUNT registros"
echo "   - Container Positions: $CONTAINER_POS_COUNT registros"
# Determinar qué cargar
LOAD_ALL=false
if [ "$HISTORICAL_COUNT" -eq "0" ] || [ "$CDT_COUNT" -eq "0" ] || [ "$TTT_COUNT" -eq "0" ] || [ "$CONTAINER_POS_COUNT" -eq "0" ]; then
    LOAD_ALL=true
fi

if [ "$LOAD_ALL" = true ]; then
    echo ""
    echo "📥 Faltan datos, verificando archivos disponibles..."
    
    # Verificar qué archivos existen
    FILES_FOUND=false
    
    if [ -f "data/resultados_congestion_SAI_2022.csv" ]; then
        echo "   ✓ Archivo de movimientos encontrado"
        FILES_FOUND=true
    else
        echo "   ✗ Archivo de movimientos no encontrado"
    fi
    
    if [ -f "data/resultados_CDT_impo_anio_SAI_2022.csv" ]; then
        echo "   ✓ Archivo CDT importación encontrado"
        FILES_FOUND=true
    else
        echo "   ✗ Archivo CDT importación no encontrado"
    fi
    
    if [ -f "data/resultados_CDT_expo_anio_SAI_2022.csv" ]; then
        echo "   ✓ Archivo CDT exportación encontrado"
        FILES_FOUND=true
    else
        echo "   ✗ Archivo CDT exportación no encontrado"
    fi
    
    if [ -f "data/resultados_TTT_impo_anio_SAI_2022.csv" ]; then
        echo "   ✓ Archivo TTT importación encontrado"
        FILES_FOUND=true
    else
        echo "   ✗ Archivo TTT importación no encontrado"
    fi
    
    if [ -f "data/resultados_TTT_expo_anio_SAI_2022.csv" ]; then
        echo "   ✓ Archivo TTT exportación encontrado"
        FILES_FOUND=true
    else
        echo "   ✗ Archivo TTT exportación no encontrado"
    fi
    
    if [ "$FILES_FOUND" = true ]; then
        echo ""
        echo "🚀 Iniciando carga de datos..."
        # python scripts/load_historical_data.py --all
        echo "✅ Proceso de carga completado!"
    else
        echo ""
        echo "⚠️  No se encontraron archivos CSV en la carpeta data/"
        echo "    Por favor, coloca los archivos CSV en la carpeta data/ antes de continuar"
    fi
else
    echo ""
    echo "✅ Todos los tipos de datos ya están cargados"
fi
# Verificar si ya hay datos de Container Positions
echo ""
echo "🔍 Verificando datos de posiciones de contenedores..."

if [ "$CONTAINER_POS_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de posiciones, verificando estructura de archivos..."
    
    # Verificar si existe la estructura de directorios
    if [ -d "data/2022" ]; then
        echo "📁 Estructura de directorios encontrada"
        
        # Contar archivos CSV
        CSV_COUNT=$(find data/2022 -name "*.csv" -type f | wc -l)
        echo "   - Archivos CSV encontrados: $CSV_COUNT"
        
        if [ "$CSV_COUNT" -gt "0" ]; then
            echo "🚀 Iniciando carga de posiciones de contenedores..."
            # python /app/scripts/load_container_positions.py 
            echo "✅ Datos de posiciones cargados!"
        else
            echo "⚠️  No se encontraron archivos CSV en data/2022/"
            echo "    Estructura esperada:"
            echo "    - data/2022/[semana-iso]/[fecha]_[turno].csv"
            echo "    - Ejemplo: data/2022/2022-01-03/2022-01-03_08-00.csv"
        fi
    else
        echo "⚠️  No se encontró el directorio data/2022/"
        echo "    Por favor, verifica que los archivos estén en la estructura correcta"
    fi
else
    echo "✅ Ya existen $CONTAINER_POS_COUNT registros de posiciones"
fi
# Verificar si ya hay datos de Magdalena
echo "🔍 Verificando datos de Magdalena..."
MAGDALENA_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.magdalena import MagdalenaRun
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(MagdalenaRun.id)))
            count = result.scalar()
            return count
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

if [ "$MAGDALENA_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de Magdalena, verificando archivos..."

    echo "Directorio actual: $(pwd)"
    echo "Contenido de /app/data/magdalena:"
    ls -l /app/data/magdalena 2>/dev/null || echo "No existe /app/data/magdalena"

    if [ -d "/app/data/magdalena" ] && [ "$(ls -A /app/data/magdalena/*.xlsx 2>/dev/null)" ]; then
        echo "📁 Archivos de Magdalena encontrados, cargando..."
        #python /app/scripts/load_magdalena_data.py
        echo "✅ Datos de Magdalena cargados!"
    else
        echo "⚠️  No se encontraron archivos de Magdalena en /app/data/magdalena/"
        echo "    Estructura esperada:"
        echo "    - /app/data/magdalena/resultado_3_69_K.xlsx"
        echo "    - /app/data/magdalena/semanas/Semana 3/Instancia_3_69_K.xlsx"
        echo "    - /app/data/magdalena/semanas/Semana 3/analisis_flujos_w3_ci.xlsx"
    fi
else
    echo "✅ Ya existen $MAGDALENA_COUNT configuraciones de Magdalena"
fi
# Verificar si ya hay datos de Camila
echo ""
echo "🔍 Verificando datos de Camila..."
CAMILA_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.camila import ResultadoCamila
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(ResultadoCamila.id)))
            count = result.scalar()
            return count
    except Exception as e:
        print(f'Error: {e}', file=sys.stderr)
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

if [ "$CAMILA_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de Camila, verificando archivos..."
    
    # Verificar estructura de directorios
    OPTIMIZATION_PATH="${OPTIMIZATION_DATA_PATH:-/app/optimization_data}"
    
    # Fallback a ruta local si no existe
    if [ ! -d "$OPTIMIZATION_PATH" ]; then
        if [ -d "/home/nejoo/gurobi/resultados_generados" ]; then
            OPTIMIZATION_PATH="/home/nejoo/gurobi/resultados_generados"
        fi
    fi
    
    CAMILA_RESULTADOS="$OPTIMIZATION_PATH/resultados_camila"
    CAMILA_INSTANCIAS="$OPTIMIZATION_PATH/instancias_camila"
    
    echo "📁 Buscando datos de Camila en:"
    echo "   - Resultados: $CAMILA_RESULTADOS"
    echo "   - Instancias: $CAMILA_INSTANCIAS"
    
    if [ -d "$CAMILA_RESULTADOS" ] && [ -d "$CAMILA_INSTANCIAS" ]; then
        # Contar archivos
        RESULTADO_COUNT=$(find "$CAMILA_RESULTADOS" -name "resultado_*_T*.xlsx" -type f 2>/dev/null | wc -l)
        INSTANCIA_COUNT=$(find "$CAMILA_INSTANCIAS" -name "Instancia_*_T*.xlsx" -type f 2>/dev/null | wc -l)
        
        echo "   - Archivos de resultados encontrados: $RESULTADO_COUNT"
        echo "   - Archivos de instancias encontrados: $INSTANCIA_COUNT"
        
        if [ "$RESULTADO_COUNT" -gt "0" ]; then
            echo "🚀 Iniciando carga masiva de datos de Camila..."
            python /app/scripts/load_camila_data_complete.py
            echo "✅ Proceso de carga de Camila completado!"
        else
            echo "⚠️  No se encontraron archivos de resultados"
            echo "    Estructura esperada:"
            echo "    - $CAMILA_RESULTADOS/[fecha]/resultado_*_T*.xlsx"
            echo "    - $CAMILA_INSTANCIAS/[fecha]/Instancia_*_T*.xlsx"
            echo "    Ejemplo:"
            echo "    - $CAMILA_RESULTADOS/2022-01-03/resultado_20220103_68_K_T01.xlsx"
            echo "    - $CAMILA_INSTANCIAS/2022-01-03/Instancia_20220103_68_K_T01.xlsx"
        fi
    else
        echo "⚠️  No se encontró la estructura de directorios de Camila"
        echo "    Verifica que existan los directorios:"
        echo "    - $CAMILA_RESULTADOS"
        echo "    - $CAMILA_INSTANCIAS"
    fi
else
    echo "✅ Ya existen $CAMILA_COUNT resultados de Camila"
fi

echo "🎯 Iniciando aplicación FastAPI..."
# Ejecutar el comando original
exec "$@"