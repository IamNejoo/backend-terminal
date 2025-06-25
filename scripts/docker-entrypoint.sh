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
    from app.models.magdalena import (
        MagdalenaRun, MagdalenaGeneral, MagdalenaOcupacion,
        MagdalenaWorkload, MagdalenaBahias, MagdalenaVolumen,
        MagdalenaInstancia, MagdalenaRealData
    )
    print('  ✓ Modelos de Magdalena importados')
except Exception as e:
    print(f'  ✗ Error importando magdalena: {e}')

try:
    from app.models.camila import (
        CamilaRun, CamilaFlujos, CamilaGruas, CamilaAsignacion,
        CamilaResultados, CamilaRealData, CamilaCuotas
    )
    print('  ✓ Modelos de Camila importados')
except Exception as e:
    print(f'  ✗ Error importando camila: {e}')

from app.core.config import get_settings

async def create_tables():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    
    print('🔄 Creando tablas...')
    
    # Crear todas las tablas
    async with engine.begin() as conn:
        # Drop all first if needed (opcional, solo para desarrollo)
        # await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    
    # Verificar qué tablas se crearon
    async with engine.connect() as conn:
        result = await conn.execute(
            text(\"SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename\")
        )
        tables = [row[0] for row in result]
        print(f'📋 Tablas creadas en la BD: {len(tables)}')
        for table in tables:
            print(f'   - {table}')
    
    await engine.dispose()
    print('✅ Proceso de creación de tablas completado!')

asyncio.run(create_tables())
"

# Esperar un momento para asegurarse de que las tablas se crearon
sleep 2

# Verificar si ya hay datos históricos
echo "🔍 Verificando datos históricos..."
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

if [ "$HISTORICAL_COUNT" -eq "0" ]; then
    echo "📊 No hay datos históricos, cargando archivo CSV..."
    
    if [ -f "data/resultados_congestion_SAI_2022.csv" ]; then
        python scripts/load_historical_data.py
        echo "✅ Datos históricos cargados!"
    else
        echo "⚠️  Archivo CSV no encontrado en data/resultados_congestion_SAI_2022.csv"
    fi
else
    echo "✅ Ya existen $HISTORICAL_COUNT registros históricos"
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
        python /app/scripts/load_magdalena_data.py
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
echo "🔍 Verificando datos de Camila..."
CAMILA_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

try:
    from app.models.camila import CamilaRun
    model_imported = True
except Exception as e:
    print(f'Error importando modelo: {e}', file=sys.stderr)
    model_imported = False

from app.core.config import get_settings

async def count_records():
    if not model_imported:
        return 0
        
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(CamilaRun.id)))
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

    echo "Contenido de /app/data/camila:"
    ls -l /app/data/camila 2>/dev/null || echo "No existe /app/data/camila"

    if [ -d "/app/data/camila" ] && [ "$(ls -A /app/data/camila/*.xlsx 2>/dev/null)" ]; then
        echo "📁 Archivos de Camila encontrados, cargando..."
        python /app/scripts/load_camila_data.py
        echo "✅ Datos de Camila cargados!"
    else
        echo "⚠️  No se encontraron archivos de Camila en /app/data/camila/"
        echo "    Archivos esperados:"
        echo "    - /app/data/camila/resultados_Semana_3_min_max_Modelo1.xlsx"
        echo "    - /app/data/camila/resultados_Semana_3_max_min_Modelo2.xlsx"
        echo "    - /app/data/camila/resultados_Semana_3_min_max_SS.xlsx"
        echo "    - /app/data/camila/resultados_Semana_3_max_min_SS.xlsx"
    fi
else
    echo "✅ Ya existen $CAMILA_COUNT configuraciones de Camila"
fi

echo "🎯 Iniciando aplicación FastAPI..."
# Ejecutar el comando original
exec "$@"