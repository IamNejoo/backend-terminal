#!/bin/bash
# scripts/docker-entrypoint.sh - Script completo con todas las tablas

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

# Modelos base
try:
    from app.models.historical_movements import HistoricalMovement
    print('  ✓ Historical movements importado')
except Exception as e:
    print(f'  ✗ Error importando historical_movements: {e}')

try:
    from app.models.movement_flow import MovementFlow
    print('  ✓ Movement Flow importado')
except Exception as e:
    print(f'  ✗ Error importando movement_flow: {e}')

try:
    from app.models.container_position import ContainerPosition
    print('  ✓ Container Position importado')
except Exception as e:
    print(f'  ✗ Error importando container_position: {e}')

try:
    from app.models.container_dwell_time import ContainerDwellTime
    from app.models.truck_turnaround_time import TruckTurnaroundTime
    print('  ✓ CDT y TTT importados')
except Exception as e:
    print(f'  ✗ Error importando CDT/TTT: {e}')

# Modelos SAI
try:
    from app.models.sai_flujos import (
        SAIConfiguration, SAIFlujo, SAIVolumenBloque, SAIVolumenSegregacion,
        SAISegregacion, SAICapacidadBloque, SAIMapeoCriterios
    )
    print('  ✓ Modelos de SAI Flujos importados')
except Exception as e:
    print(f'  ✗ Error importando sai_flujos: {e}')

# Modelos de Magdalena (Optimization)
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
    print('  ✓ Modelos de Optimization (Magdalena) importados')
except Exception as e:
    print(f'  ✗ Error importando optimization: {e}')

# Modelos de Camila
try:
    from app.models.camila import (
        ResultadoCamila, AsignacionGrua, cuotas_camiones, metricas_gruas,
        comparaciones_real, flujos_modelo, parametros_camila,
        EstadoProcesamiento, TipoOperacion, TipoAsignacion, segregaciones_mapping,logs_procesamiento_camila
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
        await conn.run_sync(Base.metadata.create_all)
    
    # Crear índices adicionales
    print('🔧 Creando índices...')
    async with engine.connect() as conn:
        try:
            # ========== ÍNDICES PARA TABLAS BASE ==========
            
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
            
            # Índices para movement_flows
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movement_flows_gkey_time 
                ON movement_flows (ime_ufv_gkey, ime_time DESC)
            '''))
            
            # ========== ÍNDICES PARA MAGDALENA (OPTIMIZATION) ==========
            
            # Índices para instancias
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_optimization_fecha 
                ON instancias (fecha_inicio, fecha_fin)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_optimization_anio_semana 
                ON instancias (anio, semana)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_optimization_participacion 
                ON instancias (participacion, con_dispersion)
            '''))
            
            # Índices para movimientos reales
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movreal_instancia_fecha 
                ON movimientos_reales (instancia_id, fecha_hora)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movreal_tipo_movimiento 
                ON movimientos_reales (tipo_movimiento)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movreal_bloques 
                ON movimientos_reales (bloque_origen, bloque_destino)
            '''))
            
            # Índices para distancias
            await conn.execute(text('''
                CREATE UNIQUE INDEX IF NOT EXISTS idx_distancia_origen_destino 
                ON distancias_reales (origen, destino)
            '''))
            
            # Índices para asignaciones
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_asignacion_instancia_segregacion 
                ON asignaciones_bloques (instancia_id, segregacion_id)
            '''))
            
            # Índices para movimientos modelo
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movmodelo_instancia_periodo 
                ON movimientos_modelo (instancia_id, periodo)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_movmodelo_bloque 
                ON movimientos_modelo (bloque_id)
            '''))
            
            # Índices para carga trabajo
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_carga_instancia_periodo 
                ON carga_trabajo (instancia_id, periodo)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_carga_bloque 
                ON carga_trabajo (bloque_id)
            '''))
            
            # Índices para ocupación
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_ocupacion_instancia_periodo 
                ON ocupacion_bloques (instancia_id, periodo)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_ocupacion_bloque 
                ON ocupacion_bloques (bloque_id)
            '''))
            
            # Índices para KPIs
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_kpi_instancia_categoria 
                ON kpis_comparativos (instancia_id, categoria)
            '''))
            
            # Índices para métricas temporales
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_metrica_instancia_tiempo 
                ON metricas_temporales (instancia_id, dia, turno)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_metrica_periodo 
                ON metricas_temporales (periodo)
            '''))
            
            # Índices para logs
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_log_instancia 
                ON logs_procesamiento (instancia_id)
            '''))
            await conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_log_fecha 
                ON logs_procesamiento (fecha_procesamiento)
            '''))
            
            # ========== ÍNDICES PARA CAMILA ==========
            
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
        base_tables = ['historical_movements', 'movement_flows', 'container_positions', 
                      'container_dwell_times', 'truck_turnaround_times']
        sai_tables = [t for t in tables if t.startswith('sai_')]
        magdalena_tables = ['instancias', 'bloques', 'segregaciones', 'movimientos_reales',
                          'movimientos_modelo', 'distancias_reales', 'resultados_generales',
                          'asignaciones_bloques', 'carga_trabajo', 'ocupacion_bloques',
                          'kpis_comparativos', 'metricas_temporales', 'logs_procesamiento']
        camila_tables = ['resultados_camila', 'asignaciones_gruas', 'cuotas_camiones',
                        'metricas_gruas', 'comparaciones_camila', 'flujos_modelo',
                        'parametros_camila', 'logs_camila', 'segregacion_mappings']
        
        print('\\n  📊 Tablas Base:')
        for table in base_tables:
            if table in tables:
                print(f'     ✓ {table}')
            else:
                print(f'     ✗ {table} (no creada)')
        
        print('\\n  📊 Tablas de SAI:')
        for table in sai_tables:
            print(f'     ✓ {table}')
        
        print('\\n  📊 Tablas de Magdalena:')
        for table in magdalena_tables:
            if table in tables:
                print(f'     ✓ {table}')
            else:
                print(f'     ✗ {table} (no creada)')
        
        print('\\n  📊 Tablas de Camila:')
        for table in camila_tables:
            if table in tables:
                print(f'     ✓ {table}')
            else:
                print(f'     ✗ {table} (no creada)')
        
        # Verificar estructura de algunas tablas clave
        print('\\n📊 Verificando estructura de tablas principales:')
        
        # Verificar columnas de instancias (Magdalena)
        result = await conn.execute(
            text(\"\"\"
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'instancias' 
                ORDER BY ordinal_position
                LIMIT 10
            \"\"\")
        )
        if result.rowcount > 0:
            print('\\n  Tabla instancias (Magdalena):')
            for col in result:
                print(f'     - {col[0]}: {col[1]}')
        
        # Verificar columnas de resultados_generales
        result = await conn.execute(
            text(\"\"\"
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'resultados_generales' 
                AND column_name LIKE '%distancia%'
                ORDER BY ordinal_position
            \"\"\")
        )
        if result.rowcount > 0:
            print('\\n  Campos de distancia en resultados_generales:')
            for col in result:
                print(f'     - {col[0]}')
        
        # Verificar columnas de resultados_camila
        result = await conn.execute(
            text(\"\"\"
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'resultados_camila' 
                ORDER BY ordinal_position
                LIMIT 10
            \"\"\")
        )
        if result.rowcount > 0:
            print('\\n  Tabla resultados_camila:')
            for col in result:
                print(f'     - {col[0]}')
    
    await engine.dispose()
    print('\\n✅ Proceso de creación de tablas completado!')

# Ejecutar la creación de tablas
asyncio.run(create_tables())
"

# Esperar un momento para asegurarse de que las tablas se crearon
sleep 2

# ========== VERIFICACIÓN Y CARGA DE DATOS ==========

# Verificar Movement Flows
echo ""
echo "🔍 Verificando datos de Movement Flows..."
MOVEMENT_FLOW_COUNT=$(python -c "
import asyncio
import sys
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.movement_flow import MovementFlow
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(MovementFlow.id)))
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

if [ "$MOVEMENT_FLOW_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de Movement Flows, verificando archivo..."
    
    if [ -f "data/data_2022.csv" ]; then
        echo "📁 Archivo data_2022.csv encontrado"
        
        FILE_SIZE=$(stat -f%z "data/data_2022.csv" 2>/dev/null || stat -c%s "data/data_2022.csv" 2>/dev/null || echo "0")
        FILE_SIZE_MB=$((FILE_SIZE / 1048576))
        echo "   - Tamaño del archivo: ${FILE_SIZE_MB} MB"
        
        if [ "$FILE_SIZE" -gt "0" ]; then
            echo "🚀 Iniciando carga de Movement Flows..."
            echo "   ⚠️  Este proceso puede tomar varios minutos debido al tamaño del archivo"
            echo "   📅 Filtrando datos desde 2017 en adelante..."
            
            python scripts/load_historical_data.py --all --clear --year 2022
            
            echo "✅ Movement Flows cargados exitosamente"
        fi
    fi
else
    echo "✅ Ya existen $MOVEMENT_FLOW_COUNT registros de Movement Flows"
fi

# Verificar Historical Movements
echo ""
echo "🔍 Verificando datos históricos..."
HISTORICAL_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar CDT
echo ""
echo "🔍 Verificando Container Dwell Time..."
CDT_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar TTT
echo ""
echo "🔍 Verificando Truck Turnaround Time..."
TTT_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar Container Positions
echo ""
echo "🔍 Verificando Container Positions..."
CONTAINER_POS_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar datos de SAI Flujos
echo ""
echo "🔍 Verificando datos de SAI Flujos..."
SAI_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

# Verificar datos de Magdalena
echo ""
echo "🔍 Verificando datos de Magdalena (Optimization)..."
MAGDALENA_COUNT=$(python -c "
import asyncio
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from app.models.optimization import Instancia
from app.core.config import get_settings

async def count_records():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    try:
        async with async_session() as db:
            result = await db.execute(select(func.count(Instancia.id)))
            count = result.scalar()
            return count
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

if [ "$MAGDALENA_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de Magdalena, verificando archivos..."
    
    # Verificar estructura de directorios
    OPTIMIZATION_PATH="${OPTIMIZATION_DATA_PATH:-/app/optimization_data}"
    
    # Fallback a ruta local si no existe
    if [ ! -d "$OPTIMIZATION_PATH" ]; then
        if [ -d "/home/nejoo/gurobi/resultados_generados" ]; then
            OPTIMIZATION_PATH="/home/nejoo/gurobi/resultados_generados"
        fi
    fi
    
    RESULTADOS_PATH="$OPTIMIZATION_PATH/resultados_magdalena"
    INSTANCIAS_PATH="$OPTIMIZATION_PATH/instancias_magdalena"
    
    echo "📁 Buscando datos en:"
    echo "   - Resultados: $RESULTADOS_PATH"
    echo "   - Instancias: $INSTANCIAS_PATH"
    
    if [ -d "$RESULTADOS_PATH" ] && [ -d "$INSTANCIAS_PATH" ]; then
        RESULTADO_COUNT=$(find "$RESULTADOS_PATH" -name "resultado_*.xlsx" -type f 2>/dev/null | wc -l)
        INSTANCIA_COUNT=$(find "$INSTANCIAS_PATH" -name "Instancia_*.xlsx" -type f 2>/dev/null | wc -l)
        
        echo "   - Archivos de resultados encontrados: $RESULTADO_COUNT"
        echo "   - Archivos de instancias encontrados: $INSTANCIA_COUNT"
        
        if [ "$RESULTADO_COUNT" -gt "0" ]; then
            echo "🚀 Iniciando carga de datos de Magdalena..."
            echo "   ⚠️  Este proceso puede tomar varios minutos"
            python /app/scripts/load_magdalena_data.py      
            echo "✅ Proceso de carga de Magdalena completado!"
        else
            echo "⚠️  No se encontraron archivos de resultados para cargar"
        fi
    else
        echo "❌ No se encontraron los directorios de datos"
    fi
else
    echo "✅ Ya existen $MAGDALENA_COUNT instancias de Magdalena"
fi
# Verificar datos de Camila
echo ""
echo "🔍 Verificando datos de Camila..."
CAMILA_COUNT=$(python -c "
import asyncio
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
    except:
        return 0
    finally:
        await engine.dispose()

count = asyncio.run(count_records())
print(count)
" 2>/dev/null || echo "0")

if [ "$CAMILA_COUNT" -eq "0" ]; then
    echo "📊 No hay datos de Camila, verificando archivos..."
    
    OPTIMIZATION_PATH="${OPTIMIZATION_DATA_PATH:-/app/optimization_data}"
    
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
        RESULTADO_COUNT=$(find "$CAMILA_RESULTADOS" -name "resultado_*_T*.xlsx" -type f 2>/dev/null | wc -l)
        INSTANCIA_COUNT=$(find "$CAMILA_INSTANCIAS" -name "Instancia_*_T*.xlsx" -type f 2>/dev/null | wc -l)
        
        echo "   - Archivos de resultados encontrados: $RESULTADO_COUNT"
        echo "   - Archivos de instancias encontrados: $INSTANCIA_COUNT"
        
        if [ "$RESULTADO_COUNT" -gt "0" ]; then
            echo "🚀 Iniciando carga masiva de datos de Camila..."
            python /app/scripts/load_camila_data_complete.py
            echo "✅ Proceso de carga de Camila completado!"
        fi
    fi
else
    echo "✅ Ya existen $CAMILA_COUNT resultados de Camila"
fi

# Resumen final
echo ""
echo "📊 Estado final de datos:"
echo "   - Movement Flows: $MOVEMENT_FLOW_COUNT registros"
echo "   - Movimientos históricos: $HISTORICAL_COUNT registros"
echo "   - Container Dwell Time: $CDT_COUNT registros"
echo "   - Truck Turnaround Time: $TTT_COUNT registros"
echo "   - Container Positions: $CONTAINER_POS_COUNT registros"
echo "   - SAI Flujos: $SAI_COUNT configuraciones"
echo "   - Magdalena (Optimization): $MAGDALENA_COUNT instancias"
echo "   - Camila: $CAMILA_COUNT resultados"

# Actualización de bloques si hay Movement Flows nuevos
if [ "$MOVEMENT_FLOW_COUNT" -gt "0" ] && ([ "$CDT_COUNT" -gt "0" ] || [ "$TTT_COUNT" -gt "0" ]); then
    echo ""
    echo "🔄 Actualizando bloques en CDT y TTT desde Movement Flows..."
    python -c "
import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from app.core.config import get_settings

async def update_blocks():
    settings = get_settings()
    engine = create_async_engine(settings.DATABASE_URL)
    
    async with engine.connect() as conn:
        # Actualizar CDT
        result = await conn.execute(text('''
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE container_dwell_times cdt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE cdt.iufv_gkey = ub.ime_ufv_gkey
              AND (cdt.patio IS NULL OR cdt.bloque IS NULL)
        '''))
        cdt_updated = result.rowcount
        
        # Actualizar TTT
        result = await conn.execute(text('''
            WITH ultimo_bloque AS (
                SELECT DISTINCT ON (ime_ufv_gkey)
                    ime_ufv_gkey,
                    patio,
                    bloque
                FROM movement_flows
                WHERE patio IS NOT NULL 
                  AND bloque IS NOT NULL
                ORDER BY ime_ufv_gkey, ime_time DESC
            )
            UPDATE truck_turnaround_times ttt
            SET 
                patio = ub.patio,
                bloque = ub.bloque,
                updated_at = CURRENT_TIMESTAMP
            FROM ultimo_bloque ub
            WHERE ttt.iufv_gkey = ub.ime_ufv_gkey
              AND (ttt.patio IS NULL OR ttt.bloque IS NULL)
        '''))
        ttt_updated = result.rowcount
        
        await conn.commit()
        
        if cdt_updated > 0 or ttt_updated > 0:
            print(f'   - CDT actualizados: {cdt_updated}')
            print(f'   - TTT actualizados: {ttt_updated}')
            print('✅ Actualización de bloques completada')
    
    await engine.dispose()

asyncio.run(update_blocks())
"
fi

echo ""
echo "🎯 Continuando con el inicio de la aplicación..."

# Ejecutar el comando original
exec "$@"