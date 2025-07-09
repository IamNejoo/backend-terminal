from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.core.database import get_db
from app.services.magdalena_service import MagdalenaService
from app.schemas.magdalena import (
    MagdalenaDashboard, 
    MagdalenaFilter,
    MagdalenaLoadRequest
)
from app.models.magdalena import MagdalenaRun

router = APIRouter()

@router.get("/metrics", response_model=MagdalenaDashboard)
async def get_magdalena_metrics(
    anio: int = Query(..., description="Año"),
    semana: int = Query(..., ge=1, le=52, description="Número de semana"),
    turno: int = Query(..., ge=1, le=21, description="Turno (1-21)"),
    participacion: int = Query(..., description="Participación (68, 69, 70)"),
    dispersion: str = Query(..., pattern="^[KN]$", description="K=con dispersión, N=sin dispersión"),

    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene las métricas calculadas para el dashboard de Magdalena.
    
    Todas las métricas son calculadas de los archivos:
    - Reubicaciones eliminadas: YARD de flujos reales
    - Eficiencia ganada: Comparación movimientos productivos
    - Balance carga: Desviación estándar de workload
    - Ocupación: Volumen TEUs / Capacidad
    """
    
    service = MagdalenaService(db)
    con_dispersion = dispersion == 'K'
    
    data = await service.get_dashboard_data(anio, semana, turno, participacion, con_dispersion)
    
    if data.get("dataNotAvailable"):
        raise HTTPException(
            status_code=404, 
            detail=f"No hay datos para: Año {anio}, Semana {semana}, Turno {turno}, P{participacion}, Dispersión {dispersion}"
        )
    
    return data

@router.post("/cargar")
async def cargar_datos_magdalena(
    request: MagdalenaLoadRequest,
    db: AsyncSession = Depends(get_db)
):
    """
    Carga datos de Magdalena desde los archivos Excel.
    
    Procesa:
    - resultado_FECHA_PARTICIPACION_K/N.xlsx
    - Flujos_wFECHA.xlsx (si existe)
    - Instancia_FECHA_PARTICIPACION_K/N.xlsx (si existe)
    """
    
    service = MagdalenaService(db)
    result = await service.cargar_archivo_completo(
        request.fecha, 
        request.participacion, 
        request.con_dispersion
    )
    
    if result["status"] == "error":
        raise HTTPException(status_code=400, detail=result["message"])
    
    return result

@router.get("/disponibles")
async def get_datos_disponibles(
    db: AsyncSession = Depends(get_db),
    anio: Optional[int] = None,
    participacion: Optional[int] = None
):
    """
    Lista las configuraciones disponibles en la base de datos
    """
    
    query = select(
        MagdalenaRun.anio,
        MagdalenaRun.semana,
        MagdalenaRun.participacion,
        MagdalenaRun.con_dispersion
    ).distinct()
    
    if anio:
        query = query.where(MagdalenaRun.anio == anio)
    if participacion:
        query = query.where(MagdalenaRun.participacion == participacion)
    
    result = await db.execute(query)
    resultados = result.all()
    
    # Agrupar por año/semana
    datos = {}
    for r in resultados:
        key = f"{r.anio}-S{r.semana:02d}"
        if key not in datos:
            datos[key] = {
                "anio": r.anio,
                "semana": r.semana,
                "participaciones": [],
                "dispersiones": []
            }
        
        if r.participacion not in datos[key]["participaciones"]:
            datos[key]["participaciones"].append(r.participacion)
        
        disp = "K" if r.con_dispersion else "N"
        if disp not in datos[key]["dispersiones"]:
            datos[key]["dispersiones"].append(disp)
    
    return {
        "total": len(datos),
        "datos": list(datos.values())
    }

@router.get("/resumen/{anio}")
async def get_resumen_anual(
    anio: int,
    db: AsyncSession = Depends(get_db)
):
    """
    Obtiene un resumen de todas las semanas de un año
    """
    
    result = await db.execute(
        select(
            MagdalenaRun.semana,
            MagdalenaRun.participacion,
            MagdalenaRun.con_dispersion,
            MagdalenaRun.eficiencia_ganada,
            MagdalenaRun.reubicaciones_eliminadas,
            MagdalenaRun.ocupacion_promedio
        ).where(
            and_(
                MagdalenaRun.anio == anio,
                MagdalenaRun.turno == 1  # Solo primer turno para resumen
            )
        )
    )
    query_result = result.all()
    
    resumen = []
    for r in query_result:
        resumen.append({
            "semana": r.semana,
            "participacion": r.participacion,
            "dispersion": "K" if r.con_dispersion else "N",
            "eficiencia_ganada": r.eficiencia_ganada,
            "reubicaciones_eliminadas": r.reubicaciones_eliminadas,
            "ocupacion_promedio": r.ocupacion_promedio
        })
    
    return {
        "anio": anio,
        "total_semanas": len(set(r.semana for r in query_result)),
        "resumen": resumen
    }

@router.get("/validar-archivos")
async def validar_archivos_disponibles(
    fecha: str = Query(..., pattern="^\d{4}-\d{2}-\d{2}$"),
    participacion: int = Query(...),
    dispersion: str = Query(..., pattern="^[KN]$")
):
    """
    Valida si existen los archivos necesarios para cargar
    """
    
    from pathlib import Path
    base_path = Path("/app/optimization_files")
    
    dispersion_suffix = 'K' if dispersion == 'K' else 'N'
    
    resultado_path = base_path / f"resultados_magdalena/{fecha}/resultado_{fecha}_{participacion}_{dispersion_suffix}.xlsx"
    flujos_path = base_path / f"instancias_magdalena/{fecha}/Flujos_w{fecha.replace('-', '')}.xlsx"
    instancia_path = base_path / f"instancias_magdalena/{fecha}/Instancia_{fecha}_{participacion}_{dispersion_suffix}.xlsx"
    
    return {
        "fecha": fecha,
        "participacion": participacion,
        "dispersion": dispersion,
        "archivos": {
            "resultado": {
                "path": str(resultado_path),
                "existe": resultado_path.exists()
            },
            "flujos": {
                "path": str(flujos_path),
                "existe": flujos_path.exists()
            },
            "instancia": {
                "path": str(instancia_path),
                "existe": instancia_path.exists()
            }
        },
        "puede_cargar": resultado_path.exists()
    }