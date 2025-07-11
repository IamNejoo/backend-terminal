# app/api/v1/router.py
from fastapi import APIRouter
from app.api.v1.endpoints import historical, camila, optimization, sai_flujos, container_positions

api_router = APIRouter()

# Incluir routers
api_router.include_router(
    historical.router,
    prefix="/historical",
    tags=["historical"]
)

api_router.include_router(
    optimization.router,
    prefix="/optimization",  # Cambiar a optimization
    tags=["optimization"]    # Cambiar a optimization
)

api_router.include_router(
    camila.router,
    prefix="/camila",
    tags=["camila"]
)

api_router.include_router(
    sai_flujos.router,
    prefix="/sai",
    tags=["sai-flujos"]
)


api_router.include_router(
    container_positions.router,
    prefix="/container-positions",
    tags=["container-positions"]
)