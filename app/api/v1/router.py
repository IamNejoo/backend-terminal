# app/api/v1/router.py
from fastapi import APIRouter
from app.api.v1.endpoints import historical, magdalena, camila

api_router = APIRouter()

# Incluir routers
api_router.include_router(
    historical.router,
    prefix="/historical",
    tags=["historical"]
)

api_router.include_router(
    magdalena.router,
    prefix="/magdalena", 
    tags=["magdalena"]
)

api_router.include_router(
    camila.router,
    prefix="/camila",
    tags=["camila"]
)