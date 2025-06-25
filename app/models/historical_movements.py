# app/models/historical_movements.py
from sqlalchemy import Column, String, Integer, Float, DateTime, Index, UniqueConstraint
from app.models.base import BaseModel

class HistoricalMovement(BaseModel):
    """
    Tabla para almacenar los datos del CSV resultados_congestion_SAI_2022.csv
    Estructura exacta como viene en el archivo
    """
    __tablename__ = "historical_movements"
    
    # Campos del CSV
    bloque = Column(String, nullable=False)
    hora = Column(DateTime, nullable=False)
    
    # Gate
    gate_entrada_contenedores = Column(Integer, default=0)
    gate_entrada_teus = Column(Integer, default=0)
    gate_salida_contenedores = Column(Integer, default=0)
    gate_salida_teus = Column(Integer, default=0)
    
    # Muelle
    muelle_entrada_contenedores = Column(Integer, default=0)
    muelle_entrada_teus = Column(Integer, default=0)
    muelle_salida_contenedores = Column(Integer, default=0)
    muelle_salida_teus = Column(Integer, default=0)
    
    # Remanejos
    remanejos_contenedores = Column(Integer, default=0)
    remanejos_teus = Column(Integer, default=0)
    
    # Patio
    patio_entrada_contenedores = Column(Integer, default=0)
    patio_entrada_teus = Column(Integer, default=0)
    patio_salida_contenedores = Column(Integer, default=0)
    patio_salida_teus = Column(Integer, default=0)
    
    # Terminal
    terminal_entrada_contenedores = Column(Integer, default=0)
    terminal_entrada_teus = Column(Integer, default=0)
    terminal_salida_contenedores = Column(Integer, default=0)
    terminal_salida_teus = Column(Integer, default=0)
    
    # Estadísticas
    minimo_contenedores = Column(Integer, default=0)
    minimo_teus = Column(Integer, default=0)
    maximo_contenedores = Column(Integer, default=0)
    maximos_teus = Column(Integer, default=0)
    promedio_contenedores = Column(Integer, default=0)
    promedio_teus = Column(Integer, default=0)
    
    __table_args__ = (
        # Evitar duplicados
        UniqueConstraint('bloque', 'hora', name='_bloque_hora_uc'),
        
        # Índices para consultas rápidas
        Index('idx_historical_hora', 'hora'),
        Index('idx_historical_bloque', 'bloque'),
        Index('idx_historical_bloque_hora', 'bloque', 'hora'),
    )