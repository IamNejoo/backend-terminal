# app/models/movement_flow.py
from sqlalchemy import Column, String, Integer, DateTime, Boolean, Index, Float
from app.models.base import BaseModel

class MovementFlow(BaseModel):
    """
    Tabla para almacenar los flujos de movimiento del terminal
    Datos provenientes del archivo data_2022.csv
    """
    __tablename__ = "movement_flows"
    
    # Campos temporales
    ime_time = Column(DateTime, nullable=False)  # Timestamp del movimiento
    
    # Posiciones
    ime_fm = Column(String(50), nullable=True)  # Posición origen (FROM) - contiene el bloque
    ime_to = Column(String(50), nullable=True)  # Posición destino (TO)
    
    # Identificador del contenedor
    ime_ufv_gkey = Column(Integer, nullable=False, index=True)  # ID único del contenedor
    
    # Tipo de movimiento
    ime_move_kind = Column(String(50), nullable=True)  # YARD, GATE, VESSEL, etc.
    
    # Criterios de clasificación
    criterio_i = Column(String(100), nullable=True)
    criterio_ii = Column(String(100), nullable=True)
    criterio_iii = Column(String(100), nullable=True)
    
    # Categoría y características
    iu_category = Column(String(10), nullable=True)  # IMPRT, EXPRT
    ig_hazardous = Column(Boolean, default=False)
    iu_requires_power = Column(Boolean, default=False)
    iu_freight_kind = Column(String(10), nullable=True)  # FCL, MTY, LCL
    ret_nominal_length = Column(String(10), nullable=True)  # NOM20, NOM40
    
    # IDs de buques/camiones
    ibcv_id = Column(String(50), nullable=True)  # ID buque/camión entrada
    ibcv_intend_id = Column(String(50), nullable=True)
    obcv_id = Column(String(50), nullable=True)  # ID buque/camión salida
    obcv_intend_id = Column(String(50), nullable=True)
    
    # Puerto destino
    pod1_id = Column(String(10), nullable=True)
    
    # Campos adicionales
    iufv_flex_string01 = Column(String(255), nullable=True)
    iufv_stow_factor = Column(String(100), nullable=True)
    iufv_stacking_factor = Column(String(100), nullable=True)
    
    # Campos calculados para facilitar queries
    patio = Column(String(20), nullable=True)  # costanera, ohiggins, tebas
    bloque = Column(String(10), nullable=True)  # C1, H5, T3, etc.
    
    __table_args__ = (
        # Índices para queries eficientes
        Index('idx_flow_time', 'ime_time'),
        Index('idx_flow_gkey_time', 'ime_ufv_gkey', 'ime_time'),
        Index('idx_flow_fm_to', 'ime_fm', 'ime_to'),
        Index('idx_flow_move_kind', 'ime_move_kind'),
        Index('idx_flow_category', 'iu_category'),
        Index('idx_flow_patio_bloque', 'patio', 'bloque'),
    )