# app/models/container_dwell_time.py
from sqlalchemy import Column, String, Integer, Float, DateTime, Boolean, Index, UniqueConstraint
from app.models.base import BaseModel

class ContainerDwellTime(BaseModel):
    """
    Tabla para almacenar los datos de Container Dwell Time (CDT)
    Solo campos esenciales para KPIs
    """
    __tablename__ = "container_dwell_times"
    
    # Identificadores principales
    iufv_gkey = Column(Integer, nullable=False)  # ID único del movimiento
    operation_type = Column(String(10), nullable=False)  # 'import' o 'export'
    
    # Tiempos críticos para CDT
    cv_it = Column(DateTime, nullable=True)  # Container vessel in time
    iufv_it = Column(DateTime, nullable=True)  # In Time - entrada al terminal
    ime_it = Column(DateTime, nullable=True)  # Import entry time
    cv_ot = Column(DateTime, nullable=True)  # Container vessel out time
    iufv_ot = Column(DateTime, nullable=True)  # Out Time - salida del terminal
    ime_ot = Column(DateTime, nullable=True)  # Import exit time
    
    # CDT calculado (en horas) - NO usar cv_dt que tiene errores
    cdt_hours = Column(Float, nullable=True)  # Calculado: (iufv_ot - iufv_it)
    
    # CAMPOS CRÍTICOS PARA FILTROS POR PATIO/BLOQUE
    ime_in_fm_pos_name = Column(String, nullable=True)  # Posición origen
    ime_in_to_pos_name = Column(String, nullable=True)  # Posición destino entrada
    ime_out_fm_pos_name = Column(String, nullable=True)  # Posición origen salida
    ime_out_to_pos_name = Column(String, nullable=True)  # Posición destino salida
    iufv_arrive_pos_name = Column(String, nullable=True)  # Bloque de llegada
    iufv_last_pos_name = Column(String, nullable=True)  # Último bloque
    
    # Campos calculados para facilitar queries
    patio = Column(String(20), nullable=True)  # 'costanera', 'ohiggins', 'tebas'
    bloque = Column(String(10), nullable=True)  # 'C1', 'H5', 'T2', etc.
    
    # Información del contenedor
    ret_nominal_length = Column(String(10), nullable=True)  # NOM20, NOM40
    ret_nominal_height = Column(String(10), nullable=True)  # NOM86, NOM96
    ret_id = Column(String(10), nullable=True)  # Código tipo contenedor
    ret_description = Column(String, nullable=True)  # Descripción tipo
    ret_iso_group = Column(String(10), nullable=True)  # Tipo ISO
    iu_freight_kind = Column(String(10), nullable=True)  # FCL, LCL, MTY
    
    # Características especiales
    ig_hazardous = Column(Boolean, default=False)  # Carga peligrosa
    iu_requires_power = Column(Boolean, default=False)  # Refrigerado
    iu_goods_and_ctr_wt_kg = Column(Float, nullable=True)  # Peso total
    iu_category = Column(String(10), nullable=True)  # IMPRT, EXPRT
    
    # Información del buque
    ib_cv_id = Column(String(50), nullable=True)  # ID buque entrada
    ib_company = Column(String(100), nullable=True)  # Naviera entrada
    ob_cv_id = Column(String(50), nullable=True)  # ID buque/truck salida
    ob_company = Column(String(100), nullable=True)  # Empresa salida
    
    # Documentación
    ig_bl_nbr = Column(String(50), nullable=True)  # Bill of Lading
    pol_id = Column(String(10), nullable=True)  # Puerto de carga
    pod1_id = Column(String(10), nullable=True)  # Puerto de descarga
    ig_origin = Column(String(10), nullable=True)  # Puerto origen
    ig_destination = Column(String(10), nullable=True)  # Puerto destino
    
    # Commodity
    rc_name = Column(String, nullable=True)  # Nombre commodity
    rc_id = Column(String(20), nullable=True)  # Código commodity
    patio = Column(String(20), nullable=True)
    bloque = Column(String(10), nullable=True)
    __table_args__ = (
        # Evitar duplicados
        UniqueConstraint('iufv_gkey', 'operation_type', name='_cdt_gkey_type_uc'),
        
        # Índices para consultas rápidas
        Index('idx_cdt_dates', 'iufv_it', 'iufv_ot'),
        Index('idx_cdt_operation', 'operation_type', 'iu_category'),
        Index('idx_cdt_positions', 'ime_in_to_pos_name', 'ime_out_fm_pos_name'),
        Index('idx_cdt_patio_bloque', 'patio', 'bloque'),
        Index('idx_cdt_container_type', 'ret_nominal_length', 'ret_nominal_height'),
        Index('idx_cdt_naviera', 'ib_company', 'ob_company'),
    )