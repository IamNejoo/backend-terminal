# app/models/container_dwell_time.py
from sqlalchemy import Column, String, Integer,Boolean, Float, DateTime, Boolean, Index, UniqueConstraint
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
    iufv_it = Column(DateTime, nullable=True)  # In Time - entrada al terminal
    iufv_ot = Column(DateTime, nullable=True)  # Out Time - salida del terminal
    iufv_dt = Column(DateTime, nullable=True)  # Discharge Time - descarga del buque
    
    # CDT calculado (en horas)
    cdt_hours = Column(Float, nullable=True)  # Calculado: (iufv_ot - iufv_it)
    
    # Posiciones/Bloques
    iufv_arrive_pos_name = Column(String, nullable=True)  # Bloque de llegada
    iufv_last_pos_name = Column(String, nullable=True)  # Último bloque
    
    # Información del contenedor
    ret_nominal_length = Column(Integer, nullable=True)  # 20, 40 pies
    ret_nominal_height = Column(String, nullable=True)  # standard, high cube
    ret_iso_group = Column(String, nullable=True)  # Tipo ISO
    iu_freight_kind = Column(String, nullable=True)  # FCL, LCL, MTY
    
    # Características especiales
    ig_hazardous = Column(Boolean, default=False)  # Carga peligrosa
    iu_requires_power = Column(Boolean, default=False)  # Refrigerado
    iu_goods_and_ctr_wt_kg = Column(Float, nullable=True)  # Peso total
    
    # Información del buque
    ib_cv_id = Column(String, nullable=True)  # ID buque entrada
    ib_company = Column(String, nullable=True)  # Naviera entrada
    ob_cv_id = Column(String, nullable=True)  # ID buque salida
    ob_company = Column(String, nullable=True)  # Naviera salida
    
    # Documentación
    ig_bl_nbr = Column(String, nullable=True)  # Bill of Lading
    ig_origin = Column(String, nullable=True)  # Puerto origen
    ig_destination = Column(String, nullable=True)  # Puerto destino
    
    # Categoría de carga
    iu_category = Column(String, nullable=True)  # Categoría
    rc_name = Column(String, nullable=True)  # Nombre commodity
    
    __table_args__ = (
        # Evitar duplicados
        UniqueConstraint('iufv_gkey', 'operation_type', name='_cdt_gkey_type_uc'),
        
        # Índices para consultas rápidas
        Index('idx_cdt_dates', 'iufv_it', 'iufv_ot'),
        Index('idx_cdt_operation', 'operation_type'),
        Index('idx_cdt_blocks', 'iufv_arrive_pos_name', 'iufv_last_pos_name'),
        Index('idx_cdt_container_type', 'ret_nominal_length', 'ret_nominal_height'),
        Index('idx_cdt_naviera', 'ib_company', 'ob_company'),
    )

