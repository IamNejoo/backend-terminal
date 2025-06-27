
# app/models/truck_turnaround_time.py
from sqlalchemy import Column, String, Boolean, Integer, Float, DateTime, Index, UniqueConstraint
from app.models.base import BaseModel


class TruckTurnaroundTime(BaseModel):
    """
    Tabla para almacenar los datos de Truck Turnaround Time (TTT)
    Solo campos esenciales para KPIs
    """
    __tablename__ = "truck_turnaround_times"
    
    # Identificadores principales
    iufv_gkey = Column(Integer, nullable=False)  # ID único del movimiento
    gate_gkey = Column(Integer, nullable=True)  # ID del movimiento de gate
    operation_type = Column(String(10), nullable=False)  # 'import' o 'export'
    
    # TTT calculado
    ttt = Column(Float, nullable=True)  # TTT en minutos (ya calculado)
    turn_time = Column(Float, nullable=True)  # Tiempo alternativo
    
    # Tiempos del proceso de gate
    pregate_ss = Column(DateTime, nullable=True)  # Pre-gate start
    pregate_se = Column(DateTime, nullable=True)  # Pre-gate end
    ingate_ss = Column(DateTime, nullable=True)  # In-gate start
    ingate_se = Column(DateTime, nullable=True)  # In-gate end
    outgate_ss = Column(DateTime, nullable=True)  # Out-gate start
    outgate_se = Column(DateTime, nullable=True)  # Out-gate end
    
    # Tiempos calculados por etapa (en minutos)
    pregate_time = Column(Float, nullable=True)  # pregate_se - pregate_ss
    ingate_time = Column(Float, nullable=True)  # ingate_se - ingate_ss
    outgate_time = Column(Float, nullable=True)  # outgate_se - outgate_ss
    
    # Tiempos operacionales
    raw_t_dispatch = Column(Float, nullable=True)  # Tiempo despacho
    raw_t_fetch = Column(Float, nullable=True)  # Tiempo búsqueda
    raw_t_put = Column(Float, nullable=True)  # Tiempo colocación
    
    # Información del camión
    truck_license_nbr = Column(String, nullable=True)  # Patente
    driver_card_id = Column(String, nullable=True)  # ID conductor
    driver_name = Column(String, nullable=True)  # Nombre conductor
    trucking_co_id = Column(String, nullable=True)  # Empresa transporte
    
    # Posición en el patio
    pos_yard_gate = Column(String, nullable=True)  # Posición/bloque
    
    # Información del contenedor (para cruzar con CDT)
    ret_nominal_length = Column(Integer, nullable=True)  # 20, 40 pies
    iu_freight_kind = Column(String, nullable=True)  # FCL, LCL, MTY
    ig_hazardous = Column(Boolean, default=False)  # Carga peligrosa
    iu_requires_power = Column(Boolean, default=False)  # Refrigerado
    
    # Para análisis temporal
    hora_inicio = Column(Integer, nullable=True)  # Hora del día (0-23) de inicio
    dia_semana = Column(Integer, nullable=True)  # Día de la semana (0-6)
    
    __table_args__ = (
        # Evitar duplicados
        UniqueConstraint('iufv_gkey', 'gate_gkey', 'operation_type', name='_ttt_gkey_gate_type_uc'),
        
        # Índices para consultas rápidas
        Index('idx_ttt_times', 'pregate_ss', 'outgate_se'),
        Index('idx_ttt_operation', 'operation_type'),
        Index('idx_ttt_truck', 'truck_license_nbr', 'trucking_co_id'),
        Index('idx_ttt_temporal', 'hora_inicio', 'dia_semana'),
        Index('idx_ttt_yard', 'pos_yard_gate'),
    )