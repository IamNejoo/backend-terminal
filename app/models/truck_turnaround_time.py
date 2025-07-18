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
    turn_time = Column(Float, nullable=True)  # Tiempo alternativo en segundos
    
    # Tiempos de entrada/salida del camión
    cv_ata = Column(DateTime, nullable=True)  # Truck arrival time
    cv_atd = Column(DateTime, nullable=True)  # Truck departure time
    cv_atay = Column(DateTime, nullable=True)  # Truck arrival yard
    cv_atdy = Column(DateTime, nullable=True)  # Truck departure yard
    
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
    truck_license_nbr = Column(String(20), nullable=True)  # Patente
    driver_card_id = Column(String(20), nullable=True)  # ID conductor
    driver_name = Column(String(100), nullable=True)  # Nombre conductor
    trucking_co_id = Column(String(50), nullable=True)  # Empresa transporte
    
    # Posición en el patio
    pos_yard_gate = Column(String, nullable=True)  # Posición/bloque
    patio = Column(String(20), nullable=True)
    bloque = Column(String(10), nullable=True)
    # Información del contenedor (para cruzar con CDT)
    ret_nominal_length = Column(String(10), nullable=True)  # NOM20, NOM40
    ret_nominal_height = Column(String(10), nullable=True)  # NOM86, NOM96
    ret_iso_group = Column(String(10), nullable=True)  # Tipo ISO
    iu_freight_kind = Column(String(10), nullable=True)  # FCL, LCL, MTY
    ig_hazardous = Column(Boolean, default=False)  # Carga peligrosa
    iu_requires_power = Column(Boolean, default=False)  # Refrigerado
    iu_category = Column(String(10), nullable=True)  # IMPRT, EXPRT
    
    # Para análisis temporal
    hora_inicio = Column(Integer, nullable=True)  # Hora del día (0-23) de inicio
    dia_semana = Column(Integer, nullable=True)  # Día de la semana (0-6)
    turno = Column(Integer, nullable=True)  # 0=noche(00-08), 1=mañana(08-16), 2=tarde(16-24)
    
    __table_args__ = (
        # Evitar duplicados
        UniqueConstraint('iufv_gkey', 'operation_type', name='_ttt_gkey_type_uc'),
        
        # Índices para consultas rápidas
        Index('idx_ttt_times', 'cv_ata', 'cv_atd', 'pregate_ss', 'outgate_se'),
        Index('idx_ttt_operation', 'operation_type', 'iu_category'),
        Index('idx_ttt_truck', 'truck_license_nbr', 'trucking_co_id'),
        Index('idx_ttt_temporal', 'hora_inicio', 'dia_semana', 'turno'),
        Index('idx_ttt_yard', 'pos_yard_gate'),
        Index('idx_ttt_valid', 'ttt'),  # Para filtrar valores válidos rápidamente
    )