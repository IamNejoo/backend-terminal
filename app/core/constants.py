# app/core/constants.py
"""
Constantes para el sistema Camila
"""

# ===================== BLOQUES =====================
# Bloques del modelo (b1-b9)
BLOCKS = ['b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8', 'b9']
NUM_BLOCKS = 9

# ===================== GRÚAS =====================
# Lista de grúas (g1-g12)
GRUAS = ['g1', 'g2', 'g3', 'g4', 'g5', 'g6', 'g7', 'g8', 'g9', 'g10', 'g11', 'g12']
NUM_GRUAS = 12

# Productividad de grúa (movimientos/hora)
# Valor tomado del parámetro 'mu' en el archivo de instancia
GRUA_PRODUCTIVITY = 30

# ===================== TIEMPO =====================
# Períodos de tiempo (1-8 para cada turno)
TIME_PERIODS = 8

# Turnos
SHIFTS = {
    1: {'name': 'Turno 1', 'hours': '08:00-16:00', 'start_hour': 8},
    2: {'name': 'Turno 2', 'hours': '16:00-24:00', 'start_hour': 16},
    3: {'name': 'Turno 3', 'hours': '00:00-08:00', 'start_hour': 0}
}

# ===================== TIPOS DE FLUJO =====================
# Variables de flujo del modelo
FLOW_TYPES = {
    'fr_sbt': 'reception',    # Flujo de recepción
    'fe_sbt': 'delivery',     # Flujo de entrega
    'fc_sbt': 'loading',      # Flujo de carga
    'fd_sbt': 'unloading'     # Flujo de descarga
}

# ===================== FUNCIONES AUXILIARES =====================

def get_block_index(block: str) -> int:
    """Convierte código de bloque a índice (0-based)
    Ej: 'b1' -> 0, 'b2' -> 1, etc.
    """
    try:
        if block.startswith('b') and len(block) > 1:
            return int(block[1:]) - 1
        else:
            raise ValueError(f"Formato de bloque inválido: {block}")
    except (ValueError, IndexError):
        raise ValueError(f"Bloque inválido: {block}")

def get_grua_index(grua: str) -> int:
    """Convierte código de grúa a índice (0-based)
    Ej: 'g1' -> 0, 'g2' -> 1, etc.
    """
    try:
        if grua.startswith('g') and len(grua) > 1:
            return int(grua[1:]) - 1
        else:
            raise ValueError(f"Formato de grúa inválido: {grua}")
    except (ValueError, IndexError):
        raise ValueError(f"Grúa inválida: {grua}")

def get_real_hour(periodo: int, turno: int) -> str:
    """Convierte período y turno a hora real
    Ej: periodo=1, turno=1 -> '08:00'
    """
    if periodo < 1 or periodo > TIME_PERIODS:
        raise ValueError(f"Período inválido: {periodo}")
    
    if turno not in SHIFTS:
        raise ValueError(f"Turno inválido: {turno}")
    
    start_hour = SHIFTS[turno]['start_hour']
    real_hour = start_hour + periodo - 1
    
    # Ajustar para formato 24 horas
    if real_hour >= 24:
        real_hour -= 24
    
    return f"{real_hour:02d}:00"

# ===================== LÍMITES Y VALIDACIONES =====================

# Límites del modelo
MAX_GRUAS_POR_BLOQUE = 2  # W en el modelo
MIN_PERIODOS_OPERACION = 2  # K en el modelo
MAX_GRUAS_ACTIVAS = 12     # Rmax en el modelo

# Validaciones
VALID_DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
VALID_MODEL_TYPES = ['minmax', 'maxmin']

# ===================== COLORES PARA VISUALIZACIÓN =====================

# Colores para estados de utilización
UTILIZATION_COLORS = {
    'high': '#ef4444',      # Rojo - Alta utilización (>80%)
    'medium': '#f59e0b',    # Naranja - Media utilización (50-80%)
    'low': '#10b981',       # Verde - Baja utilización (<50%)
    'none': '#6b7280'       # Gris - Sin utilización
}

# Colores para congestión
CONGESTION_COLORS = {
    'critical': '#dc2626',  # Rojo oscuro - Congestión crítica
    'high': '#f87171',      # Rojo - Alta congestión
    'medium': '#fbbf24',    # Amarillo - Congestión media
    'low': '#34d399',       # Verde - Baja congestión
    'optimal': '#10b981'    # Verde oscuro - Óptimo
}