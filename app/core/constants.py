# app/core/constants.py
"""Constantes y utilidades para el modelo Camila"""

# Configuración de bloques
BLOCKS_INTERNAL = ['b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'b8', 'b9']
BLOCKS_DISPLAY = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']

# Configuración de grúas
GRUAS = ['g1', 'g2', 'g3', 'g4', 'g5', 'g6', 'g7', 'g8', 'g9', 'g10', 'g11', 'g12']
GRUA_PRODUCTIVITY = 20  # movimientos/hora por grúa

# Configuración temporal
TIME_PERIODS = 8  # horas por turno
SHIFTS = {
    1: {'name': 'Mañana', 'hours': '08:00-16:00', 'start': 8},
    2: {'name': 'Tarde', 'hours': '16:00-24:00', 'start': 16},
    3: {'name': 'Noche', 'hours': '00:00-08:00', 'start': 0}
}

# Días de la semana
DAYS_ES = {
    'Monday': 'Lunes',
    'Tuesday': 'Martes',
    'Wednesday': 'Miércoles',
    'Thursday': 'Jueves',
    'Friday': 'Viernes',
    'Saturday': 'Sábado',
    'Sunday': 'Domingo'
}

# Tipos de flujo
FLOW_TYPES = {
    'fr_sbt': 'reception',  # Recepción (gate → bloque)
    'fe_sbt': 'delivery',   # Entrega (bloque → gate)
    'fc_sbt': 'loading',    # Carga (bloque → barco)
    'fd_sbt': 'unloading'   # Descarga (barco → bloque)
}

# Funciones de conversión
def block_internal_to_display(internal: str) -> str:
    """Convierte nombre interno a display (b1 → C1)"""
    try:
        # Manejar diferentes formatos
        if internal.upper() in BLOCKS_DISPLAY:
            return internal.upper()
        
        if internal.lower() in BLOCKS_INTERNAL:
            idx = BLOCKS_INTERNAL.index(internal.lower())
            return BLOCKS_DISPLAY[idx]
        
        # Intentar extraer número
        import re
        match = re.search(r'\d+', internal)
        if match:
            num = int(match.group())
            if 1 <= num <= 9:
                return f'C{num}'
                
    except (ValueError, IndexError):
        pass
    
    raise ValueError(f"Bloque interno no válido: {internal}")

def block_display_to_internal(display: str) -> str:
    """Convierte nombre display a interno (C1 → b1)"""
    try:
        if display.lower() in BLOCKS_INTERNAL:
            return display.lower()
            
        if display.upper() in BLOCKS_DISPLAY:
            idx = BLOCKS_DISPLAY.index(display.upper())
            return BLOCKS_INTERNAL[idx]
            
    except (ValueError, IndexError):
        pass
    
    raise ValueError(f"Bloque display no válido: {display}")

def get_block_index(block: str) -> int:
    """Obtiene el índice del bloque (0-8)"""
    # Intentar como interno
    if block.lower() in BLOCKS_INTERNAL:
        return BLOCKS_INTERNAL.index(block.lower())
    
    # Intentar como display
    if block.upper() in BLOCKS_DISPLAY:
        return BLOCKS_DISPLAY.index(block.upper())
    
    # Intentar extraer número
    import re
    match = re.search(r'\d+', block)
    if match:
        num = int(match.group())
        if 1 <= num <= 9:
            return num - 1
            
    raise ValueError(f"No se puede obtener índice para bloque: {block}")

def get_grua_index(grua: str) -> int:
    """Obtiene el índice de la grúa (0-11)"""
    if grua.lower() in GRUAS:
        return GRUAS.index(grua.lower())
    
    # Intentar extraer número
    import re
    match = re.search(r'\d+', grua)
    if match:
        num = int(match.group())
        if 1 <= num <= 12:
            return num - 1
            
    raise ValueError(f"No se puede obtener índice para grúa: {grua}")

def validate_time_period(tiempo: int) -> bool:
    """Valida que el período de tiempo sea válido"""
    return 1 <= tiempo <= TIME_PERIODS

def get_shift_hours(shift: int) -> tuple:
    """Obtiene las horas de inicio y fin de un turno"""
    if shift not in SHIFTS:
        raise ValueError(f"Turno inválido: {shift}")
    
    start = SHIFTS[shift]['start']
    end = start + TIME_PERIODS
    return start, end