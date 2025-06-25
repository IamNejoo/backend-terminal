import re
import os

def parse_filename(filename):
    name = filename.replace('.xlsx', '').lower()
    semana = None
    modelo_tipo = None
    semana_match = re.search(r'semana[_-]?(\d+)', name)
    if semana_match:
        semana = int(semana_match.group(1))
    if 'min_max' in name or 'minmax' in name:
        modelo_tipo = 'minmax'
    elif 'max_min' in name or 'maxmin' in name:
        modelo_tipo = 'maxmin'
    if semana is None or modelo_tipo is None:
        return None
    return semana, modelo_tipo

files = [f for f in os.listdir('.') if f.endswith('.xlsx')]
for f in files:
    print(f, "->", parse_filename(f))
