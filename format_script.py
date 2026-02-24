import re

def format_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Quitar los espacios al final de las líneas (W293)
    lines = [line.rstrip() + '\n' for line in lines]

    # Eliminar linea en blanco extra al final si existe o añadir si falta (W292)
    while lines and lines[-1].strip() == '':
        lines.pop()
    lines.append('\n')

    content = "".join(lines)

    # Corregir F401: 'typing.Dict' imported but unused
    content = content.replace("from typing import List, Dict", "from typing import List")

    # Corregir E701: multiple statements on one line
    content = content.replace('if not isinstance(text, str): return ""', 'if not isinstance(text, str):\n        return ""')
    content = content.replace('if not resolved_teams: return pd.DataFrame()', 'if not resolved_teams:\n        return pd.DataFrame()')
    content = content.replace('if not name or not candidates_dict: return None, 0, None', 'if not name or not candidates_dict:\n        return None, 0, None')
    content = content.replace('if df_opta is None: return', 'if df_opta is None:\n        return')

    # Corregir E722: do not use bare 'except'
    content = content.replace('except:\n', 'except Exception:\n')

    # Corregir E302/E305: expected 2 blank lines (asegurarse de que hay dos líneas en blanco antes de def y '__main__')
    # Buscar patrón de 1 sola línea en blanco antes de 'def ' o 'if __name__'
    content = re.sub(r'([^\n])\n(def |if __name__ == "__main__":)', r'\1\n\n\n\2', content)
    content = re.sub(r'([^\n])\n\n(def |if __name__ == "__main__":)', r'\1\n\n\n\2', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    format_file('tools/generate_player_mapping.py')
