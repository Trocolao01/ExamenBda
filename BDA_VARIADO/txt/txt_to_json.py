import json  # Falta este import
import re
print('Hola')
# Leer el archivo de texto
with open("pokemon.txt", "r", encoding="utf-8") as file:
    data = file.read()

pattern = r"Evento: (.+)\nNombre del entrenador: (.+)\nEquipo de Pokemon: (.+)\nResultado de la batalla: (.+)"
matches = re.findall(pattern, data)

# Convertir a formato JSON
eventos = [{"Evento": m[0], "Nombre del entrenador": m[1], "Equipo de Pokemon": m[2], "Resultado de la batalla": m[3]} for m in matches]

# Guardar como JSON
with open("pokemon.json", "w", encoding="utf-8") as json_file:
    json.dump(eventos, json_file, indent=2, ensure_ascii=False)

print("✅ Archivo JSON generado correctamente.")

