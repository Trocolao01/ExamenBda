import json

# Función para convertir TXT a JSON
def txt_to_json(txt_file, json_file):
    # Leer el archivo de texto
    with open(txt_file, 'r') as file:
        lines = file.readlines()

    # Crear una lista para almacenar los registros como diccionarios
    json_data = []

    # Iterar sobre cada línea del archivo de texto
    for line in lines:
        # Eliminar saltos de línea y separar por coma
        fields = line.strip().split(', ')

        # Crear un diccionario para cada línea
        record = {
            "id": int(fields[0]),
            "nombre": fields[1],
            "edad": int(fields[2])
        }

        # Añadir el diccionario a la lista
        json_data.append(record)

    # Escribir el resultado en un archivo JSON
    with open(json_file, 'w') as json_output:
        json.dump(json_data, json_output, indent=4)

    print(f"Conversión de TXT a JSON completada. Guardado en {json_file}")

# Llamar a la función con tus archivos
txt_to_json('datos.txt', 'datos.json')
