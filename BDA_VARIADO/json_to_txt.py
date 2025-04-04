import json

# Función para convertir JSON a TXT
def json_to_txt(json_file, txt_file):
    # Leer el archivo JSON
    with open(json_file, 'r') as json_input:
        data = json.load(json_input)

    # Crear un archivo de texto para escribir los resultados
    with open(txt_file, 'w') as txt_output:
        # Iterar sobre cada objeto en el archivo JSON
        for record in data:
            # Escribir cada objeto en una línea en formato "id, nombre, edad"
            line = f"{record['id']}, {record['nombre']}, {record['edad']}\n"
            txt_output.write(line)

    print(f"Conversión de JSON a TXT completada. Guardado en {txt_file}")

# Llamar a la función con tus archivos
json_to_txt('datos.json', 'datos_convertidos.txt')
