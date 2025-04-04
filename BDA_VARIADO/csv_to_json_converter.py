import csv
import json
import sys

def convert_value(value):
    """Intenta convertir un valor a int o float, si no es posible, devuelve el string original."""
    if value.isdigit():  # Si es un número entero
        return int(value)
    try:
        return float(value)  # Intenta convertirlo a float si tiene decimales
    except ValueError:
        return value  # Devuelve el valor original si no es numérico

def csv_to_json(csv_filename, json_filename):
    try:
        with open(csv_filename, mode='r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)  # Usa la primera fila como claves
            
            # Convertir cada valor detectando números
            data = [{key: convert_value(value) for key, value in row.items()} for row in csv_reader]
        
        # Guardar como JSON
        with open(json_filename, mode='w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4, ensure_ascii=False)
        
        print(f" Archivo convertido correctamente: {json_filename}")
    
    except FileNotFoundError:
        print(f" Error: El archivo {csv_filename} no se encontró.")
    except Exception as e:
        print(f" Ocurrió un error: {e}")

# Verifica si el script se ejecuta con argumentos en la terminal
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python csv_to_json_converter.py archivo.csv archivo.json")
    else:
        csv_to_json(sys.argv[1], sys.argv[2])
