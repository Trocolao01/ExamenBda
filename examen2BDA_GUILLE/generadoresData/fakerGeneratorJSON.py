from faker import Faker
import json

fake = Faker()

# Nombre del archivo de salida
filename = "fake_data.json"

# Lista para almacenar los registros
data = []

# Generar 2000 registros
for _ in range(2000):
    record = {
        "Nombre": fake.name(),
        "Dirección": fake.address().replace("\n", " "),
        "Correo Electrónico": fake.email(),
        "Teléfono": fake.phone_number()
    }
    data.append(record)

# Escribir los registros en formato JSON
with open(filename, mode='w') as file:
    json.dump(data, file, indent=4)

print(f"Archivo JSON generado: {filename}")
