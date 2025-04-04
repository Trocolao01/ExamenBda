import csv
from faker import Faker

# Inicializar el generador Faker
fake = Faker()

# Nombre del archivo de salida
filename = "fake_data.csv"

# Abrir el archivo en modo escritura
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    
    # Escribir los encabezados del CSV
    writer.writerow(['Nombre', 'Dirección', 'Correo Electrónico', 'Teléfono'])
    
    # Generar y escribir 2000 registros
    for _ in range(2000):
        name = fake.name()
        address = fake.address().replace("\n", " ")
        email = fake.email()
        phone = fake.phone_number()
        writer.writerow([name, address, email, phone])

print(f"Archivo CSV generado: {filename}")
