from faker import Faker


fake = Faker()

# Nombre del archivo de salida
filename = "fake_data.txt"

# Abrir el archivo en modo escritura
with open(filename, mode='w') as file:
    # Generar y escribir 2000 registros
    for _ in range(2000):
        name = fake.name()
        address = fake.address().replace("\n", " ")
        email = fake.email()
        phone = fake.phone_number()
        file.write(f"Nombre: {name}\nDirección: {address}\nCorreo Electrónico: {email}\nTeléfono: {phone}\n\n")

print(f"Archivo TXT generado: {filename}")
