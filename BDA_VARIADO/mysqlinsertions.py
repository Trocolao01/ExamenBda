import mysql.connector
import csv

# Conexión a MySQL
conexion = mysql.connector.connect(
    host="localhost",
    user="tu_usuario",
    password="tu_contraseña",
    database="red_social"  # Base de datos de ejemplo
)

cursor = conexion.cursor()

# Consulta SQL compleja con JOIN: Obtener amigos de un usuario específico
usuario_id = 42  # Ejemplo de ID de usuario
query = """
    SELECT u.nombre, a.nombre AS amigo
    FROM usuarios u
    JOIN amigos am ON u.id = am.id_usuario
    JOIN usuarios a ON a.id = am.id_amigo
    WHERE u.id = %s
"""
cursor.execute(query, (usuario_id,))

# Obtener los resultados
resultados = cursor.fetchall()

# Exportar a CSV
with open('resultados_mysql_complejo.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    # Escribir los encabezados de columna
    writer.writerow([i[0] for i in cursor.description])
    # Escribir los resultados
    writer.writerows(resultados)

# Cerrar la conexión
cursor.close()
conexion.close()

print("Datos exportados a resultados_mysql_complejo.csv")
