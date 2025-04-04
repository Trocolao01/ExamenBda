from neo4j import GraphDatabase
import csv

# Conexión a Neo4j
uri = "bolt://localhost:7687"  # URI de Neo4j
driver = GraphDatabase.driver(uri, auth=("neo4j", "tu_contraseña"))

# Consulta Cypher compleja: Obtener usuarios y sus amigos
query = """
    MATCH (u:Usuario)-[:AMIGO_DE]->(a:Usuario)
    RETURN u.nombre AS usuario, a.nombre AS amigo
    LIMIT 100  # Limitar a los primeros 100 amigos
"""

# Ejecutar la consulta
with driver.session() as session:
    resultados = session.run(query)
    
    # Exportar a CSV
    with open('resultados_neo4j_complejo.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        # Escribir los encabezados de columna
        writer.writerow(resultados.keys())
        
        # Escribir los resultados
        for record in resultados:
            writer.writerow(record.values())

# Cerrar la conexión
driver.close()

print("Datos exportados a resultados_neo4j_complejo.csv")
