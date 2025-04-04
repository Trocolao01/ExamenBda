from neo4j import GraphDatabase
import csv
from pywebhdfs.webhdfs import PyWebHdfsClient

# Conexión a Neo4j
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "my-secret-pw"))  # Autenticación con tu contraseña

# Consulta Neo4j
query = "MATCH (p:Person) RETURN p.name, p.age"  # Cambia por tu consulta
with driver.session() as session:
    result = session.run(query)
    records = result.data()

# Guardar resultado en CSV
csv_file_path = "resultado_neo4j.csv"
with open(csv_file_path, 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(["name", "age"])
    for record in records:
        writer.writerow([record['p.name'], record['p.age']])

# Subir a HDFS
hdfs = PyWebHdfsClient(host='localhost', port='9870', user_name='root')
with open(csv_file_path, 'r') as file:
    content = file.read()

hdfs.create_file("/data/neo4j/resultado_neo4j.csv", content)
print("Resultado de Neo4j subido a HDFS en formato CSV")
