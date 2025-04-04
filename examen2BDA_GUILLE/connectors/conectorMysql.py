

from binascii import Error
import csv
import json
import mysql.connector
import requests

def connect_to_mysql(host='localhost', user='root', password='my-secret-pw', database=None):
    """Conecta a MySQL y devuelve la conexión."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=6969,
            auth_plugin='mysql_native_password'  # 🔹 Agregamos esta línea para evitar errores
        )
        if connection.is_connected():
            print("✅ Conexión exitosa a MySQL")
            return connection
    except Error as e:
        print(f"❌ Error al conectar a MySQL: {e}")
        return None

def create_database(db_name):
    """Crea una base de datos si no existe."""
    connection = connect_to_mysql()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            print(f"📂 Base de datos '{db_name}' creada o ya existente.")
        except Error as e:
            print(f"❌ Error al crear la base de datos: {e}")
        finally:
            cursor.close()
            connection.close()

def create_table(db_name, table_name, schema):
    """Crea una tabla en la base de datos."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})")
            print(f"📋 Tabla '{table_name}' creada en '{db_name}'.")
        except Error as e:
            print(f"❌ Error al crear la tabla: {e}")
        finally:
            cursor.close()
            connection.close()

def insert_data(db_name, table_name, columns, values):
    """Inserta datos en una tabla."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            placeholders = ', '.join(['%s'] * len(values))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            cursor.execute(query, values)
            connection.commit()
            print("✅ Datos insertados correctamente.")
        except Error as e:
            print(f"❌ Error al insertar datos: {e}")
        finally:
            cursor.close()
            connection.close()

def update_data(db_name, table_name, set_clause, condition):
    """Actualiza datos en una tabla."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
            cursor.execute(query)
            connection.commit()
            print("✅ Datos actualizados correctamente.")
        except Error as e:
            print(f"❌ Error al actualizar datos: {e}")
        finally:
            cursor.close()
            connection.close()

def delete_data(db_name, table_name, condition):
    """Elimina datos de una tabla."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            query = f"DELETE FROM {table_name} WHERE {condition}"
            cursor.execute(query)
            connection.commit()
            print("🗑️ Datos eliminados correctamente.")
        except Error as e:
            print(f"❌ Error al eliminar datos: {e}")
        finally:
            cursor.close()
            connection.close()

def select_data(db_name, table_name, columns='*', condition='1=1'):
    """Recupera datos de una tabla."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            query = f"SELECT {columns} FROM {table_name} WHERE {condition}"
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                print(row)
            return results
        except Error as e:
            print(f"❌ Error al consultar datos: {e}")
        finally:
            cursor.close()
            connection.close()

##################################################################INSERTORES DESDE AQUI##############################################################################################################
def insert_data_from_csv(db_name, table_name, columns, csv_file):
    """Inserta datos en una tabla desde un archivo CSV."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            with open(csv_file, newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                next(reader)  # Saltar la cabecera si existe
                for row in reader:
                    placeholders = ', '.join(['%s'] * len(row))
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, row)
            connection.commit()
            print("✅ Datos insertados desde CSV correctamente.")
        except Error as e:
            print(f"❌ Error al insertar datos desde CSV: {e}")
        finally:
            cursor.close()
            connection.close()

def insert_data_from_json(db_name, table_name, columns, json_file):
    """Inserta datos en una tabla desde un archivo JSON."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            with open(json_file, encoding='utf-8') as file:
                data = json.load(file)
                for entry in data:
                    values = tuple(entry[col] for col in columns)
                    placeholders = ', '.join(['%s'] * len(values))
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, values)
            connection.commit()
            print("✅ Datos insertados desde JSON correctamente.")
        except Error as e:
            print(f"❌ Error al insertar datos desde JSON: {e}")
        finally:
            cursor.close()
            connection.close()

def insert_data_from_txt(db_name, table_name, columns, txt_file, delimiter='|'):
    """Inserta datos en una tabla desde un archivo TXT delimitado."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            with open(txt_file, encoding='utf-8') as file:
                for line in file:
                    values = tuple(line.strip().split(delimiter))
                    placeholders = ', '.join(['%s'] * len(values))
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, values)
            connection.commit()
            print("✅ Datos insertados desde TXT correctamente.")
        except Error as e:
            print(f"❌ Error al insertar datos desde TXT: {e}")
        finally:
            cursor.close()
            connection.close()

def insert_data_from_api(db_name, table_name, columns, api_url):
    """Inserta datos en una tabla desde una API."""
    connection = connect_to_mysql(database=db_name)
    if connection:
        try:
            cursor = connection.cursor()
            response = requests.get(api_url)
            if response.status_code == 200:
                data = response.json()
                for entry in data:
                    values = tuple(entry[col] for col in columns)
                    placeholders = ', '.join(['%s'] * len(values))
                    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(query, values)
                connection.commit()
                print("✅ Datos insertados desde API correctamente.")
            else:
                print(f"❌ Error en la solicitud API: Código {response.status_code}")
        except Error as e:
            print(f"❌ Error al insertar datos desde API: {e}")
        finally:
            cursor.close()
            connection.close()

# 🛠️ EJEMPLO DE USO
if __name__ == "__main__":
    create_database('test_db')
    create_table('test_db', 'users', 'id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), age INT')
    insert_data('test_db', 'users', ['name', 'age'], ('Juan', 30))
    select_data('test_db', 'users')
    insert_data_from_csv('test_db', 'users', ['name', 'age'], 'users.csv')
    insert_data_from_json('test_db', 'users', ['name', 'age'], 'users.json')
    insert_data_from_txt('test_db', 'users', ['name', 'age'], 'users.txt', delimiter='|')
    insert_data_from_api('test_db', 'users', ['name', 'age'], 'https://pokeapi.co/api/v2/pokemon/ditto')

