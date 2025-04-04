
------------------------------------------------------------------------

pig -x mapreduce replace_nulls.pig


data = LOAD '/home/data.csv' USING PigStorage(',') AS (nombre:chararray, 
                                                                  ocupacion:chararray, 
                                                                  estado:chararray, 
                                                                  activo:chararray);

-- Reemplazar celdas vacías con "NULL"
clean_data = FOREACH data GENERATE 
    (nombre IS NULL OR nombre == '' ? 'NULL' : nombre) AS nombre,
    (ocupacion IS NULL OR ocupacion == '' ? 'NULL' : ocupacion) AS ocupacion,
    (estado IS NULL OR estado == '' ? 'NULL' : estado) AS estado,
    (activo IS NULL OR activo == '' ? 'NULL' : activo) AS activo;

STORE clean_data INTO '/home/clean_data' USING PigStorage(',');

------------------------------------------------------------------------


data = LOAD '/home/clean_data' USING PigStorage(',') AS (nombre:chararray, 
                                                                    ocupacion:chararray, 
                                                                    estado:chararray, 
                                                                    activo:chararray);

-- Contar cuántas celdas tienen "NULL"
filtered_data = FILTER data BY 
    ( (nombre == 'NULL' ? 1 : 0) +
      (ocupacion == 'NULL' ? 1 : 0) +
      (estado == 'NULL' ? 1 : 0) +
      (activo == 'NULL' ? 1 : 0) ) <= 2;

STORE filtered_data INTO '/home/final_data' USING PigStorage(',');

------------------------------------------------------------------------
%%writefile calcular_gastos.pig
orders = LOAD 'input/examen.csv' USING PigStorage(',') AS (
    order_id: int,
    order_date: chararray,
    total_amount: double,
    order_item_id: int,
    quantity: double,
    unit_price: double,
    user_id: int,
    username: chararray,
    email: chararray,
    age: int,
    country: chararray
);

user_spending = FOREACH (GROUP orders BY user_id) {
    generate group AS user_id, SUM(orders.total_amount) AS total_spent;
}

STORE user_spending INTO 'user_spending_output' USING PigStorage(',');

------------------------------------------------------------------------
%%writefile calcular_gastos_pais.pig
-- Load the CSV file
orders = LOAD 'input/examen.csv' USING PigStorage(',') AS (
    order_id: int,
    order_date: chararray,
    total_amount: double,
    order_item_id: int,
    quantity: double,
    unit_price: double,
    user_id: int,
    username: chararray,
    email: chararray,
    age: int,
    country: chararray
);

-- Group the data by country and user_id, and calculate the total amount spent by each user in each country
user_country_spending = FOREACH (GROUP orders BY (country, user_id)) {
    generate
        FLATTEN(group) AS (country, user_id),
        SUM(orders.total_amount) AS total_spent;
}

-- Group the data by country and find the user who spent the highest amount in each country
top_spender_per_country = FOREACH (GROUP user_country_spending BY country) {
    ordered = ORDER user_country_spending BY total_spent DESC;
    top_user = LIMIT ordered 1;
    generate group AS country, top_user.user_id AS top_spender_id, top_user.total_spent AS total_spent;
}

-- Store the result
STORE top_spender_per_country INTO 'top_spender_per_country_output' USING PigStorage(',');
------------------------------------------------------------------------



-- 1. Cargar datos desde un archivo CSV
data = LOAD '/home/data.csv' USING PigStorage(',') AS (id:int, nombre:chararray, edad:int, salario:double);

-- 2. Filtrar datos por una condición
filtered_data = FILTER data BY edad > 30;

-- 3. Seleccionar columnas específicas
selected_data = FOREACH data GENERATE nombre, edad;

-- 4. Reemplazar valores nulos con un valor por defecto
clean_data = FOREACH data GENERATE 
    (nombre IS NULL ? 'Desconocido' : nombre) AS nombre,
    (edad IS NULL ? 0 : edad) AS edad;

-- 5. Ordenar datos por una columna
sorted_data = ORDER data BY edad DESC;

-- 6. Contar registros en el conjunto de datos
counted = GROUP data ALL;
total_count = FOREACH counted GENERATE COUNT(data);

-- 7. Agrupar datos por una columna
grouped_data = GROUP data BY edad;

-- 8. Calcular el salario promedio por edad
salary_avg = FOREACH grouped_data GENERATE group AS edad, AVG(data.salario) AS salario_promedio;

-- 9. Contar registros por grupo
count_per_group = FOREACH grouped_data GENERATE group AS edad, COUNT(data) AS count;

-- 10. Eliminar duplicados
distinct_data = DISTINCT data;

-- 11. Unir dos conjuntos de datos
data1 = LOAD '/home/data1.csv' USING PigStorage(',') AS (id:int, nombre:chararray);
data2 = LOAD '/home/data2.csv' USING PigStorage(',') AS (id:int, salario:double);
joined_data = JOIN data1 BY id, data2 BY id;

-- 12. Filtrar valores nulos
data_no_nulls = FILTER data BY nombre IS NOT NULL;

-- 13. Convertir datos a mayúsculas
uppercase_names = FOREACH data GENERATE UPPER(nombre) AS nombre;

-- 14. Contar ocurrencias de cada valor en una columna
grouped_names = GROUP data BY nombre;
counted_names = FOREACH grouped_names GENERATE group AS nombre, COUNT(data) AS count;

-- 15. Extraer datos en un rango de fechas
data_with_dates = FILTER data BY fecha >= '2022-01-01' AND fecha <= '2022-12-31';

-- 16. Realizar una unión entre dos datasets
union_data = UNION data1, data2;

-- 17. Calcular suma de salarios por ocupación
grouped_by_job = GROUP data BY ocupacion;
salary_sum = FOREACH grouped_by_job GENERATE group AS ocupacion, SUM(data.salario) AS total_salario;

-- 18. Calcular mínimo y máximo de una columna
min_max = FOREACH grouped_data GENERATE group AS edad, MIN(data.salario) AS min_salario, MAX(data.salario) AS max_salario;

-- 19. Filtrar valores mayores a la media
average_salary = FOREACH grouped_data GENERATE AVG(data.salario) AS avg_salario;
data_above_avg = FILTER data BY salario > average_salary;

-- 20. Generar nuevas columnas a partir de condiciones
categorized_salary = FOREACH data GENERATE nombre, (salario > 50000 ? 'Alto' : 'Bajo') AS categoria;

-- 21. Aplicar una transformación matemática a una columna
data_log = FOREACH data GENERATE nombre, LOG(salario) AS salario_log;

-- 22. Concatenar valores de columnas
concatenated = FOREACH data GENERATE CONCAT(nombre, ' - ', ocupacion) AS descripcion;

-- 23. Filtrar valores extremos (outliers)
data_no_outliers = FILTER data BY salario BETWEEN 20000 AND 100000;

-- 24. Dividir dataset en dos partes
split data INTO training IF RANDOM() < 0.7, test IF RANDOM() >= 0.7;

-- 25. Extraer el año de una fecha
year_data = FOREACH data GENERATE SUBSTRING(fecha, 0, 4) AS año;

-- 26. Obtener los primeros 10 registros
top_10 = LIMIT data 10;

-- 27. Convertir tipo de dato
data_cast = FOREACH data GENERATE nombre, (int) edad AS edad_int;

-- 28. Crear un campo de índice incremental
data_indexed = RANK data;

-- 29. Contar cuántas veces aparece cada valor en una columna
grouped_values = GROUP data BY nombre;
value_counts = FOREACH grouped_values GENERATE group AS nombre, COUNT(data) AS total;

-- 30. Aplicar una función personalizada (UDF)
REGISTER 'my_udfs.jar';
DEFINE my_function mypackage.MyFunction();
processed_data = FOREACH data GENERATE my_function(nombre) AS nombre_modificado;

-- 31. Filtrar valores usando una expresión regular
regex_filtered = FILTER data BY nombre MATCHES 'A.*';

-- 32. Ordenar por múltiples columnas
multi_sorted = ORDER data BY edad ASC, salario DESC;

-- 33. Generar un ID único
unique_id_data = FOREACH data GENERATE CONCAT(nombre, '_', (chararray) edad) AS unique_id;

-- 34. Transformar valores categóricos en numéricos
encoded_data = FOREACH data GENERATE (ocupacion == 'Ingeniero' ? 1 : 0) AS ocupacion_binaria;

-- 35. Encontrar el primer y último valor de un grupo
first_last = FOREACH grouped_data GENERATE group AS edad, MIN(data.nombre) AS primer_nombre, MAX(data.nombre) AS ultimo_nombre;

-- 36. Convertir texto en minúsculas
lowercase_names = FOREACH data GENERATE LOWER(nombre) AS nombre;

-- 37. Crear una tabla pivotada
pivoted_data = FOREACH grouped_data GENERATE group AS categoria, COUNT(data) AS total;

-- 38. Filtrar valores únicos
unique_values = DISTINCT data;

-- 39. Generar estadísticas descriptivas
stats = FOREACH grouped_data GENERATE group AS edad, MIN(data.salario) AS min_salario, MAX(data.salario) AS max_salario, AVG(data.salario) AS avg_salario;

-- 40. Convertir fecha en timestamp
data_timestamp = FOREACH data GENERATE ToUnixTime(ToDate(fecha, 'yyyy-MM-dd')) AS timestamp;

-- 41. Contar valores nulos en una columna
null_counts = FOREACH data GENERATE COUNT(nombre IS NULL ? 1 : 0) AS nulos_nombre;

-- 42. Combinar registros duplicados
combined_data = FOREACH grouped_data GENERATE group AS edad, SUM(data.salario) AS total_salario;

-- 43. Extraer subcadenas de texto
substring_data = FOREACH data GENERATE SUBSTRING(nombre, 0, 3) AS prefijo;

-- 44. Normalizar valores entre 0 y 1
normalized_data = FOREACH data GENERATE (salario - MIN(salario)) / (MAX(salario) - MIN(salario)) AS salario_normalizado;

-- 45. Reemplazar caracteres en una cadena
replaced_data = FOREACH data GENERATE REPLACE(nombre, ' ', '_') AS nombre_sin_espacios;

-- 46. Agrupar por múltiples columnas
grouped_multiple = GROUP data BY (edad, ocupacion);

-- 47. Filtrar palabras clave específicas
keyword_filtered = FILTER data BY nombre MATCHES '.*Software.*';

-- 48. Generar valores aleatorios
random_data = FOREACH data GENERATE nombre, RANDOM() AS valor_aleatorio;

-- 49. Extraer parte de un campo de fecha
date_parts = FOREACH data GENERATE GetYear(fecha) AS año, GetMonth(fecha) AS mes;

-- 50. Convertir lista en cadena de texto
flattened = FOREACH grouped_data GENERATE FLATTEN(data);


-- 1. Cargar datos desde un archivo JSON
data_json = LOAD '/home/data.json' USING JsonLoader('id:int, nombre:chararray, edad:int, salario:double');

-- 2. Convertir un campo numérico a string
data_string = FOREACH data GENERATE nombre, (chararray) edad AS edad_str;

-- 3. Filtrar registros con salario dentro de un rango específico
salary_range = FILTER data BY salario >= 30000 AND salario <= 80000;

-- 4. Aplicar una función de ventana para obtener el salario acumulado
ranked_data = RANK data BY salario DESC;

-- 5. Extraer la primera letra de un nombre
first_letter = FOREACH data GENERATE nombre, SUBSTRING(nombre, 0, 1) AS inicial;

-- 6. Dividir un campo de texto en palabras separadas
words = FOREACH data GENERATE TOKENIZE(nombre) AS nombre_palabras;

-- 7. Contar la cantidad de palabras en un campo de texto
word_count = FOREACH words GENERATE nombre, SIZE(nombre_palabras) AS cantidad_palabras;

-- 8. Obtener los valores únicos de una columna específica
unique_jobs = DISTINCT (FOREACH data GENERATE ocupacion);

-- 9. Generar un campo de clasificación basada en rangos de edad
age_category = FOREACH data GENERATE nombre, (edad < 18 ? 'Menor' : (edad <= 60 ? 'Adulto' : 'Senior')) AS categoria;

-- 10. Agrupar datos y obtener la mediana
grouped_data = GROUP data BY edad;
median_salary = FOREACH grouped_data GENERATE group AS edad, MEDIAN(data.salario) AS salario_mediana;

-- 11. Convertir un conjunto de datos en un solo JSON
json_data = FOREACH data GENERATE ToJson(data) AS json;

-- 12. Contar registros distintos en un grupo
distinct_count = FOREACH grouped_data GENERATE group AS edad, COUNT(DISTINCT data.nombre) AS nombres_unicos;

-- 13. Filtrar registros que contengan una palabra específica en una columna
data_filtered = FILTER data BY nombre MATCHES '.*Juan.*';

-- 14. Reemplazar valores en una columna
data_replaced = FOREACH data GENERATE REPLACE(nombre, 'Juan', 'Carlos') AS nombre_modificado;

-- 15. Generar un campo booleano basado en una condición
boolean_data = FOREACH data GENERATE nombre, (salario > 50000 ? 1 : 0) AS es_alto_salario;

-- 16. Transformar una fecha en día de la semana
day_of_week = FOREACH data GENERATE nombre, GetDayOfWeek(fecha) AS dia_semana;

-- 17. Obtener los datos más recientes de un conjunto ordenado
latest_data = LIMIT (ORDER data BY fecha DESC) 10;

-- 18. Crear una matriz de co-ocurrencias entre valores
data_pairs = COGROUP data BY ocupacion, data BY estado;

-- 19. Concatenar valores de una columna agrupada
concatenated_data = FOREACH grouped_data GENERATE group AS edad, CONCAT_WS(',', data.nombre) AS nombres_concatenados;

-- 20. Generar etiquetas personalizadas para intervalos de salario
salary_labels = FOREACH data GENERATE nombre, (salario < 30000 ? 'Bajo' : (salario < 70000 ? 'Medio' : 'Alto')) AS nivel_ingresos;

-- 21. Generar identificadores secuenciales
ranked_names = RANK data BY nombre;

-- 22. Crear un hash único de una fila
hashed_data = FOREACH data GENERATE nombre, MD5_CONCAT(nombre, salario) AS hash_id;

-- 23. Calcular la moda de una columna
data_mode = FOREACH grouped_data GENERATE group AS edad, MODE(data.salario) AS salario_moda;

-- 24. Convertir texto a una lista separada por comas
text_list = FOREACH grouped_data GENERATE group AS ocupacion, FLATTEN(STRSPLIT(data.nombre, ',')) AS nombres_lista;

-- 25. Filtrar registros donde la suma de dos campos supere un valor
filtered_sum = FILTER data BY (edad + salario) > 60000;

-- 26. Obtener registros que contengan caracteres especiales
special_chars = FILTER data BY nombre MATCHES '.*[@#$%^&*()].*';

-- 27. Extraer los dominios de direcciones de correo electrónico
domain_data = FOREACH data GENERATE nombre, REGEX_EXTRACT(email, '.*@(.*)', 1) AS dominio;

-- 28. Obtener la longitud de una cadena
string_length = FOREACH data GENERATE nombre, SIZE(nombre) AS longitud;

-- 29. Contar caracteres específicos dentro de una cadena
char_count = FOREACH data GENERATE nombre, REPLACE(nombre, '[^a]', '') AS letras_a;

-- 30. Transformar un conjunto en una tabla dinámica
pivot_table = FOREACH grouped_data GENERATE group AS categoria, COUNT(data) AS total;

-- 31. Sumar valores de una columna basada en condiciones
data_sum = FOREACH grouped_data GENERATE group AS edad, SUM((data.salario > 40000 ? data.salario : 0)) AS suma_altos;

-- 32. Aplicar una función personalizada a una columna
data_custom = FOREACH data GENERATE nombre, MyUDF(salario) AS salario_modificado;

-- 33. Obtener la desviación estándar de una columna
std_dev_data = FOREACH grouped_data GENERATE group AS edad, STDDEV(data.salario) AS salario_std_dev;

-- 34. Obtener valores únicos y su frecuencia
data_frequency = FOREACH grouped_data GENERATE group AS ocupacion, COUNT(data) AS frecuencia;

-- 35. Convertir un campo en una matriz de tokens
token_matrix = FOREACH data GENERATE nombre, TOKENIZE(ocupacion) AS ocupacion_tokens;

-- 36. Obtener el nombre más frecuente
data_most_frequent = FOREACH grouped_data GENERATE group AS edad, TOP(1, data.nombre);

-- 37. Calcular la covarianza entre dos columnas
covariance_data = FOREACH grouped_data GENERATE group AS edad, COVAR_POP(data.edad, data.salario) AS covarianza;

-- 38. Generar datos de ejemplo con valores aleatorios
data_random = FOREACH data GENERATE nombre, RANDOM() * 100 AS valor_aleatorio;

-- 39. Convertir texto en formato camelCase
camel_case_data = FOREACH data GENERATE nombre, INITCAP(nombre) AS nombre_camel;

-- 40. Obtener registros con valores repetidos
duplicated_data = FILTER data BY COUNT(data) > 1;

-- 41. Clasificar registros en cuartiles
quartile_data = FOREACH data GENERATE nombre, NTILE(4) OVER (ORDER BY salario) AS cuartil;

-- 42. Aplicar transformaciones en cascada
processed_data = FOREACH (FILTER (ORDER data BY salario DESC) BY salario > 30000) GENERATE nombre, salario;

-- 43. Filtrar registros con una longitud específica
data_length_filtered = FILTER data BY SIZE(nombre) > 5;

-- 44. Convertir coordenadas de grados a radianes
radians_data = FOREACH data GENERATE nombre, RADIANS(latitud) AS latitud_rad;

-- 45. Reordenar columnas
data_reordered = FOREACH data GENERATE salario, nombre, edad;

-- 46. Obtener la suma acumulativa de una columna
data_cumsum = FOREACH data GENERATE nombre, SUM_OVER(salario) AS suma_acumulada;

-- 47. Generar un dataset con valores ficticios
mock_data = FOREACH data GENERATE 'Anonimo' AS nombre, RANDOM() * 100000 AS salario_falso;


-- Ejemplos fáciles

-- 1. Cargar datos y mostrar
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
DUMP data;

-- 2. Filtrar datos
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
filtered_data = FILTER data BY age > 30;
DUMP filtered_data;

-- 3. Contar el número de registros
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
count = COUNT data;
DUMP count;

-- 4. Agrupar y contar por categoría
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);
grouped = GROUP data BY city;
city_count = FOREACH grouped GENERATE group AS city, COUNT(data) AS count;
DUMP city_count;

-- 5. Ordenar por columna
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
sorted_data = ORDER data BY age DESC;
DUMP sorted_data;

-- Ejemplos intermedios

-- 1. Cargar datos y aplicar transformaciones
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
transformed_data = FOREACH data GENERATE name, age + 1 AS age;
DUMP transformed_data;

-- 2. Unir dos datasets
data1 = LOAD 'data1.txt' USING PigStorage(',') AS (id:int, name:chararray);
data2 = LOAD 'data2.txt' USING PigStorage(',') AS (id:int, address:chararray);
joined_data = JOIN data1 BY id, data2 BY id;
DUMP joined_data;

-- 3. Calcular la media de una columna
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, score:int);
grouped_data = GROUP data ALL;
average = FOREACH grouped_data GENERATE AVG(data.score);
DUMP average;

-- 4. Filtrar y ordenar por varias columnas
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);
filtered_sorted_data = FILTER data BY age > 30;
sorted_filtered_data = ORDER filtered_sorted_data BY city, age DESC;
DUMP sorted_filtered_data;

-- 5. Operaciones matemáticas y agregaciones
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, sales:double);
grouped = GROUP data BY name;
aggregated_data = FOREACH grouped GENERATE group AS name, SUM(data.sales) AS total_sales;
DUMP aggregated_data;

-- Ejemplos difíciles

-- 1. Uso de UDF (User Defined Function)
DEFINE MyUDF org.apache.pig.piggybank.evaluation.math.Add(0);
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
modified_data = FOREACH data GENERATE name, MyUDF(age, 10) AS updated_age;
DUMP modified_data;

-- 2. Anidar operaciones
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, city:chararray, score:int);
grouped = GROUP data BY city;
top_scores = FOREACH grouped {
    sorted_data = ORDER data BY score DESC;
    top = LIMIT sorted_data 1;
    GENERATE group AS city, top;
}
DUMP top_scores;

-- 3. Combinar varias uniones y filtros
data1 = LOAD 'data1.txt' USING PigStorage(',') AS (id:int, value:chararray);
data2 = LOAD 'data2.txt' USING PigStorage(',') AS (id:int, value:chararray);
data3 = LOAD 'data3.txt' USING PigStorage(',') AS (id:int, value:chararray);
joined1 = JOIN data1 BY id, data2 BY id;
joined2 = JOIN joined1 BY id, data3 BY id;
filtered = FILTER joined2 BY $2 == 'some_value' AND $4 == 'another_value';
DUMP filtered;

-- 4. Uso de funciones de ventana
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int, city:chararray);
grouped = GROUP data BY city;
windowed_data = FOREACH grouped {
    ordered_data = ORDER data BY age DESC;
    top_3 = LIMIT ordered_data 3;
    GENERATE group AS city, top_3;
}
DUMP windowed_data;

-- 5. Generación de un histograma
data = LOAD 'data.txt' USING PigStorage(',') AS (name:chararray, age:int);
grouped = GROUP data BY age;
histogram = FOREACH grouped GENERATE group AS age, COUNT(data) AS count;
DUMP histogram;






-----------------------------------------------------------------------------------



-- Script 1: Cargar datos y realizar una agregación simple por grupo (Count)
-- Cargar datos desde un archivo
data = LOAD 'data.txt' USING PigStorage(',') AS (id:int, nombre:chararray, edad:int);

-- Agrupar los datos por nombre
grouped_data = GROUP data BY nombre;

-- Contar el número de registros por nombre
result = FOREACH grouped_data GENERATE group AS nombre, COUNT(data) AS total_personas;

-- Almacenar el resultado
STORE result INTO 'output' USING PigStorage(',');

-- -----------------------------------------------

-- Script 2: Unir dos conjuntos de datos (JOIN) con filtrado
-- Cargar los datos
clientes = LOAD 'clientes.txt' USING PigStorage(',') AS (id_cliente:int, nombre:chararray, ciudad:chararray);
compras = LOAD 'compras.txt' USING PigStorage(',') AS (id_compra:int, id_cliente:int, monto:double);

-- Unir los conjuntos de datos por el id_cliente
join_data = JOIN clientes BY id_cliente, compras BY id_cliente;

-- Filtrar los resultados para mostrar solo compras superiores a 100
filtrados = FILTER join_data BY monto > 100;

-- Seleccionar las columnas necesarias
resultado = FOREACH filtrados GENERATE clientes::nombre AS nombre_cliente, compras::monto AS monto_compra;

-- Almacenar el resultado
STORE resultado INTO 'output_compras' USING PigStorage(',');

-- -----------------------------------------------

-- Script 3: Análisis de tendencias con ventanas (Windowing)
-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (fecha:chararray, producto:chararray, cantidad:int, precio:double);

-- Calcular la venta total por producto
total_ventas = FOREACH ventas GENERATE producto, (cantidad * precio) AS total_venta;

-- Ordenar por producto y fecha
ordered_ventas = ORDER total_ventas BY producto, fecha;

-- Calcular el total acumulado por producto usando una ventana
windowed_ventas = RANK ordered_ventas BY producto;

-- Filtrar para obtener las ventas superiores a 1000
result = FILTER windowed_ventas BY total_venta > 1000;

-- Almacenar el resultado
STORE result INTO 'output_ventas' USING PigStorage(',');

-- -----------------------------------------------

-- Script 4: Usando un UDF (User Defined Function)
REGISTER 'mylib.jar';

-- Cargar los datos
datos = LOAD 'productos.txt' USING PigStorage(',') AS (producto:chararray, precio:double);

-- Aplicar un UDF para calcular el descuento
descuento = FOREACH datos GENERATE producto, precio, ApplyDiscount(precio) AS precio_descuento;

-- Almacenar el resultado
STORE descuento INTO 'output_descuento' USING PigStorage(',');

-- -----------------------------------------------

-- Script 5: Encontrar las diferencias entre dos conjuntos de datos (Difference)
-- Cargar los datos
set1 = LOAD 'set1.txt' USING PigStorage(',') AS (id:int, nombre:chararray);
set2 = LOAD 'set2.txt' USING PigStorage(',') AS (id:int, nombre:chararray);

-- Encontrar los registros que están en set1 pero no en set2
diferencias = DIFFERENCE set1, set2;

-- Almacenar el resultado
STORE diferencias INTO 'output_diferencias' USING PigStorage(',');

-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, cantidad:int, precio:double);

-- Calcular el total de ventas y el promedio por producto
ventas_agregadas = GROUP ventas BY producto;
resultados = FOREACH ventas_agregadas GENERATE group AS producto, 
                                    AVG(ventas.cantidad) AS promedio_cantidad, 
                                    AVG(ventas.precio) AS promedio_precio;

-- Ordenar los resultados por el promedio de cantidad descendente
resultados_ordenados = ORDER resultados BY promedio_cantidad DESC;

-- Almacenar el resultado
STORE resultados_ordenados INTO 'output_promedio' USING PigStorage(',');



-- Cargar los datos
clientes = LOAD 'clientes.txt' USING PigStorage(',') AS (id_cliente:int, nombre:chararray, ciudad:chararray);

-- Agrupar los datos por el nombre del cliente
grouped_clientes = GROUP clientes BY nombre;

-- Filtrar los registros donde el nombre de cliente se repite más de una vez
duplicados = FILTER grouped_clientes BY COUNT(clientes) > 1;

-- Almacenar el resultado
STORE duplicados INTO 'output_duplicados' USING PigStorage(',');


-- Cargar los datos
clientes = LOAD 'clientes.txt' USING PigStorage(',') AS (id_cliente:int, nombre:chararray, ciudad:chararray);
compras = LOAD 'compras.txt' USING PigStorage(',') AS (id_compra:int, id_cliente:int, monto:double);

-- Realizar una unión externa (outer join) por id_cliente
outer_join = JOIN clientes BY id_cliente LEFT OUTER, compras BY id_cliente;

-- Seleccionar las columnas necesarias y manejar valores nulos
resultado = FOREACH outer_join GENERATE clientes::nombre AS nombre_cliente, 
                                      compras::monto AS monto_compra;

-- Almacenar el resultado
STORE resultado INTO 'output_outer_join' USING PigStorage(',');



-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, cantidad:int, precio:double);

-- Calcular el valor acumulado de las ventas
ventas_ordenadas = ORDER ventas BY precio DESC;
ventas_acumuladas = CUME_DIST() OVER (PARTITION BY producto ORDER BY precio DESC) AS acumulado FROM ventas_ordenadas;

-- Filtrar para mostrar solo los productos con un acumulado superior al 0.5
resultados = FILTER ventas_acumuladas BY acumulado > 0.5;

-- Almacenar el resultado
STORE resultados INTO 'output_acumulado' USING PigStorage(',');


-- Cargar los datos
productos = LOAD 'productos.txt' USING PigStorage(',') AS (id_producto:int, nombre:chararray);
categorias = LOAD 'categorias.txt' USING PigStorage(',') AS (id_categoria:int, nombre_categoria:chararray);

-- Realizar un "Cross Join" entre productos y categorías
cross_join = CROSS productos, categorias;

-- Seleccionar las columnas y generar los resultados
resultado = FOREACH cross_join GENERATE productos::nombre AS nombre_producto, categorias::nombre_categoria AS categoria;

-- Almacenar el resultado
STORE resultado INTO 'output_cross_join' USING PigStorage(',');


-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, clientes:{(id_cliente:int, cantidad:int)});

-- Aplicar FLATTEN para convertir los registros anidados en registros planos
ventas_planas = FOREACH ventas GENERATE producto, FLATTEN(clientes);

-- Almacenar el resultado
STORE ventas_planas INTO 'output_flatten' USING PigStorage(',');


-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, cantidad:int, precio:double);

-- Calcular la correlación entre cantidad y precio
correlacion = CORR(ventas.cantidad, ventas.precio);

-- Mostrar el resultado
DUMP correlacion;


-- Cargar los datos
clientes = LOAD 'clientes.txt' USING PigStorage(',') AS (id_cliente:int, nombre:chararray, edad:int);

-- Dividir el conjunto de datos en dos grupos: uno con clientes mayores de 30 y otro con menores
split clientes by (edad > 30) into grupo_mayores, grupo_menores;

-- Almacenar los resultados
STORE grupo_mayores INTO 'output_mayores' USING PigStorage(',');
STORE grupo_menores INTO 'output_menores' USING PigStorage(',');


-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, cantidad:int, precio:double);

-- Calcular el total de ventas por producto
ventas_totales = GROUP ventas BY producto;
total_ventas = FOREACH ventas_totales GENERATE group AS producto, 
                                   SUM(ventas.cantidad) AS total_cantidad, 
                                   SUM(ventas.precio) AS total_precio;

-- Almacenar el resultado
STORE total_ventas INTO 'output_total_ventas' USING PigStorage(',');


-- Cargar los datos
ventas = LOAD 'ventas.txt' USING PigStorage(',') AS (producto:chararray, cantidad:int, precio:double);

-- Calcular el máximo y mínimo de la cantidad de ventas
max_min = FOREACH (GROUP ventas ALL) GENERATE MAX(ventas.cantidad) AS max_cantidad, MIN(ventas.cantidad) AS min_cantidad;

-- Almacenar el resultado
STORE max_min INTO 'output_max_min' USING PigStorage(',');
