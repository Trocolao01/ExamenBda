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
