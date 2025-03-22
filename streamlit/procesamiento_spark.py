from pyspark.sql import SparkSession
import json

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Baseball_Data").getOrCreate()

# Cargar el archivo CSV en un DataFrame de Spark
df = spark.read.csv("streamlit/baseball.csv", header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show()

# Crear una vista temporal para consultas SQL
df.createOrReplaceTempView("baseball")

# Consulta para obtener los jugadores del Salón de la Fama con mayor carrera
query_hall_of_fame = """
SELECT name, start_year, end_year, hall_of_fame, career_length
FROM baseball
WHERE hall_of_fame = 'Y'
ORDER BY career_length DESC
LIMIT 10
"""
hall_of_fame_players = spark.sql(query_hall_of_fame)
hall_of_fame_players.show()

# Consulta para obtener jugadores nacidos entre 1903 y 1950
query_birth_years = """
SELECT name, birth
FROM baseball
WHERE birth BETWEEN '1903-01-01' AND '1950-12-31'
ORDER BY birth
"""
players_1903_1950 = spark.sql(query_birth_years)
players_1903_1950.show(20)

# Guardar ambos DataFrames en formato JSON
hall_of_fame_players.write.mode("overwrite").json("streamlit/hall_of_fame_players.json")
players_1903_1950.write.mode("overwrite").json("results/players_1903_1950.json")

# Convertir el DataFrame de jugadores 1903-1950 a JSON y guardarlo en un archivo local
results = players_1903_1950.toJSON().collect()
with open('results/players_1903_1950.json', 'w') as file:
    json.dump(results, file, indent=4)

# Detener la sesión de Spark
spark.stop()