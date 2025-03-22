from pyspark.sql import SparkSession
import json

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Baseball_Data").getOrCreate()

# Cargar el archivo CSV en un DataFrame de Spark
df = spark.read.csv("streamlit/baseball.csv", header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show()

# Crear vista temporal para consultas SQL
df.createOrReplaceTempView("baseball")

# Consulta SQL para jugadores en el Salón de la Fama
query_hof = """
SELECT name, start_year, end_year, hall_of_fame, career_length
FROM baseball
WHERE hall_of_fame = 'Y'
ORDER BY career_length DESC
LIMIT 10
"""
hall_of_fame_players = spark.sql(query_hof)
hall_of_fame_players.show()

# Guardar el resultado en JSON
hall_of_fame_players.write.mode("overwrite").json("streamlit/hall_of_fame_players.json")

# Consulta SQL para jugadores que comenzaron entre 1903 y 1950
query_start_years = """
SELECT name, start_year
FROM baseball
WHERE start_year BETWEEN 1903 AND 1950
ORDER BY start_year
"""
players_1903_1950 = spark.sql(query_start_years)
players_1903_1950.show(20)

# Guardar en JSON
players_1903_1950.write.mode("overwrite").json("results")

# Guardar los resultados en un archivo JSON
results = players_1903_1950.toJSON().collect()
with open('results/data.json', 'w') as file:
    json.dump(results, file)

# Detener la sesión de Spark
spark.stop()