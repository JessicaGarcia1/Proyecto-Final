from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Baseball_Data").getOrCreate()

# Cargar el archivo CSV en un DataFrame de Spark
df = spark.read.csv("streamlit/baseball.csv", header=True, inferSchema=True)

# Mostrar las primeras filas del DataFrame
df.show()

# Realizar una consulta SQL (como ejemplo, se seleccionan ciertos jugadores)
df.createOrReplaceTempView("baseball")  # Crear vista temporal para consultas SQL
query = """
SELECT name, start_year, end_year, hall_of_fame, career_length
FROM baseball
WHERE hall_of_fame = 'Y'
ORDER BY career_length DESC
LIMIT 10
"""
# Ejecutar la consulta SQL
hall_of_fame_players = spark.sql(query)

# Mostrar el resultado de la consulta
hall_of_fame_players.show()

# Guardar el DataFrame procesado en formato JSON
hall_of_fame_players.write.mode("overwrite").json("streamlit/hall_of_fame_players.json")

# Detener la sesión de Spark
spark.stop()