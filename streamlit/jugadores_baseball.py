import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

# Cargar el dataset
df = pd.read_csv('baseball.csv')

# Título del dashboard
st.title("Jugadores de Béisbol")

# Muestra las primeras filas del dataset
st.subheader("Primeros datos")
st.write(df.head())

# Filtro para seleccionar un jugador
player_name = st.selectbox("Selecciona un jugador", df['name'].unique())
player_data = df[df['name'] == player_name]

# Muestra los detalles del jugador seleccionado
st.subheader(f"Detalles de {player_name}")
st.write(player_data)

# Gráfico de carrera de jugadores por años de inicio y fin
st.subheader("Carrera de Jugadores")
fig, ax = plt.subplots()
ax.bar(df['name'], df['career_length'])
ax.set_xlabel('Jugador')
ax.set_ylabel('Años de carrera')
ax.set_title('Carrera de Jugadores de Béisbol')
st.pyplot(fig)

# Filtro para ver jugadores que están en el Hall of Fame
hall_of_fame = st.checkbox("Mostrar solo jugadores en el Hall of Fame")
if hall_of_fame:
    df = df[df['hall_of_fame'] == 'Y']

# Muestra el dataset filtrado
st.subheader("Jugadores Filtrados")
st.write(df)
