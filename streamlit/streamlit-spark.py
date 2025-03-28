import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

# Initialize connection. Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])
  

client = init_connection()

# Pull data from the collection. Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data():
    db = client.Baseball_players
    items = db.Baseball_players.find()
    items = list(items)  # make hashable for st.cache_data
    return items


def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
      "event_type": job,
    "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
      }
   }


    headers = {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app
    st.write(response)

st.title("Spark & streamlit")

st.header("spark-submit Job")

github_user  =  st.text_input('Github user', value='JessicaGarcia1')
github_repo  =  st.text_input('Github repo', value='Proyecto-Final')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='***')
code_url     =  st.text_input('Code URL', value='')
dataset_url  =  st.text_input('Dataset URL', value='')


if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        st.write(response.json())

st.header("spark-submit results")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/JessicaGarcia1/Proyecto-Final/refs/heads/main/results/data.json')

if st.button("GET spark results"):
    get_spark_results(url_results)

if st.button("Query mongodb collection"):
    items = get_data()

    # Print results.
    for item in items:
        st.write(item)

# Initialize connection.
conn = st.connection("postgresql", type="sql")

if st.button("Query Postgresql table"):
  # Perform query.
  df = conn.query('SELECT * FROM baseball_players;', ttl="10m")
  print(df)
  # Print results.
  for row in df.itertuples():
    st.write(row)
