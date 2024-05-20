# from pydrive.auth import GoogleAuth
# from pydrive.drive import GoogleDrive

# def authenticate_with_google_drive():
#     gauth = GoogleAuth()
#     gauth.LocalWebserverAuth()  # Opens a browser window for authentication
#     drive = GoogleDrive(gauth)
#     return drive


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 19),
}

# Initialize the DAG
dag = DAG(
    'dvc_and_github_integration',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
)

def extract_data():
    logging.info("Extracting data...")
    urls = ['https://www.dawn.com/', 'https://www.bbc.com/']
    extracted_data = []
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        titles = [title.text for title in soup.find_all('h1')]
        extracted_data.append({'url': url, 'links': links, 'titles': titles})
    df = pd.DataFrame(extracted_data)
    df.to_csv('extracted_data.csv', index=False)
    logging.info("Data extraction complete.")

def transform_data():
    logging.info("Transforming data...")
    df = pd.read_csv('extracted_data.csv')
    df['cleaned_titles'] = df['titles'].apply(lambda x: x.strip().lower())
    df.to_csv('transformed_data.csv', index=False)
    logging.info("Data transformation complete.")


# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)


# Define the task dependencies
extract_task >> transform_task 