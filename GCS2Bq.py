#Importing all the necessary packages and operators
import os
import pandas as pd
import dask.dataframe as dd
import json
import datetime
from textblob import TextBlob
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'Twitter_dataset')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'Covid_delta_tweets')

dag = models.DAG(
    dag_id='storage_to_bq',
    start_date=days_ago(1),
    schedule_interval='@daily',
    tags=['twitter_data_transfer']
)

def sentiment_calc(text):
    try:
        return TextBlob(text).sentiment.polarity
    except:
        return None
    
    
# Function to concatanate all the csv files and perform necessary transformations
def transform_data():
    #Read .CSV files into dask dataframe
    dask_df = dd.read_csv('gs://akshat_egen_bucket1/twitter_message/*.csv', dtype={'Place': 'object'})
    df = dask_df.compute()


    df['Tweet_time'] = pd.to_datetime(df['Tweet_time'])
    df['Tweet_time'] = pd.to_datetime(df["Tweet_time"].dt.strftime("%m-%d-%Y %H:%M:%S"))
   
 #Cleaning tweet_text, user_name and tweet_source columns
    User_name = df['User_name']
    User_name = User_name.str.lower()
    User_name = User_name.str.replace(r'[^A-Za-z0-9 ]', '').replace(r'[^\x00-\x7F]+', '') 
    User_name = User_name.str.replace('#', '').replace('_', ' ').replace('@', '')
    df['User_name'] = User_name
    
    tweet_string= df['text']
    tweet_string = tweet_string.str.lower()
    tweet_string = tweet_string.str.replace(r'[^A-Za-z0-9 ]', '').replace(r'[^\x00-\x7F]+', '') 
    tweet_string = tweet_string.str.replace(r'https?://[^\s<>"]+|www\.[^\s<>"]+', "")
    tweet_string = tweet_string.str.replace('#', '').replace('_', ' ').replace('@', '')
    df['text'] = tweet_string
    
    tweet_source = df['Tweet_source']
    tweet_source = tweet_source.str.extract(r'<.+>([\w\s]+)<.+>', expand = False) # Extacting text from HTML tag
    df['Tweet_source'] = tweet_source

    
    #Sentiment Analysis of tweets

    df['sentiment_polarity'] = df['text'].apply(sentiment_calc)
    df.loc[df['sentiment_polarity'] > 0, 'Sentiment'] = "Positive"
    df.loc[df['sentiment_polarity'] == 0, 'Sentiment'] = "Neutral"
    df.loc[df['sentiment_polarity'] < 0, 'Sentiment'] = "Negative"
    df.drop("sentiment_polarity", axis=1, inplace= True)

    
    df.to_csv('gs://akshat_egen_bucket1/transformed_data/transformed.csv', index=False)
    
# Calling the transform function into a Python operator
transform_csv = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_data,
    dag=dag
    )

# Create a dataset in BigQuery

create_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_bq_dataset', dataset_id=DATASET_NAME, dag=dag
)

# Loading the transformed csv file into BQ Table

load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bq',
    bucket='akshat_egen_bucket1',
    source_objects=['transformed_data/transformed.csv'],
    destination_project_dataset_table= f"{DATASET_NAME}.{TABLE_NAME}",
    
    schema_fields=[

        {'name': 'Tweet_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Tweet_time', 'type': 'DATETIME', 'mode': 'NULLABLE'},
        {'name': 'Tweet_source', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'User_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'User_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Place', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'User_follower_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'User_friend_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'Tweet_text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'Tweet_Sentiment', 'type': 'STRING', 'mode': 'NULLABLE'}],

    write_disposition='WRITE_TRUNCATE',
    skip_leading_rows = 1,
    dag=dag
)


delete_dataset = BigQueryDeleteDatasetOperator(task_id='delete_bq_dataset', dataset_id=DATASET_NAME, delete_contents=True, dag=dag)

# Setting up the task sequence for the DAG
transform_csv >> create_dataset >> load_csv

