#import wget
dataset_url="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/VE0IVQ/5VNGY6"
import os
import os.path as osp
import pickle
import pandas as pd
import os
from sqlalchemy import create_engine, inspect, types
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import psycopg2
import json
import ast
from textblob import TextBlob
import numpy as np

args = {"owner": "Airflow", "start_date": days_ago(1)}
dag = DAG(dag_id="etl_dag", default_args=args, schedule_interval=None)


def database_connect():
  connection_uri = "postgresql+psycopg2://{}:{}@{}:{}/itversity_retail_db".format(
      "itversity_retail_user",
      "itversity_retail_user",
      "localhost",
      5432,
  )
  engine = create_engine(connection_uri, pool_pre_ping=True)
  connection = engine.raw_connection()
  #engine.connect()
  return connection

def load_to_db(df, table_name, engine):
   df.to_sql(table_name, engine, if_exists="replace")

def combine_pickle_files(directory_path):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store the merged data

    for file_name in os.listdir(directory_path):
        if file_name.endswith('.pkl'):
            file_path = osp.join(directory_path, file_name)
            with open(file_path, 'rb') as f:
                content =  pd.read_pickle(f) #pickle.load(f)
                if isinstance(content, pd.DataFrame):
                    combined_df = pd.concat([combined_df, content], ignore_index=True)
    return combined_df

def twitch_etl():
    directory_path = 'ICWSM19_data/'
    df_twitch=combine_pickle_files(directory_path)
    db_engine = database_connect()

    #raw_df = read_data(dataset)
    #clean_df = transform_data(raw_df)

    #raw_db_table_name = "raw_survey_dataset"
    clean_db_table_name = "twitch_data"

    #load_to_db(raw_df, raw_db_table_name, db_engine)
    load_to_db(df_twitch, clean_db_table_name, db_engine)

    #check_table_exists(raw_db_table_name, db_engine)
    #check_table_exists(clean_db_table_name, db_engine)
    db_engine.dispose()

# Example usage:
#directory_path = 'ICWSM19_data/'
#df_twitch=combine_pickle_files(directory_path)
#print(df_twitch.head)

#filename = wget.download(dataset_url)
#df = pd.read_csv(dataset_url)

def extract_text_emoticon(fragment_list):
    text = ""
    emoticon_id = ""
    print(fragment_list)
    for item in fragment_list:
        if isinstance(item, dict):  # Make sure it's a dictionary
            if "text" in item:
                text += item["text"] + " "
            if "emoticon_id" in item:
                emoticon_id += item["emoticon_id"] + " "
    
    return text.strip(), emoticon_id.strip()

# If fragments2 contains strings, convert them to list

#twitch_etl()
directory_path = 'ICWSM19_data/'
pd.set_option('display.max_columns', None)
df_twitch=combine_pickle_files(directory_path)

df_twitch1 = pd.concat([pd.DataFrame(x) for x in df_twitch['fragments']], keys=df_twitch.index).reset_index(level=1,drop=True)
df_twitch = df_twitch.drop('fragments', axis=1).join(df_twitch1).reset_index(drop=True)

#df_twitch = pd.concat([pd.DataFrame(x) for x in df_twitch['fragments']], keys=df_twitch['body']).reset_index(level=1, drop=True).reset_index()
#df_twitch['fragments2'] = df_twitch['fragments'].apply(lambda x: eval(x) if isinstance(x, str) else x)

#df_twitch['text'], df_twitch['emoticon_id'] = zip(*df_twitch['fragments'].apply(lambda x: extract_text_emoticon(ast.literal_eval(x))))

#df_twitch['fragments2'] = df_twitch['fragments'].apply(json.dumps)
#print(df_twitch)

df_commenters=df_twitch[["commenter_id","commenter_type"]].drop_duplicates()
#print(df_commenters)

#df_twitch.to_csv("fact_output.csv")  
#df_commenters.to_csv("commenters_output.csv")  
# Set data types
dtype_dic_chat = {'body': types.String(), 'channel_id': types.INTEGER(),'commenter_id': types.INTEGER(),'created_at': types.DateTime(), 'offset':types.DECIMAL, 'updated_at':types.DateTime(), 'video_id':types.INTEGER, 'emoticon_id':types.INTEGER, 'text':types.String(), 'unique_id':types.INTEGER, 'polarity':types.FLOAT}

df_twitch['polarity']=np.nan
#for index, row in df_twitch.iterrows():
#    # Create an object
#    obj = TextBlob(row['body'])
#    # detect sentiment
#    sentiment = obj.sentiment
#    # Get polarity
#    polarity = sentiment.polarity
#    # Print polarity and subjetivity
#    #row[['polarity']]=float(polarity)
#    df_twitch.loc[index,'polarity']  = float(polarity)

#df_twitch['polarity'] = df_twitch.apply(lambda x: (TextBlob(x['body']).sentiment.polarity), axis=1)
#print(df_twitch)

#df_twitch.to_csv("chat_fact.csv")
print(df_twitch['channel_id'].unique())