#import wget
#dataset_url="https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/VE0IVQ/5VNGY6"
import os
import os.path as osp
import pandas as pd
import os
from sqlalchemy import create_engine, types
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import numpy as np
from textblob import TextBlob

args = {"owner": "Airflow", "start_date": days_ago(1)}
dag = DAG(dag_id="etl_dag", default_args=args, schedule_interval=None)


def database_connect():
  connection_uri = "postgresql+psycopg2://{}:{}@{}:{}".format(
      "airflow",
      "airflow",
      "postgres",
      5432,
  )
  engine = create_engine(connection_uri, pool_pre_ping=True)
 
  engine.connect()
  return engine

def load_to_db(df, table_name, engine, dtype_dic, primary_key):
   df.to_sql(table_name, engine, if_exists="replace", dtype=dtype_dic)
   engine.execute(f'alter table {table_name} add primary key({primary_key})')
   

def add_foreign_keys(engine):
    engine.execute(f'alter table chat_fact ADD CONSTRAINT fk_chat_commenters FOREIGN KEY (commenter_id) REFERENCES commenter_dimension (commenter_id)')


def combine_pickle_files(directory_path):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store the merged data
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.pkl'):
            file_path = osp.join(directory_path, file_name)
            with open(file_path, 'rb') as f:
                content =  pd.read_pickle(f)
                combined_df = pd.concat([combined_df, content], ignore_index=True)
    return combined_df


def twitch_etl():
    directory_path = 'ICWSM19_data/'
    df_twitch=combine_pickle_files(directory_path)
    df_twitch1 = pd.concat([pd.DataFrame(x) for x in df_twitch['fragments']], keys=df_twitch.index).reset_index(level=1,drop=True)
    df_twitch = df_twitch.drop('fragments', axis=1).join(df_twitch1).reset_index(drop=True)
    
    df_commenters=df_twitch[["commenter_id","commenter_type"]].drop_duplicates()
    df_twitch = df_twitch.drop('commenter_type', axis=1)
    df_twitch['unique_id'] = df_twitch.index
    df_twitch['polarity'] = df_twitch.apply(lambda x: (TextBlob(x['body']).sentiment.polarity), axis=1)

    chat_db_table_name = "chat_fact"
    commenter_db_table_name="commenter_dimension"


    dtype_dic_chat = {'body': types.String(), 'channel_id': types.INTEGER(),'commenter_id': types.INTEGER(),'created_at': types.DateTime(), 'offset':types.DECIMAL, 'updated_at':types.DateTime(), 'video_id':types.INTEGER, 'emoticon_id':types.INTEGER, 'text':types.String(), 'unique_id':types.INTEGER, 'polarity':types.FLOAT}
    dtype_dic_commenter = {'commenter_id': types.INTEGER(), 'commenter_type': types.String()}


    db_engine = database_connect()

    load_to_db(df_twitch, chat_db_table_name, db_engine, dtype_dic_chat,'unique_id')
    load_to_db(df_commenters, commenter_db_table_name, db_engine, dtype_dic_commenter,'commenter_id')

    add_foreign_keys(db_engine)

    db_engine.dispose()

with dag:
    run_etl_task = PythonOperator(task_id="run_etl_task", python_callable=twitch_etl)
    run_etl_task