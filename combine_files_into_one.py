import pandas as pd
import os
import os.path as osp

def combine_pickle_files(directory_path):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame to store the merged data
    for file_name in os.listdir(directory_path):
        if file_name.endswith('.pkl'):
            file_path = osp.join(directory_path, file_name)
            with open(file_path, 'rb') as f:
                content =  pd.read_pickle(f)
                combined_df = pd.concat([combined_df, content], ignore_index=True)
                print('file processed',file_name)
    combined_df.to_parquet('ICWSM19_data/combined/df.parquet.gzip',compression='gzip') 
    return combined_df

combine_pickle_files("ICWSM19_data")