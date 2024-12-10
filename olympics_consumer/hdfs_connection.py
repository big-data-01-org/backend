#kubectl port-forward svc/hadoop-namenode 9870:9870

import json
import pandas as pd
from hdfs import InsecureClient
import joblib

# Connect to the HDFS server
hdfs_url = 'http://hadoop-namenode:9870'  # HDFS NameNode URL
user = 'root'  # Replace with your HDFS user
client = InsecureClient(hdfs_url, user=user)

# Static path for HDFS
hdfs_path = "/user/root"  # Replace with the fixed HDFS path

def append_to_olympics_csv(csv_string):
    """
    Append new data from a CSV string to the olympics.csv file in HDFS, avoiding duplicates.

    Args:
        csv_string (str): CSV data in string format to be appended.
    """
    file_path = f"{hdfs_path}/olympics.csv"
    try:
        # Read the existing data from HDFS
        with client.read(file_path) as file:
            olympic_data = pd.read_csv(file)
        
        # Create a DataFrame from the new CSV string
        new_data = pd.read_csv(pd.compat.StringIO(csv_string))
        
        # Append the new data to the existing DataFrame
        updated_data = pd.concat([olympic_data, new_data], ignore_index=True)
        
        # Remove duplicate rows
        updated_data = updated_data.drop_duplicates()
        
        # Save the updated DataFrame back to HDFS
        with client.write(file_path, overwrite=True) as file:
            updated_data.to_csv(file, index=False)
        
        print(f"Data successfully appended to {file_path} (duplicates removed)")
    except Exception as e:
        print(f"Error appending data to olympics.csv: {e}")


#print("Saving test data to HDFS...")
#save_file_to_hdfs(pd.read_json("./hdr.json"), 'hdr.json', 'json')  
#save_file_to_hdfs(pd.read_csv("./processed_olympics_dataset.csv"), 'olympics.csv', 'csv')        