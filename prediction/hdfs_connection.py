#kubectl port-forward svc/hadoop-namenode 9870:9870

import json
import pandas as pd
from hdfs import InsecureClient
import joblib

# Connect to the HDFS server
hdfs_url = 'http://hadoop-namenode:9870'  # HDFS NameNode URL
user = 'root'  # Replace with your HDFS user
client = InsecureClient(hdfs_url, user=user)


def list_files(path):
    """List files in the specified HDFS path."""
    try:
        files = client.list(path)
        print(f"Files in {path}:", files)
    except Exception as e:
        print(f"Error listing files in {path}:", e)

# Static path for HDFS
hdfs_path = "/user/root"  # Replace with the fixed HDFS path


def load_country_data(country):
    """Load and process data for a specific country from a PKL file in HDFS."""
    file_path = f"{hdfs_path}/models/{country}_model.pkl"
    try:
        with client.read(file_path) as file:
            country_data = joblib.load(file)
        return country_data
    except Exception as e:
        print(f"Error loading data for {country} from {file_path}: {e}")
        return None



#print("Saving test data to HDFS...")
#save_file_to_hdfs(pd.read_json("./hdr.json"), 'hdr.json', 'json')  
#save_file_to_hdfs(pd.read_csv("./processed_olympics_dataset.csv"), 'olympics.csv', 'csv')        