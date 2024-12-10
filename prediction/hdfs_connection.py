#kubectl port-forward svc/hadoop-namenode 9870:9870

import json
import pandas as pd
from hdfs import InsecureClient
import joblib
from io import BytesIO

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


def load_country_model(country):
    """Load and process data for a specific country from a PKL file in HDFS."""
    file_path = f"{hdfs_path}/models/{country}_model.pkl"
    try:
        with client.read(file_path) as file:
            file_content = BytesIO(file.read())
            country_data = joblib.load(file_content)
        print(country_data)
        return country_data
    except Exception as e:
        print(f"Error loading data for {country} from {file_path}: {e}")
        return None

def load_hdr_data():
    """Load and process HDR data from a static path in HDFS."""
    file_path = f"{hdfs_path}/hdr.json"
    try:
        with client.read(file_path) as file:
            hdr_json_data = [json.loads(line) for line in file]
        hdr_flattened = [
            {
                'country': country_data['country'],
                'year': record['year'],
                'value': record['value'],
                'NOC': country_data['NOC']
            }
            for country_data in hdr_json_data for record in country_data['data']
        ]
        return pd.DataFrame(hdr_flattened)
    except Exception as e:
        print(f"Error loading HDR data from {file_path}: {e}")
        return None
    
def get_hdr_value(country, year):
    """Get the HDR value for a specific country and year."""
    hdr_data = load_hdr_data()
    if hdr_data is None:
        print("HDR data could not be loaded.")
        return None
    
    result = hdr_data[(hdr_data['NOC'] == country) & (hdr_data['year'] == year)]
    if not result.empty:
        return result.iloc[0]['value']
    else:
        print(f"No HDR data found for country: {country} and year: {year}")
        return None



#print("Saving test data to HDFS...")
#save_file_to_hdfs(pd.read_json("./hdr.json"), 'hdr.json', 'json')  
#save_file_to_hdfs(pd.read_csv("./processed_olympics_dataset.csv"), 'olympics.csv', 'csv')        