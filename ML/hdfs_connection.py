#kubectl port-forward svc/hadoop-namenode 9870:9870

import json
import pandas as pd
from hdfs import InsecureClient
import joblib

# Connect to the HDFS server
hdfs_url = 'http://localhost:9870'  # HDFS NameNode URL
user = 'root'  # Replace with your HDFS user
client = InsecureClient(hdfs_url, user=user)

# Example: List files in the root directory
print("Listing files in the root directory...")
try:
    files = client.list('/user/root')
    print("Files in root directory:", files)
except Exception as e:
    print("Error connecting to HDFS:", e)

# Static path for HDFS
hdfs_path = "/user/root"  # Replace with the fixed HDFS path

def load_hdr_data(client):
    """Load and process HDR data from a static path in HDFS."""
    file_path = f"{hdfs_path}/hdr.json"
    try:
        with client.read(file_path) as file:
            hdr_json_data = json.load(file)
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

def load_olympic_data(client):
    """Load Olympic data from a static path in HDFS."""
    file_path = f"{hdfs_path}/olympics.csv"
    try:
        with client.read(file_path) as file:
            olympic_data = pd.read_csv(file)
        return olympic_data
    except Exception as e:
        print(f"Error loading Olympic data from {file_path}: {e}")
        return None
    
def save_file_to_hdfs(client, df, file_name, file_format='csv'):
    """
    Save a DataFrame to HDFS in the specified format.

    Args:
        client (InsecureClient): HDFS client instance.
        df (pd.DataFrame): DataFrame to save.
        file_name (str): Name of the file (e.g., 'output.pkl').
        file_format (str): Format to save the file ('csv', 'json', or 'pkl'). Defaults to 'csv'.
    """
    file_path = f"{hdfs_path}/{file_name}"
    try:
        with client.write(file_path, overwrite=True) as file:
            if file_format == 'csv':
                df.to_csv(file, index=False)
            elif file_format == 'json':
                df.to_json(file, orient='records', lines=True)
            elif file_format == 'pkl':
                joblib.dump(df, file)
            else:
                raise ValueError("Unsupported file format. Use 'csv', 'json', or 'pkl'.")
        print(f"File saved successfully to {file_path}")
    except Exception as e:
        print(f"Error saving file to {file_path}: {e}")

test_data = pd.DataFrame({

    'country': ['USA', 'USA', 'USA', 'USA', 'USA'],
    'year': [2010, 2011, 2012, 2013, 2014],
    'value': [10, 20, 30, 40, 50],
    'NOC': ['USA', 'USA', 'USA', 'USA', 'USA']
})

print("Saving test data to HDFS...")
save_file_to_hdfs(client, test_data, 'testData.json', 'json')        