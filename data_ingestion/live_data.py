import time
import pandas as pd
from producer import KafkaProducer

def filter_csv(input_file, output_file, fields_to_keep):
    """
    Reads a CSV file, filters specific fields, and writes the result to a new CSV file.

    :param input_file: Path to the input CSV file.
    :param output_file: Path to save the output CSV file.
    :param fields_to_keep: List of field names to keep in the output file.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(input_file)
        
        # Filter the DataFrame to keep only the specified fields
        filtered_df = df[fields_to_keep]
        filtered_df = filtered_df.drop_duplicates()
        # Save the filtered DataFrame to a new CSV file
        filtered_df.to_csv(output_file, index=False)
        print(f"Filtered CSV saved to: {output_file}")
    except FileNotFoundError:
        print(f"Error: The file {input_file} does not exist.")
    except KeyError as e:
        print(f"Error: One or more fields do not exist in the CSV file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def simulate_live_data(after:int, before: int):
    producer = KafkaProducer()
    df = pd.read_csv("./processed_olympics_dataset.csv")
    for index, row in df.iterrows():
        if row["Year"] <= before and row["Year"] >after:
            message = str(row["NOC"])+","+str(row["Year"])+","+str(row["Event"])+","+str(row["Medal"])
            producer.produce_message("olympics",message)
            time.sleep(3)

def main():
    filter_csv("./olympics_dataset.csv","./processed_olympics_dataset.csv", ["NOC","Year","Event","Medal"])
    #create kafka events for everything after 2016 (olympics 2020) and before 2024
    while True:
        simulate_live_data(2016,2020)

main()