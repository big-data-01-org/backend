import csv
import json
import os
import pandas as pd

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

def iso_csv_to_noc_json(input_csv, output_json, noc_csv):
    """
    Reads a CSV file, parses it into a nested JSON format, and replaces countryIsoCode
    with the corresponding NOC code before saving to a JSON file.
    
    :param input_csv: Path to the input CSV file.
    :param output_json: Path to save the output JSON file.
    :param noc_csv: Path to the CSV file containing the NOC-countryIsoCode mapping.
    """
    try:
        # Load the NOC-countryIsoCode mapping into a dictionary
        noc_df = pd.read_csv(noc_csv)
        noc_mapping = dict(zip(noc_df["countryIsoCode"], noc_df["NOC"]))
        
        # Load the input CSV file
        df = pd.read_csv(input_csv)

        # Check if required columns are present
        required_columns = {"countryIsoCode", "country", "year", "value"}
        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise KeyError(f"Missing required columns: {', '.join(missing)}")
        
        # Replace countryIsoCode with NOC in place
        df["countryIsoCode"] = df["countryIsoCode"].map(noc_mapping)

        # Group by countryIsoCode and country, then aggregate year and value
        result = (
            df.groupby(["countryIsoCode", "country"])
            .apply(lambda x: [{"year": row["year"], "value": row["value"]} for _, row in x.iterrows()])
            .reset_index(name="data")
        )

        # Convert to a list of dictionaries and iterate to rename fields
        json_data = result.to_dict(orient="records")
        for record in json_data:
            # Iterate over each record and rename the field
            record["NOC"] = record.pop("countryIsoCode")  # Renaming the field if needed
            # Alternatively, you could rename the field here, but it's already replaced.

        # Save the JSON data to a file
        with open(output_json, "w") as json_file:
            json.dump(json_data, json_file, indent=4)
        
        print(f"Nested JSON saved to: {output_json}")
    except FileNotFoundError:
        print(f"Error: The file {input_csv} or {noc_csv} does not exist.")
    except KeyError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def generate_iso_to_noc():
    iso_df = pd.read_csv("./generated_hdr_files/ISO_codes.csv")
    noc_df = pd.read_csv("./generated_hdr_files/NOC_codes.csv")
    """
    Joins two DataFrames on 'country' and 'Team', and keeps 'NOC', 'countryIsoCode', and 'country'.

    :param df1: First DataFrame containing 'countryIsoCode' and 'country'.
    :param df2: Second DataFrame containing 'NOC' and 'Team'.
    :return: A DataFrame with the joined data.
    """

    # Perform the join on 'country' and 'Team'
    joined_df = pd.merge(
        iso_df, 
        noc_df, 
        left_on="country", 
        right_on="Team", 
        how="inner"  # Inner join to only keep matching rows
    )

    joined_df.to_csv("./generated_hdr_files/noc_iso_country.csv", columns=["NOC", "countryIsoCode", "country"])

def handle_name_edge_cases():
    edge_cases = [
        #left is ISO name, right is NOC name. we should convert names in the iso
        #codes file to NOC if they are in this edge case list
        ["Bolivia (Plurinational State of)","Bolivia"],
        ["Congo (Democratic Republic of the)", "Congo"],
        ["Micronesia (Federated States of)","Federated States of Micronesia"],
        ['"Hong Kong, China (SAR)"',"Hong Kong"],
        ["Iran (Islamic Republic of)","Iran"],
        ["Lao People's Democratic Republic", "Laos"],
        ["Korea (Republic of)","South Korea"],
        ["Moldova (Republic of)", "Moldova"],
        ["Eswatini (Kingdom of)", "Eswatini"],
        ["Tanzania (United Republic of)", "Tanzania"],
        ["Venezuela (Bolivarian Republic of)","Venezuela"],
        ['"Palestine, State of"', "Palestine"],
        ["Syrian Arab Republic","Syria"],
        ["Timor-Leste","Timor Leste"],
        ["Guinea-Bissau", "Guinea Bissau"]
    ]
    ISO_file = "./generated_hdr_files/ISO_codes.csv"
    ISO_codes = pd.read_csv(ISO_file)
    corrected_data: list = []
    for index, row in ISO_codes.iterrows():
        name = row["country"]
        for edge_case in edge_cases:
            if name == edge_case[0]:
                name = edge_case[1]
        datapoint: dict = {"countryIsoCode": row["countryIsoCode"], "country":name}
        corrected_data.append(datapoint)
    with open(ISO_file, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["countryIsoCode", "country"])
        writer.writeheader()  # Write the header row
        writer.writerows(corrected_data)  # Write the data rows
        
def main():
    os.makedirs("./generated_hdr_files", exist_ok=True)    
    fields = ["countryIsoCode","country","year","value"]
    filter_csv("./hdr_data.csv", "./generated_hdr_files/cleaned_hdr_data.csv", fields)
    filter_csv("./hdr_data.csv", "./generated_hdr_files/ISO_codes.csv", ["countryIsoCode","country"])
    handle_name_edge_cases()
    filter_csv("./olympics_dataset.csv", "./generated_hdr_files/NOC_codes.csv", ["NOC","Team"])
    generate_iso_to_noc()
    iso_csv_to_noc_json("./generated_hdr_files/cleaned_hdr_data.csv", "./generated_hdr_files/hdr.json", "./generated_hdr_files/noc_iso_country.csv")

main()
