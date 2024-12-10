import json
import pandas as pd
from hdfs import InsecureClient
import joblib

from hdfs_connection import load_country_model, get_hdr_value

def predict(country, year):
    """Predict the number of medals for a specific country in a given year."""
    # Load the model for the specified country
    country_model = load_country_model(country)
    if country_model is None:
        return "No Country data found"
    hdr_value = get_hdr_value(country, year)
    # Prepare the input data for prediction
    X_pred = pd.DataFrame({'Year': [year], 'value': [hdr_value]})

    #You need to load in the hdr data
    #Then find the value for the year
    # and then create a dataframe that looks like the training data
    # Then make prediction
    
    # Make the prediction
    y_pred = country_model.predict(X_pred)
    
    return y_pred

print(predict('USA', 2020))