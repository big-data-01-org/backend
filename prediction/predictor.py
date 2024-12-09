import json
import pandas as pd
from hdfs import InsecureClient
import joblib

from hdfs_connection import load_country_data

def predict(country, year):
    """Predict the number of medals for a specific country in a given year."""
    # Load the model for the specified country
    country_data = load_country_data(country)
    if country_data is None:
        return None
    
    # Prepare the input data for prediction
    X_pred = pd.DataFrame({'Year': [year], 'value': [country_data['value'].iloc[-1]]})
    
    # Make the prediction
    model = country_data['model'].iloc[-1]
    y_pred = model.predict(X_pred)
    
    return y_pred[0]

print(predict('USA', 2020))