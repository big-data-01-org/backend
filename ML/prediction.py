import os
import joblib  # For saving models to disk
import pandas as pd
import numpy as np
import json
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from hdfs_connection import load_hdr_data, load_olympic_data, list_files, save_file_to_hdfs


def preprocess_olympic_data(olympic_data):
    deduplicated_data = olympic_data[
        olympic_data['Medal'] != 'No medal'
    ].drop_duplicates(subset=['Year', 'Event', 'Medal', 'NOC'])
    
    all_combinations = pd.MultiIndex.from_product(
        [olympic_data['Year'].unique(), olympic_data['NOC'].unique()],
        names=['Year', 'NOC']
    ).to_frame(index=False)
    
    total_medals = deduplicated_data.groupby(['Year', 'NOC'])['Medal'].count().reset_index(name='Total Medals')
    total_medals = pd.merge(all_combinations, total_medals, on=['Year', 'NOC'], how='left').fillna({'Total Medals': 0})
    
    return total_medals


def merge_data(hdr_data, medal_data):
    merged_data = pd.merge(medal_data, hdr_data, left_on=['Year', 'NOC'], right_on=['year', 'NOC'])
    return merged_data[['Year', 'NOC', 'Total Medals', 'value']]


def train_and_evaluate_models(data):
    models = {}
    metrics_results = {}
    
    for country in data['NOC'].unique():
        print(f"Processing country: {country}")
        country_data = data[data['NOC'] == country]
        
        X_train = country_data[country_data['Year'] < 2016][['Year', 'value']]
        y_train = country_data[country_data['Year'] < 2016]['Total Medals']
        X_test = country_data[country_data['Year'] >= 2016][['Year', 'value']]
        y_test = country_data[country_data['Year'] >= 2016]['Total Medals']
        
        if X_train.empty or X_test.empty:
            print(f"Skipping country {country} due to insufficient data.")
            continue
        
        estimator = create_pipeline()
        estimator.fit(X_train, y_train)
        models[country] = estimator
        
        y_pred = estimator.predict(X_test)
        metrics_results[country] = evaluate_model(y_test, y_pred)
        print_metrics(country, metrics_results[country])
    
    return models, metrics_results


def create_pipeline():
    preprocessor = ColumnTransformer(
        transformers=[
            ('num', StandardScaler(), ['Year', 'value'])
        ]
    )
    return Pipeline([
        ("preprocessor", preprocessor),
        ("polynomial_features", PolynomialFeatures(degree=2)),
        ("linear_regression", LinearRegression())
    ])


def evaluate_model(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_true, y_pred)
    return {"MAE": mae, "MSE": mse, "RMSE": rmse, "R2-score": r2}


def print_metrics(country, metrics):
    print(
        f"Metrics for {country} - (MAE): {metrics['MAE']:.2f}, "
        f"(MSE): {metrics['MSE']:.2f}, (RMSE): {metrics['RMSE']:.2f}, "
        f"R2-score: {metrics['R2-score']:.2f}"
    )


def save_models(models, save_dir="models"):
    """Saves all trained models to the specified directory."""
    os.makedirs(save_dir, exist_ok=True)
    for country, model in models.items():
        model_path = os.path.join(save_dir, f"{country}_model.pkl")
        joblib.dump(model, model_path)
        save_file_to_hdfs(model, f"{country}_model.pkl", file_format='pkl')
        print(f"Saved model for {country} to {model_path}")


def main():
    hdr_data = load_hdr_data()
    olympic_data = load_olympic_data()
    
    total_medals = preprocess_olympic_data(olympic_data)
    merged_data = merge_data(hdr_data, total_medals)
    
    models, metrics_results = train_and_evaluate_models(merged_data)
    
    # Save the models
    save_models(models)
    list_files('/user/root/models')
    
    metrics_df = pd.DataFrame.from_dict(metrics_results, orient='index')
    print(metrics_df)
    
    if 'USA' in metrics_df.index:
        print(metrics_df.loc['USA'])
    else:
        print("No metrics available for USA.")

main()