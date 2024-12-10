from fastapi import FastAPI
import uvicorn

app = FastAPI()

def predict(country: str, year: int) -> str:
    # Dummy prediction logic
    return f"Prediction for {country} in {year}"

@app.get("/predict")
def get_prediction(country: str, year: int):
    return {"result": predict(country, year)}

if __name__ == "__main__":
    
    uvicorn.run(app, host="127.0.0.1", port=8000)
