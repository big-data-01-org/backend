from fastapi import FastAPI
import uvicorn
from predictor import predict

app = FastAPI()


@app.get("/predict")
def get_prediction(country: str, year: int):
    return {"result": predict(country, year)}

if __name__ == "__main__":
    
    uvicorn.run(app, host="127.0.0.1", port=8000)
