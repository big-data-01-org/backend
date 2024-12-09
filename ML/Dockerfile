# app/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y

COPY ML .

RUN pip3 install -r requirements.txt

CMD ["python3", "prediction.py"]
