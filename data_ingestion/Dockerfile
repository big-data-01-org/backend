# app/Dockerfile

FROM python:3.9-slim

WORKDIR /app

RUN apt-get update && apt-get install -y

COPY . /app

RUN pip3 install -r requirements.txt

CMD ["python3", "process_hdr.py"]