FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_taxi_data.py .
COPY gcs.json .

ENTRYPOINT ["python", "load_taxi_data.py"]