FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY models ./models
COPY config ./config
COPY database ./database
COPY detectors ./detectors

EXPOSE 9200
CMD ["uvicorn", "detectors.main:app", "--host", "0.0.0.0", "--port", "9200"]
