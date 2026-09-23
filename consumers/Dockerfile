FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY models ./models
COPY config ./config
COPY kafka ./kafka
COPY consumers ./consumers

EXPOSE 9150
CMD ["uvicorn", "consumers.main:app", "--host", "0.0.0.0", "--port", "9150"]
