FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY models ./models
COPY config ./config
COPY kafka ./kafka
COPY seed_k8s ./seed_k8s

CMD ["python", "-m", "seed_k8s.main"]
