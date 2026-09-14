FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY reasoning_worker/requirements.txt ./reasoning_worker/requirements.txt
RUN pip install --no-cache-dir -r reasoning_worker/requirements.txt

COPY common ./common
COPY reasoning_worker ./reasoning_worker

EXPOSE 9100
CMD ["uvicorn", "reasoning_worker.main:app", "--host", "0.0.0.0", "--port", "9100"]
