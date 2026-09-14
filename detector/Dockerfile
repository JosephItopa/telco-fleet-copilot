FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1

COPY detector/requirements.txt ./detector/requirements.txt
RUN pip install --no-cache-dir -r detector/requirements.txt

COPY common ./common
COPY detector ./detector

EXPOSE 9000
CMD ["uvicorn", "detector.main:app", "--host", "0.0.0.0", "--port", "9000"]
