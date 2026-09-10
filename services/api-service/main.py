import os
import time
import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="AIOps API Service")

PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://localhost:8001")

REQUESTS = Counter("api_requests_total", "Total API requests", ["endpoint", "status"])
LATENCY = Histogram("api_request_latency_seconds", "API request latency", ["endpoint"])
DEPENDENCY_FAILURES = Counter("api_dependency_failures_total", "Payment dependency failures")
HEALTH = Gauge("api_health", "API health: 1 healthy, 0 unhealthy")


@app.on_event("startup")
def startup():
    HEALTH.set(1)


@app.get("/")
def root():
    return {"service": "api-service", "status": "running"}


@app.get("/health")
def health():
    HEALTH.set(1)
    return {"service": "api-service", "status": "healthy"}


@app.get("/call-payment")
def call_payment():
    start = time.perf_counter()
    try:
        response = requests.get(f"{PAYMENT_SERVICE_URL}/pay", timeout=10)
        status = str(response.status_code)
        REQUESTS.labels("/call-payment", status).inc()
        return {
            "service": "api-service",
            "dependency": "payment-service",
            "payment_status": response.status_code,
            "latency_ms": round((time.perf_counter() - start) * 1000, 2),
            "payment_response": response.json(),
        }
    except requests.RequestException as exc:
        DEPENDENCY_FAILURES.inc()
        REQUESTS.labels("/call-payment", "503").inc()
        return JSONResponse(status_code=503, content={
            "service": "api-service",
            "dependency": "payment-service",
            "status": "dependency_failure",
            "latency_ms": round((time.perf_counter() - start) * 1000, 2),
            "error": str(exc),
        })
    finally:
        LATENCY.labels("/call-payment").observe(time.perf_counter() - start)


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
