import random
import time
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

app = FastAPI(title="AIOps Payment Service")

failure_mode = "normal"

REQUESTS = Counter("payment_requests_total", "Total payment requests", ["status"])
LATENCY = Histogram("payment_request_latency_seconds", "Payment latency")
FAILURES = Counter("payment_failures_total", "Payment failures", ["failure_mode"])
HEALTH = Gauge("payment_health", "Payment health: 1 healthy, 0 unhealthy")
FAILURE_SWITCH = Gauge("payment_failure_switch", "Current failure mode as numeric state")


def failure_value(mode: str) -> int:
    return {
        "normal": 0,
        "high_latency": 1,
        "error_spike": 2,
        "service_down": 3,
        "high_cpu": 4,
        "high_memory": 5,
    }.get(mode, -1)


@app.on_event("startup")
def startup():
    HEALTH.set(1)
    FAILURE_SWITCH.set(0)


@app.get("/")
def root():
    return {"service": "payment-service", "status": "running"}


@app.get("/health")
def health():
    if failure_mode == "service_down":
        HEALTH.set(0)
        return JSONResponse(status_code=503, content={
            "service": "payment-service", "status": "unhealthy"
        })
    HEALTH.set(1)
    return {"service": "payment-service", "status": "healthy"}


@app.get("/failure")
def get_failure():
    return {"service": "payment-service", "failure_mode": failure_mode}


@app.post("/failure/{mode}")
def set_failure(mode: str):
    global failure_mode
    allowed = ["normal", "high_latency", "error_spike", "service_down", "high_cpu", "high_memory"]
    if mode not in allowed:
        return JSONResponse(status_code=400, content={
            "error": "Unsupported failure mode", "allowed_modes": allowed
        })
    failure_mode = mode
    FAILURE_SWITCH.set(failure_value(mode))
    HEALTH.set(0 if mode == "service_down" else 1)
    return {"service": "payment-service", "failure_mode": failure_mode}


@app.get("/pay")
def process_payment():
    start = time.perf_counter()

    if failure_mode == "service_down":
        FAILURES.labels("service_down").inc()
        REQUESTS.labels("503").inc()
        return JSONResponse(status_code=503, content={"error": "Payment service unavailable"})

    if failure_mode == "high_latency":
        time.sleep(5)

    if failure_mode == "high_cpu":
        result = 0
        for i in range(7_000_000):
            result += i * i

    if failure_mode == "high_memory":
        data = ["AIOPS"] * 2_000_000
        _ = len(data)

    if failure_mode == "error_spike" and random.random() < 0.7:
        FAILURES.labels("error_spike").inc()
        REQUESTS.labels("500").inc()
        LATENCY.observe(time.perf_counter() - start)
        return JSONResponse(status_code=500, content={"error": "Injected payment processing failure"})

    REQUESTS.labels("200").inc()
    LATENCY.observe(time.perf_counter() - start)
    return {
        "service": "payment-service",
        "payment_status": "successful",
        "transaction_id": random.randint(10000, 99999),
        "failure_mode": failure_mode,
    }


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
