# Robust Microservice and Cluster-Based AIOps Platform — MVP

A small runnable AIOps proof of concept with:
- API Service -> Payment Service dependency
- FastAPI
- Docker Compose
- Prometheus-compatible `/metrics`
- Telemetry collection by an AIOps controller
- Failure injection: normal, high_latency, error_spike, service_down, high_cpu, high_memory
- Threshold-based anomaly detection
- JSON incident records

## Run

```bash
docker compose up --build
```

Services:
- API Service: http://localhost:8000
- Payment Service: http://localhost:8001
- AIOps Controller: http://localhost:9000
- Prometheus: http://localhost:9090

## Quick demo

Normal:
```bash
curl http://localhost:8000/call-payment
```

Inject payment latency:
```bash
curl -X POST http://localhost:8001/failure/high_latency
for i in {1..10}; do curl -s http://localhost:8000/call-payment; echo; done
```

Ask the AIOps controller to collect and detect:
```bash
curl -X POST http://localhost:9000/analyze
curl http://localhost:9000/incidents
```

Reset:
```bash
curl -X POST http://localhost:8001/failure/normal
```

## Architecture

```text
API Service ---> Payment Service
     |                 |
     +------ metrics --+
             |
             v
       AIOps Controller
       |      |       |
   telemetry detector incidents
             |
             v
      Prometheus (scrape)
```

This project intentionally uses simple thresholds rather than an LLM or ML model. The next iterations can add statistical/ML detection, dependency graphs, evidence-grounded diagnosis, approval, remediation, and Kubernetes/OpenShift deployment.
