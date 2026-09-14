# Robust Microservice and Cluster-Based AIOps Platform (Prototype)

Real-time detection, recommendation, and remediation prototype for a large app
fleet (200+ apps across clusters). The original single `aiops-controller` is
split into two services, plus a dashboard:

- **detector** — collects telemetry every 3 minutes, detects anomalies, publishes findings.
- **reasoning-worker** — async worker that calls an LLM to produce grounded recommendations, and serves the findings API.
- **dashboard** — Streamlit UI presenting findings, evidence, and AI recommendations.

```
Prometheus ──(PromQL, every 180s)──> detector ──POST /findings──> reasoning-worker ──> dashboard
        (unreachable -> sample records)         (async queue + LLM)
```

## Stack

- FastAPI (both microservices)
- LLM via the NVIDIA integrate API (`openai/gpt-oss-20b`), OpenAI-compatible client
- Streamlit dashboard

No Kafka: the detector calls the worker over HTTP and the worker uses an in-process
async queue for reasoning. This keeps the prototype small while preserving the
detector / reasoning separation.

## Layout

```
common/              shared domain models (Finding, AIAnalysis)
detector/            collection, normalization, detection rules, publisher
reasoning_worker/    async queue, LLM reasoning, deterministic fallback catalog, findings API
dashboard/           Streamlit UI
```

## Configuration (.env)

| Variable | Default | Purpose |
| --- | --- | --- |
| `NVIDIA_API_KEY` | `your_nvidia_api_key_here` | LLM key (leave placeholder to run on the deterministic fallback) |
| `NVIDIA_BASE_URL` | `https://integrate.api.nvidia.com/v1` | OpenAI-compatible endpoint |
| `NVIDIA_MODEL` | `openai/gpt-oss-20b` | Model name |
| `PROMETHEUS_URL` | `http://localhost:9090` | Prometheus base URL |
| `POLL_INTERVAL_SECONDS` | `180` | Detector collection interval (3 minutes) |
| `FALLBACK_ENABLED` | `true` | Use sample records when Prometheus is unreachable |
| `INCIDENT_COOLDOWN_SECONDS` | `900` | Suppress repeat findings for the same target/anomaly |
| `WORKER_URL` | `http://localhost:9100` | Detector -> worker and dashboard -> worker |
| `DETECTOR_URL` | `http://localhost:9000` | Dashboard -> detector |
| `REASON_MIN_SEVERITY` | `high` | Minimum severity that triggers the LLM |
| `DEDUP_WINDOW_SECONDS` | `900` | Worker-side suppression of repeat target/anomaly findings |

## Run

### Docker

```bash
docker compose up --build
```

- Dashboard: http://localhost:8501
- Detector: http://localhost:9000
- Reasoning worker: http://localhost:9100

### Local

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r reasoning_worker/requirements.txt -r detector/requirements.txt -r dashboard/requirements.txt

uvicorn detector.main:app --port 9000
uvicorn reasoning_worker.main:app --port 9100
streamlit run dashboard/app.py
```

## How it works

1. The detector's scheduler runs at startup and every `POLL_INTERVAL_SECONDS`.
   It queries Prometheus (`{__name__=~"api_.*|payment_.*", app=~".+"}` and the
   cluster equivalent). With retries and bounded timeouts.
2. If Prometheus is unreachable and `FALLBACK_ENABLED=true`, it uses bundled
   sample records (`detector/samples.py`) and marks the source `fallback`. The
   dashboard shows a FALLBACK banner so sample data is never mistaken for live data.
3. Snapshots are grouped per app+cluster and per cluster, then scored by
   threshold rules (`detector/rules.py`): health, 5xx ratio, average latency,
   dependency failures, node readiness, CPU/memory saturation, pending pods,
   unavailable workloads.
4. New findings are de-duplicated by cooldown and POSTed to the reasoning worker.
5. The worker enqueues critical/high findings for LLM reasoning. The LLM only
   explains and recommends from the supplied evidence; it never reports an action
   as executed. If the LLM fails, a deterministic catalog recommendation is used.
6. The dashboard reads the worker's `/findings` and `/summary` and renders the
   fleet view, per-cluster breakdown, evidence, and the AI recommendation.

## Endpoints

Detector (9000): `GET /`, `GET /health`, `GET /status`, `POST /analyze`,
`GET /findings`, `GET /snapshot`, `GET /cooldowns`.

Reasoning worker (9100): `GET /`, `GET /health`, `GET /status`, `GET /summary`,
`POST /findings`, `GET /findings`, `GET /findings/{id}`,
`POST /findings/{id}/reanalyze`.

## Notes and limitations

- State is in-memory: restarting a service clears its findings. The next
  iteration should back findings with Postgres/TimescaleDB.
- Set `FALLBACK_ENABLED=false` to fail loudly instead of using sample records.
- The LLM is strictly downstream of detection and policy. Keep remediation behind
  human approval before adding any executor.
