# Deployment

## Local (Docker Compose)

```bash
cp .env.example .env      # set NVIDIA_API_KEY
docker compose up --build
```

Then scale the fleet out:

```bash
docker compose up -d --scale collector=3 --scale consumer=4 --scale detector=4
```

Endpoints: dashboard `:8501`, API `:8000`, collector `:9100`, consumer `:9150`,
detector `:9200`, inference `:9300`, Kafka `:9092`, Postgres `:5432`.

## Kubernetes

```bash
docker build -f collectors/Dockerfile -t <registry>/collector:latest .
docker build -f consumers/Dockerfile  -t <registry>/consumer:latest  .
docker build -f detectors/Dockerfile  -t <registry>/detector:latest  .
docker build -f inference/Dockerfile  -t <registry>/inference:latest .
docker build -f api/Dockerfile        -t <registry>/api:latest        .
docker build -f dashboard/Dockerfile  -t <registry>/dashboard:latest  .

kubectl apply -f deployment/k8s/00-namespace-rbac.yaml
kubectl apply -f deployment/k8s/10-config.yaml
kubectl apply -f deployment/k8s/20-postgres.yaml
kubectl apply -f deployment/k8s/30-kafka.yaml
kubectl apply -f deployment/k8s/40-platform.yaml
```

`00-namespace-rbac.yaml` grants the collector ServiceAccount read-only access to
pods, nodes, deployments, events and the metrics API across the cluster. It needs
no write permissions. Point the collector at additional clusters by giving it
kubeconfig contexts (`K8S_CONTEXTS`) or running a collector Deployment per
cluster, each with its own ServiceAccount.

## Image notes

The manifests use `telco-fleet-copilot/<service>:latest`. Substitute your registry
path, and set `NVIDIA_API_KEY` in the `aiops-secrets` Secret (do not commit it).
