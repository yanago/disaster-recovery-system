# Disaster Recovery Replay System

Replays historical security events from an Apache Iceberg data lake to Kafka topics or REST endpoints. Built with **core Java** (no Spring) and **Pekko Actor** framework.

## Features

- **Data source**: Apache Iceberg (Hadoop catalog, local or HDFS)
- **Outputs**: Apache Kafka topics and REST API (HTTP POST)
- **Replay job API**: Create, list, get, start, pause, resume, cancel, status, metrics
- **Throttling**: Configurable `max_events_per_second` and `batch_size`
- **Demo**: 50,000+ events generated on first run; built-in REST receiver for testing

## Requirements

- Java 17+
- Maven 3.8+

For Kafka replay: a running Kafka cluster (e.g. `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`).

## Build

```bash
mvn -DskipTests package
```

Produces `target/disaster-recovery-system-0.1.0-SNAPSHOT.jar` (shaded).

## Run locally

```bash
# Default: HTTP on 8080, receiver on 9090, init 50k events into ./local-warehouse
java -jar target/disaster-recovery-system-0.1.0-SNAPSHOT.jar
```

Options (environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | 8080 | Management API port |
| `RECEIVER_PORT` | 9090 | Demo REST receiver port |
| `WAREHOUSE_PATH` | ./local-warehouse | Iceberg warehouse directory |
| `INIT_DEMO_DATA` | true | Create/fill demo table on startup |
| `DEMO_EVENTS` | 50000 | Number of demo events |
| `ENABLE_RECEIVER` | true | Start demo REST receiver |

## Replay Job Management API

Base URL: `http://localhost:8080` (or your `HTTP_PORT`).

### Create job

```bash
curl -s -X POST http://localhost:8080/api/v1/replay/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "source_table": "security.events",
    "destination_type": "REST",
    "rest_endpoint_url": "http://localhost:9090/ingest",
    "max_events_per_second": 500,
    "batch_size": 100,
    "description": "Demo replay to local receiver"
  }'
```

Response: `{"id":"<job-uuid>"}`. Use `id` for all job operations.

### List jobs

```bash
curl -s http://localhost:8080/api/v1/replay/jobs
```

### Get job details

```bash
curl -s http://localhost:8080/api/v1/replay/jobs/{id}
```

### Start / Pause / Resume / Cancel

```bash
curl -s -X POST http://localhost:8080/api/v1/replay/jobs/{id}/start
curl -s -X POST http://localhost:8080/api/v1/replay/jobs/{id}/pause
curl -s -X POST http://localhost:8080/api/v1/replay/jobs/{id}/resume
curl -s -X POST http://localhost:8080/api/v1/replay/jobs/{id}/cancel
```

### Job status and metrics

```bash
curl -s http://localhost:8080/api/v1/replay/jobs/{id}/status
curl -s http://localhost:8080/api/v1/replay/jobs/{id}/metrics
```

### Health and metrics

```bash
curl -s http://localhost:8080/health
curl -s http://localhost:8080/metrics
```

## Demo flow (REST destination)

1. Start the app (creates 50k events in Iceberg and starts receiver on 9090).
2. Create a replay job (REST → `http://localhost:9090/ingest`).
3. Start the job: `POST .../jobs/{id}/start`.
4. Optionally pause/resume or cancel.
5. Check job metrics and `GET http://localhost:9090/stats` to see `received_total`.

## Kafka destination

Use a real Kafka cluster and set in the create-job body:

- `destination_type`: `KAFKA`
- `kafka_bootstrap_servers`: e.g. `kafka:9092`
- `kafka_topic`: e.g. `security-events`

## Kubernetes

Build image and load into kind/minikube (example):

```bash
docker build -t disaster-recovery-system:0.1.0-SNAPSHOT .
# kind: kind load docker-image disaster-recovery-system:0.1.0-SNAPSHOT
# minikube: eval $(minikube docker-env) && docker build -t disaster-recovery-system:0.1.0-SNAPSHOT .
kubectl apply -f k8s/deployment.yaml
kubectl port-forward svc/disaster-recovery-replay 8080:8080 9090:9090
```

Then use the same API and demo flow against `http://localhost:8080` and `http://localhost:9090`.

## Event schema

Each event has:

- `cid` (string) – customer id
- `event_timestamp` (ISO-8601)
- `event_time` (long, epoch ms)
- `event_type` (string)
- `event_id` (UUID string)

## License

Apache 2.0.
