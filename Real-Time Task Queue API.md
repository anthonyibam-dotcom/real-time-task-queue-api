# Real-Time Task Queue API

[![FastAPI](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi)](https://fastapi.tiangolo.com/) [![Celery](https://img.shields.io/badge/Celery-5.x-37814A?logo=celery)](https://docs.celeryq.dev/) [![Redis](https://img.shields.io/badge/Redis-7.x-DC382D?logo=redis)](https://redis.io/) [![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docs.docker.com/compose/)

The **Real-Time Task Queue API** is a production-oriented reference service for accepting long-running jobs through FastAPI, executing them with Celery workers backed by Redis, and streaming progress to clients through WebSockets. It includes two representative workloads: chunked CSV grade imports and asynchronous student-performance report generation in CSV or PDF format.

## Architecture

```mermaid
flowchart LR
    C[Client] -->|POST task| API[FastAPI REST API]
    API -->|enqueue + status| R[(Redis broker/result backend)]
    R --> W[Celery Worker]
    W -->|PROGRESS/SUCCESS events| P[(Redis Pub/Sub)]
    P -->|live JSON stream| WS[WebSocket /ws/tasks/{task_id}]
    WS --> C
    API -->|GET status| R
```

## Capabilities

The API exposes an asynchronous CSV upload endpoint, a report-generation endpoint, and a task-status endpoint. Workers publish structured progress payloads such as `{"status":"PROGRESS","progress":65,"message":"Processing row 650 of 1000"}`. WebSocket subscribers receive the same event stream and are disconnected cleanly after `SUCCESS`, `FAILURE`, or `REVOKED`.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/tasks/bulk-import` | Upload a CSV and enqueue a bulk import. |
| `POST` | `/api/v1/tasks/generate-report` | Enqueue CSV or PDF report generation. |
| `GET` | `/api/v1/tasks/{task_id}` | Read Celery state and progress metadata. |
| `WS` | `/ws/tasks/{task_id}` | Subscribe to live task events. |
| `GET` | `/health` | Container health endpoint. |

## One-command launch

Copy the environment template and launch the complete stack.

```bash
cp .env.example .env
docker-compose up --build
```

Interactive API documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs), while Redis is exposed locally on port `6379`. The `web` and `celery_worker` services share upload and report volumes so task inputs and generated artifacts are visible to both processes.

## REST examples

Create a sample grade file and enqueue it:

```bash
printf 'student_id,name,grade\nS-1,Ada Lovelace,98\nS-2,Alan Turing,95\n' > grades.csv
curl -X POST http://localhost:8000/api/v1/tasks/bulk-import \
  -F 'file=@grades.csv'
```

The response contains a `task_id`. Poll its status:

```bash
curl http://localhost:8000/api/v1/tasks/<task_id>
```

Trigger a PDF report:

```bash
curl -X POST http://localhost:8000/api/v1/tasks/generate-report \
  -H 'Content-Type: application/json' \
  -d '{"report_format":"pdf"}'
```

## WebSocket live output

Install the Python requirements if running the client outside Docker, then pass the task identifier returned by either REST trigger:

```bash
python scripts/test_ws.py <task_id> --base-url ws://localhost:8000
```

A live run prints output similar to:

```json
{
  "task_id": "8d6…",
  "status": "PROGRESS",
  "progress": 65,
  "message": "Processing row 650 of 1000"
}
{
  "task_id": "8d6…",
  "status": "SUCCESS",
  "progress": 100,
  "message": "Bulk import completed"
}
```

## Local development and tests

Create a virtual environment, install dependencies, and run the suite:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

The REST tests mock Celery task submission and result retrieval so they can run without a live broker. The WebSocket test uses an in-process event-stream fixture. For an end-to-end run, start Docker Compose and use `scripts/test_ws.py`.

## Operational notes

Redis is configured as both the Celery broker and result backend for a compact reference deployment. In a hardened production environment, add authentication and TLS for Redis, configure structured logging and metrics, validate uploaded CSV schemas, move generated artifacts to object storage, and add authentication around REST and WebSocket routes. The task status TTL defaults to 24 hours and is configurable through `TASK_STATUS_TTL_SECONDS`.

## License

This reference implementation is provided for internal prototyping and educational use.
