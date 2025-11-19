# DataFlow Studio

A lightweight data movement experience inspired by Airbyte with a Fivetran-like UI. The project bundles a Go backend for
simulating high-speed extracts/loads and a modern Next.js frontend for composing and triggering pipelines between MySQL,
SQL Server, Postgres, and Apache Iceberg.

## Backend (Go)

* Location: `backend/`
* Endpoints:
  * `GET /health` – health check.
  * `GET /connectors` – list available source and destination connectors.
  * `GET /pipelines` – list saved pipelines.
  * `POST /pipelines` – create a pipeline definition `{ name, sourceType, destType, sourceConfig, destConfig }`.
  * `POST /pipelines/{name}/run` – trigger a pipeline execution and return a summary.

Run locally:

```bash
cd backend
PORT=8080 go run ./cmd/server
```

## Frontend (Next.js)

* Location: `frontend/`
* Set `NEXT_PUBLIC_API_BASE` to the backend URL (defaults to `http://localhost:8080`).
* Provides a Fivetran-inspired dashboard to browse connectors, configure pipelines, and trigger runs.

Run locally:

```bash
cd frontend
npm install
npm run dev
```

## Notes

The connectors and pipeline engine run in-memory for fast feedback without external databases. Validation paths ensure
required fields exist, while simulated extract/load paths mimic throughput and latency for demo purposes.
