# FastAPI and React Analytics Dashboard Design

## 1. Goal and audience

Build an English-language analytics application for postgraduate applications,
GitHub presentation, and technical interviews. It must expose the accepted
MySQL aggregate through a typed, read-only API and present it in a polished
four-page dashboard.

This phase is also a guided learning exercise. The user writes and runs each
small implementation step. The assistant explains the relevant concept,
provides one bounded exercise at a time, reviews the output, and only then
advances. The assistant must not generate the entire implementation directly
in the worktree.

## 2. Selected approach

Use a separated application stack:

- Python 3.13 and FastAPI for the HTTP API;
- Pydantic for request and response contracts;
- mysql-connector-python and parameterized SQL for MySQL access;
- React, TypeScript, Vite, React Router, and ECharts for the dashboard;
- pytest for backend tests and a suitable Vite-compatible frontend test stack;
- Docker Compose for final packaging after local development is complete.

React and TypeScript were selected over Jinja with plain JavaScript because the
four-page UI benefits from typed API contracts, reusable components, routing,
and isolated chart transformations. Streamlit was rejected because it would
demonstrate a data-analysis prototype rather than a complete API and frontend
application.

## 3. Architecture and trust boundaries

```text
React + TypeScript Dashboard
             |
             | HTTP / JSON
             v
FastAPI read-only API
             |
             | parameterized SELECT statements
             v
MySQL hourly_airline_metrics and metric_publications
```

The browser never connects directly to MySQL and never receives database
credentials. FastAPI validates parameters, applies query limits, converts rows
to stable JSON contracts, and sanitizes failures. Kafka, Spark, and HDFS remain
upstream processing and analytical-storage components and do not accept
dashboard requests.

There is no user login. The application is a local portfolio demonstration and
all endpoints are read-only. The API uses a dedicated MySQL account with SELECT
permission only. CORS permits only configured local frontend origins.

## 4. API contract

All routes use the `/api/v1` prefix. Successful responses use explicit Pydantic
models; failure responses use:

```json
{
  "error": {
    "code": "INVALID_DATE_RANGE",
    "message": "date_from must not be later than date_to"
  }
}
```

### 4.1 Health

`GET /api/v1/health`

Reports whether the FastAPI process is serving and whether MySQL can execute
`SELECT 1`. Database unavailability returns HTTP 503 without credentials, SQL,
or stack traces.

### 4.2 Overview

`GET /api/v1/overview`

Returns metric-row count, successful-response total, success-token total,
distinct-airline count, distinct-date count, and latest publication identity
and completion time.

### 4.3 Global timeline

`GET /api/v1/timeline?date_from=YYYY-MM-DD&date_to=YYYY-MM-DD`

Returns date/hour aggregates ordered chronologically. Both dates are optional
and default to the complete accepted range. The API rejects an inverted range
with HTTP 400 and malformed dates with HTTP 422.

### 4.4 Airline ranking

`GET /api/v1/airlines?metric=responses|tokens&limit=1..50`

Returns airline totals in deterministic descending metric order with airline
code as the tie breaker. Defaults are `metric=tokens` and `limit=10`.

### 4.5 Airline timeline

`GET /api/v1/airlines/{airline_code}/timeline`

Returns chronological date/hour metrics for one normalized airline code. An
unknown airline returns HTTP 404. Codes are validated before SQL execution.

### 4.6 Hourly heatmap

`GET /api/v1/hourly-heatmap?metric=responses|tokens&date_from=...&date_to=...`

Returns date, hour, and value cells for the requested metric and range. Empty
valid ranges return an empty item list rather than invented zero rows.

### 4.7 Publication and provenance

`GET /api/v1/publication`

Returns the latest published database audit row plus accepted-run provenance:
HDFS root, output version, counts, SHA-256, component versions, processing
duration, and data-quality totals. Publication facts come from MySQL. Facts not
stored in MySQL come from a versioned, read-only `accepted-run.json` checked
into the repository. The endpoint describes measured evidence and does not
claim live Kafka or Spark monitoring.

## 5. Backend boundaries

The backend is divided into focused units:

```text
api/
|-- main.py
|-- config.py
|-- database.py
|-- models.py
|-- repository.py
|-- service.py
|-- errors.py
`-- routers/
    |-- health.py
    |-- overview.py
    |-- airlines.py
    |-- timeline.py
    `-- publication.py
```

- Routers own HTTP parameter parsing and status codes.
- Services own business rules and compose repository results.
- The repository owns parameterized SELECT statements and row conversion.
- Models own external JSON types.
- Database code owns a bounded connection pool and guarantees connection
  return after every request.
- Configuration reads validated environment variables and never exposes the
  password in representations or logs.
- Exception handlers map known failures to the common error envelope.

No route performs INSERT, UPDATE, DELETE, DDL, snapshot publication, or Spark
execution.

## 6. Frontend information architecture

Use a sidebar application layout with four routes.

### 6.1 Overview

Show four primary KPI cards, the global time trend, Top 10 airlines, latest
publication identity and time, and a manual Refresh action. This is the primary
README and application screenshot.

### 6.2 Airline Analysis

Allow `responses` or `tokens`, Top 5/10/20, airline ranking, selected-airline
trend, exact values, and share of the overall metric.

### 6.3 Time Analysis

Allow date-range filtering and show an hourly trend and date-by-hour heatmap
with exact tooltip values.

### 6.4 Pipeline & Data Quality

Show the verified flow:

```text
GDS Log -> Kafka -> Spark -> HDFS -> MySQL -> FastAPI -> React
```

Also show accepted input, valid, dead-letter, HDFS size, serving rows, versions,
publication time, and aggregate SHA-256. Label these as acceptance evidence,
not real-time health metrics.

## 7. Frontend boundaries and behavior

Suggested structure:

```text
frontend/src/
|-- api/client.ts
|-- api/types.ts
|-- components/
|-- pages/
|-- charts/
|-- App.tsx
`-- main.tsx
```

The API client owns HTTP behavior. TypeScript interfaces mirror the public API
contracts. Pages compose reusable cards and charts. Chart components accept
already transformed typed data and do not issue HTTP requests.

Each page loads only its required data on first entry. Refresh is manual; no
automatic polling implies false real-time behavior. Date, metric, and ranking
selections are stored in URL query parameters so a reload preserves them.

Every request has visible loading, error with Retry, empty, and success states.
Numbers use locale-aware thousands separators. Empty datasets do not render
misleading charts. The UI and screenshots are English; teaching explanations
and developer notes may be Chinese.

## 8. Local runtime and packaging

Routine frontend/API development starts only MySQL, FastAPI, and Vite. Kafka,
Spark, and HDFS are unnecessary because MySQL already contains the accepted
complete snapshot. The full stack is started only for final end-to-end
demonstration.

Development flow:

- persistent Docker MySQL on the selected host port;
- FastAPI in the existing WSL Python virtual environment;
- Vite in WSL and the dashboard in a Windows browser.

After local behavior is accepted, add pinned API and frontend Docker images and
Compose lifecycle scripts. Container health checks must be lightweight, and
stopping containers must preserve MySQL data by default.

## 9. Error handling and security

- Use parameterized SQL exclusively.
- Bound limits and date ranges before querying.
- Use deterministic ordering for rankings and timelines.
- Set bounded connection, read, and write timeouts.
- Return HTTP 422 for schema validation, 400 for invalid business ranges, 404
  for unknown airlines, 503 for database unavailability, and sanitized 500 for
  unexpected failures.
- Do not return SQL text, filesystem paths containing secrets, environment
  values, stack traces, or database exception details to the browser.
- API logs include request identity, route, status, and duration but not
  passwords or complete database connection strings.

## 10. Verification strategy

Backend unit tests cover configuration, query parameters, deterministic SQL
arguments, row conversion, service rules, response schemas, status codes, and
sanitized errors. Repository behavior uses controlled fakes at the database
boundary. Guarded integration tests query real Docker MySQL and reconcile the
accepted 3,203-row snapshot.

Frontend tests cover routing, API result rendering, loading/error/empty states,
Retry, filters, URL persistence, numeric formatting, and chart-data
transformations. Avoid tests that merely assert framework or mock existence.

Final acceptance manually and automatically verifies all routes, four pages,
filters, API/database reconciliation, failure and recovery, production builds,
container restart behavior, README commands, and English screenshots.

## 11. Guided implementation sequence

Every learning unit follows concept explanation, a failing test written by the
user, minimal implementation written by the user, automated and manual
verification, user explanation, and a small commit.

The ordered units are:

1. HTTP, JSON, and a minimal FastAPI application;
2. Pydantic response models;
3. configuration and database connections;
4. repository and parameterized SQL;
5. overview endpoint;
6. airline, timeline, heatmap, and publication endpoints;
7. error handling and real-MySQL integration;
8. Node.js, npm, Vite, React, and TypeScript foundations;
9. React components, props, and state;
10. typed API client;
11. router and four-page layout;
12. Overview data rendering;
13. ECharts components;
14. filters and loading/error/empty states;
15. Docker packaging and final acceptance.

## 12. Explicit exclusions

This phase does not implement authentication, data mutation, WebSocket
streaming, automatic polling, fabricated live status, alerting, a Kafka/Spark
administration console, cloud deployment, or a mobile application.

