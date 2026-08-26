# FastAPI and React Dashboard Guided Implementation Plan

**Goal:** Let the user personally build and understand a typed, read-only
FastAPI API and a four-page React/ECharts dashboard over the accepted MySQL
snapshot.

**Architecture:** React and TypeScript call versioned JSON endpoints. FastAPI
validates requests, applies business rules, and delegates parameterized SELECT
queries to a MySQL repository. The browser never receives database credentials.

**Teaching contract:** The assistant must not implement this plan wholesale.
For each task, explain the concept, give only the current bounded exercise, let
the user type and run it, inspect the output, ask the user to explain the result,
and then commit before continuing. Use test-driven development for behavior.

**Selected stack:** Python 3.13, FastAPI, Pydantic, Uvicorn,
mysql-connector-python, pytest, HTTPX, React, TypeScript, Vite, React Router,
ECharts, Vitest, Testing Library, Docker Compose.

---

## Phase A: FastAPI foundations

### Task 1: Verify prerequisites and pin the API toolchain

**Learning objective:** Understand the difference between a Python package, an
optional dependency group, a development server, and a test client.

**Files:**

- Modify: `pyproject.toml`
- Create: `tests/api/test_dependencies.py`

**Steps:**

1. Check the active worktree, virtual environment, Python, pip, Node, npm,
   Docker, and MySQL health without changing them.
2. Write a failing dependency-contract test requiring exact compatible FastAPI,
   Uvicorn, Pydantic, and HTTPX versions under an `api` optional extra.
3. Run the focused test and explain why it fails.
4. Add only the accepted pinned versions to `pyproject.toml`.
5. Install editable extras with `python -m pip install -e '.[dev,mysql,api]'`.
6. Run the focused test and the existing ordinary suite.
7. Commit: `build: pin FastAPI development stack`.

### Task 2: Build the smallest FastAPI application

**Learning objective:** Understand HTTP, JSON, GET, routes, status codes, ASGI,
and the difference between importing an application and running Uvicorn.

**Files:**

- Create: `src/gds_pipeline/api/__init__.py`
- Create: `src/gds_pipeline/api/main.py`
- Create: `tests/api/test_health.py`

**Steps:**

1. Write a failing test for `GET /api/v1/health` returning HTTP 200 and a small
   typed JSON body without checking MySQL yet.
2. Run it and confirm the application or route is missing.
3. Create the minimal application factory and health router.
4. Run the focused test.
5. Start Uvicorn locally and call the route with `curl`.
6. Open FastAPI's generated `/docs` page and identify request and response
   schemas.
7. Explain request, response, JSON, status code, and route in the user's own
   words.
8. Commit: `feat: add minimal FastAPI health route`.

### Task 3: Define typed responses and a common error envelope

**Learning objective:** Understand Pydantic models, runtime validation, OpenAPI,
and why an external contract is different from an internal dictionary.

**Files:**

- Create: `src/gds_pipeline/api/models.py`
- Create: `src/gds_pipeline/api/errors.py`
- Modify: `src/gds_pipeline/api/main.py`
- Modify: `tests/api/test_health.py`
- Create: `tests/api/test_errors.py`

**Steps:**

1. Write failing tests for the exact health response model and common error
   envelope.
2. Implement only the required models and exception handler.
3. Verify OpenAPI contains the response schemas.
4. Prove an internal error does not expose a traceback, SQL, or credentials.
5. Run focused and ordinary tests.
6. Commit: `feat: define API response contracts`.

### Task 4: Add validated API configuration

**Learning objective:** Understand environment variables, defaults, secrets,
configuration validation, and dependency injection.

**Files:**

- Create: `src/gds_pipeline/api/config.py`
- Create: `tests/api/test_config.py`
- Modify: `pyproject.toml` if a configuration dependency is justified

**Steps:**

1. Write failing tests for required MySQL settings, pool size, timeouts, CORS
   origins, accepted-run metadata path, and hidden password representation.
2. Reuse existing MySQL environment names where possible; do not create a
   second secret file.
3. Implement immutable validated settings.
4. Reject blank hosts, invalid ports, nonpositive pool sizes, and wildcard CORS.
5. Run focused and ordinary tests.
6. Commit: `feat: validate API runtime settings`.

## Phase B: Read-only MySQL serving layer

### Task 5: Create the connection-pool boundary

**Learning objective:** Understand database connections, cursors, pools,
context managers, cleanup, and health probes.

**Files:**

- Create: `src/gds_pipeline/api/database.py`
- Create: `tests/api/test_database.py`
- Create: `scripts/mysql-create-api-user.sh`
- Modify: `infrastructure/mysql/.env.example`
- Modify: `src/gds_pipeline/api/main.py`
- Modify: `tests/api/test_health.py`

**Steps:**

1. Write failing tests proving borrowed connections and cursors are always
   returned or closed on success and failure.
2. Add separate API username/password environment keys to the ignored local
   environment contract; never reuse the publication account in FastAPI.
3. Create an idempotent operator script that grants the API account only
   `SELECT` on the serving and publication tables. This script must also work
   when the existing persistent MySQL volume has already been initialized.
4. Prove against real MySQL that the API account can run `SELECT` but receives
   a permission error for `INSERT`, `UPDATE`, and `DELETE`.
5. Implement a bounded mysql-connector connection pool behind a narrow
   interface using only the read-only API credentials.
6. Add a `SELECT 1` readiness probe to the health service.
7. Map database unavailability to sanitized HTTP 503.
8. Use a controlled fake for unit tests; do not require Docker by default.
9. Manually verify against the running `gds-mysql` container.
10. Commit: `feat: add read-only API database boundary`.

### Task 6: Implement the read-only repository

**Learning objective:** Understand parameterized SQL, aggregation, deterministic
ordering, row mapping, and the repository boundary.

**Files:**

- Create: `src/gds_pipeline/api/repository.py`
- Create: `tests/api/test_repository.py`

**Steps:**

1. Define repository result dataclasses or typed records independently from
   HTTP models.
2. Write failing tests for overview, global timeline, airline ranking, airline
   existence/timeline, heatmap, and latest publication queries.
3. Assert placeholders and argument tuples rather than interpolated user input.
4. Implement one query at a time with deterministic ordering.
5. Reject any repository SQL containing INSERT, UPDATE, DELETE, or DDL.
6. Test cursor and connection cleanup on query failure.
7. Run focused and ordinary tests.
8. Commit: `feat: query MySQL analytics read model`.

### Task 7: Add accepted-run provenance

**Learning objective:** Understand versioned metadata, evidence provenance,
schema validation, and separation between measured facts and live state.

**Files:**

- Create: `config/accepted-run.json`
- Create: `src/gds_pipeline/api/provenance.py`
- Create: `tests/api/test_provenance.py`

**Steps:**

1. Write a failing loader test using literal accepted counts, versions, HDFS
   root, duration, size, and SHA-256.
2. Create the versioned JSON from already accepted evidence only.
3. Validate its schema and cross-check its aggregate totals/hash against the
   latest published MySQL audit row in the service layer.
4. Fail startup or readiness clearly when required metadata is malformed; do
   not silently invent defaults.
5. Commit: `feat: expose accepted pipeline provenance`.

## Phase C: Business API

### Task 8: Implement overview and publication endpoints

**Learning objective:** Understand router/service/repository separation and
response composition.

**Files:**

- Create: `src/gds_pipeline/api/service.py`
- Create: `src/gds_pipeline/api/routers/overview.py`
- Create: `src/gds_pipeline/api/routers/publication.py`
- Modify: `src/gds_pipeline/api/models.py`
- Modify: `src/gds_pipeline/api/main.py`
- Create: `tests/api/test_overview.py`
- Create: `tests/api/test_publication.py`

**Steps:**

1. Write failing API tests for exact response contracts and no-data/database
   failure behavior.
2. Implement service composition without SQL in routers.
3. Return latest publication and accepted provenance with explicit evidence
   labels.
4. Reconcile real endpoint totals with 3,203 rows, 1,310,068 responses, and
   2,145,511 tokens.
5. Commit: `feat: serve overview and publication evidence`.

### Task 9: Implement ranking and airline timeline endpoints

**Learning objective:** Understand enums, path/query validation, normalization,
limits, 404 semantics, and stable sorting.

**Files:**

- Create: `src/gds_pipeline/api/routers/airlines.py`
- Modify: `src/gds_pipeline/api/models.py`
- Modify: `src/gds_pipeline/api/service.py`
- Modify: `src/gds_pipeline/api/main.py`
- Create: `tests/api/test_airlines.py`

**Steps:**

1. Write parameterized failing tests for `responses|tokens`, limit 1..50,
   uppercase normalization, unknown codes, and deterministic ties.
2. Implement ranking and one-airline timeline.
3. Verify SQL receives only validated values.
4. Call endpoints manually for `CA` and one unknown code.
5. Commit: `feat: serve airline analytics`.

### Task 10: Implement timeline and heatmap endpoints

**Learning objective:** Understand date parsing, optional filters, business
validation, chronological ordering, and empty-result semantics.

**Files:**

- Create: `src/gds_pipeline/api/routers/timeline.py`
- Modify: `src/gds_pipeline/api/models.py`
- Modify: `src/gds_pipeline/api/service.py`
- Modify: `src/gds_pipeline/api/main.py`
- Create: `tests/api/test_timeline.py`

**Steps:**

1. Write failing tests for omitted dates, one-sided bounds, full bounds,
   malformed dates, inverted ranges, and valid empty results.
2. Implement shared date-range validation and both endpoints.
3. Verify HTTP 422 versus 400 semantics.
4. Check a small real date range manually and compare with direct MySQL SQL.
5. Commit: `feat: serve time analytics`.

### Task 11: Prove the complete API on real MySQL

**Learning objective:** Understand the difference between unit, integration,
contract, and acceptance testing.

**Files:**

- Create: `tests/integration/test_api_mysql.py`
- Modify: `pyproject.toml` markers
- Create: `scripts/api-up.sh`
- Create: `scripts/api-smoke.sh`

**Steps:**

1. Add guarded integration tests that are skipped by default.
2. Query the existing accepted snapshot without modifying or cleaning it.
3. Reconcile every endpoint against independent SQL and accepted constants.
4. Prove unavailable MySQL produces 503 and recovery succeeds.
5. Run the complete ordinary suite and guarded integration suite.
6. Commit: `test: prove read-only API against MySQL`.

---

## Phase D: React and TypeScript foundations

### Task 12: Verify Node and scaffold the frontend deliberately

**Learning objective:** Understand Node.js, npm, package.json, dependency locks,
Vite, browser modules, and the frontend development server.

**Files:**

- Create: `frontend/` through the pinned Vite React TypeScript template
- Modify generated files only after explaining each one

**Steps:**

1. Verify Node and npm versions before installing anything.
2. Scaffold into `frontend/`, inspect every generated top-level file, and remove
   demo assets only after understanding their purpose.
3. Pin production and development dependencies, including React Router,
   ECharts, Vitest, Testing Library, jsdom, and type packages.
4. Run the generated development server, production build, and initial test.
5. Commit the lockfile and scaffold: `build: scaffold typed React frontend`.

### Task 13: Learn components, props, state, and testing

**Learning objective:** Understand React rendering, JSX/TSX, components, props,
state, events, and accessible queries.

**Files:**

- Create: `frontend/src/components/MetricCard.tsx`
- Create: `frontend/src/components/MetricCard.test.tsx`
- Modify: `frontend/src/App.tsx`

**Steps:**

1. Write a failing test for a metric label and formatted numeric value.
2. Implement a typed reusable component.
3. Add a temporary manual counter or selection exercise to learn state, then
   remove it before committing if it is not product behavior.
4. Explain props versus state.
5. Commit: `feat: add typed dashboard components`.

### Task 14: Create the typed API client

**Learning objective:** Understand `fetch`, promises, async/await, JSON parsing,
TypeScript interfaces, runtime HTTP errors, and separation of transport from UI.

**Files:**

- Create: `frontend/src/api/types.ts`
- Create: `frontend/src/api/client.ts`
- Create: `frontend/src/api/client.test.ts`

**Steps:**

1. Translate approved Pydantic response contracts into TypeScript types.
2. Write failing tests for success, structured API failure, invalid JSON, and
   aborted requests.
3. Implement a small client without React dependencies.
4. Configure the Vite development proxy to `/api` rather than hard-coding a
   production hostname.
5. Commit: `feat: add typed analytics API client`.

### Task 15: Build the four-page application shell

**Learning objective:** Understand client-side routing, nested layouts,
navigation, active links, and URL-based application state.

**Files:**

- Create: `frontend/src/components/AppLayout.tsx`
- Create: `frontend/src/components/Sidebar.tsx`
- Create: four files under `frontend/src/pages/`
- Modify: `frontend/src/App.tsx`
- Create: routing/navigation tests

**Steps:**

1. Write failing tests for all four routes and accessible navigation labels.
2. Implement the sidebar and placeholder page headings.
3. Verify direct URL refresh works through Vite fallback.
4. Keep the UI English and responsive at common desktop widths.
5. Commit: `feat: add analytics dashboard navigation`.

## Phase E: Dashboard data and charts

### Task 16: Implement shared request states and Overview

**Learning objective:** Understand effects, request lifecycle, cancellation,
loading/error/empty/success states, Retry, and manual refresh.

**Files:**

- Create shared loading/error/empty components
- Create a reusable request hook only if duplication demonstrates the need
- Modify: `frontend/src/pages/OverviewPage.tsx`
- Create Overview tests

**Steps:**

1. Write failing tests for all request states and formatted KPI values.
2. Fetch overview/publication data on entry and on explicit Refresh only.
3. Prevent stale requests from overwriting newer results.
4. Render latest publication identity and completion time.
5. Commit: `feat: render dashboard overview`.

### Task 17: Add ECharts through pure transformations

**Learning objective:** Understand the difference between domain data, chart
configuration, React component lifecycle, resize handling, and cleanup.

**Files:**

- Create chart transformation functions and tests
- Create Timeline, Airline Ranking, Airline Comparison, and Heatmap components
- Add ECharts lifecycle tests where valuable

**Steps:**

1. Write pure-function tests using literal inputs and expected axes/series.
2. Implement one chart at a time, starting with the Overview timeline.
3. Initialize, resize, update, and dispose chart instances correctly.
4. Use accessible text summaries so information is not chart-only.
5. Commit: `feat: visualize analytics with ECharts`.

### Task 18: Complete Airline, Time, and Pipeline pages

**Learning objective:** Understand controlled filters, URLSearchParams,
dependent requests, derived values, and evidence presentation.

**Files:**

- Modify the three remaining page files
- Add page behavior tests

**Steps:**

1. Implement Airline metric/Top N/code filters in URL query parameters.
2. Implement Time date/metric filters and preserve them on refresh.
3. Implement Pipeline architecture and acceptance evidence without polling or
   live-status language.
4. Test empty ranges, unknown airlines, Retry, and navigation persistence.
5. Commit: `feat: complete analytics dashboard pages`.

### Task 19: Visual polish and accessibility

**Learning objective:** Understand visual hierarchy, responsive layout,
semantic HTML, keyboard navigation, focus, contrast, and reduced motion.

**Files:**

- Modify frontend styles and components
- Add only behavior-level accessibility tests

**Steps:**

1. Establish a restrained English portfolio visual system.
2. Verify sidebar, filters, buttons, headings, tables, and summaries by keyboard.
3. Check desktop and narrow viewport behavior.
4. Capture reviewed screenshots only after data is real.
5. Commit: `style: polish analytics dashboard`.

---

## Phase F: Packaging and final acceptance

### Task 20: Containerize API and frontend

**Learning objective:** Understand development versus production builds,
multi-stage images, environment injection, reverse proxying, health checks, and
persistent versus disposable state.

**Files:**

- Create pinned API and frontend Dockerfiles
- Create or extend Compose configuration
- Create safe up/down scripts
- Add configuration tests

**Steps:**

1. Write failing configuration tests before Dockerfiles/Compose.
2. Build the frontend into static assets and serve it through a small pinned
   web image or an explicitly justified API static setup.
3. Run the API as a non-root user with a lightweight health check.
4. Connect API to MySQL through the existing external project network.
5. Preserve MySQL by default and never copy secrets into images.
6. Commit: `build: package analytics web application`.

### Task 21: Full acceptance and portfolio handoff

**Learning objective:** Understand release evidence, reproducibility,
limitations, documentation, and defensible CV claims.

**Files:**

- Create: `docs/api-dashboard-runbook.md`
- Modify: `README.md`
- Create ignored local acceptance evidence and screenshots

**Steps:**

1. Run all backend and frontend unit tests and guarded integrations.
2. Validate API response totals against independent MySQL SQL.
3. Exercise all four pages, filters, direct refresh, failure, Retry, restart,
   and production build.
4. Measure startup, representative API latency, build size, and container
   resource use without overstating local measurements.
5. Document exact commands, architecture, screenshots, results, and limits.
6. Draft accurate English CV bullets and interview explanations.
7. Run static checks, inspect ignored secrets/evidence, and review Git status.
8. Commit: `docs: record API dashboard acceptance`.

## Completion criteria

- Every approved `/api/v1` endpoint is typed, read-only, tested, documented,
  and reconciled with the accepted snapshot.
- The four English pages provide loading, error, empty, success, Retry, manual
  refresh, and persistent filter behavior where applicable.
- No credentials reach the browser, repository, logs, screenshots, or images.
- Default tests do not require Docker; guarded tests state their requirements.
- Container stop/start preserves MySQL data.
- README and CV claims describe only measured behavior.
- The user can explain the request path from React through FastAPI to MySQL and
  the upstream path from GDS logs through Kafka, Spark, and HDFS.
