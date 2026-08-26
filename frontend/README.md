# GDS Analytics Dashboard

A responsive React and TypeScript dashboard for exploring the validated GDS
analytics snapshot served by the project's read-only FastAPI application.

## Features

- Overview KPI cards for metric rows, airlines, successful responses, and tokens
- Publication metadata with manual and 30-second automatic overview refresh
- Ranked airline table with Top 5, Top 10, and Top 20 selection
- Per-airline hourly response and token timeline
- System-wide hourly response and token timeline
- Airline-by-hour activity heatmap
- API health and publication status page
- Loading states and safe user-facing error messages
- Responsive navigation and project-specific visual branding

## Technology

- React
- TypeScript
- React Router
- Apache ECharts
- Vite
- Vitest
- Testing Library
- ESLint

Only the required ECharts modules are registered. This reduced the minified
JavaScript bundle from approximately 1.36 MB to approximately 799 kB.

## Requirements

Use the Node.js version declared in the repository root `.nvmrc` file:

```bash
nvm use
```

Install dependencies when setting up the frontend for the first time:

```bash
npm ci
```

## Run the complete application locally

The dashboard requires MySQL and the FastAPI backend to provide live data.

### 1. Start MySQL

From the repository root:

```bash
bash scripts/mysql-up.sh
```

Confirm that MySQL is healthy:

```bash
docker ps \
  --filter name=gds-mysql \
  --format 'table {{.Names}}\t{{.Status}}'
```

### 2. Start the FastAPI backend

From the repository root in a dedicated WSL terminal:

```bash
source .venv/bin/activate

set -a
source infrastructure/mysql/.env
set +a

python -m uvicorn \
  gds_pipeline.api.main:create_runtime_app \
  --factory \
  --host 127.0.0.1 \
  --port 8000
```

The API is available inside WSL at:

```text
http://127.0.0.1:8000
```

### 3. Start the frontend

In a second WSL terminal:

```bash
cd frontend
nvm use
npm run dev -- --host 0.0.0.0
```

Open the `Network` URL printed by Vite, for example:

```text
http://172.26.246.69:5173
```

The WSL IP address can change after restarting WSL. Run `hostname -I` when the
previous address no longer works.

Vite proxies frontend `/api` requests to the FastAPI server at
`http://127.0.0.1:8000`.

## Pages

| Route | Page | Description |
|---|---|---|
| `/` | Overview | Snapshot KPIs, publication metadata, and automatic refresh |
| `/airlines` | Airline Analysis | Ranked airlines and selectable airline timeline |
| `/time` | Time Analysis | System timeline and airline activity heatmap |
| `/pipeline` | Pipeline & Data Quality | API health and publication metadata |

## API endpoints

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/health` | API and database readiness |
| `GET /api/v1/overview` | Dashboard overview metrics |
| `GET /api/v1/airlines?limit=10` | Ranked airlines |
| `GET /api/v1/airlines/{code}/timeline` | Per-airline hourly metrics |
| `GET /api/v1/timeline` | System-wide hourly metrics |
| `GET /api/v1/hourly-heatmap?limit=10` | Airline-by-hour heatmap |
| `GET /api/v1/publication` | Published snapshot metadata |

## Quality checks

Run all frontend tests with one worker. A single worker is more reliable when
the repository is stored under `/mnt/c` in WSL:

```bash
pkill -f vitest || true

npx vitest run \
  --pool=forks
  --maxWorkers=1
```

Run linting and create a production build:

```bash
npm run lint
npm run build
```

The verified frontend baseline contains 44 passing tests across 13 test files.

The production build currently emits Vite's chunk-size advisory because the
main JavaScript bundle remains above 500 kB. This is a performance optimization
opportunity and does not indicate a failed build.

## Troubleshooting

### Browser reports `ERR_CONNECTION_RESET`

Start Vite with:

```bash
npm run dev -- --host 0.0.0.0
```

Then open the WSL `Network` address instead of `127.0.0.1` from Windows.

### Pages show `Unable to load...`

Confirm that:

- MySQL is running and healthy;
- the FastAPI process is running on WSL port 8000;
- `curl http://127.0.0.1:8000/api/v1/overview` returns JSON;
- `curl http://127.0.0.1:5173/api/v1/overview` works through the Vite proxy.

### Vitest worker startup times out

Terminate stale workers and rerun with one worker:

```bash
pkill -f vitest || true

npx vitest run \
  --pool=forks
  --maxWorkers=1
```
