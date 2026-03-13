# Telecom Fraud Detection & Prevention

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Lakebase](https://img.shields.io/badge/Lakebase-Postgres_Interface-336791?style=for-the-badge)](https://docs.databricks.com/en/database/lakebase/)
[![Databricks Apps](https://img.shields.io/badge/Databricks_Apps-React_+_FastAPI-61DAFB?style=for-the-badge)](https://docs.databricks.com/en/dev-tools/databricks-apps/)

End-to-end fraud prevention on Databricks — from data pipeline to analyst workbench to AI investigation agent.

## What It Does

- **Data Pipeline**: Medallion architecture (Bronze/Silver/Gold) generating 100K synthetic transactions and 10K device profiles with fraud scoring
- **Fraud Analyst App**: React + FastAPI workbench for case investigation, backed by Lakebase for sub-200ms queries
- **AI Agent**: ReAct-style investigation assistant using Foundation Model API with 5 database-backed tools

**Configuration**: Uses `telecommunications` catalog and `fraud_data` schema by default. Both can be changed via job parameters.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks Workspace                   │
│                                                           │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐           │
│  │  Volumes  │───▶│  Bronze  │───▶│  Silver  │           │
│  │  (NDJSON) │    │  Tables  │    │  Tables  │           │
│  └──────────┘    └──────────┘    └──────────┘           │
│       ▲                                │                  │
│  Auto Loader                           ▼                  │
│  (Streaming)              ┌──────────────────┐           │
│                           │   Gold Tables    │           │
│                           └────────┬─────────┘           │
│                                    ▼                      │
│                           ┌──────────────────┐           │
│                           │   Risk Engine    │           │
│                           │  (fraud scores)  │           │
│                           └────────┬─────────┘           │
│                                    ▼                      │
│                           ┌──────────────────┐           │
│                           │  Delta Tables    │           │
│                           │ (Unity Catalog)  │           │
│                           └────────┬─────────┘           │
│                                    │ CDF Sync             │
│                                    ▼                      │
│  ┌─────────────────────────────────────────────────┐    │
│  │              Lakebase (Postgres)                  │    │
│  │  Synced: transaction_risk, silver_device_sdk     │    │
│  │  Custom: analyst_decisions (writable)             │    │
│  └──────────────────────┬──────────────────────────┘    │
│                         │ asyncpg (wire protocol)        │
│                         ▼                                 │
│  ┌─────────────────────────────────────────────────┐    │
│  │           FastAPI Backend (App)                   │    │
│  │  Routes: /cases, /dashboard, /chat               │    │
│  │  Agent: ReAct loop + 5 tools                     │    │
│  └──────────────────────┬──────────────────────────┘    │
│                         │ REST API                        │
│                         ▼                                 │
│  ┌─────────────────────────────────────────────────┐    │
│  │           React Frontend (SPA)                    │    │
│  │  Pages: Case Queue, Case Detail, Dashboard        │    │
│  └─────────────────────────────────────────────────┘    │
│                                                           │
│  ┌─────────────────────────────────────────────────┐    │
│  │      Foundation Model API (Claude Sonnet 4.5)     │    │
│  │  Tool-calling agent for fraud investigation       │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Installation Guidelines

1. **Clone** the project into your Databricks workspace (or clone locally and use the Databricks CLI).
2. **Open** the Asset Bundle Editor in the Databricks UI (or use the CLI from the project root).
3. **Deploy**: Click "Deploy" in the UI, or run `databricks bundle deploy`.
4. **Run**: In the Deployments tab, click "Run" on the **Fraud Data Pipeline** job, or run `databricks bundle run fraud_data_pipeline`.

The pipeline runs 17 tasks and generates raw unstructured data plus all tables (including cell registry, fraud analyst roster, and network CDR-like data) for the Fraud Detection dashboard and downstream applications.

## Quick Start (CLI)

### Prerequisites

```bash
pip install databricks-cli
databricks auth login -p <your-profile> --host https://<workspace>.cloud.databricks.com/
```

Copy `env.example` to `.env` and set your profile if needed. See `.databrickscfg.example` for host/auth.

### Deploy Pipeline and Run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run fraud_data_pipeline
```

### Deploy the Fraud Analyst App

```bash
# Build the frontend
cd apps/fraud-analyst/frontend && npm install && npm run build && cd ../../..

# Sync app files to workspace
databricks sync apps/fraud-analyst /Workspace/Users/<user>/fraud-analyst \
  --watch=false --exclude "frontend/node_modules" --exclude "frontend/src" \
  --exclude "frontend/public" --exclude "__pycache__" -p <your-profile>

# Deploy the app
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<user>/fraud-analyst -p <your-profile>
```

The app requires a Lakebase instance with synced tables and a Foundation Model serving endpoint. See [docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md) for Lakebase setup.

## Pipeline Flow

```
Device_ID_Reference
    ├─> Cell_Registry
    │       └─> Raw_Network_Data ─> Bronze_Network_Data ─> Silver_Network_Data ─> Gold_Network_Data ─┐
    ├─> Raw_Device_SDK ─> Bronze_Device_SDK ─> Silver_Device_SDK ─> Gold_Device_SDK ────────────────┤
    └─> Raw_App_Transactions ─> Bronze_Transactions ─> Silver_Transactions ─> Gold_Transactions ──┴─> Risk_Engine ─> analyst_assignment
```

All three gold tables (Gold_Transactions, Gold_Device_SDK, Gold_Network_Data) feed the **Risk Engine**, which applies transaction, device, and network rules (e.g. impossible travel, cell–IP mismatch, rapid cell hop, roaming anomaly, VPN/emulator) and writes `transaction_risk_engine`.

Raw data is written as JSON Lines (NDJSON) before Bronze ingestion (paths use the configured catalog/schema):
- Device profiles: `/Volumes/{catalog}/{schema}/raw_device_sdk/`
- Transactions: `/Volumes/{catalog}/{schema}/raw_app_transactions/`
- Network (CDR-like): `/Volumes/{catalog}/{schema}/raw_network_data/`

## Fraud Analyst App

The `apps/fraud-analyst/` directory contains a full-stack Databricks App for case investigation.

### Data Flow

**Read path (sub-200ms):** Browser → FastAPI → asyncpg → Lakebase → Response. Zero caching — every request hits Lakebase live.

**Write path (dual write):** Analyst decisions write to Lakebase `analyst_decisions` (immediate), then async write-back to Delta via SQL Statement API (eventual consistency).

**Agent path:** Browser → FastAPI → Foundation Model API (with tool definitions) → Tool call → Lakebase query → Tool result → Final response → Browser.

### API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/health` | Lakebase connectivity check |
| `GET` | `/api/cases` | Paginated case queue with filters |
| `GET` | `/api/cases/filters` | Distinct filter values |
| `GET` | `/api/cases/{id}` | Case detail + device profile |
| `GET` | `/api/cases/{id}/history` | Decision history |
| `GET` | `/api/cases/{id}/customer-history` | Customer transaction history |
| `POST` | `/api/cases/{id}/action` | Analyst decision write-back |
| `GET` | `/api/dashboard/stats` | Aggregate KPIs (8 metrics) |
| `GET` | `/api/dashboard/trends` | Daily time-series (30 days) |
| `GET` | `/api/dashboard/geo` | Regional breakdown |
| `GET` | `/api/dashboard/top-risk` | Top 10 pending high-risk cases |
| `POST` | `/api/chat` | AI assistant (non-streaming) |
| `POST` | `/api/chat/stream` | AI assistant (SSE streaming) |

### Security

| Control | Implementation |
|---------|---------------|
| SQL Injection | Parameterized queries (`$1`, `$2`) via asyncpg |
| Input Validation | Pydantic `Literal` on decisions; `Field(max_length=...)` on strings; regex on path params |
| Rate Limiting | `slowapi` on `/api/chat` (10 requests/minute per IP) |
| CORS | Same-origin only (`allow_origins=[]`) |
| Error Sanitization | Generic messages returned to client |
| Audit Logging | Structured log entry for every analyst decision |
| Authentication | OAuth + Service Principal (remote) / CLI profile (local) |
| Credential Rotation | asyncpg pool rebuilt every 45 minutes for token refresh |

### Tech Stack

| Layer | Technology |
|-------|-----------|
| Database | Lakebase (Postgres), asyncpg, Change Data Feed |
| Backend | FastAPI, Pydantic, aiohttp, slowapi |
| Frontend | React 19, Vite, TailwindCSS v4, recharts, pigeon-maps |
| AI | Foundation Model API (Claude Sonnet 4.5), OpenAI SDK |
| Auth | Databricks SDK, OAuth, Service Principal |

## Common Commands

```bash
# Deploy changes
databricks bundle deploy --force

# Run pipeline
databricks bundle run fraud_data_pipeline

# Deploy/run via script (blueprint style)
./scripts/deploy.sh dev deploy
./scripts/deploy.sh dev run

# Production deployment
databricks bundle deploy -t prod
databricks bundle run fraud_data_pipeline -t prod
```

## Configuration

### Catalog and Schema

The pipeline uses **`telecommunications`** as the catalog and **`fraud_data`** as the schema by default. You can override these by editing `databricks.yml`:

```yaml
variables:
  catalog: "your_catalog"
  schema: "your_schema"
```

These variables are passed as job parameters to all pipeline tasks. Tables, volumes, and raw data paths all use the configured catalog and schema.

Then redeploy: `databricks bundle deploy --force`.

### SQL Warehouse

The dashboard uses the SQL warehouse defined by `warehouse_id` in `databricks.yml` (default: **Shared Endpoint**). To use a different warehouse, set the `warehouse_id` lookup to your warehouse name. The dashboard is deployed with the bundle when you run `databricks bundle deploy`.

## Documentation

- **[docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md)** -- Pipeline design, table schemas, fraud detection logic
- **[databricks.yml](databricks.yml)** -- Bundle configuration, job, and dashboards (single file, blueprint style)

## Project Structure

```
telco_fraud_detection/
├── databricks.yml                          # Asset Bundle config
├── notebooks/                              # Pipeline Python scripts (job tasks)
├── dashboards/                             # AI/BI dashboard (fraud_detection.lvdash.json)
├── scripts/                                # deploy.sh, run_job.py
├── docs/                                   # PIPELINE_DETAILS.md, pipeline.json
├── apps/fraud-analyst/
│   ├── app.yaml                            # Databricks App manifest
│   ├── app.py                              # FastAPI entry point
│   ├── requirements.txt                    # Python dependencies
│   ├── server/
│   │   ├── config.py                       # Dual-mode auth (local/remote)
│   │   ├── db.py                           # asyncpg pool + Delta writer
│   │   ├── llm.py                          # Foundation Model API client
│   │   ├── models.py                       # Pydantic models
│   │   ├── agent/                          # ReAct agent + 5 tools
│   │   └── routes/                         # cases, dashboard, chat
│   └── frontend/
│       └── src/
│           ├── App.tsx                     # Router + nav
│           ├── lib/api.ts                  # Typed API client
│           ├── components/                 # ChatPanel, ScoreBar, StatusBadge, etc.
│           └── pages/                      # CaseQueue, CaseDetail, Dashboard
├── env.example                             # Example env vars
├── CONTRIBUTING.md                         # Contribution terms
├── SECURITY.md                             # Security policy
└── requirements.txt                        # Python dependencies
```

## Contributing

1. **git clone** this project locally.
2. Use the Databricks CLI to validate and test changes against a workspace: `databricks bundle validate -t dev`
3. Contribute via pull requests (PRs), with a review from a teammate when possible.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution terms.

## Third-Party Package Licenses

(c) 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the [Databricks License](https://databricks.com/db-license-source). Third-party libraries are subject to the licenses set forth below.

| Package      | License    | Copyright      |
| ------------ | ---------- | -------------- |
| pandas       | BSD-3      | BSD            |
| numpy        | BSD-3      | NumPy          |
| scikit-learn | BSD-3      | scikit-learn   |
| matplotlib   | PSF        | Matplotlib     |
| seaborn      | BSD-3      | Seaborn        |
| hdbscan      | Apache-2.0 | Leland McInnes |
| faker        | MIT        | Faker          |
| fastapi      | MIT        | Tiangolo       |
| asyncpg      | Apache-2.0 | MagicStack     |
| recharts     | MIT        | recharts       |
| pigeon-maps  | MIT        | pigeon-maps    |

---

For detailed pipeline information, see [docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md).
