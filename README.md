# Telecom Fraud Detection & Prevention

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Lakebase](https://img.shields.io/badge/Lakebase-Postgres_Interface-336791?style=for-the-badge)](https://docs.databricks.com/en/database/lakebase/)
[![Databricks Apps](https://img.shields.io/badge/Databricks_Apps-React_+_FastAPI-61DAFB?style=for-the-badge)](https://docs.databricks.com/en/dev-tools/databricks-apps/)

End-to-end fraud prevention on Databricks — from data pipeline to analyst workbench to AI investigation agent.

## What It Does

- **Data Pipeline**: Medallion architecture (Bronze/Silver/Gold) generating 100K synthetic transactions and 10K device profiles with fraud scoring
- **Fraud Analyst Workbench**: Three role-based views (Executive, Management, Analyst) backed by Lakebase, Genie Spaces, Knowledge Assistants, and Foundation Model API
- **AI Agent**: ReAct-style investigation assistant with 5 database-backed tools
- **Fraud Engine Demo (Chapter 0)**: Interactive C-level demo showcasing the fraud detection engine with animated CDR rule evaluation, AgentBricks AI summaries, and prevention recommendations

**Default: Telecom Fraud.** The app is industry-configurable — edit `apps/fraud-analyst/server/industry_config.py` to retarget for fintech, insurance, e-commerce, or any scored risk/fraud use case. See [`apps/fraud-analyst/README.md`](apps/fraud-analyst/README.md) for the full app setup guide.

## Architecture

```
                          Databricks Workspace
  +============+    +==========+    +==========+    +==========+
  |  Volumes   | -> |  Bronze  | -> |  Silver  | -> |   Gold   |
  |  (NDJSON)  |    |  Tables  |    |  Tables  |    |  Tables  |
  +============+    +==========+    +==========+    +=====+====+
                                                          |
                                                    +-----v------+
                                                    | Risk Engine|
                                                    +-----+------+
                                                          |
                                                   +------v--------+
                                                   | Delta Tables  |
                                                   |(Unity Catalog)|
                                                   +------+--------+
                                                          | CDF Sync
                                                          v
  +-----------------------------------------------------------+
  |                    Lakebase (Postgres)                    |
  |  Synced: transactions_synced, device_sdk_synced           |
  |  Writable: analyst_review, decision_audit_log             |
  +-----------------------------+-----------------------------+
                                | asyncpg
                                v
  +-----------------------------------------------------------+
  |              FastAPI Backend (Databricks App)             |
  |  /api/executive  /api/dashboard  /api/cases  /api/chat    |
  +-----------------------------+-----------------------------+
                                |
  +---------+  +--------+  +--------+  +------------+
  |  Genie  |  | React  |  | FMAPI  |  | Knowledge  |
  |  Space  |  |  SPA   |  | Agent  |  | Assistant  |
  +---------+  +--------+  +--------+  +------------+
  (Executive)  (3 views)   (Analyst)    (Playbook)
```

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI v0.229+: `pip install databricks-cli`

```bash
databricks auth login --profile <your-profile> --host https://<workspace>.cloud.databricks.com/
```

### 1. Deploy the Data Pipeline

```bash
databricks bundle validate -p <your-profile>
databricks bundle deploy -p <your-profile>
databricks bundle run fraud_data_pipeline -p <your-profile>
```

The pipeline runs 17 tasks generating 100K transactions, 10K device profiles, network CDR data, and fraud scores. Uses `telecommunications` catalog and `fraud_data` schema by default — change in `databricks.yml`.

### 2. Deploy the Fraud Analyst App

See [`apps/fraud-analyst/README.md`](apps/fraud-analyst/README.md) for the full setup guide. Summary:

1. Create a Lakebase instance and sync the Delta tables
2. Create a Genie Space pointing at the case data
3. Create a Knowledge Assistant with your fraud playbooks (optional)
4. Edit `apps/fraud-analyst/app.yaml` with your connection details
5. Deploy:

```bash
# Upload app to workspace
databricks workspace import-dir apps/fraud-analyst \
  /Workspace/Users/<you>/fraud-analyst --overwrite -p <your-profile>

# Create and deploy the app
databricks apps create <app-name> -p <your-profile>
databricks apps deploy <app-name> \
  --source-code-path /Workspace/Users/<you>/fraud-analyst -p <your-profile>
```

## Fraud Engine Demo (Chapter 0)

A standalone interactive demo designed for C-level and GTM presentations. Three data sources (CDR Data, Device SDK, Transaction History) feed the fraud detection engine, which evaluates every transaction against CDR rules and produces one of three outcomes.

| Scenario | Outcome | What It Shows |
|----------|---------|---------------|
| **International SIM Swap** | Auto-Blocked | All 4 CDR rules fail (impossible travel, cell/IP mismatch, rapid cell hop, roaming anomaly). Engine blocks instantly. |
| **After-Hours Activity** | Sent to Review | Mixed signals — AgentBricks AI summarizes the case and assigns it to an analyst. Links to the Analyst case queue. |
| **Routine Device Upgrade** | Approved | All checks pass. Zero-friction approval. Customer never notices. |

Each scenario shows a two-column layout: transaction analysis with animated CDR rule evaluation on the left, AgentBricks summary + AI recommendation + engine decision on the right.

**Two demo modes:** Manual (click scenario tabs) or Auto-Play All (hands-free, ~13s per scenario).

Deploy as a separate Databricks App — see [`apps/fraud-chapter0/`](apps/fraud-chapter0/) for source.

```bash
databricks workspace import-dir apps/fraud-chapter0 \
  /Workspace/Users/<you>/fraud-chapter0 --overwrite -p <your-profile>
databricks apps create fraud-chapter0 -p <your-profile>
databricks apps deploy fraud-chapter0 \
  --source-code-path /Workspace/Users/<you>/fraud-chapter0 -p <your-profile>
```

## Fraud Analyst Workbench

Three role-based views, each with dedicated AI capabilities:

| View | Audience | Key Features |
|------|----------|-------------|
| **Executive** | C-suite, VP | Financial P&L, quarterly trends, regional teams, risk distribution, **Genie Space** chat |
| **Management** | Fraud ops managers | Pipeline health, **AI forecast** (30-day), engine performance, team metrics, SLA tracking, mitigation effectiveness |
| **Analyst** | Fraud investigators | Case queue, case detail + device profile, **AI investigation agent** (tool-calling), **Knowledge Assistant** (company playbook) |

**Industry-configurable**: All domain-specific values (thresholds, mitigations, AI prompts, Genie/KA endpoints) live in `server/industry_config.py`.

### Data Flow

**Read path (sub-200ms):** Browser → FastAPI → asyncpg → Lakebase → Response

**Write path (dual write):** Analyst decisions → Lakebase `analyst_review` (immediate) + Delta write-back via SQL Statement API (eventual consistency via CDF)

**Agent path:** Browser → FastAPI → Foundation Model API (tool-calling) → Lakebase query → Response

### Security

| Control | Implementation |
|---------|---------------|
| SQL Injection | Parameterized queries (`$1`, `$2`) via asyncpg |
| Input Validation | Pydantic `Literal` on decisions; `Field(max_length=...)` on strings; regex on path params |
| Rate Limiting | `slowapi` on all API endpoints |
| CORS | Same-origin only (`allow_origins=[]`) |
| Credentials | Databricks Secrets API (no plaintext passwords) |
| Authentication | OAuth + Service Principal (remote) / CLI profile (local) |
| Credential Rotation | asyncpg pool rebuilt every 45 minutes for token refresh |

## Pipeline Flow

```
Device_ID_Reference
    ├─> Cell_Registry
    │       └─> Raw_Network_Data ─> Bronze ─> Silver ─> Gold_Network_Data ─┐
    ├─> Raw_Device_SDK ─> Bronze ─> Silver ─> Gold_Device_SDK ─────────────┤
    └─> Raw_App_Transactions ─> Bronze ─> Silver ─> Gold_Transactions ───┴─> Risk_Engine ─> analyst_assignment
```

All three gold tables feed the **Risk Engine**, which applies transaction, device, and network rules (impossible travel, cell–IP mismatch, rapid cell hop, roaming anomaly, VPN/emulator) and writes `transaction_risk_engine`.

## Configuration

### Catalog and Schema

Default: **`telecommunications.fraud_data`**. Override in `databricks.yml`:

```yaml
variables:
  catalog: "your_catalog"
  schema: "your_schema"
```

### App Industry Configuration

Edit `apps/fraud-analyst/server/industry_config.py` to change:
- Score thresholds, risk buckets, mitigation actions
- AI agent system prompt and domain knowledge
- Genie Space ID, Knowledge Assistant endpoint, LLM model
- Table names, status labels, decision values

See [`apps/fraud-analyst/README.md`](apps/fraud-analyst/README.md) for details.

## Project Structure

```
fraud_prevention_in_telco/
├── databricks.yml                          # Asset Bundle config (pipeline + dashboard)
├── notebooks/                              # Pipeline tasks (17 Python scripts)
├── dashboards/                             # AI/BI dashboard (fraud_detection.lvdash.json)
├── scripts/                                # deploy.sh, run_job.py
├── docs/                                   # PIPELINE_DETAILS.md, pipeline.json
├── apps/fraud-analyst/
│   ├── app.yaml                            # Databricks App manifest
│   ├── app.py                              # FastAPI entry point
│   ├── README.md                           # App setup guide
│   ├── server/
│   │   ├── industry_config.py              # ★ Industry-specific configuration
│   │   ├── config.py                       # Workspace auth
│   │   ├── db.py                           # Lakebase + Delta connections
│   │   ├── llm.py                          # Foundation Model API client
│   │   ├── models.py                       # Pydantic models
│   │   ├── agent/                          # ReAct agent + 5 tools
│   │   └── routes/
│   │       ├── cases.py                    # Analyst view API
│   │       ├── dashboard.py                # Management view API
│   │       ├── executive.py                # Executive view API
│   │       └── chat.py                     # AI chat + Knowledge Assistant
│   └── frontend/
│       ├── src/                            # React source (TypeScript)
│       └── dist/                           # Pre-built frontend
├── apps/fraud-chapter0/
│   ├── app.yaml                            # Standalone demo app manifest
│   ├── app.py                              # FastAPI static file server
│   ├── requirements.txt                    # Python deps (fastapi, uvicorn)
│   └── frontend/
│       ├── src/pages/FraudEngineLive.tsx   # ★ Interactive fraud engine demo
│       └── dist/                           # Pre-built frontend
├── CONTRIBUTING.md
└── SECURITY.md
```

## Documentation

- **[apps/fraud-analyst/README.md](apps/fraud-analyst/README.md)** — App setup, prerequisites, per-view data mapping
- **[docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md)** — Pipeline design, table schemas, fraud detection logic
- **[databricks.yml](databricks.yml)** — Bundle configuration (pipeline job + dashboard)

## Contributing

1. Clone this project locally.
2. Validate changes: `databricks bundle validate -t dev`
3. Contribute via pull requests with a review from a teammate.

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
