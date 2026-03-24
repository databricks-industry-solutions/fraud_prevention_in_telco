# Telecom Fraud Ops Workbench

A three-view Databricks App for fraud operations — Executive, Management, and Analyst — backed by Lakebase, Foundation Model API, Genie Spaces, and Knowledge Assistants.

**Default: Telecom Fraud.** To retarget for fintech, insurance, e-commerce, healthcare, or any industry with scored risk/fraud cases, swap out `server/industry_config.py` for your specific needs. That's the only file you need to change.

### Prerequisites

Create these Databricks resources before deploying:

| Resource | Purpose | How to Create |
|----------|---------|---------------|
| **Unity Catalog + Schema** | Stores your case/transaction data as Delta tables | Catalog Explorer → Create Catalog/Schema |
| **Lakebase Instance** | Postgres for sub-200ms reads | Catalog Explorer → Lakebase → Create Instance |
| **Synced Tables** | Replicate Delta → Lakebase | `databricks database create-synced-database-table` |
| **Genie Space** | NL data exploration (Executive view) | AI/BI → Genie → New Space (point at your case table) |
| **Knowledge Assistant** | RAG over internal docs (Analyst view) | AI/BI → Knowledge Assistant → Create |
| **Model Serving Endpoint** | LLM for AI agent (Analyst view) | Serving → Create Endpoint (or use Foundation Model) |
| **SQL Warehouse** | For Delta write-back and AI forecast | SQL Warehouses → Create (Pro/Serverless required for ai_forecast) |

### Step-by-Step Setup

**1. Prepare your data**

Your primary case table needs at minimum these columns:
- Unique ID, timestamp, score (0-100), review status, region, customer name
- Dollar amount/exposure, risk engine labels, lat/long (optional)

Run your data pipeline to populate the Delta tables in Unity Catalog.

**2. Create the Lakebase instance and sync tables**

```bash
# Create instance
databricks database create-database-instance my-lakebase --capacity=CU_1 --profile PROFILE

# Sync your primary table
databricks database create-synced-database-table --json '{
  "name": "YOUR_CATALOG.YOUR_SCHEMA.cases_synced",
  "database_instance_name": "my-lakebase",
  "logical_database_name": "databricks_postgres",
  "spec": {
    "source_table_full_name": "YOUR_CATALOG.YOUR_SCHEMA.your_case_table",
    "scheduling_policy": "TRIGGERED",
    "primary_key_columns": ["your_id_column"]
  }
}' --profile PROFILE
```

Create the writable tables directly in Lakebase (analyst_review, decision_audit_log, monthly_targets) — see the existing schema in the telco deployment for reference.

**3. Create the Genie Space**

- Go to AI/BI → Genie → New Space
- Add your synced case table as a data source
- Configure sample questions relevant to your domain
- Copy the Genie Space ID from the URL

**4. Create the Knowledge Assistant (optional)**

- Go to AI/BI → Knowledge Assistant → Create
- Upload your internal fraud playbooks, SOPs, and reference docs
- Deploy to a serving endpoint
- Copy the endpoint name

**5. Edit `server/industry_config.py`**

Update every value for your industry. The file is organized by section:

```python
# AI Integrations
GENIE_SPACE_ID_DEFAULT = "your-genie-space-id"
KA_ENDPOINT_DEFAULT = "your-ka-endpoint-name"
SERVING_ENDPOINT_DEFAULT = "databricks-claude-sonnet-4-5"

# Data
DELTA_CATALOG = "your_catalog"
DELTA_SCHEMA = "your_schema"
PRIMARY_TABLE = "your_cases_synced"
...

# Thresholds, mitigations, AI prompts — see inline comments
```

**6. Update `app.yaml`**

Set the environment variables for your Lakebase instance:

```yaml
env:
  - name: PGHOST
    value: "your-lakebase-instance.database.cloud.databricks.com"
  - name: PGUSER
    value: "your-app-sp-application-id"
  - name: PGPASSWORD_SECRET_SCOPE
    value: "your-secret-scope"
  - name: PGPASSWORD_SECRET_KEY
    value: "pg-password"
  - name: WAREHOUSE_ID
    value: "your-warehouse-id"
```

**7. Deploy**

```bash
# Create the app (first time only)
databricks apps create fraud-analyst --profile PROFILE

# Upload source and deploy
databricks workspace import-dir ./apps/fraud-analyst /Workspace/Users/you@company.com/fraud-analyst --overwrite --profile PROFILE
databricks apps deploy fraud-analyst --source-code-path /Workspace/Users/you@company.com/fraud-analyst --profile PROFILE
```

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    React Frontend                        │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────────┐ │
│  │Executive │  │  Management  │  │     Analyst        │ │
│  │  View    │  │    View      │  │      View          │ │
│  │          │  │              │  │                     │ │
│  │ Genie    │  │ Pipeline     │  │ Case Queue + Detail │ │
│  │ Space    │  │ AI Forecast  │  │ AI Agent Chat       │ │
│  │ Chat     │  │ Team Perf    │  │ Knowledge Assistant │ │
│  └──────────┘  └──────────────┘  └───────────────────┘ │
└─────────────┬───────────────────────────┬───────────────┘
              │ REST API (FastAPI)        │
┌─────────────┴───────────────────────────┴───────────────┐
│                    Python Backend                         │
│  industry_config.py ← ALL domain-specific values         │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌──────────┐  │
│  │Lakebase │  │SQL Stmt  │  │ FMAPI   │  │ Genie /  │  │
│  │(asyncpg)│  │API(Delta)│  │(OpenAI) │  │ KA API   │  │
│  └────┬────┘  └────┬─────┘  └────┬────┘  └────┬─────┘  │
└───────┼────────────┼─────────────┼─────────────┼────────┘
        │            │             │             │
   Lakebase PG   Delta Lake    Claude/GPT   Genie + KA
   (fast reads)  (write-back)  (AI agent)   (NL query)
```

## What Each View Shows

### Executive View
| Component | Data Source | Config Key |
|-----------|-----------|------------|
| Financial P&L | `PRIMARY_TABLE` aggregates | `FLAGGED_THRESHOLD` |
| Quarterly Trends | `PRIMARY_TABLE` by quarter | `DELTA_CATALOG/SCHEMA` |
| Regional Teams | `PRIMARY_TABLE` + `TARGETS_TABLE` by region | `TARGETS_TABLE` |
| Risk Distribution | `PRIMARY_TABLE` score buckets | `RISK_BUCKETS` |
| Genie Chat | Databricks Genie API | `GENIE_SPACE_ID_DEFAULT` |

### Management View
| Component | Data Source | Config Key |
|-----------|-----------|------------|
| Pipeline Summary | `PRIMARY_TABLE` period comparison | `FLAGGED_THRESHOLD` |
| Engine Performance | `PRIMARY_TABLE` auto-block stats | `HIGH_RISK_THRESHOLD` |
| 30-Day AI Forecast | `DELTA_CATALOG.DELTA_SCHEMA` via ai_forecast() | `DELTA_CATALOG/SCHEMA` |
| Team Performance | `PRIMARY_TABLE` by analyst | `FLAGGED_THRESHOLD` |
| SLA Tracking | `REVIEW_TABLE` + `AUDIT_TABLE` | `REVIEW_TABLE`, `AUDIT_TABLE` |
| Mitigation Effectiveness | `REVIEW_TABLE` JOIN `PRIMARY_TABLE` | `MITIGATIONS` |

### Analyst View
| Component | Data Source | Config Key |
|-----------|-----------|------------|
| Case Queue | `PRIMARY_TABLE` with filters/sorting | `PRIMARY_TABLE` |
| Case Detail | `PRIMARY_TABLE` + `DEVICE_TABLE` | `DEVICE_TABLE` |
| Decision Form | Writes to `REVIEW_TABLE` + `AUDIT_TABLE` + Delta | `ALLOWED_DECISIONS`, `MITIGATIONS` |
| AI Investigation Chat | Foundation Model API with tool calling | `SERVING_ENDPOINT_DEFAULT`, `AGENT_SYSTEM_PROMPT` |
| Company Playbook Chat | Knowledge Assistant endpoint | `KA_ENDPOINT_DEFAULT` |

## Frontend Customization

The page title is loaded at runtime from `/api/config` — change `app_title` in `app.py` and it updates without a rebuild.

The following labels are **compiled into the frontend** and require a React rebuild to change:

| Label | Where it appears | Default value |
|-------|-----------------|---------------|
| Navbar title | Top bar | "Telecom Fraud Operations" |
| "Device Profile" section | Analyst case detail | "Device Profile" |
| "subscriber_*" field labels | Analyst case detail | subscriber_device_model, etc. |
| "SIM Swap" risk badge | Analyst case detail | Pattern match on risk_reason |
| Tab names | Executive / Management / Analyst | Hardcoded |

For a **full industry rebrand**, rebuild the frontend:

```bash
cd frontend
npm install
# Edit src/ files to change labels
npm run build
```

The backend API responses use generic field names (`transaction_id`, `fraud_score`, `case_exposure_usd`) which work across industries. The frontend renders these as-is — column headers like "Score", "Amount", "Region" are already generic.

## File Structure

```
apps/fraud-analyst/
├── app.py                      # FastAPI app, routes, health check
├── app.yaml                    # Databricks App config (env vars, resources)
├── requirements.txt            # Python dependencies
├── server/
│   ├── industry_config.py      # ★ THE ONLY FILE TO EDIT FOR A NEW INDUSTRY
│   ├── db.py                   # Lakebase + Delta connections
│   ├── config.py               # Workspace auth (OAuth, host)
│   ├── llm.py                  # Foundation Model API client
│   ├── models.py               # Pydantic request/response models
│   ├── agent/
│   │   ├── agent.py            # ReAct agent loop
│   │   ├── prompts.py          # Imports from industry_config.py
│   │   └── tools.py            # Agent tools (query DB, search cases)
│   └── routes/
│       ├── cases.py            # Analyst view API
│       ├── dashboard.py        # Management view API
│       ├── executive.py        # Executive view API
│       └── chat.py             # AI chat + Knowledge Assistant
└── frontend/
    └── dist/                   # Compiled React app (pre-built)
```
