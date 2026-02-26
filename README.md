# Telecom Fraud Detection Data Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-Solution_Accelerator-FF3621?style=for-the-badge&logo=databricks)](https://databricks.com)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Enabled-00A1C9?style=for-the-badge)](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)
[![Serverless](https://img.shields.io/badge/Serverless-Compute-00C851?style=for-the-badge)](https://docs.databricks.com/en/compute/serverless.html)

A Databricks Asset Bundle implementing a **medallion architecture** (Bronze → Silver → Gold) for generating synthetic telecom fraud detection data.

## What It Does

Generates 9 tables of synthetic fraud detection data:

- Device profiles and transaction data
- Risk scoring and fraud detection
- Analyst review workflows
- Ready for dashboards and ML models

**Configuration**: Uses `telecommunications` catalog and `fraud_data` schema by default. Both can be changed via job parameters.

## Installation Guidelines

1. **Clone** the project into your Databricks workspace (or clone locally and use the Databricks CLI).
2. **Open** the Asset Bundle Editor in the Databricks UI (or use the CLI from the project root).
3. **Deploy**: Click "Deploy" in the UI, or run `databricks bundle deploy`.
4. **Run**: In the Deployments tab (🚀), click "Run" on the **Fraud Data Pipeline** job, or run `databricks bundle run fraud_data_pipeline`.

The pipeline runs 11 tasks and generates raw unstructured data plus all tables for the Fraud Detection dashboard and downstream applications.

## Quick Start (CLI)

### Prerequisites

```bash
pip install databricks-cli
databricks auth login -p <your-profile> --host https://<workspace>.cloud.databricks.com/
```

Copy `env.example` to `.env` and set your profile if needed. See `.databrickscfg.example` for host/auth.

### Deploy and Run

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run fraud_data_pipeline
```

## Pipeline Flow

```
Device_ID_Reference
    ├─> Raw_Device_SDK ─> Bronze_Device_SDK ─> Silver_Device_SDK ─> Gold_Device_SDK ─┐
    └─> Raw_App_Transactions ─> Bronze_Transactions ─> Silver_Transactions ─> Gold_Transactions ─> Risk_Engine ─> analyst_simulation
                                                                                    ^
                                                                                    │
                                                                         (both gold tables feed Risk_Engine)
```

Raw data is written as JSON Lines (NDJSON) before Bronze ingestion (paths use the configured catalog/schema):
- Device profiles: `/Volumes/{catalog}/{schema}/raw_device_sdk/`
- Transactions: `/Volumes/{catalog}/{schema}/raw_app_transactions/`

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

- **[docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md)** – Pipeline design, table schemas, fraud detection logic
- **[databricks.yml](databricks.yml)** – Bundle configuration, job, and dashboards (single file, blueprint style)

## Project Structure (Industry Solutions Blueprint)

```
datapipeline/
├── databricks.yml       # Bundle config, variables, jobs, dashboards (single file)
├── notebooks/           # Pipeline Python scripts (job tasks run from here)
├── dashboards/          # AI/BI dashboard (fraud_detection.lvdash.json)
├── scripts/             # deploy.sh, run_job.py (workspace job reset)
├── apps/                # Optional Databricks apps (placeholder)
├── docs/                # PIPELINE_DETAILS.md, pipeline.json
├── .github/             # GitHub workflows (optional)
├── env.example          # Example env vars for bundle
└── requirements.txt    # Python dependencies
```

## Contributing

1. **git clone** this project locally.
2. Use the Databricks CLI to validate and test changes against a workspace: `databricks bundle validate -t dev`
3. Contribute via pull requests (PRs), with a review from a teammate when possible.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution terms.

## Third-Party Package Licenses

© 2025 Databricks, Inc. All rights reserved. The source in this project is provided subject to the [Databricks License](https://databricks.com/db-license-source). Third-party libraries are subject to the licenses set forth below.

| Package      | License   | Copyright |
| ------------ | --------- | --------- |
| pandas       | BSD-3     | BSD       |
| numpy        | BSD-3     | NumPy     |
| scikit-learn | BSD-3     | scikit-learn |
| matplotlib   | PSF       | Matplotlib |
| seaborn      | BSD-3     | Seaborn   |
| hdbscan      | Apache-2.0| Leland McInnes |
| faker        | MIT       | Faker     |

---

For detailed pipeline information, see [docs/PIPELINE_DETAILS.md](docs/PIPELINE_DETAILS.md).
