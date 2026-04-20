# iot-hello-world

IoT sensor data pipeline built with Lakeflow Spark Declarative Pipelines (SDP) and Declarative Automation Bundles (DAB), targeting Databricks Free Edition (serverless).

## Pipeline

Medallion architecture processing the `/databricks-datasets/iot/iot_devices.json` sample dataset:

- **Bronze** — Raw ingest, materialized view over source JSON
- **Silver** — Cleaned and standardized fields, null coordinate filtering
- **Gold** — Country-level aggregation of device stats (temp, humidity, CO2, battery)

## Setup

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
2. Generate a Personal Access Token: User Settings → Developer → Access Tokens
3. Update `databricks.yml` with your workspace URL
4. Push this repo to GitHub
5. In Databricks, connect the repo as a Git Folder

## Deploy

From the Databricks workspace:

1. Navigate to the Git Folder containing this bundle
2. Open `databricks.yml` → click the deployments icon
3. Select the `dev` target → click Deploy
4. Under Bundle Resources, click play on `iot_refresh` to trigger the pipeline

Or from the CLI:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run iot_refresh -t dev
```

## Project Structure

```
iot-hello-world/
├── databricks.yml              # Bundle config
├── pyproject.toml              # Python project + dev dependencies
├── README.md
├── resources/
│   ├── iot_etl.pipeline.yml    # SDP pipeline definition
│   └── iot_job.job.yml         # Lakeflow Job definition
└── src/
    └── iot_etl/
        ├── explorations/       # Ad-hoc notebooks (not in pipeline)
        ├── transformations/    # SDP pipeline source files
        └── README.md
```
