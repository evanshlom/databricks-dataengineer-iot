# iot-hello-world

Air quality data pipeline built with Lakeflow Spark Declarative Pipelines (SDP) and Declarative Automation Bundles (DAB), ingesting real-time data from [Sensor.Community](https://sensor.community) on Databricks Free Edition (serverless).

## Pipeline

Medallion architecture processing live air quality sensor data from 15,000+ sensors worldwide:

- **Ingestion** — Scheduled notebook polls Sensor.Community API, writes JSON to a Unity Catalog Volume
- **Bronze** — Auto Loader streaming table incrementally ingests JSON files from the Volume
- **Silver** — Cleaned and standardized fields (PM10, PM2.5, temperature, humidity, pressure)
- **Gold** — Country-level aggregation of air quality stats

## Setup

1. Sign up for [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
2. Update `databricks.yml` with your workspace URL
3. Create the catalog, schema, and volume:
   ```sql
   CREATE CATALOG IF NOT EXISTS catalog1;
   CREATE SCHEMA IF NOT EXISTS catalog1.iot_hello_world;
   CREATE VOLUME IF NOT EXISTS catalog1.iot_hello_world.raw_landing;
   ```
4. Push this repo to GitHub
5. In Databricks, connect the repo as a Git Folder

## Deploy

From the Databricks workspace:

1. Navigate to the Git Folder containing this bundle
2. Open `databricks.yml` → click the deployments icon
3. Select the `dev` target → click Deploy

## Run

1. Under Bundle Resources, click play on `sensor_ingest` to fetch data from Sensor.Community
2. Click play on `iot_refresh` to run the SDP pipeline (bronze → silver → gold)
3. Verify: `SELECT * FROM catalog1.iot_hello_world.sensor_gold_country_stats`

The ingestion job is scheduled every 30 minutes. The pipeline job can be triggered manually or scheduled separately.

## Project Structure

```
iot-hello-world/
├── databricks.yml                  # Bundle config
├── pyproject.toml                  # Python project + dev dependencies
├── README.md
├── resources/
│   ├── iot_etl.pipeline.yml        # SDP pipeline definition
│   ├── iot_job.job.yml             # Lakeflow Job: triggers pipeline refresh
│   └── ingestion_job.job.yml       # Lakeflow Job: polls Sensor.Community API
└── src/
    └── iot_etl/
        ├── ingestion/              # Scheduled API polling notebook
        ├── explorations/           # Ad-hoc notebooks (not in pipeline)
        ├── transformations/        # SDP pipeline source files
        └── README.md
```