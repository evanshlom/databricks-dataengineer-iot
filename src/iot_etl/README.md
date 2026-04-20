# iot_etl

Source code for the iot-hello-world SDP pipeline.

## Folders

- **ingestion/** — Scheduled notebook (`.ipynb`) that polls the Sensor.Community REST API and writes JSON files to a Unity Catalog Volume (`/Volumes/catalog1/iot_hello_world/raw_landing/sensor_data`). Runs as a separate Lakeflow Job on a 30-minute schedule. NOT part of the SDP pipeline.

- **transformations/** — SDP pipeline source files (`.py` with `@dp.table` and `@dp.materialized_view` decorators). These are referenced by the pipeline definition in `resources/iot_etl.pipeline.yml` via glob pattern and run by the SDP engine.

- **explorations/** — Ad-hoc Jupyter notebooks (`.ipynb`) for interactive data profiling, schema inspection, and EDA. NOT part of the pipeline and not referenced in any resource YAML.