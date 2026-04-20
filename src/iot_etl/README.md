# iot_etl

Source code for the iot-hello-world SDP pipeline.

## Folders

- **transformations/** — SDP pipeline source files (`.py` with `@dp.materialized_view` decorators). These are referenced by the pipeline definition in `resources/iot_etl.pipeline.yml` via glob pattern and run by the SDP engine.

- **explorations/** — Ad-hoc Jupyter notebooks (`.ipynb`) for interactive data profiling, schema inspection, and EDA. These are NOT part of the pipeline and are not referenced in any resource YAML.
