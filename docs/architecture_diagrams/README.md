# Architecture Diagrams

These Graphviz `.dot` files provide colorful end-to-end diagrams for the four pipelines in this project.

Files:
- `01_databricks_delta_pipeline.dot`
- `02_redshift_pipeline.dot`
- `03_snowflake_pipeline.dot`
- `04_walmart_realtime_pipeline.dot`

Render example:
```bash
dot -Tpng 01_databricks_delta_pipeline.dot -o 01_databricks_delta_pipeline.png
dot -Tsvg 04_walmart_realtime_pipeline.dot -o 04_walmart_realtime_pipeline.svg
```
