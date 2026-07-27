# LA_Coding_Projects — Lead Time Dashboard

**Purpose:** Analyze lead time and supply chain efficiency of Insider Store production orders. Dashboard shows: realized lead time time-series, stage decomposition, deadline adherence, open OP pipeline.

**Tech Stack:**
- Python 3 (Jupyter notebooks)
- pandas, numpy
- plotly (go + express)
- ipywidgets (interactive filters)
- google-cloud-bigquery (BigQuery client)
- SQL (standard SQL on BigQuery)

**Key Data Sources (current):**
- `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` — **primary and only source for SQL_OPS**. Snapshot diário (grain: op_code × product_sku × ingestion_date). Stages already normalized. See `project/databases` for full schema.
- `insider-data-lake.integrated.muninn_production_orders` — used only for `canceled_production_reason` (INT_CANCEL classification in KR1). NOT used in lead_time_dashboard.

**Main Files:**
- `notebooks/lead_time_dashboard.ipynb` — primary deliverable (lead time dashboard)
- `2_Códigos/KR1_Plan_Freeze_Rate_v20260420.ipynb` — KR1 metric (plan freeze rate)
- `2_Códigos/Alerta_Risco_Cadeia_v20260429.ipynb` — supply chain risk alert
- `1_Inputs/1_SQL/plano_vs_atual.sql` — reference SQL for plan vs actual analysis
- `base.md` — full database schema documentation (source of truth)
- `plan.md`, `plan_2.md` — business definitions for KR1
