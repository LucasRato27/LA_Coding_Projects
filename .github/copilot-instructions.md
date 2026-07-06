# Copilot Instructions — Insider LA Coding Projects

## Project Overview

Advanced Analytics workspace for **Insider Store Supply Chain / SOP (Sales & Operations Planning)**. The core activity is building BigQuery SQL queries, Jupyter notebooks, and structured analysis documents to track production planning efficiency (KR1 = Plan Freeze Rate, ICP, lead time).

**BigQuery projects:** `insider-data-lake` (main) · `insider-lake-sensitive` (lead time dashboard) · **Region:** `southamerica-east1`

---

## Architecture & Data Flow

```
1_Inputs/1_SQL/     → BigQuery SQL queries (source of truth for data extraction)
2_Códigos/          → Jupyter notebooks that run queries and produce outputs
3_Outputs/          → CSVs exported from notebooks, named {topic}_{YYYYMMDD}.csv
4_Analysis/         → Ad-hoc root cause analyses (one folder per investigation)
notebooks/          → Standalone dashboard notebooks (e.g. lead_time_dashboard.ipynb)
base.md             → Full BigQuery table reference (columns, join keys, usage notes)
```

Each analysis in `4_Analysis/` follows the template from `ddal_mp_atrasada`:
- `README.md` — objective, methodology, result table, root cause summary, file list
- `*.sql` — targeted BigQuery query
- `root_cause_summary.md` — structured cause breakdown (**not** `root_cause_report.md`)
- `*.csv` — query outputs

---

## BigQuery Access Pattern

All notebooks connect via `bigquery_starter.ipynb` pattern — copy this boilerplate to start any new notebook:

```python
from google.cloud import bigquery
import pandas as pd

PROJECT_ID = 'insider-data-lake'
client = bigquery.Client(project=PROJECT_ID)

df = client.query("""
    SELECT * FROM `insider-data-lake.DATASET.TABLE` LIMIT 10
""").to_dataframe()
```

**SQL `{param}` placeholders:** SQL files in `analytics/*/sql/` use Python-style `{param_name}` placeholders (e.g., `{dia_ref}` in `stock_health_abc_icp.sql`). Substitute with `.format()` before executing:

```python
with open("sql/query.sql") as f:
    sql = f.read().format(dia_ref="2026-06-05")
df = client.query(sql).to_dataframe()
```

**BigQuery CTE style:** All SQL files use `WITH` blocks with `-- ─── N. Section name ─────` section headers. Each CTE is self-documented with its grain and purpose in a comment block at the top.

Credentials are loaded automatically from `secrets/`. Virtual environment: `.venv-1/` (`source .venv-1/bin/activate`).

---

## Core BigQuery Tables

### `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`
**The primary table.** Daily snapshots — grain: `(op_code, product_sku, ingestion_date)`. ~12.3M rows, partitioned on `ingestion_date`. Used in all KR1, cascade, and plan freeze calculations.

```sql
-- Mandatory filter on every query using this table
WHERE production_order_type NOT IN ('flexible', 'converted')
  AND cycle_name IS NOT NULL
  AND (supplier_relationship_status IS NULL
       OR supplier_relationship_status NOT IN ('terminated', 'discontinued'))
```

Change detection pattern (self-join baseline vs current):
```sql
JOIN ON cycle_name, ingestion_date = baseline_date  -- baseline vs state
LAG(PARTITION BY op_code, product_sku ORDER BY ingestion_date)  -- day-over-day
```

### `insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
Latest snapshot only (no `ingestion_date` history). **Use only for ad-hoc tests and point-in-time validation** — never in production queries. See `1_Inputs/1_SQL/Testes/`.

### `insider-data-lake.integrated.muninn_production_orders`
OP metadata from ERP. ~12,400 rows. JOIN key: `po.order_code = h.op_code`. **Critical for** `canceled_production_reason` — the only field that separates INT_CANCEL from EXT_CANCEL.

### `insider-lake-sensitive.landing_br.muninn_production_orders_raw`
Daily snapshot of Muninn OP table. Grain: `(id, ingestion_date)`. Used exclusively in `notebooks/lead_time_dashboard.ipynb` for production stage timestamps. JOIN: `po.order_code = scemi.op_code`. Prefer `ANY_VALUE(order_code)` when grouping by `id`.

### `insider-data-lake.sop_gold.stock_health`
Daily stock health snapshot per SKU. **Grain: `sku × dia`** (~2,185 SKUs × N days; validated: 1,258,939 rows, `sku + dia` is unique). Used in `analytics/icp_saude_estoque/`.

Key columns:
- **Identification:** `sku`, `nome_sku`, `produto_pai`, `categoria`, `variante_cor`, `dia`, `isoweek`, `mes`, `id_previsao`
- **Stock:** `estoque_passado`, `estoque_projetado`, `estoque_passado_ou_projetado`
- **Coverage/health:** `estoque_passado_ou_projetado_d` (days of coverage), `estoque_atual_d_vendas_l7d`, `estoque_seguranca_d`, `estoque_excesso_d`, `stock_classification`, `stock_classification_l7d_sales`
- **Demand/receipts:** `qtd_a_receber`, `qtd_venda_prevista_diarizada`, `qtd_venda_media_l7d`
- **Financial:** `sku_sales_average_price`, `sku_production_cost`, `receita_prevista_diarizada`, `custo_de_estoque`

Standard JOIN: `LEFT JOIN integrated.skus AS sk ON sk.sku = sh.sku` → brings `sku_state` and `product_name` (canonical join key for ABC and ICP).
⚠️ `produto_pai` in stock_health ≈ `product_name` in integrated.skus — always use `sk.product_name` as the canonical join key (avoids naming drift).

### `insider-data-lake.sop_silver.demand_prediction_input` — ABC Curve
Source for the ABC curve. Grain: `product_name × reference_date`. Standard pattern (canonical — used in `lead_time_dashboard` and `icp_saude_estoque`):

```sql
-- Last 3 closed months
WHERE DATE(reference_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
  AND DATE(reference_date) <  DATE_TRUNC(CURRENT_DATE(), MONTH)
  AND product_name IS NOT NULL
-- Classification thresholds (cumulative revenue share):
-- A ≤ 80% | B ≤ 95% | C > 95%
```

ABC is at **`product_name` level** (not SKU). Join to SKU-level tables via `integrated.skus.product_name`.

### `insider-data-lake.sop_silver.portfolio_skp_clustering`
Clusterização estratégica de produtos. **Grão: `product_name` (NÃO tem coluna `sku`)**. JOIN sempre por `product_name`. Campos relevantes: `product_name`, `cluster` (KILL · Long tail · Core · Hero · Lancamento · Breakthrough), `skp_state`, `processed_at`, `pctl_revenue_company`, `mc3_ratio`. **⚠️ NÃO contém scores de scorecard** — o `TO_JSON_STRING(pc)` serializado em `portfolio_cluster_payload` também não os contém.

### `insider-data-lake.sop_bronze.eval_produto_portfolio`
Scorecard de avaliação de portfólio por produto. **Grão: `product_name`.** Fonte dos 3 pilares do scorecard (escala 0–1 → ×100 para exibição):
- `score_vendas_geral` → **Tração Comercial**
- `score_viabilidade_financeira` → **Unit Economics**
- `score_satisf_cliente` → **Satisfação e Marca**

Outros campos úteis: `product_devolution_ratio` (T&D produto), `category4_devolution_ratio` (T&D categoria), `mc3_ratio`, `rating_product`, `revenue_after_discounts`.

**Query padrão:**
```sql
SELECT
  product_name,
  ROUND(score_vendas_geral * 100, 1)           AS score_tracao_comercial,
  ROUND(score_viabilidade_financeira * 100, 1)  AS score_unit_economics,
  ROUND(score_satisf_cliente * 100, 1)          AS score_satisfacao_marca
FROM `insider-data-lake.sop_bronze.eval_produto_portfolio`
```
⚠️ Cobertura parcial (~126 de 287 produtos ativos em 2026-06); produtos ausentes ficam com `—` em análises cruzadas. Fazer JOIN com `portfolio_skp_clustering` por `product_name` para obter `cluster`.

### `insider-data-lake.sop_silver.scale_ra`
⚠️ **Grain: `supplier_name × reference_date` — NO product-level breakdown.** Validated columns for ICP: `total_planned_quantity_icp`, `total_received_quantity_icp`, `icp` (pre-computed ratio). Use for **supplier-level ICP trend only**. For product-level ICP, calculate from `supply_chain_efficiency_model_input` grouping by `product_name`.

---

## Key Domain Concepts

| Term | Definition |
|---|---|
| `cycle_name` | Base cycles: `^C\d{2}20\d{2}$` (e.g. `C062026`). Extras: everything else. |
| **Baseline** | First `ingestion_date` per cycle where `COUNTIF(stage = 'pending') = 0` — the "frozen plan" reference point. |
| **INT_CANCEL** | `canceled_production_reason = 'Revisão de Demanda (In Season)'` → internal fault. |
| **EXT_CANCEL** | Any other cancellation reason → external/supplier fault. |
| **INT_DATE** | `dt_planned_entry_warehouse` changed vs baseline → internal. |
| **INT_GRADE** | `planned_quantity` per SKU changed vs baseline → internal. |
| **EXT_DATE_REV** | `dt_reviewed_entry_warehouse` changed without touching `dt_planned` → external, excluded from KR1. |
| **KR1 (Plan Freeze Rate)** | `1 − (vol_int_any / vol_original)`. Measures plan stability. 3 INT types: INT_DATE, INT_GRADE, INT_CANCEL. No double-counting. |
| **% EXT** | % allocated volume with EXT_CANCEL or EXT_DATE_REV. Uses **peak (high-water mark)** across cycle lifetime — not final state — to prevent reversals from masking real cascades. |

---

## Output Naming Conventions

- `3_Outputs/`: `{topic}_{YYYYMMDD}.csv` (e.g. `kr1_evolucao_20260430.csv`)
- `3_Outputs/alerta_risco_cadeia/`: `{artifact}_{YYYYMMDD}.csv`
- Analysis summaries: `root_cause_summary.md` (not `root_cause_report.md`)
- Notebooks in `2_Códigos/`: `{AnalysisName}_v{YYYYMMDD}.ipynb`

---

## Portfolio Analytics — T&D e Priorização de Produto Físico

Pipeline de análise de **Trocas e Devoluções (T&D)** para priorização de melhorias no time de Produto Físico (PF). Localização: `analyses/relatorio_td/`.

### Tabelas do Pipeline de Reversas

| Tabela | Projeto | Uso |
|---|---|---|
| `business.insider_orders` | `insider-data-lake` | Pedidos válidos: `order_status = 'paid'`, `is_cancelled = FALSE`, exclusão cupons internos (TF-/TFIN/IR/Item errado), lojas `shopify_insider-world` + `shopify_insider-store-loja` |
| `business.insider_order_items` | `insider-data-lake` | Itens: `sku`, `quantity`, `product_title`, `variant_color`, `variant_size`. Sem dedup necessário — GROUP BY (order_id, sku) |
| `fpa.analytical_dre` | `insider-data-lake` | Receita e volume: `revenue_after_discounts`, `non_refunded_quantity`. Grão: attribution-weighted (agregar por product_name via JOIN com orders + sku_dim) |
| `integrated.skus` | `insider-data-lake` | Dimensão SKU: `product_name`, `category`, `gender`, `color`, `size`, `sku_state` (fallback para color/size) |
| `prepared_br.prepared__troquecommerce_order_details_br` | `insider-lake-sensitive` | Reversas; dedup `(order_name, id_reversa, sku) ORDER BY updated_at DESC`; filtro `status <> 'Cancelado'`, `id_reversa IS NOT NULL` |
| `sop_silver.return_reason_tags` | `insider-data-lake` | Tags qualitativas; campo `tags` é ARRAY — usar `UNNEST(tags)`; dedup por `created_at DESC`. ⚠️ Grão `(order_name, sku)` — sem `id_reversa` |
| `sop_silver.portfolio_skp_clustering` | `insider-data-lake` | Cluster estratégico (`product_name`, `cluster`). **Grão: `product_name`, sem `sku`** |
| `sop_bronze.eval_produto_portfolio` | `insider-data-lake` | Scorecard de portfólio com 3 pilares (ver abaixo) |

### Scorecard de Portfólio (`eval_produto_portfolio`)

Os 3 pilares vêm **exclusivamente** de `sop_bronze.eval_produto_portfolio` — nunca do `portfolio_cluster_payload`:

| Campo na tabela | Pilar exibido | Escala |
|---|---|---|
| `score_vendas_geral` | Tração Comercial | 0–1 → ×100 |
| `score_viabilidade_financeira` | Unit Economics | 0–1 → ×100 |
| `score_satisf_cliente` | Satisfação e Marca | 0–1 → ×100 |

> **⚠️ `portfolio_cluster_payload`** é `TO_JSON_STRING` de `portfolio_skp_clustering` — **não contém os score columns**. Para obter os pilares, sempre fazer query direta a `eval_produto_portfolio`.

### Sistema de Sinais de Priorização

**`sinal_priorizacao`** (5 buckets, SQL via CUME_DIST scores):

```
commercial_score = 0.6 × percentil_receita + 0.4 × percentil_unidades
td_score         = 0.5 × percentil_td_rate + 0.3 × percentil_volume_td + 0.2 × percentil_delta_vs_categoria
priority_score   = 0.5 × td_score + 0.5 × commercial_score
```

| Sinal | Critério |
|---|---|
| Priorizar melhoria | td_score ≥ 0.70 e commercial_score ≥ 0.60 |
| Alerta em produto relevante | commercial_score ≥ 0.70 e 0.50 ≤ td_score < 0.70 |
| Monitorar | td_score ≥ 0.70 e commercial_score < 0.60 (ou zona cinza) |
| Não priorizar agora | td_score < 0.50 ou commercial_score < 0.40 |

**`sinal_kill_keep`** (2 estados, Python `np.select` — regra QA):

| Sinal | Critério | Decisão |
|---|---|---|
| Priorizar melhoria | td_score ≥ 0.70 e commercial_score ≥ 0.60 | Alta T&D + Alta Tração → investir |
| Não Priorizar (Avaliar Descontinuação/Reformulação) | td_score ≥ 0.70 e commercial_score < 0.60 | Alta T&D + Baixa Tração → avaliar saída |

### Padrão de Output

- **Excel** (`export_priority_workbook`): abas `farol_executivo`, `resumo_farol`, `benchmark_categoria`, `resumo_tags`, `priorizar_melhoria`, `relatorio_pf` (scorecard × T&D × top 3 motivos em HTML para o time PF), `amostra_analitica`
- **Tweet analítico** (`build_tweet_heuristic`): ≤80 palavras por produto, heurístico (sem LLM) — combina Top 3 tags (%), concentração cor/tamanho se ≥50%, T&D vs categoria, tendência
- **Coluna `top_3_motivos_td`**: HTML (`<br>` entre itens), gerada por `build_scorecard_product_view`

### `analyses/relatorio_td/td_analysis_functions.py` — Utility Module

Pure-pandas, side-effect-free library for T&D pipelines (works in notebooks, scripts, or Streamlit). Key exports:

- **`PrioritizationThresholds`** — frozen dataclass with canonical threshold values (`td_score_prioritize=0.70`, `commercial_score_prioritize=0.60`, etc.). **Keep in sync with the SQL `CASE` thresholds** in `02_td_farol_executivo_produto.sql`.
- **`EXECUTIVE_REQUIRED_COLUMNS`** / **`ANALYTICAL_REQUIRED_COLUMNS`** — DataFrame contracts; `validate_columns()` enforces them at entry points.
- **`classify_priority(df)`** — Python mirror of the SQL `sinal_priorizacao` CASE statement; use for QA / sensitivity analysis.
- **`validate_no_tag_duplication(analytical_df, executive_df)`** — QA guardrail: confirms the executive T&D numerator was NOT built by summing tag rows (which would double-count).
- **`build_llm_context_rows(df)`** — produces compact dicts for LLM summarizers (used by `build_tweet_heuristic` in the notebook).
- **`TAG_TO_ACTION`** — maps raw tags → action categories (Modelagem, Grade, Tecido, Investigação adicional, PDP/Comunicação).

### Resultados de Referência (2026-06-11, últimos 12 meses)

- 287 produtos · R$ 461M receita · 242.383 retornos
- **44 produtos** em "Priorizar melhoria" — 36,8% do T&D com 27,6% da receita
- Split: **90,8% físico · 8,5% outros/desistência · 0,7% logístico**
- Top tags: `caimento_ruim` (128 produtos), `modelagem_ruim` (95), `tamanho_pequeno` (68), `tamanho_grande` (67)

---

## AI Agent Tooling

**This project uses two AI assistants with distinct roles:**

| Assistant | Where | Role |
|---|---|---|
| **GitHub Copilot** | VS Code chat / inline | Code generation, SQL writing, notebook editing, file changes |
| **Serena** | Serena chat (separate session) | Project memory, cross-session context, knowledge base updates |

> ⚠️ **Copilot does not have access to Serena's memory.** If you are talking to Copilot, Serena is NOT active. To persist learnings across sessions, you must explicitly trigger a Serena update after completing a task.

### Feedback Loop — After Any Task Touching the Database

After any task that discovers new patterns, corrects table behavior, or reveals data quirks, run a **Serena retroalimentação cycle**:

1. **Summarize the finding** (table name, column, behavior discovered, example query)
2. **Open a Serena session** and paste: `"Update project memory: [finding]"` so it's available in future sessions
3. **Update `base.md`** if the finding affects table documentation (columns, joins, known limitations)
4. **Update `.github/copilot-instructions.md`** if the finding affects agent behavior (patterns, conventions, gotchas)

Example triggers for a retroalimentação cycle:
- A column behaves differently than `base.md` describes
- A new JOIN pattern is validated in BigQuery
- A query filter turns out to be wrong or incomplete
- A new table or dataset becomes relevant to the project

---

## AI Agent Persona (from CLAUDE.md)

When acting as an analyst: apply the **Problema → Hipóteses → Evidências → Conclusão → Decisão** framework. Always provide two tracks: (1) quick win / viable path and (2) 10x moonshot provocation. Outputs must include: analysis, recommendations, risks, next steps, and limitations. Declare sources and confidence levels; flag data gaps explicitly.

---

## Superpowers Skills

This workspace uses the **Superpowers** methodology. Skills are in `skills/`. Read and follow the relevant `SKILL.md` before any task.

**RULE: If there is even a 1% chance a skill applies, read and follow it. This is not optional.**

### Skill Trigger Map

| Situation | Read this skill first |
|---|---|
| Building / adding anything new | `skills/brainstorming/SKILL.md` |
| Design approved, need a plan | `skills/writing-plans/SKILL.md` |
| Executing tasks from a plan | `skills/executing-plans/SKILL.md` or `skills/subagent-driven-development/SKILL.md` |
| Debugging an issue | `skills/systematic-debugging/SKILL.md` |
| About to declare something fixed | `skills/verification-before-completion/SKILL.md` |
| Writing or reviewing code | `skills/requesting-code-review/SKILL.md` |
| Creating new skills | `skills/writing-skills/SKILL.md` |
| Creating, editing or exporting `.xlsx` / `.csv` files | `skills/sheets_writer/SKILL.md` |
| Creating, generating, or **converting a local `.ipynb`** into a Deepnote-compatible notebook (SQL cells + BigQuery + Plotly) | `skills/deepnote-notebook/SKILL.md` |
| User wants to learn a concept or topic (stateful, multi-session teaching) | `skills/teach/SKILL.md` |

### Workflow Order (mandatory)
1. **brainstorming** → refine idea, get design approval
2. **writing-plans** → break approved design into tasks
3. **executing-plans** or **subagent-driven-development** → work through tasks
4. **verification-before-completion** → confirm it actually works

### Red Flags — Stop if you think:
- *"This is too simple to need a skill"* → Check anyway.
- *"I need context first"* → Skill check comes BEFORE exploring.
- *"Let me just do this one thing"* → Check BEFORE doing anything.

### Available Skills
`brainstorming` · `writing-plans` · `executing-plans` · `subagent-driven-development` · `systematic-debugging` · `verification-before-completion` · `test-driven-development` · `requesting-code-review` · `receiving-code-review` · `dispatching-parallel-agents` · `finishing-a-development-branch` · `using-git-worktrees` · `using-superpowers` · `writing-skills` · `data-storytelling` · `caveman` · `grill-me` · `handoff` · `teach` · `sheets_writer` · `deepnote-notebook`
