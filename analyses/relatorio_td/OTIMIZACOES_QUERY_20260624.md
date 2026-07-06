# Optimization Changes — TD_Priorizacao_Melhorias (2026-06-24)

## Context

The `EXECUTIVE_QUERY` BigQuery SQL had 29 CTEs and was too heavy to run in Deepnote.
The following changes reduce the query to ~24 CTEs, eliminate all double table scans,
remove all `CUME_DIST` window functions from BigQuery, and delete the `ANALYTICAL_QUERY`
entirely (it was debug-only and not used in production).

---

## 1. Parameters cell — new flags

Add two new flags to the parameters cell (Block 2):

```python
# deprecated — ANALYTICAL_QUERY removed on 2026-06-24 (was debug-only).
# Kept as False for compatibility with Block 9 references.
LOAD_ANALITICA = False

# If True, includes the sell-through sub-pipeline (5 CTEs: sku_map_st, first_sale,
# sales_windows, launch_stock, sell_through). False = lighter query;
# sell_through_* = NULL but scoring works normally via fallback.
LOAD_SELLTHROUGH = True
```

Remove or keep the old `LOAD_ANALITICA = True` line — replace it with the two lines above.

---

## 2. EXECUTIVE_QUERY — fix 4 double table scans

### 2a. `portfolio_clustering` CTE — add `first_sale_date`

In the `portfolio_clustering` CTE, add `ANY_VALUE(first_sale_date) AS first_sale_date`
so that the sell-through pipeline can reuse this CTE instead of re-scanning
`portfolio_skp_clustering`:

**Before:**
```sql
portfolio_clustering AS (
  SELECT
    product_name,
    ANY_VALUE(cluster) AS portfolio_cluster,
    ANY_VALUE(TO_JSON_STRING(pc)) AS portfolio_cluster_payload
  FROM `insider-data-lake.sop_silver.portfolio_skp_clustering` pc
  GROUP BY product_name
),
```

**After:**
```sql
portfolio_clustering AS (
  -- Includes first_sale_date for sell-through reuse without double scan.
  SELECT
    product_name,
    ANY_VALUE(cluster) AS portfolio_cluster,
    ANY_VALUE(TO_JSON_STRING(pc)) AS portfolio_cluster_payload,
    ANY_VALUE(first_sale_date) AS first_sale_date
  FROM `insider-data-lake.sop_silver.portfolio_skp_clustering` pc
  GROUP BY product_name
),
```

### 2b. Sell-through CTEs — make conditional + fix double scans

Replace the existing 5 sell-through CTEs (`sku_map_st`, `first_sale`, `sales_windows`,
`launch_stock`, `sell_through`) with a Python f-string conditional pattern.

Convert the entire `EXECUTIVE_QUERY` string from a plain string literal `"""..."""`
to an f-string `f"""..."""`.

Define these three Python variables **before** `EXECUTIVE_QUERY`:

```python
_ST_CTES = """
,
-- ─── Sell-through: SKU → product_name mapping ───────────────────────────────
sku_map_st AS (
  -- Derives from sku_dim (already scanned) — eliminates re-scan of integrated.skus
  SELECT DISTINCT product_name, sku
  FROM sku_dim
  WHERE product_name IS NOT NULL
),

-- ─── Sell-through: first sale date per product ──────────────────────────────
first_sale AS (
  -- Derives from portfolio_clustering (already scanned) — eliminates re-scan
  SELECT product_name, first_sale_date
  FROM portfolio_clustering
  WHERE first_sale_date IS NOT NULL
),

-- ─── Sell-through: units sold in 30/60/90-day windows ───────────────────────
sales_windows AS (
  -- Reuses orders + order_items_grouped (already filtered) — eliminates re-scan
  -- of insider_order_items and insider_orders.
  SELECT
    sm.product_name,
    SUM(CASE
      WHEN o.data_compra BETWEEN fs.first_sale_date
             AND DATE_ADD(fs.first_sale_date, INTERVAL 30 DAY)
      THEN oi.qt_items ELSE 0
    END) AS sold_30d,
    SUM(CASE
      WHEN o.data_compra BETWEEN fs.first_sale_date
             AND DATE_ADD(fs.first_sale_date, INTERVAL 60 DAY)
      THEN oi.qt_items ELSE 0
    END) AS sold_60d,
    SUM(CASE
      WHEN o.data_compra BETWEEN fs.first_sale_date
             AND DATE_ADD(fs.first_sale_date, INTERVAL 90 DAY)
      THEN oi.qt_items ELSE 0
    END) AS sold_90d
  FROM sku_map_st sm
  JOIN first_sale fs ON sm.product_name = fs.product_name
  JOIN order_items_grouped oi ON sm.sku = oi.sku
  JOIN orders o ON oi.order_id = o.order_id
  GROUP BY 1
),

-- ─── Sell-through: physical stock on first sale date ────────────────────────
launch_stock AS (
  SELECT
    sm.product_name,
    SUM(st.physical_stock) AS initial_stock
  FROM sku_map_st sm
  JOIN first_sale fs ON sm.product_name = fs.product_name
  JOIN `insider-data-lake.integrated.stock` st
    ON sm.sku = st.sku
   AND st.stock_date = fs.first_sale_date
  GROUP BY 1
),

-- ─── Sell-through: ratio sold / initial stock ───────────────────────────────
sell_through AS (
  SELECT
    sw.product_name,
    SAFE_DIVIDE(sw.sold_30d, ls.initial_stock) AS sell_through_30d,
    SAFE_DIVIDE(sw.sold_60d, ls.initial_stock) AS sell_through_60d,
    SAFE_DIVIDE(sw.sold_90d, ls.initial_stock) AS sell_through_90d
  FROM sales_windows sw
  LEFT JOIN launch_stock ls ON sw.product_name = ls.product_name
)""" if LOAD_SELLTHROUGH else ""

_ST_COLS = """
    st.sell_through_30d,
    st.sell_through_60d,
    st.sell_through_90d,""" if LOAD_SELLTHROUGH else """
    NULL AS sell_through_30d,
    NULL AS sell_through_60d,
    NULL AS sell_through_90d,"""

_ST_JOIN = """  LEFT JOIN sell_through st
    ON spm.product_name = st.product_name""" if LOAD_SELLTHROUGH else ""
```

Then in the SQL, after `trend_classified`, replace the sell-through CTEs block with:

```sql
){_ST_CTES},

-- ─── MC3 ────────────────────────────────────────────────────────────────────
mc3_base AS ( ... )
```

And in the final aggregation SELECT:
- Replace the `sell_through_*` column lines with `{_ST_COLS}`
- Replace the `LEFT JOIN sell_through st` line with `{_ST_JOIN}`

---

## 3. EXECUTIVE_QUERY — remove `percentiles`, `scored`, `final` CTEs

Remove the last 3 CTEs entirely (`percentiles`, `scored`, `final`) and their
`SELECT ... FROM final ORDER BY ...` final clause.

Replace with a simpler aggregation CTE `product_base` that just assembles the
raw metrics (no CUME_DIST, no scoring, no sinal_priorizacao):

```sql
-- === FINAL AGGREGATION ====================================================
-- Scoring (percentiles, td_score, commercial_score_v2, sinal_priorizacao)
-- moved to Python via compute_scores() — eliminates 8 CUME_DIST from BigQuery.

product_base AS (
  SELECT
    spm.product_name,
    spm.category,
    spm.gender,
    spm.portfolio_cluster,
    spm.portfolio_cluster_payload,

    spm.qt_pedidos,
    spm.qt_skus,
    spm.qt_items_vendidos,
    spm.receita_liquida,

    COALESCE(tpm.qt_reversas, 0) AS qt_reversas,
    COALESCE(tpm.qt_items_returned, 0) AS qt_items_returned,
    COALESCE(tpm.qt_trocas, 0) AS qt_trocas,
    COALESCE(tpm.qt_devolucoes, 0) AS qt_devolucoes,
    COALESCE(tpm.qt_reversas_fisico, 0) AS qt_reversas_fisico,
    COALESCE(tpm.qt_reversas_logistico, 0) AS qt_reversas_logistico,
    COALESCE(tpm.valor_troca, 0) AS valor_troca,
    COALESCE(tpm.valor_devolucao, 0) AS valor_devolucao,

    SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos) AS td_rate,
    cm.td_rate_categoria,
    SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos) - cm.td_rate_categoria AS delta_vs_categoria,
    SAFE_DIVIDE(
      SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos),
      cm.td_rate_categoria
    ) AS ratio_vs_categoria,

    tt.top_1_problema, tt.top_1_pct,
    tt.top_2_problema, tt.top_2_pct,
    tt.top_3_problema, tt.top_3_pct,
    tt.pct_top_3_total,

    mc.principal_cor_afetada,  mc.principal_cor_pct,
    ms.principal_tamanho_afetado, ms.principal_tamanho_pct,

    tb.reversas_ultimos_3m,
    tb.reversas_3m_anteriores,
    tb.tendencia_reversas,

    cs.comentarios_amostra,
{_ST_COLS}
    -- MC3 raw — scoring computed downstream in Python (compute_scores)
    mb.mc3_ratio,
    mb.mc3_ratio_cat4,
    mb.mc3_ratio_portfolio,
    mb.net_profit_after_marketing_costs

  FROM sales_product_metrics spm
  LEFT JOIN td_product_metrics tpm  ON spm.product_name = tpm.product_name AND spm.category = tpm.category AND spm.gender = tpm.gender
  LEFT JOIN category_metrics cm     ON spm.category = cm.category
  LEFT JOIN top_tags tt             ON spm.product_name = tt.product_name AND spm.category = tt.category AND spm.gender = tt.gender
  LEFT JOIN main_color mc           ON spm.product_name = mc.product_name AND spm.category = mc.category AND spm.gender = mc.gender
  LEFT JOIN main_size ms            ON spm.product_name = ms.product_name AND spm.category = ms.category AND spm.gender = ms.gender
  LEFT JOIN trend_classified tb     ON spm.product_name = tb.product_name AND spm.category = tb.category AND spm.gender = tb.gender
  LEFT JOIN comments_sample cs      ON spm.product_name = cs.product_name AND spm.category = cs.category AND spm.gender = cs.gender
{_ST_JOIN}
  LEFT JOIN mc3_base mb             ON spm.product_name = mb.product_name
)

SELECT * FROM product_base
```

> **Important:** The final `ORDER BY` clause is also removed — ordering is handled in pandas.
> The columns `share_receita_portfolio`, `share_unidades_portfolio`, `share_td_portfolio`,
> `receita_media_mensal_vs_categoria`, all `percentil_*` columns, `td_score`,
> `commercial_score_v2`, `priority_score`, `sinal_priorizacao`, and `resumo_pre_llm`
> are **no longer returned by the SQL** — they are computed in Python by `compute_scores()`.

---

## 4. Delete `_SHARED_CTES` and `ANALYTICAL_QUERY` cells

Delete the two cells that define `_SHARED_CTES` and `ANALYTICAL_QUERY`.
These were used only for the analytical grain (`order × sku × tag`) which was debug-only.

---

## 5. Simplify the `LOAD_ANALITICA` execution cell

The cell that previously ran `ANALYTICAL_QUERY` when `LOAD_ANALITICA=True` should be
replaced with:

```python
# ANALYTICAL_QUERY removed on 2026-06-24 (was debug-only; grain order × sku × tag).
# analytical_df kept as None for compatibility with QA and export references.
analytical_df = None
print("ℹ️  analytical_df = None  (ANALYTICAL_QUERY removed)")
```

---

## 6. New function `compute_scores()` in Block 5 (utility functions cell)

Add this function **after** `prepare_executive_df` and **before** `classify_priority`.
It replaces the removed BigQuery CTEs in Python:

```python
def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Compute percentile-based scores and classification signals from raw BQ output.

    Replaces the BigQuery CTEs ``percentiles``, ``scored`` and ``final`` removed
    for performance. Moves all CUME_DIST() and scoring logic to pandas.

    Input:  raw executive_df (product_name x category x gender grain).
    Output: same df with added columns:
        share_receita_portfolio, share_unidades_portfolio, share_td_portfolio,
        receita_media_mensal_vs_categoria,
        mc3_vs_categoria_score, mc3_vs_portfolio_score,
        percentil_receita, percentil_unidades, percentil_td_rate,
        percentil_volume_td, percentil_delta_vs_categoria,
        percentil_receita_vs_categoria, representatividade_mc3_score,
        percentil_sell_through_30d/60d/90d (NaN if LOAD_SELLTHROUGH=False),
        td_score, commercial_score_v2, priority_score,
        sinal_priorizacao, resumo_pre_llm.
    """
    out = df.copy()

    # ── Share columns (moved from SQL SUM() OVER ()) ─────────────────────────
    total_receita  = out["receita_liquida"].sum()
    total_unidades = out["qt_items_vendidos"].sum()
    total_td       = out["qt_items_returned"].sum()
    out["share_receita_portfolio"]   = np.where(total_receita  > 0, out["receita_liquida"]   / total_receita,  0.0)
    out["share_unidades_portfolio"]  = np.where(total_unidades > 0, out["qt_items_vendidos"] / total_unidades, 0.0)
    out["share_td_portfolio"]        = np.where(total_td       > 0, out["qt_items_returned"] / total_td,       0.0)

    # ── receita vs category average (moved from SQL AVG() OVER PARTITION BY) ─
    cat_mean = out.groupby("category")["receita_liquida"].transform("mean")
    out["receita_media_mensal_vs_categoria"] = np.where(cat_mean > 0, out["receita_liquida"] / cat_mean, np.nan)

    # ── MC3 discrete scores (moved from SQL CASE statements) ─────────────────
    mc3_ratio = pd.to_numeric(out.get("mc3_ratio"),            errors="coerce")
    mc3_cat4  = pd.to_numeric(out.get("mc3_ratio_cat4"),       errors="coerce")
    mc3_port  = pd.to_numeric(out.get("mc3_ratio_portfolio"),  errors="coerce")
    has_cat   = mc3_ratio.notna() & mc3_cat4.notna()
    has_port  = mc3_ratio.notna() & mc3_port.notna()

    out["mc3_vs_categoria_score"] = np.where(
        ~has_cat, np.nan,
        np.where(mc3_ratio >= mc3_cat4,        1.00,
        np.where(mc3_ratio >= 0.90*mc3_cat4,   0.75,
        np.where(mc3_ratio >= 0.75*mc3_cat4,   0.50, 0.25)))
    )
    out["mc3_vs_portfolio_score"] = np.where(
        ~has_port, np.nan,
        np.where(mc3_ratio >= mc3_port,        1.00,
        np.where(mc3_ratio >= 0.90*mc3_port,   0.75,
        np.where(mc3_ratio >= 0.75*mc3_port,   0.50, 0.25)))
    )

    # ── Global percentiles (CUME_DIST equivalent: rank pct=True) ─────────────
    def _pct(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series, errors="coerce").rank(pct=True, na_option="keep")

    out["percentil_receita"]             = _pct(out["receita_liquida"])
    out["percentil_unidades"]            = _pct(out["qt_items_vendidos"])
    out["percentil_td_rate"]             = _pct(out["td_rate"])
    out["percentil_volume_td"]           = _pct(out["qt_items_returned"])
    out["percentil_delta_vs_categoria"]  = _pct(out["delta_vs_categoria"])
    out["representatividade_mc3_score"]  = _pct(
        pd.to_numeric(out.get("net_profit_after_marketing_costs"), errors="coerce")
    )

    # Percentile within category (CUME_DIST OVER PARTITION BY category)
    out["percentil_receita_vs_categoria"] = (
        out.groupby("category")["receita_media_mensal_vs_categoria"]
        .rank(pct=True, na_option="keep")
    )

    # Sell-through percentiles (NaN when LOAD_SELLTHROUGH=False or no coverage)
    for col in ["sell_through_30d", "sell_through_60d", "sell_through_90d"]:
        pct_col   = f"percentil_{col}"
        st_series = pd.to_numeric(out.get(col, pd.Series(dtype=float)), errors="coerce")
        out[pct_col] = st_series.rank(pct=True, na_option="keep") if st_series.notna().any() else np.nan

    # ── TD score ─────────────────────────────────────────────────────────────
    out["td_score"] = (
        0.5 * out["percentil_td_rate"]
      + 0.3 * out["percentil_volume_td"]
      + 0.2 * out["percentil_delta_vs_categoria"]
    )

    # ── Commercial score v2 ───────────────────────────────────────────────────
    # v2 = 0.70 x tracao_vendas_score + 0.30 x mc3_score
    # Fallback (a): sell-through unavailable -> uses percentil_receita/unidades
    # Fallback (b): mc3 unavailable -> uses only tracao_vendas_score
    st_score = (
        0.30 * out["percentil_sell_through_30d"]
      + 0.30 * out["percentil_sell_through_60d"]
      + 0.25 * out["percentil_sell_through_90d"]
      + 0.15 * out["percentil_receita_vs_categoria"]
    )
    simple_fallback      = 0.60 * out["percentil_receita"] + 0.40 * out["percentil_unidades"]
    st_score_or_fallback = st_score.combine_first(simple_fallback)

    mc3_score = (
        0.50 * out["mc3_vs_categoria_score"]
      + 0.30 * out["mc3_vs_portfolio_score"]
      + 0.20 * out["representatividade_mc3_score"]
    )

    has_mc3 = out["mc3_vs_categoria_score"].notna()
    out["commercial_score_v2"] = np.where(
        has_mc3,
        0.70 * st_score_or_fallback + 0.30 * mc3_score,
        st_score_or_fallback,
    )

    # ── Priority score ────────────────────────────────────────────────────────
    out["priority_score"] = 0.50 * out["td_score"] + 0.50 * out["commercial_score_v2"]

    # ── sinal_priorizacao (mirrors the removed SQL CTE final) ────────────────
    thr  = PrioritizationThresholds()
    td   = out["td_score"]
    cs   = out["commercial_score_v2"]
    qt_v = out["qt_items_vendidos"]
    qt_r = out["qt_items_returned"]

    conditions = [
        (qt_v < thr.min_items_vendidos) | (qt_r < thr.min_items_returned),
        (td >= thr.td_score_prioritize)    & (cs >= thr.commercial_score_prioritize),
        (td >= thr.td_score_prioritize)    & (cs <  thr.commercial_score_prioritize),
        (cs >= thr.commercial_score_alert) & (td >= thr.td_score_alert_min) & (td < thr.td_score_prioritize),
        (td < thr.low_td_score) | (cs < thr.low_commercial_score),
    ]
    labels = [
        "Sem evidência suficiente",
        "Priorizar melhoria",
        "Monitorar",
        "Alerta em produto relevante",
        "Não priorizar agora",
    ]
    out["sinal_priorizacao"] = np.select(conditions, labels, default="Monitorar")

    # ── resumo_pre_llm (moved from SQL CONCAT in CTE final) ──────────────────
    def _resumo(row: pd.Series) -> str:
        tags    = ", ".join(filter(None, [row.get("top_1_problema"), row.get("top_2_problema"), row.get("top_3_problema")]))
        pct_top3 = row.get("pct_top_3_total") or 0
        cor     = row.get("principal_cor_afetada")
        cor_pct = row.get("principal_cor_pct") or 0
        tam     = row.get("principal_tamanho_afetado")
        tam_pct = row.get("principal_tamanho_pct") or 0
        trend   = row.get("tendencia_reversas")
        parts   = [
            f"Top problemas: {tags or 'sem tag'}.",
            f" Top 3 concentram {round(100 * float(pct_top3), 1)}% das reversas com tag.",
        ]
        try:
            if cor and float(cor_pct) >= 0.50:
                parts.append(f" Há concentração relevante na cor {cor}.")
            if tam and float(tam_pct) >= 0.50:
                parts.append(f" Há concentração relevante no tamanho {tam}.")
        except (TypeError, ValueError):
            pass
        if trend:
            parts.append(f" Tendência: {trend}.")
        return "".join(parts)

    out["resumo_pre_llm"] = out.apply(_resumo, axis=1)
    return out
```

---

## 7. Update `EXECUTIVE_REQUIRED_COLUMNS` in Block 5

The score columns (`td_score`, `commercial_score_v2`, `sinal_priorizacao`,
`priority_score`) are no longer returned by BigQuery — they are computed by
`compute_scores()`. Remove them from `EXECUTIVE_REQUIRED_COLUMNS`:

```python
# Before (included score columns returned by SQL)
EXECUTIVE_REQUIRED_COLUMNS = {
    "product_name", "category", "sinal_priorizacao", "priority_score",
    "td_score", "commercial_score_v2", "qt_items_vendidos", "receita_liquida",
    "qt_items_returned", "td_rate", "td_rate_categoria", "delta_vs_categoria",
}

# After (only raw BQ output columns)
EXECUTIVE_REQUIRED_COLUMNS = {
    "product_name", "category",
    "qt_items_vendidos", "receita_liquida",
    "qt_items_returned", "td_rate", "td_rate_categoria", "delta_vs_categoria",
}
```

---

## 8. Update Block 6 — call `compute_scores()` after `prepare_executive_df()`

Replace the Block 6 preparation cell with:

```python
# Step 1: validate minimum columns, coerce numerics, sort by available fields.
_executive_raw = prepare_executive_df(executive_df)

# Step 2: compute_scores() replicates in pandas the removed BigQuery CTEs
# percentiles/scored/final (8 CUME_DIST → rank(pct=True), scoring, sinal_priorizacao).
executive_prepared_df = compute_scores(_executive_raw)

print(f"✅ executive_prepared_df: {executive_prepared_df.shape}")
print(f"   Sinais: {executive_prepared_df['sinal_priorizacao'].value_counts().to_dict()}")
display(executive_prepared_df.head(3))
```

---

## 9. Update QA cell (Block 9)

Change QA check 3 description from "Python vs SQL" to "internal consistency check"
(since both `compute_scores` and `classify_priority` now run in Python):

```python
# ── QA 3: Internal scoring consistency (compute_scores vs classify_priority) ─
if "sinal_confere_sql" in classified_df.columns:
    match_rate = classified_df["sinal_confere_sql"].mean()
    if match_rate < 1.0:
        n_div = (~classified_df["sinal_confere_sql"]).sum()
        raise ValueError(
            f"❌ QA FAILED: Internal scoring inconsistency. "
            f"compute_scores() vs classify_priority() diverge on {n_div} product(s) "
            f"({match_rate:.2%} match). "
            f"Check that PrioritizationThresholds is consistent between both functions."
        )
print(f"✅ QA 3: Internal scoring consistent — {match_rate:.2%} match")

# ── QA 6: Tag duplication — skipped (ANALYTICAL_QUERY removed 2026-06-24) ────
print("⏭️  QA 6: Skipped (analytical_df = None — ANALYTICAL_QUERY removed).")
```

---

## Summary of what was removed from BigQuery SQL

| Removed | Reason |
|---|---|
| CTE `sku_map_st` (standalone) | Now derived from `sku_dim` inline |
| CTE `first_sale` (standalone, re-scanned `portfolio_skp_clustering`) | Now derived from `portfolio_clustering` inline |
| `sales_windows` re-scanning `insider_orders` + `insider_order_items` | Now reuses `orders` + `order_items_grouped` |
| CTE `percentiles` (8× `CUME_DIST`, 3× `SUM OVER()`, 1× `AVG OVER()`) | Moved to `compute_scores()` |
| CTE `scored` (inline score formula duplication) | Moved to `compute_scores()` |
| CTE `final` (`sinal_priorizacao` CASE, `resumo_pre_llm` CONCAT) | Moved to `compute_scores()` |
| `_SHARED_CTES` variable | `ANALYTICAL_QUERY` removed |
| `ANALYTICAL_QUERY` (entire cell) | Debug-only, not used in production |
| `ORDER BY` in final `SELECT` | Delegated to pandas |

## Summary of what was added to Python

| Added | Where |
|---|---|
| `LOAD_SELLTHROUGH` flag | Block 2 (parameters) |
| `_ST_CTES`, `_ST_COLS`, `_ST_JOIN` f-string fragments | Before `EXECUTIVE_QUERY` |
| `compute_scores(df)` function | Block 5 (after `prepare_executive_df`) |
| Call to `compute_scores()` in preparation step | Block 6 |
