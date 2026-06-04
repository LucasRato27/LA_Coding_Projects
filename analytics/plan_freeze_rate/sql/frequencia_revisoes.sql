-- =============================================================================
-- FREQUÊNCIA DE REVISÕES POR OP-SKU
-- Compara snapshots consecutivos (diários) para contar quantas vezes cada
-- OP-SKU teve mudança de dt_planned, dt_reviewed e planned_quantity.
-- Separa revisões internas (INT) de externas (EXT).
-- =============================================================================

WITH CTE_SNAPSHOTS AS (
    -- Todos os snapshots ordenados por OP-SKU e data
    SELECT
        h.op_code,
        h.product_sku,
        h.cycle_name,
        h.supplier_name,
        h.product_name,
        h.ingestion_date,
        h.current_production_stage,
        h.planned_quantity,
        h.dt_planned_entry_warehouse,
        h.dt_reviewed_entry_warehouse,
        -- Valores do snapshot anterior (via LAG)
        LAG(h.dt_planned_entry_warehouse) OVER (
            PARTITION BY h.op_code, h.product_sku ORDER BY h.ingestion_date
        ) AS prev_dt_planned,
        LAG(h.dt_reviewed_entry_warehouse) OVER (
            PARTITION BY h.op_code, h.product_sku ORDER BY h.ingestion_date
        ) AS prev_dt_reviewed,
        LAG(h.planned_quantity) OVER (
            PARTITION BY h.op_code, h.product_sku ORDER BY h.ingestion_date
        ) AS prev_planned_qty,
        LAG(h.ingestion_date) OVER (
            PARTITION BY h.op_code, h.product_sku ORDER BY h.ingestion_date
        ) AS prev_ingestion_date
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
    WHERE h.cycle_name IS NOT NULL
        AND h.production_order_type NOT IN ('flexible', 'converted')
),

CTE_CHANGES AS (
    -- Identifica cada mudança entre snapshots consecutivos
    SELECT
        s.op_code,
        s.product_sku,
        s.cycle_name,
        s.supplier_name,
        s.product_name,
        s.ingestion_date,
        s.prev_ingestion_date,

        -- Mudança de dt_planned (INT_DATE)
        CASE
            WHEN s.prev_dt_planned IS NOT NULL
                 AND s.dt_planned_entry_warehouse IS NOT NULL
                 AND s.dt_planned_entry_warehouse != s.prev_dt_planned
            THEN TRUE
            ELSE FALSE
        END AS changed_dt_planned,

        -- Magnitude da mudança de dt_planned (em dias)
        CASE
            WHEN s.prev_dt_planned IS NOT NULL
                 AND s.dt_planned_entry_warehouse IS NOT NULL
                 AND s.dt_planned_entry_warehouse != s.prev_dt_planned
            THEN ABS(DATE_DIFF(s.dt_planned_entry_warehouse, s.prev_dt_planned, DAY))
            ELSE 0
        END AS magnitude_dt_planned,

        -- Mudança de dt_reviewed SEM mudança de dt_planned (EXT_DATE_REV)
        CASE
            WHEN s.prev_dt_reviewed IS NOT NULL
                 AND s.dt_reviewed_entry_warehouse IS NOT NULL
                 AND s.dt_reviewed_entry_warehouse != s.prev_dt_reviewed
                 AND (s.dt_planned_entry_warehouse = s.prev_dt_planned
                      OR s.prev_dt_planned IS NULL)
            THEN TRUE
            ELSE FALSE
        END AS changed_dt_reviewed_ext,

        -- Magnitude da mudança de dt_reviewed (em dias)
        CASE
            WHEN s.prev_dt_reviewed IS NOT NULL
                 AND s.dt_reviewed_entry_warehouse IS NOT NULL
                 AND s.dt_reviewed_entry_warehouse != s.prev_dt_reviewed
                 AND (s.dt_planned_entry_warehouse = s.prev_dt_planned
                      OR s.prev_dt_planned IS NULL)
            THEN ABS(DATE_DIFF(s.dt_reviewed_entry_warehouse, s.prev_dt_reviewed, DAY))
            ELSE 0
        END AS magnitude_dt_reviewed,

        -- Mudança de grade / planned_quantity (INT_GRADE)
        CASE
            WHEN s.prev_planned_qty IS NOT NULL
                 AND s.planned_quantity IS NOT NULL
                 AND s.planned_quantity != s.prev_planned_qty
            THEN TRUE
            ELSE FALSE
        END AS changed_grade,

        -- Delta de quantidade
        CASE
            WHEN s.prev_planned_qty IS NOT NULL
                 AND s.planned_quantity IS NOT NULL
                 AND s.planned_quantity != s.prev_planned_qty
            THEN s.planned_quantity - s.prev_planned_qty
            ELSE 0
        END AS delta_grade_qty

    FROM CTE_SNAPSHOTS AS s
    WHERE s.prev_ingestion_date IS NOT NULL  -- Ignora primeiro snapshot (sem anterior)
)

-- Agregação por OP-SKU: contagem total de revisões
SELECT
    c.op_code,
    c.product_sku,
    c.cycle_name,
    c.supplier_name,
    c.product_name,
    CASE
        WHEN REGEXP_CONTAINS(c.cycle_name, r'^C\d{2}20\d{2}$') THEN 'Base'
        ELSE 'Extra'
    END AS cycle_type,

    -- Contagens de revisões
    COUNTIF(c.changed_dt_planned) AS n_rev_planned,
    COUNTIF(c.changed_dt_reviewed_ext) AS n_rev_reviewed_ext,
    COUNTIF(c.changed_grade) AS n_rev_grade,
    COUNTIF(c.changed_dt_planned) + COUNTIF(c.changed_dt_reviewed_ext) + COUNTIF(c.changed_grade) AS n_rev_total,

    -- Magnitudes acumuladas
    SUM(c.magnitude_dt_planned) AS total_magnitude_dt_planned,
    SUM(c.magnitude_dt_reviewed) AS total_magnitude_dt_reviewed,
    SUM(ABS(c.delta_grade_qty)) AS total_abs_delta_grade,

    -- Primeira e última data de revisão (para timeline)
    MIN(CASE WHEN c.changed_dt_planned OR c.changed_dt_reviewed_ext OR c.changed_grade
             THEN c.ingestion_date END) AS first_revision_date,
    MAX(CASE WHEN c.changed_dt_planned OR c.changed_dt_reviewed_ext OR c.changed_grade
             THEN c.ingestion_date END) AS last_revision_date

FROM CTE_CHANGES AS c
GROUP BY c.op_code, c.product_sku, c.cycle_name, c.supplier_name, c.product_name
ORDER BY n_rev_total DESC, c.cycle_name, c.op_code, c.product_sku
