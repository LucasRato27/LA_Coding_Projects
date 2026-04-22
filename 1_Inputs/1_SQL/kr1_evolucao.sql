-- =============================================================================
-- EVOLUÇÃO TEMPORAL DO KR1
-- Para cada snapshot semanal (segunda-feira), compara cada OP-SKU contra o
-- baseline e agrega volumes alterados por ciclo.
-- Permite visualizar como o KR1 "degrada" ao longo do tempo.
-- =============================================================================

WITH CTE_BASELINE_CORRECTED AS (
    -- Primeiro ingestion_date sem OPs pending para cada ciclo
    SELECT
        cycle_name,
        MIN(ingestion_date) AS baseline_date
    FROM (
        SELECT
            h.cycle_name,
            h.ingestion_date,
            COUNTIF(h.current_production_stage = 'pending') AS n_pending
        FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
        WHERE h.cycle_name IS NOT NULL
            AND h.production_order_type NOT IN ('flexible', 'converted')
        GROUP BY h.cycle_name, h.ingestion_date
    )
    WHERE n_pending = 0
    GROUP BY cycle_name
),

CTE_PLANO_ORIGINAL AS (
    -- Estado de cada OP-SKU no snapshot de baseline
    SELECT
        h.op_code,
        h.product_sku,
        h.cycle_name,
        h.planned_quantity AS baseline_planned_qty,
        h.dt_planned_entry_warehouse AS baseline_dt_planned,
        h.dt_reviewed_entry_warehouse AS baseline_dt_reviewed,
        b.baseline_date
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
    INNER JOIN CTE_BASELINE_CORRECTED AS b
        ON h.cycle_name = b.cycle_name
        AND h.ingestion_date = b.baseline_date
    WHERE h.production_order_type NOT IN ('flexible', 'converted')
),

CTE_CYCLE_WEEK_RANGE AS (
    -- Janela semanal de análise por ciclo (do baseline até a última ingestão disponível)
    SELECT
        b.cycle_name,
        b.baseline_date,
        MAX(h.ingestion_date) AS max_ingestion_date
    FROM CTE_BASELINE_CORRECTED AS b
    INNER JOIN `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
        ON h.cycle_name = b.cycle_name
    WHERE h.production_order_type NOT IN ('flexible', 'converted')
        AND h.ingestion_date >= b.baseline_date
    GROUP BY b.cycle_name, b.baseline_date
),

CTE_CYCLE_WEEKS AS (
    -- Calendário semanal por ciclo
    SELECT
        cycle_name,
        baseline_date,
        snapshot_week
    FROM CTE_CYCLE_WEEK_RANGE,
    UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_TRUNC(baseline_date, WEEK(MONDAY)),
            DATE_TRUNC(max_ingestion_date, WEEK(MONDAY)),
            INTERVAL 1 WEEK
        )
    ) AS snapshot_week
),

CTE_WEEKLY_SNAPSHOTS AS (
    -- Para cada OP-SKU do baseline e cada semana do ciclo,
    -- carrega o último snapshot conhecido até o fim daquela semana.
    SELECT
        orig.op_code,
        orig.product_sku,
        orig.cycle_name,
        w.snapshot_week,
        ARRAY_AGG(
            STRUCT(
                h.current_production_stage,
                h.planned_quantity,
                h.dt_planned_entry_warehouse,
                h.dt_reviewed_entry_warehouse
            )
            ORDER BY h.ingestion_date DESC
            LIMIT 1
        )[SAFE_OFFSET(0)] AS snap
    FROM CTE_PLANO_ORIGINAL AS orig
    INNER JOIN CTE_CYCLE_WEEKS AS w
        ON orig.cycle_name = w.cycle_name
    LEFT JOIN `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
        ON h.op_code = orig.op_code
        AND h.product_sku = orig.product_sku
        AND h.cycle_name = orig.cycle_name
        AND h.production_order_type NOT IN ('flexible', 'converted')
        AND h.ingestion_date >= orig.baseline_date
        AND h.ingestion_date <= DATE_ADD(w.snapshot_week, INTERVAL 6 DAY)
    GROUP BY orig.op_code, orig.product_sku, orig.cycle_name, w.snapshot_week
),

CTE_CANCELAMENTOS AS (
    SELECT
        po.order_code AS op_code,
        po.canceled_production_reason
    FROM `insider-data-lake.integrated.muninn_production_orders` AS po
    WHERE po.canceled_production_reason IS NOT NULL
),

CTE_DETAIL AS (
    -- Flags de mudança INT/EXT para cada OP-SKU em cada semana
    SELECT
        w.snapshot_week,
        orig.cycle_name,

        CASE
            WHEN REGEXP_CONTAINS(orig.cycle_name, r'^C\d{2}20\d{2}') THEN 'Base'
            ELSE 'Extra'
        END AS cycle_type,

        orig.baseline_planned_qty,

        -- INT_DATE
        CASE
            WHEN w.snap.dt_planned_entry_warehouse IS NOT NULL
                 AND orig.baseline_dt_planned IS NOT NULL
                 AND w.snap.dt_planned_entry_warehouse != orig.baseline_dt_planned
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_int_date,

        -- INT_CANCEL
        CASE
            WHEN w.snap.current_production_stage = 'canceled'
                 AND canc.canceled_production_reason = 'Revisão de Demanda (In Season)'
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_int_cancel,

        -- INT_GRADE
        CASE
            WHEN w.snap.planned_quantity IS NOT NULL
                 AND orig.baseline_planned_qty IS NOT NULL
                 AND w.snap.planned_quantity != orig.baseline_planned_qty
            THEN ABS(w.snap.planned_quantity - orig.baseline_planned_qty)
            ELSE 0
        END AS vol_int_grade,

        -- INT_ANY (consolidado)
        CASE
            WHEN (w.snap.dt_planned_entry_warehouse != orig.baseline_dt_planned)
                 OR (w.snap.current_production_stage = 'canceled'
                     AND canc.canceled_production_reason = 'Revisão de Demanda (In Season)')
                 OR (w.snap.planned_quantity != orig.baseline_planned_qty)
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_int_any,

        -- EXT_ANY (para referência)
        CASE
            WHEN (w.snap.current_production_stage = 'canceled'
                  AND (canc.canceled_production_reason IS NULL
                       OR canc.canceled_production_reason != 'Revisão de Demanda (In Season)'))
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_ext_cancel,

        -- EXT_DATE_REV
        CASE
            WHEN w.snap.dt_reviewed_entry_warehouse IS NOT NULL
                 AND orig.baseline_dt_reviewed IS NOT NULL
                 AND w.snap.dt_reviewed_entry_warehouse != orig.baseline_dt_reviewed
                 AND (w.snap.dt_planned_entry_warehouse = orig.baseline_dt_planned
                      OR w.snap.dt_planned_entry_warehouse IS NULL
                      OR orig.baseline_dt_planned IS NULL)
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_ext_date_rev,

        -- EXT_ANY (consolidado)
        CASE
            WHEN (w.snap.current_production_stage = 'canceled'
                  AND (canc.canceled_production_reason IS NULL
                       OR canc.canceled_production_reason != 'Revisão de Demanda (In Season)'))
                 OR (w.snap.dt_reviewed_entry_warehouse IS NOT NULL
                     AND orig.baseline_dt_reviewed IS NOT NULL
                     AND w.snap.dt_reviewed_entry_warehouse != orig.baseline_dt_reviewed
                     AND (w.snap.dt_planned_entry_warehouse = orig.baseline_dt_planned
                          OR w.snap.dt_planned_entry_warehouse IS NULL
                          OR orig.baseline_dt_planned IS NULL))
            THEN orig.baseline_planned_qty
            ELSE 0
        END AS vol_ext_any

    FROM CTE_PLANO_ORIGINAL AS orig
    INNER JOIN CTE_WEEKLY_SNAPSHOTS AS w
        ON orig.op_code = w.op_code
        AND orig.product_sku = w.product_sku
    LEFT JOIN CTE_CANCELAMENTOS AS canc
        ON orig.op_code = canc.op_code
)

-- Agregação por semana × ciclo
SELECT
    snapshot_week,
    cycle_name,
    cycle_type,
    SUM(baseline_planned_qty)   AS vol_original,
    SUM(vol_int_date)           AS vol_int_date,
    SUM(vol_int_cancel)         AS vol_int_cancel,
    SUM(vol_int_grade)          AS vol_int_grade,
    SUM(vol_int_any)            AS vol_int_any,
    SUM(vol_ext_cancel)         AS vol_ext_cancel,
    SUM(vol_ext_date_rev)       AS vol_ext_date_rev,
    SUM(vol_ext_any)            AS vol_ext_any
FROM CTE_DETAIL
GROUP BY snapshot_week, cycle_name, cycle_type
ORDER BY snapshot_week, cycle_name
