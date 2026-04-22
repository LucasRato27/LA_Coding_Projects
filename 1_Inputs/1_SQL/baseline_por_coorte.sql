-- =============================================================================
-- BASELINE POR COORTE
-- Para cada cycle_name, encontra o primeiro ingestion_date em que NENHUMA OP
-- do ciclo está em 'pending'. Esse é o snapshot de congelamento do ciclo.
-- =============================================================================

WITH CTE_OPS_PENDING AS (
    -- Para cada ciclo e data de ingestão, conta quantas OPs ainda estão pending
    SELECT
        h.cycle_name,
        h.ingestion_date,
        COUNTIF(h.current_production_stage = 'pending') AS n_pending,
        COUNT(DISTINCT h.op_code) AS n_ops
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
    WHERE h.cycle_name IS NOT NULL
        AND h.production_order_type NOT IN ('flexible', 'converted')
    GROUP BY h.cycle_name, h.ingestion_date
),

CTE_BASELINE AS (
    -- Primeiro ingestion_date sem OPs pending para cada ciclo
    SELECT
        cycle_name,
        MIN(ingestion_date) AS baseline_date,
        MAX(n_ops) AS n_ops_at_baseline
    FROM CTE_OPS_PENDING
    WHERE n_pending = 0
    GROUP BY cycle_name
)

SELECT
    b.cycle_name,
    b.baseline_date,
    b.n_ops_at_baseline,
    -- Classificação: ciclo base vs extra
    CASE
        WHEN REGEXP_CONTAINS(b.cycle_name, r'^C\d{2}20\d{2}$') THEN 'Base'
        ELSE 'Extra'
    END AS cycle_type
FROM CTE_BASELINE AS b
ORDER BY b.baseline_date DESC, b.cycle_name
