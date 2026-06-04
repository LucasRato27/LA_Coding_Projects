-- =============================================================================
-- TIMELINE DE REVISÕES (DRILL-DOWN)
-- Retorna todos os snapshots de uma OP-SKU com as mudanças marcadas.
-- Usar como query parametrizada (substituir {op_code} e {product_sku}).
-- =============================================================================

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
    h.received_quantity,
    h.cutted_quantity,

    -- Valores anteriores
    LAG(h.dt_planned_entry_warehouse) OVER w AS prev_dt_planned,
    LAG(h.dt_reviewed_entry_warehouse) OVER w AS prev_dt_reviewed,
    LAG(h.planned_quantity) OVER w AS prev_planned_qty,

    -- Flags de mudança
    CASE
        WHEN LAG(h.dt_planned_entry_warehouse) OVER w IS NOT NULL
             AND h.dt_planned_entry_warehouse != LAG(h.dt_planned_entry_warehouse) OVER w
        THEN 'INT_DATE'
    END AS change_dt_planned,

    CASE
        WHEN LAG(h.dt_reviewed_entry_warehouse) OVER w IS NOT NULL
             AND h.dt_reviewed_entry_warehouse != LAG(h.dt_reviewed_entry_warehouse) OVER w
             AND (h.dt_planned_entry_warehouse = LAG(h.dt_planned_entry_warehouse) OVER w
                  OR LAG(h.dt_planned_entry_warehouse) OVER w IS NULL)
        THEN 'EXT_DATE_REV'
    END AS change_dt_reviewed,

    CASE
        WHEN LAG(h.planned_quantity) OVER w IS NOT NULL
             AND h.planned_quantity != LAG(h.planned_quantity) OVER w
        THEN 'INT_GRADE'
    END AS change_grade

FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
WHERE h.op_code = '{op_code}'
    AND h.product_sku = '{product_sku}'
    AND h.production_order_type NOT IN ('flexible', 'converted')
WINDOW w AS (PARTITION BY h.op_code, h.product_sku ORDER BY h.ingestion_date)
ORDER BY h.ingestion_date
