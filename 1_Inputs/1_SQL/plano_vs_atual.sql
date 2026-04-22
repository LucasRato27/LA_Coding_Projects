-- =============================================================================
-- PLANO ORIGINAL (estado no baseline) + ESTADO ATUAL
-- Extrai para cada OP-SKU: o estado no momento do congelamento do ciclo
-- e o estado mais recente, lado a lado, com flags de mudança INT/EXT.
-- =============================================================================

WITH CTE_BASELINE AS (
    -- Primeiro ingestion_date sem OPs pending para cada ciclo
    SELECT
        h.cycle_name,
        MIN(h.ingestion_date) AS baseline_date
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
    WHERE h.cycle_name IS NOT NULL
    GROUP BY h.cycle_name
    HAVING COUNTIF(h.current_production_stage = 'pending') = 0
),

CTE_BASELINE_CORRECTED AS (
    -- Para ciclos onde o primeiro dia já pode ter tido pending em algum snapshot anterior,
    -- pegamos o MIN(ingestion_date) onde não há pending
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
        h.supplier_id,
        h.supplier_name,
        h.product_name,
        h.product_color,
        h.product_size,
        h.is_finished_product_order,
        h.production_order_type,
        h.current_production_stage AS baseline_stage,
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

CTE_LATEST_SNAPSHOT AS (
    -- Snapshot mais recente disponível por OP-SKU (MAX por OP-SKU, não global)
    -- Evita tratar como "inalteradas" OPs ausentes na última data de ingestão global
    SELECT
        h.op_code,
        h.product_sku,
        h.cycle_name,
        h.current_production_stage AS current_stage,
        h.planned_quantity AS current_planned_qty,
        h.dt_planned_entry_warehouse AS current_dt_planned,
        h.dt_reviewed_entry_warehouse AS current_dt_reviewed,
        h.received_quantity AS current_received_qty,
        h.cutted_quantity AS current_cutted_qty,
        h.dt_max_entry_warehouse AS current_dt_max_entry,
        h.ingestion_date AS latest_ingestion_date
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` AS h
    INNER JOIN (
        SELECT op_code, product_sku, MAX(ingestion_date) AS max_date
        FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`
        WHERE production_order_type NOT IN ('flexible', 'converted')
        GROUP BY op_code, product_sku
    ) AS m
        ON h.op_code = m.op_code
        AND h.product_sku = m.product_sku
        AND h.ingestion_date = m.max_date
    WHERE h.production_order_type NOT IN ('flexible', 'converted')
),

CTE_CANCELAMENTOS AS (
    -- Motivo de cancelamento das OPs (para separar INT_CANCEL de EXT_CANCEL)
    SELECT
        po.order_code AS op_code,
        po.canceled_production_reason
    FROM `insider-data-lake.integrated.muninn_production_orders` AS po
    WHERE po.canceled_production_reason IS NOT NULL
)

SELECT
    -- Identificadores
    orig.op_code,
    orig.product_sku,
    orig.cycle_name,
    orig.supplier_id,
    orig.supplier_name,
    orig.product_name,
    orig.product_color,
    orig.product_size,
    orig.is_finished_product_order,
    orig.production_order_type,
    orig.baseline_date,

    -- Classificação do ciclo
    CASE
        WHEN REGEXP_CONTAINS(orig.cycle_name, r'^C\d{2}20\d{2}') THEN 'Base'
        ELSE 'Extra'
    END AS cycle_type,

    -- Estado no baseline
    orig.baseline_stage,
    orig.baseline_planned_qty,
    orig.baseline_dt_planned,
    orig.baseline_dt_reviewed,

    -- Estado atual
    curr.current_stage,
    curr.current_planned_qty,
    curr.current_dt_planned,
    curr.current_dt_reviewed,
    curr.current_received_qty,
    curr.current_cutted_qty,
    curr.current_dt_max_entry,
    curr.latest_ingestion_date,

    -- Cancelamento
    canc.canceled_production_reason,

    -- =========================================================================
    -- FLAGS DE MUDANÇA INTERNA (INT)
    -- =========================================================================
    
    -- INT_DATE: dt_planned mudou em relação ao baseline
    CASE
        WHEN curr.current_dt_planned IS NOT NULL
             AND orig.baseline_dt_planned IS NOT NULL
             AND curr.current_dt_planned != orig.baseline_dt_planned
        THEN TRUE
        ELSE FALSE
    END AS is_int_date,

    -- INT_CANCEL: OP cancelada por decisão interna (In Season)
    CASE
        WHEN curr.current_stage = 'canceled'
             AND canc.canceled_production_reason = 'Revisão de Demanda (In Season)'
        THEN TRUE
        ELSE FALSE
    END AS is_int_cancel,

    -- INT_GRADE: planned_quantity mudou no nível SKU
    CASE
        WHEN curr.current_planned_qty IS NOT NULL
             AND orig.baseline_planned_qty IS NOT NULL
             AND curr.current_planned_qty != orig.baseline_planned_qty
        THEN TRUE
        ELSE FALSE
    END AS is_int_grade,

    -- Flag consolidada: qualquer mudança interna
    CASE
        WHEN (curr.current_dt_planned != orig.baseline_dt_planned)
             OR (curr.current_stage = 'canceled' AND canc.canceled_production_reason = 'Revisão de Demanda (In Season)')
             OR (curr.current_planned_qty != orig.baseline_planned_qty)
        THEN TRUE
        ELSE FALSE
    END AS is_int_any,

    -- =========================================================================
    -- FLAGS DE MUDANÇA EXTERNA (EXT) — para acompanhamento, fora do OKR
    -- =========================================================================
    
    -- EXT_CANCEL: cancelada por motivo externo
    CASE
        WHEN curr.current_stage = 'canceled'
             AND (canc.canceled_production_reason IS NULL
                  OR canc.canceled_production_reason != 'Revisão de Demanda (In Season)')
        THEN TRUE
        ELSE FALSE
    END AS is_ext_cancel,

    -- EXT_DATE_REV: dt_reviewed mudou SEM que dt_planned tenha mudado
    CASE
        WHEN curr.current_dt_reviewed IS NOT NULL
             AND orig.baseline_dt_reviewed IS NOT NULL
             AND curr.current_dt_reviewed != orig.baseline_dt_reviewed
             AND (curr.current_dt_planned = orig.baseline_dt_planned
                  OR curr.current_dt_planned IS NULL
                  OR orig.baseline_dt_planned IS NULL)
        THEN TRUE
        ELSE FALSE
    END AS is_ext_date_rev,

    -- Flag consolidada: qualquer mudança externa
    CASE
        WHEN (curr.current_stage = 'canceled'
              AND (canc.canceled_production_reason IS NULL
                   OR canc.canceled_production_reason != 'Revisão de Demanda (In Season)'))
             OR (curr.current_dt_reviewed != orig.baseline_dt_reviewed
                 AND curr.current_dt_planned = orig.baseline_dt_planned)
        THEN TRUE
        ELSE FALSE
    END AS is_ext_any,

    -- Deltas numéricos para análise
    DATE_DIFF(curr.current_dt_planned, orig.baseline_dt_planned, DAY) AS delta_days_planned,
    DATE_DIFF(curr.current_dt_reviewed, orig.baseline_dt_reviewed, DAY) AS delta_days_reviewed,
    COALESCE(curr.current_planned_qty, 0) - COALESCE(orig.baseline_planned_qty, 0) AS delta_planned_qty

FROM CTE_PLANO_ORIGINAL AS orig
LEFT JOIN CTE_LATEST_SNAPSHOT AS curr
    ON orig.op_code = curr.op_code
    AND orig.product_sku = curr.product_sku
LEFT JOIN CTE_CANCELAMENTOS AS canc
    ON orig.op_code = canc.op_code
ORDER BY orig.cycle_name, orig.op_code, orig.product_sku
