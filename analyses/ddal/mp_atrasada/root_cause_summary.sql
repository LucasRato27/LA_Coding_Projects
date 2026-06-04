-- Tabela de causa raiz no formato:
-- Causa Raiz | Qtd OPs | Volume (Pecas) | % do Volume Total
--
-- Universo:
-- - Fornecedor DDAL
-- - dt_planned_entry_warehouse nos ultimos 45 dias ate analysis_date
-- - dt_min_entry_warehouse nulo ou maior que analysis_date
-- - exclui OPs em estagio pending ou canceled

WITH params AS (
  SELECT DATE '2026-04-27' AS analysis_date
),

current_sc AS (
  SELECT
    sc.op_code,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT sc.current_production_stage IGNORE NULLS ORDER BY sc.current_production_stage),
      ' | '
    ) AS current_production_stages,
    COUNTIF(sc.current_production_stage IN ('pending', 'canceled')) > 0 AS has_excluded_stage,
    MIN(DATE(sc.dt_min_entry_warehouse)) AS dt_min_entry_warehouse,
    MAX(sc.dt_planned_entry_warehouse) AS dt_planned_entry_warehouse,
    MAX(sc.dt_planned_production_start) AS dt_planned_production_start,
    MAX(sc.planned_quantity_op) AS planned_quantity_op
  FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
  WHERE REGEXP_CONTAINS(UPPER(COALESCE(sc.supplier_name, '')), r'DDAL')
    AND sc.production_order_type NOT IN ('flexible', 'converted')
  GROUP BY sc.op_code
),

base AS (
  SELECT
    sc.op_code,
    sc.planned_quantity_op,
    po.expected_fabric_receiving_date,
    po.real_fabric_receiving_date,
    po.expected_production_delivery_date,
    po.real_production_start_date,
    q.first_audit_result_standardized,
    q.first_audit_deliberation_standardized
  FROM current_sc AS sc
  INNER JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
    ON po.order_code = sc.op_code
  LEFT JOIN `insider-data-lake.sop_gold.quality_inspection_data` AS q
    ON q.op_code = sc.op_code
  CROSS JOIN params
  WHERE sc.dt_planned_entry_warehouse
      BETWEEN DATE_SUB(params.analysis_date, INTERVAL 45 DAY)
      AND params.analysis_date
    AND (
      sc.dt_min_entry_warehouse IS NULL
      OR sc.dt_min_entry_warehouse > params.analysis_date
    )
    AND NOT sc.has_excluded_stage
),

total AS (
  SELECT
    SUM(planned_quantity_op) AS total_volume
  FROM base
),

causes AS (
  SELECT
    1 AS sort_order,
    'Lead Time Produtivo (45 dias)' AS causa_raiz,
    op_code,
    planned_quantity_op
  FROM base
  WHERE DATE_DIFF(expected_production_delivery_date, real_production_start_date, DAY) < 45

  UNION ALL

  SELECT
    2 AS sort_order,
    'Mp entregue dentro de um Lead Time menor que 45 dias' AS causa_raiz,
    op_code,
    planned_quantity_op
  FROM base
  WHERE DATE_DIFF(expected_production_delivery_date, real_fabric_receiving_date, DAY)
    BETWEEN 0 AND 44

  UNION ALL

  SELECT
    3 AS sort_order,
    'Atraso de Mp' AS causa_raiz,
    op_code,
    planned_quantity_op
  FROM base
  WHERE real_fabric_receiving_date > expected_fabric_receiving_date

  UNION ALL

  SELECT
    4 AS sort_order,
    'Reprovacao de Qualidade (Entrega Autorizada)' AS causa_raiz,
    op_code,
    planned_quantity_op
  FROM base
  WHERE first_audit_result_standardized = 'qualita_rejected'
    AND first_audit_deliberation_standardized = 'insider_approved'

  UNION ALL

  SELECT
    5 AS sort_order,
    'Reprovacao de Qualidade (Corrigir e Reauditar)' AS causa_raiz,
    op_code,
    planned_quantity_op
  FROM base
  WHERE first_audit_result_standardized = 'qualita_rejected'
    AND first_audit_deliberation_standardized = 'insider_rejected'
)

SELECT
  causa_raiz,
  COUNT(DISTINCT op_code) AS qtd_ops,
  SUM(planned_quantity_op) AS volume_pecas,
  SAFE_DIVIDE(SUM(planned_quantity_op), ANY_VALUE(total.total_volume)) AS pct_do_volume_total
FROM causes
CROSS JOIN total
GROUP BY sort_order, causa_raiz
ORDER BY volume_pecas DESC, sort_order;
