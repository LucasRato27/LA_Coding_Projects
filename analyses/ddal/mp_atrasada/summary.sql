-- Percentual de OPs DDAL detratoras do indicador de acuracia com MP atrasada.
-- Universo: OPs com entrega planejada nos ultimos 45 dias e ainda nao recebidas no CD
-- (dt_min_entry_warehouse IS NULL ou dt_min_entry_warehouse > analysis_date).
-- OPs em estagio pending ou canceled sao excluidas.

WITH params AS (
  SELECT DATE '2026-04-27' AS analysis_date
),

current_sc AS (
  SELECT
    sc.op_code,
    ANY_VALUE(sc.cycle_name) AS cycle_name,
    ANY_VALUE(sc.supplier_name) AS sc_supplier_name,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT sc.current_production_stage IGNORE NULLS ORDER BY sc.current_production_stage),
      ' | '
    ) AS current_production_stages,
    COUNTIF(sc.current_production_stage IN ('pending', 'canceled')) > 0 AS has_excluded_stage,
    MIN(DATE(sc.dt_min_entry_warehouse)) AS dt_min_entry_warehouse,
    MAX(sc.dt_planned_entry_warehouse) AS dt_planned_entry_warehouse,
    MAX(sc.dt_reviewed_entry_warehouse) AS dt_reviewed_entry_warehouse,
    MAX(sc.planned_quantity_op) AS planned_quantity_op,
    MAX(sc.received_quantity_op) AS received_quantity_op
  FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
  WHERE REGEXP_CONTAINS(UPPER(COALESCE(sc.supplier_name, '')), r'DDAL')
    AND sc.production_order_type NOT IN ('flexible', 'converted')
  GROUP BY sc.op_code
),

base AS (
  SELECT
    sc.op_code AS order_code,
    sc.cycle_name,
    sc.sc_supplier_name,
    sc.dt_min_entry_warehouse,
    sc.dt_planned_entry_warehouse,
    sc.dt_reviewed_entry_warehouse,
    sc.planned_quantity_op,
    sc.received_quantity_op,
    po.status,
    po.production_stage,
    po.expected_fabric_receiving_date,
    po.real_fabric_receiving_date,
    DATE_DIFF(
      po.real_fabric_receiving_date,
      po.expected_fabric_receiving_date,
      DAY
    ) AS fabric_receiving_delay_days,
    s.id AS supplier_id,
    s.alias AS supplier_alias,
    s.legal_name AS supplier_legal_name
  FROM current_sc AS sc
  INNER JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
    ON po.order_code = sc.op_code
  INNER JOIN `insider-data-lake.integrated.muninn_apparel_manufacturers` AS am
    ON am.id = po.apparel_manufacturer_id
  INNER JOIN `insider-data-lake.integrated.muninn_suppliers` AS s
    ON s.id = am.supplier_id
  CROSS JOIN params
  WHERE REGEXP_CONTAINS(
      UPPER(CONCAT(COALESCE(s.alias, ''), ' ', COALESCE(s.legal_name, ''))),
      r'DDAL'
    )
    AND sc.dt_planned_entry_warehouse
      BETWEEN DATE_SUB(params.analysis_date, INTERVAL 45 DAY)
      AND params.analysis_date
    AND (
      sc.dt_min_entry_warehouse IS NULL
      OR sc.dt_min_entry_warehouse > params.analysis_date
    )
    AND NOT sc.has_excluded_stage
)

SELECT
  (SELECT analysis_date FROM params) AS analysis_date,
  DATE_SUB((SELECT analysis_date FROM params), INTERVAL 45 DAY) AS window_start_date,
  (SELECT analysis_date FROM params) AS window_end_date,
  ANY_VALUE(supplier_id) AS supplier_id,
  ANY_VALUE(supplier_alias) AS supplier_alias,
  ANY_VALUE(supplier_legal_name) AS supplier_legal_name,
  COUNT(DISTINCT order_code) AS total_detractor_ops_not_arrived,
  COUNT(DISTINCT IF(dt_min_entry_warehouse IS NULL, order_code, NULL)) AS ops_without_entry_warehouse_date,
  COUNT(DISTINCT IF(dt_min_entry_warehouse > (SELECT analysis_date FROM params), order_code, NULL)) AS ops_with_future_entry_warehouse_date,
  COUNT(DISTINCT IF(
    expected_fabric_receiving_date IS NOT NULL
    AND real_fabric_receiving_date IS NOT NULL,
    order_code,
    NULL
  )) AS ops_with_both_fabric_dates,
  COUNT(DISTINCT IF(
    real_fabric_receiving_date > expected_fabric_receiving_date,
    order_code,
    NULL
  )) AS ops_fabric_late,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(
      real_fabric_receiving_date > expected_fabric_receiving_date,
      order_code,
      NULL
    )),
    COUNT(DISTINCT order_code)
  ) AS pct_late_over_detractor_ops,
  SAFE_DIVIDE(
    COUNT(DISTINCT IF(
      real_fabric_receiving_date > expected_fabric_receiving_date,
      order_code,
      NULL
    )),
    COUNT(DISTINCT IF(
      expected_fabric_receiving_date IS NOT NULL
      AND real_fabric_receiving_date IS NOT NULL,
      order_code,
      NULL
    ))
  ) AS pct_late_over_ops_with_both_dates,
  AVG(IF(
    real_fabric_receiving_date > expected_fabric_receiving_date,
    fabric_receiving_delay_days,
    NULL
  )) AS avg_delay_days_late_ops,
  MIN(IF(
    real_fabric_receiving_date > expected_fabric_receiving_date,
    fabric_receiving_delay_days,
    NULL
  )) AS min_delay_days_late_ops,
  MAX(IF(
    real_fabric_receiving_date > expected_fabric_receiving_date,
    fabric_receiving_delay_days,
    NULL
  )) AS max_delay_days_late_ops,
  MIN(dt_planned_entry_warehouse) AS min_planned_entry_warehouse,
  MAX(dt_planned_entry_warehouse) AS max_planned_entry_warehouse
FROM base;
