-- Lista de OPs DDAL detratoras, com status de atraso de recebimento de tecido.
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
)

SELECT
  sc.op_code AS order_code,
  sc.cycle_name,
  sc.sc_supplier_name,
  sc.current_production_stages,
  sc.dt_min_entry_warehouse,
  sc.dt_planned_entry_warehouse,
  sc.dt_reviewed_entry_warehouse,
  sc.planned_quantity_op,
  sc.received_quantity_op,
  po.status,
  po.production_stage,
  s.id AS supplier_id,
  s.alias AS supplier_alias,
  s.legal_name AS supplier_legal_name,
  po.expected_fabric_receiving_date,
  po.real_fabric_receiving_date,
  DATE_DIFF(
    po.real_fabric_receiving_date,
    po.expected_fabric_receiving_date,
    DAY
  ) AS fabric_receiving_delay_days,
  CASE
    WHEN po.expected_fabric_receiving_date IS NULL THEN 'MISSING_EXPECTED_DATE'
    WHEN po.real_fabric_receiving_date IS NULL THEN 'MISSING_REAL_DATE'
    WHEN po.real_fabric_receiving_date > po.expected_fabric_receiving_date THEN 'LATE'
    WHEN po.real_fabric_receiving_date = po.expected_fabric_receiving_date THEN 'ON_TIME'
    WHEN po.real_fabric_receiving_date < po.expected_fabric_receiving_date THEN 'EARLY'
  END AS fabric_receiving_status
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
ORDER BY
  fabric_receiving_status,
  fabric_receiving_delay_days DESC,
  sc.dt_planned_entry_warehouse DESC,
  sc.op_code;
