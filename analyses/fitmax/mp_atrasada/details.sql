-- Todas as OPs Fitmax detratoras do ICP.
-- Universo: dt_planned_entry_warehouse nos ultimos 90 dias ate analysis_date,
-- ainda nao recebidas no CD. Exclui pending e canceled.
-- Janela ampliada para 90 dias para capturar OPs com prazo a partir de 2026-03-03.

WITH params AS (
  SELECT
    DATE '2026-06-01'                                              AS analysis_date,
    DATE_SUB(DATE '2026-06-01', INTERVAL 90 DAY)                   AS window_start
),

current_sc AS (
  SELECT
    sc.op_code,
    ANY_VALUE(sc.cycle_name)                                       AS cycle_name,
    ANY_VALUE(sc.supplier_name)                                    AS sc_supplier_name,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT sc.current_production_stage IGNORE NULLS
                ORDER BY sc.current_production_stage),
      ' | '
    )                                                              AS current_production_stages,
    COUNTIF(sc.current_production_stage IN ('pending', 'canceled')) > 0
                                                                   AS has_excluded_stage,
    MIN(DATE(sc.dt_min_entry_warehouse))                           AS dt_min_entry_warehouse,
    MAX(sc.dt_planned_entry_warehouse)                             AS dt_planned_entry_warehouse,
    MAX(sc.dt_reviewed_entry_warehouse)                            AS dt_reviewed_entry_warehouse,
    MAX(sc.dt_planned_production_start)                            AS dt_planned_production_start,
    MAX(sc.planned_quantity_op)                                    AS planned_quantity_op,
    MAX(sc.received_quantity_op)                                   AS received_quantity_op
  FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
  WHERE REGEXP_CONTAINS(UPPER(COALESCE(sc.supplier_name, '')), r'FITMAX')
    AND sc.production_order_type NOT IN ('flexible', 'converted')
  GROUP BY sc.op_code
)

SELECT
  sc.op_code                                                       AS order_code,
  sc.cycle_name,
  sc.sc_supplier_name,
  sc.current_production_stages,
  sc.dt_min_entry_warehouse,
  sc.dt_planned_entry_warehouse,
  sc.dt_reviewed_entry_warehouse,
  sc.dt_planned_production_start,
  sc.planned_quantity_op,
  sc.received_quantity_op,
  po.status,
  po.production_stage,
  s.id                                                             AS supplier_id,
  s.alias                                                          AS supplier_alias,
  s.legal_name                                                     AS supplier_legal_name,
  po.planned_production_delivery_date,
  po.expected_production_delivery_date,
  po.expected_fabric_receiving_date,
  po.real_fabric_receiving_date,
  DATE_DIFF(
    po.real_fabric_receiving_date,
    po.expected_fabric_receiving_date,
    DAY
  )                                                                AS fabric_receiving_delay_days,
  DATE_DIFF(
    po.expected_production_delivery_date,
    po.real_fabric_receiving_date,
    DAY
  )                                                                AS mp_to_expected_end_lead_time_days,
  DATE_DIFF(
    po.expected_production_delivery_date,
    po.real_production_start_date,
    DAY
  )                                                                AS productive_lead_time_days,
  CASE
    WHEN po.expected_fabric_receiving_date IS NULL                 THEN 'SEM_DATA_ESPERADA'
    WHEN po.real_fabric_receiving_date     IS NULL                 THEN 'SEM_DATA_REAL'
    WHEN po.real_fabric_receiving_date > po.expected_fabric_receiving_date THEN 'ATRASADA'
    WHEN po.real_fabric_receiving_date < po.expected_fabric_receiving_date THEN 'ADIANTADA'
    ELSE                                                                'NO_PRAZO'
  END                                                              AS fabric_receiving_status,
  DATE_DIFF(
    (SELECT analysis_date FROM params),
    sc.dt_planned_entry_warehouse,
    DAY
  )                                                                AS days_overdue
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
    r'FITMAX'
  )
  AND sc.dt_planned_entry_warehouse
    BETWEEN params.window_start AND params.analysis_date
  AND (
    sc.dt_min_entry_warehouse IS NULL
    OR sc.dt_min_entry_warehouse > params.analysis_date
  )
  AND NOT sc.has_excluded_stage
ORDER BY
  days_overdue DESC,
  sc.dt_planned_entry_warehouse,
  sc.op_code;
