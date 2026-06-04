-- Detalhe das OPs DDAL detratoras do indicador de acuracia:
-- OPs com entrega planejada nos ultimos 45 dias e ainda nao recebidas no CD.
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

ddal_ops AS (
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
    po.planned_production_delivery_date,
    po.expected_production_delivery_date,
    po.expected_fabric_receiving_date,
    po.real_fabric_receiving_date,
    DATE_DIFF(
      po.real_fabric_receiving_date,
      po.expected_fabric_receiving_date,
      DAY
    ) AS fabric_receiving_delay_days,
    DATE_DIFF(
      po.expected_production_delivery_date,
      po.real_fabric_receiving_date,
      DAY
    ) AS mp_to_expected_production_end_lead_time_days,
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
),

quality AS (
  SELECT
    op_code,
    supplier_name AS quality_supplier_name,
    audit_count,
    dt_first_audit_completed,
    first_audit_result_standardized,
    first_audit_deliberation_standardized,
    first_audit_defective_rate,
    pieces_rejected_in_first_audit_insider,
    query_execution_date
  FROM `insider-data-lake.sop_gold.quality_inspection_data`
)

SELECT
  d.order_code,
  d.status,
  d.production_stage,
  d.supplier_id,
  d.supplier_alias,
  d.supplier_legal_name,
  d.cycle_name,
  d.sc_supplier_name,
  d.current_production_stages,
  d.dt_min_entry_warehouse,
  d.dt_planned_entry_warehouse,
  d.dt_reviewed_entry_warehouse,
  d.planned_quantity_op,
  d.received_quantity_op,
  d.planned_production_delivery_date,
  d.expected_production_delivery_date,
  d.expected_fabric_receiving_date,
  d.real_fabric_receiving_date,
  d.fabric_receiving_delay_days,
  d.mp_to_expected_production_end_lead_time_days,
  d.real_fabric_receiving_date > d.expected_fabric_receiving_date AS is_mp_late,
  d.mp_to_expected_production_end_lead_time_days < 45 AS is_mp_to_expected_end_lt_45_days,
  q.quality_supplier_name,
  q.audit_count,
  q.dt_first_audit_completed,
  q.first_audit_result_standardized,
  q.first_audit_deliberation_standardized,
  q.first_audit_defective_rate,
  q.pieces_rejected_in_first_audit_insider,
  q.first_audit_result_standardized = 'qualita_rejected' AS is_quality_rejected,
  q.first_audit_result_standardized = 'qualita_rejected'
    AND q.first_audit_deliberation_standardized = 'insider_approved'
    AS is_quality_rejected_delivery_authorized,
  q.first_audit_result_standardized = 'qualita_rejected'
    AND q.first_audit_deliberation_standardized = 'insider_rejected'
    AS is_quality_rejected_correct_reaudit,
  q.query_execution_date AS quality_query_execution_date
FROM ddal_ops AS d
LEFT JOIN quality AS q
  ON q.op_code = d.order_code
ORDER BY
  is_mp_late DESC,
  is_quality_rejected DESC,
  is_mp_to_expected_end_lt_45_days DESC,
  d.planned_production_delivery_date DESC,
  d.order_code;
