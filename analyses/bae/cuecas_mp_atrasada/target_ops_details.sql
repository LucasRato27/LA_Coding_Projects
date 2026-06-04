-- Detalhe das OPs BAE BRASIL apontadas na analise de cuecas:
-- OPF37N1913: Cueca Boxer Comfort Anti Suor Masculino
-- OPF37N1940: Cueca Boxer Performance Anti Suor Masculino

WITH current_sc AS (
  SELECT
    sc.op_code,
    ANY_VALUE(sc.cycle_name) AS cycle_name,
    ANY_VALUE(sc.supplier_name) AS sc_supplier_name,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT sc.current_production_stage IGNORE NULLS ORDER BY sc.current_production_stage),
      ' | '
    ) AS current_production_stages,
    MIN(DATE(sc.dt_min_entry_warehouse)) AS dt_min_entry_warehouse,
    MAX(sc.dt_planned_entry_warehouse) AS dt_planned_entry_warehouse,
    MAX(sc.dt_reviewed_entry_warehouse) AS dt_reviewed_entry_warehouse,
    MAX(sc.dt_planned_production_start) AS dt_planned_production_start,
    MAX(sc.planned_quantity_op) AS planned_quantity_op,
    MAX(sc.received_quantity_op) AS received_quantity_op
  FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
  WHERE sc.op_code IN ('OPF37N1913', 'OPF37N1940')
    AND sc.production_order_type NOT IN ('flexible', 'converted')
  GROUP BY sc.op_code
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
  WHERE op_code IN ('OPF37N1913', 'OPF37N1940')
)

SELECT
  sc.op_code AS order_code,
  po.status,
  po.production_stage,
  sc.cycle_name,
  sc.sc_supplier_name,
  sc.current_production_stages,
  sc.dt_min_entry_warehouse,
  sc.dt_planned_entry_warehouse,
  sc.dt_reviewed_entry_warehouse,
  sc.dt_planned_production_start,
  sc.planned_quantity_op,
  sc.received_quantity_op,
  po.planned_production_delivery_date,
  po.expected_production_delivery_date,
  po.expected_fabric_receiving_date,
  po.real_fabric_receiving_date,
  DATE_DIFF(po.real_fabric_receiving_date, po.expected_fabric_receiving_date, DAY)
    AS fabric_receiving_delay_days,
  DATE_DIFF(po.expected_production_delivery_date, po.real_fabric_receiving_date, DAY)
    AS mp_to_expected_production_end_lead_time_days,
  DATE_DIFF(po.expected_production_delivery_date, po.real_production_start_date, DAY)
    AS productive_lead_time_days,
  po.real_fabric_receiving_date > po.expected_fabric_receiving_date AS is_mp_late,
  DATE_DIFF(po.expected_production_delivery_date, po.real_fabric_receiving_date, DAY)
    BETWEEN 0 AND 44 AS is_mp_to_expected_end_lt_45_days,
  DATE_DIFF(po.expected_production_delivery_date, po.real_production_start_date, DAY)
    < 45 AS is_productive_lt_45_days,
  q.quality_supplier_name,
  q.audit_count,
  q.dt_first_audit_completed,
  q.first_audit_result_standardized,
  q.first_audit_deliberation_standardized,
  q.first_audit_defective_rate,
  q.pieces_rejected_in_first_audit_insider,
  q.query_execution_date AS quality_query_execution_date
FROM current_sc AS sc
INNER JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
  ON po.order_code = sc.op_code
LEFT JOIN quality AS q
  ON q.op_code = sc.op_code
ORDER BY sc.op_code;
