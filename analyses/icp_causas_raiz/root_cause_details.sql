-- Causas raízes das OPs detratoras do ICP (todos os fornecedores).
-- Grão: 1 linha por OP detratora.
--
-- Definição de detratora: OP com dt_planned_entry_warehouse dentro da janela
-- de análise que NÃO chegou ao CD até analysis_date.
-- Estágios pending e canceled são excluídos.
--
-- Causas raízes avaliadas:
--   1) Atraso de matéria-prima  → real_fabric_receiving_date > expected_fabric_receiving_date
--   2) Reprovação de qualidade  → first_audit_result_standardized = 'qualita_rejected'
--   3) Lead time MP→fim < 45d  → DATE_DIFF(expected_production_delivery_date,
--                                           real_fabric_receiving_date, DAY) < 45

WITH params AS (
  SELECT
    DATE '2026-04-27'                                          AS analysis_date,
    DATE_SUB(DATE '2026-04-27', INTERVAL 45 DAY)              AS window_start
),

-- Estado atual de cada OP (latest snapshot da tabela denormalizada)
current_sc AS (
  SELECT
    sc.op_code,
    ANY_VALUE(sc.cycle_name)                                   AS cycle_name,
    ANY_VALUE(sc.supplier_name)                                AS supplier_name,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT sc.current_production_stage IGNORE NULLS
                ORDER BY sc.current_production_stage),
      ' | '
    )                                                          AS current_production_stages,
    COUNTIF(sc.current_production_stage IN ('pending', 'canceled')) > 0
                                                               AS has_excluded_stage,
    MIN(DATE(sc.dt_min_entry_warehouse))                       AS dt_min_entry_warehouse,
    MAX(sc.dt_planned_entry_warehouse)                         AS dt_planned_entry_warehouse,
    MAX(sc.dt_reviewed_entry_warehouse)                        AS dt_reviewed_entry_warehouse,
    MAX(sc.planned_quantity_op)                                AS planned_quantity_op,
    MAX(sc.received_quantity_op)                               AS received_quantity_op
  FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
  WHERE sc.production_order_type NOT IN ('flexible', 'converted')
  GROUP BY sc.op_code
),

-- OPs detratoras: planejadas na janela e não chegaram ao CD até analysis_date
icp_detractors AS (
  SELECT
    sc.op_code,
    sc.cycle_name,
    sc.supplier_name                                           AS sc_supplier_name,
    sc.current_production_stages,
    sc.dt_min_entry_warehouse,
    sc.dt_planned_entry_warehouse,
    sc.dt_reviewed_entry_warehouse,
    sc.planned_quantity_op,
    sc.received_quantity_op,
    po.status,
    po.production_stage,
    po.expected_production_delivery_date,
    po.planned_production_delivery_date,
    po.expected_fabric_receiving_date,
    po.real_fabric_receiving_date,
    DATE_DIFF(
      po.real_fabric_receiving_date,
      po.expected_fabric_receiving_date,
      DAY
    )                                                          AS fabric_receiving_delay_days,
    DATE_DIFF(
      po.expected_production_delivery_date,
      po.real_fabric_receiving_date,
      DAY
    )                                                          AS mp_to_expected_end_lt_days,
    s.id                                                       AS supplier_id,
    s.alias                                                    AS supplier_alias,
    s.legal_name                                               AS supplier_legal_name
  FROM current_sc AS sc
  INNER JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
    ON po.order_code = sc.op_code
  INNER JOIN `insider-data-lake.integrated.muninn_apparel_manufacturers` AS am
    ON am.id = po.apparel_manufacturer_id
  INNER JOIN `insider-data-lake.integrated.muninn_suppliers` AS s
    ON s.id = am.supplier_id
  CROSS JOIN params
  WHERE sc.dt_planned_entry_warehouse
      BETWEEN params.window_start AND params.analysis_date
    AND (
      sc.dt_min_entry_warehouse IS NULL
      OR sc.dt_min_entry_warehouse > params.analysis_date
    )
    AND NOT sc.has_excluded_stage
),

quality AS (
  SELECT
    op_code,
    audit_count,
    dt_first_audit_completed,
    first_audit_result_standardized,
    first_audit_deliberation_standardized,
    SUM(first_quantity_sent_to_audition)                           AS total_quantity_audited,
    SAFE_DIVIDE(
      SUM(IF(first_audit_result_standardized = 'qualita_rejected',
             first_quantity_sent_to_audition, 0)),
      SUM(first_quantity_sent_to_audition)
    )                                                              AS taxa_reprovacao_qualita,
    SAFE_DIVIDE(
      SUM(IF(first_audit_deliberation_standardized = 'insider_rejected',
             first_quantity_sent_to_audition, 0)),
      SUM(first_quantity_sent_to_audition)
    )                                                              AS taxa_reprovacao_etapa
  FROM `insider-data-lake.sop_gold.quality_inspection_data`
  GROUP BY op_code, audit_count, dt_first_audit_completed,
           first_audit_result_standardized, first_audit_deliberation_standardized
)

SELECT
  -- Contexto da análise
  (SELECT analysis_date FROM params)                           AS analysis_date,
  (SELECT window_start FROM params)                            AS window_start_date,

  -- Identificação da OP
  d.op_code                                                    AS order_code,
  d.cycle_name,
  d.supplier_id,
  d.supplier_alias,
  d.supplier_legal_name,
  d.current_production_stages,
  d.status,
  d.production_stage,

  -- Datas de entrega
  d.dt_planned_entry_warehouse,
  d.dt_reviewed_entry_warehouse,
  d.dt_min_entry_warehouse,
  DATE_DIFF(
    (SELECT analysis_date FROM params),
    d.dt_planned_entry_warehouse,
    DAY
  )                                                            AS days_overdue,

  -- Quantidades
  d.planned_quantity_op,
  d.received_quantity_op,

  -- ─── CAUSA RAIZ 1: Atraso de Matéria-Prima ───────────────────────────────
  d.expected_fabric_receiving_date,
  d.real_fabric_receiving_date,
  d.fabric_receiving_delay_days,
  CASE
    WHEN d.expected_fabric_receiving_date IS NULL              THEN 'SEM_DATA_ESPERADA'
    WHEN d.real_fabric_receiving_date     IS NULL              THEN 'SEM_DATA_REAL'
    WHEN d.real_fabric_receiving_date > d.expected_fabric_receiving_date THEN 'ATRASADA'
    WHEN d.real_fabric_receiving_date < d.expected_fabric_receiving_date THEN 'ADIANTADA'
    ELSE                                                            'NO_PRAZO'
  END                                                          AS mp_receiving_status,
  d.real_fabric_receiving_date > d.expected_fabric_receiving_date
                                                               AS is_mp_late,

  -- ─── CAUSA RAIZ 3: Lead Time MP → Fim < 45 dias ──────────────────────────
  d.expected_production_delivery_date,
  d.mp_to_expected_end_lt_days,
  d.mp_to_expected_end_lt_days < 45                           AS is_lt_mp_to_end_lt_45d,

  -- ─── CAUSA RAIZ 2: Reprovação de Qualidade ───────────────────────────────
  q.audit_count,
  q.dt_first_audit_completed,
  q.first_audit_result_standardized,
  q.first_audit_deliberation_standardized,
  q.first_audit_result_standardized = 'qualita_rejected'      AS is_quality_rejected,
  q.total_quantity_audited,
  q.taxa_reprovacao_qualita,
  q.taxa_reprovacao_etapa,
  CASE
    WHEN q.first_audit_result_standardized IS NULL             THEN 'SEM_DADO_QUALIDADE'
    WHEN q.first_audit_result_standardized = 'qualita_approved' THEN 'APROVADA'
    WHEN q.first_audit_result_standardized = 'qualita_rejected'
     AND q.first_audit_deliberation_standardized = 'insider_approved' THEN 'REPROVADA_LIBERADA'
    WHEN q.first_audit_result_standardized = 'qualita_rejected'
     AND q.first_audit_deliberation_standardized = 'insider_rejected' THEN 'REPROVADA_REAUDIT'
    ELSE q.first_audit_result_standardized
  END                                                          AS quality_status,

  -- ─── Classificação Combinada de Causa Raiz ────────────────────────────────
  -- Hierarquia: múltiplas causas > causas duplas > causa única > sem causa
  CASE
    WHEN (d.real_fabric_receiving_date > d.expected_fabric_receiving_date)
     AND (q.first_audit_result_standardized = 'qualita_rejected')
     AND (d.mp_to_expected_end_lt_days < 45)                  THEN 'MULTIPLAS_CAUSAS'
    WHEN (d.real_fabric_receiving_date > d.expected_fabric_receiving_date)
     AND (q.first_audit_result_standardized = 'qualita_rejected') THEN 'ATRASO_MP_E_QUALIDADE'
    WHEN (d.real_fabric_receiving_date > d.expected_fabric_receiving_date)
     AND (d.mp_to_expected_end_lt_days < 45)                  THEN 'ATRASO_MP_E_LT_CURTO'
    WHEN (q.first_audit_result_standardized = 'qualita_rejected')
     AND (d.mp_to_expected_end_lt_days < 45)                  THEN 'QUALIDADE_E_LT_CURTO'
    WHEN d.real_fabric_receiving_date > d.expected_fabric_receiving_date THEN 'ATRASO_MP'
    WHEN q.first_audit_result_standardized = 'qualita_rejected' THEN 'REPROVACAO_QUALIDADE'
    WHEN d.mp_to_expected_end_lt_days < 45                     THEN 'LT_CURTO_MP_A_FIM'
    ELSE                                                            'SEM_CAUSA_IDENTIFICADA'
  END                                                          AS root_cause_category

FROM icp_detractors AS d
LEFT JOIN quality AS q
  ON q.op_code = d.op_code

ORDER BY
  d.dt_planned_entry_warehouse,
  days_overdue DESC,
  d.supplier_alias,
  d.op_code;
