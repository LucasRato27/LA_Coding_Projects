-- ============================================================
-- icp_detractors_root_cause.sql
-- OPs detratoras do cohort de 45 dias com classificação de causa-raiz
--
-- Parâmetro obrigatório:
--   {reference_date}  → data de referência (D-1), formato YYYY-MM-DD
--                       Exemplo: '2026-06-07'
--
-- A skill substitui {reference_date} por string antes de executar:
--   sql.format(reference_date='2026-06-07')
--
-- Grão de saída: uma linha por op_code detratora.
-- ============================================================

WITH

-- ─── Parâmetro central ───────────────────────────────────────────────────────
params AS (
    SELECT DATE '{reference_date}' AS reference_date
),

-- ─── 1. Cohort de 45 dias — filtros padrão ICP ──────────────────────────────
cohort AS (
    SELECT
        sc.op_code,
        sc.supplier_name,
        sc.product_name,
        sc.planned_quantity_op,
        sc.received_quantity_op,
        sc.dt_planned_entry_warehouse,
        sc.dt_largest_entry_warehouse,
        sc.current_production_stage
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc
    CROSS JOIN params AS p
    WHERE sc.current_production_stage NOT IN (
              'canceled', 'order_request_validation', 'pending'
          )
      AND sc.production_order_type = 'committed'
      AND sc.op_code != 'OPF33N68'
      AND sc.product_name NOT IN (
          'Tech T-shirt Gola V Defeitos Leves Masculino',
          'Daily T-shirt Defeitos Leves Masculino',
          'Tech T-shirt Gola U Defeitos Leves Masculino'
      )
      AND sc.dt_planned_entry_warehouse >  DATE_SUB(p.reference_date, INTERVAL 45 DAY)
      AND sc.dt_planned_entry_warehouse <= p.reference_date
),

-- ─── 2. Deduplicar: grão OP×SKU → grão OP ───────────────────────────────────
-- planned_quantity_op e received_quantity_op são repetidos em cada linha de SKU;
-- usar MAX() garante o valor correto sem somar duplicatas.
cohort_dedup AS (
    SELECT
        op_code,
        ANY_VALUE(supplier_name)               AS supplier_name,
        ANY_VALUE(product_name)                AS product_name,
        MAX(planned_quantity_op)               AS planned_quantity_op,
        MAX(received_quantity_op)              AS received_quantity_op,
        ANY_VALUE(dt_planned_entry_warehouse)  AS dt_planned_entry_warehouse,
        MAX(dt_largest_entry_warehouse)        AS dt_largest_entry_warehouse,
        ANY_VALUE(current_production_stage)    AS current_production_stage
    FROM cohort
    GROUP BY op_code
),

-- ─── 3. Filtrar OPs detratoras ───────────────────────────────────────────────
-- Detratora = não recebida (NULL) OU recebida após a data de referência.
detractors AS (
    SELECT
        cd.*,
        p.reference_date
    FROM cohort_dedup AS cd
    CROSS JOIN params AS p
    WHERE cd.dt_largest_entry_warehouse IS NULL
       OR cd.dt_largest_entry_warehouse > p.reference_date
),

-- ─── 4. Dados de qualidade ───────────────────────────────────────────────────
quality AS (
    SELECT
        op_code,
        first_audit_result_standardized,
        first_audit_deliberation_standardized,
        first_audit_defective_rate
    FROM `insider-data-lake.sop_gold.quality_inspection_data`
)

-- ─── 5. Output final — uma linha por OP detratora ───────────────────────────
SELECT
    d.op_code,
    d.supplier_name,
    d.product_name,
    d.planned_quantity_op,
    COALESCE(d.received_quantity_op, 0)                                        AS received_quantity_op,
    d.planned_quantity_op - COALESCE(d.received_quantity_op, 0)                AS gap_quantity,
    d.dt_planned_entry_warehouse,
    DATE_DIFF(d.reference_date, d.dt_planned_entry_warehouse, DAY)             AS days_overdue,
    d.current_production_stage,
    d.dt_largest_entry_warehouse,

    -- ── Classificação de causa-raiz (ordem de prioridade) ─────────────────────
    CASE
        -- 1. Reprovação de qualidade: em auditoria OU reprovado sem liberação da Insider
        WHEN d.current_production_stage = 'quality_inspection'
          OR (
              q.first_audit_result_standardized = 'qualita_rejected'
              AND COALESCE(q.first_audit_deliberation_standardized, 'insider_no_deliberation')
                  != 'insider_approved'
             )
            THEN 'REPROVACAO_QUALIDADE'

        -- 2. Atraso confirmado de matéria-prima
        WHEN d.current_production_stage IN ('raw_material_receiving', 'raw_material_pending')
            THEN 'ATRASO_MP_CONFIRMADO'

        -- 3. Produção concluída ou em faturamento — bloqueio de NF/fiscal
        WHEN d.current_production_stage IN ('finished', 'items_delivery_and_invoicing')
             AND d.dt_largest_entry_warehouse IS NULL
            THEN 'ATRASO_FATURAMENTO_NF'

        -- 4. Atraso de produção — OP vencida (days_overdue > 0)
        WHEN d.current_production_stage = 'cut_fabric_and_sewing_process'
             AND DATE_DIFF(d.reference_date, d.dt_planned_entry_warehouse, DAY) > 0
            THEN 'ATRASO_PRODUCAO'

        -- 5. Em corte/costura mas ainda dentro do prazo
        WHEN d.current_production_stage = 'cut_fabric_and_sewing_process'
            THEN 'ATRASO_PRODUCAO_DENTRO_PRAZO'

        -- 6. Nenhuma condição anterior se aplica
        ELSE 'SEM_CAUSA_IDENTIFICADA'
    END AS root_cause_category,

    -- ── Flags de qualidade ────────────────────────────────────────────────────
    (
        q.first_audit_result_standardized = 'qualita_rejected'
        AND COALESCE(q.first_audit_deliberation_standardized, 'insider_no_deliberation')
            != 'insider_approved'
    )                                                                           AS quality_flag,
    CAST(q.first_audit_defective_rate AS FLOAT64)                              AS quality_defective_rate

FROM detractors AS d
LEFT JOIN quality AS q ON q.op_code = d.op_code
ORDER BY gap_quantity DESC
