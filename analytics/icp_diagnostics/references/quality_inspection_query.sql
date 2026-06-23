-- ============================================================
-- quality_inspection_query.sql
-- Dados de auditoria de qualidade para OPs do cohort de 45 dias
--
-- Parâmetro obrigatório:
--   {reference_date}  → data de referência (D-1), formato YYYY-MM-DD
--                       Exemplo: '2026-06-07'
--
-- Grão de saída: uma linha por op_code do cohort (com ou sem dado de qualidade).
-- OPs sem registro em quality_inspection_data aparecem com is_blocked = FALSE.
-- ============================================================

WITH

-- ─── Parâmetro central ───────────────────────────────────────────────────────
params AS (
    SELECT DATE '{reference_date}' AS reference_date
),

-- ─── OPs do cohort de 45 dias ────────────────────────────────────────────────
cohort_ops AS (
    SELECT
        sc.op_code,
        ANY_VALUE(sc.supplier_name) AS supplier_name,
        ANY_VALUE(sc.product_name)  AS product_name
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
    GROUP BY sc.op_code
)

-- ─── Output: cohort × qualidade (LEFT JOIN preserva OPs sem dado de qualidade) ─
SELECT
    co.op_code,
    co.supplier_name,
    co.product_name,

    -- Mapeamento: colunas reais → aliases do spec
    CAST(q.first_audit_defective_rate           AS FLOAT64)  AS first_audit_defective_rate,
    CAST(q.audit_count                          AS INT64)    AS audit_count,
    CAST(q.pieces_rejected_in_first_audit_insider AS INT64)  AS pieces_rejected_insider,
    CAST(q.dt_first_audit_completed             AS DATE)     AS last_audit_date,
    q.first_audit_deliberation_standardized                  AS deliberation_status,

    -- is_blocked = reprovada pela Qualita E sem liberação da Insider
    COALESCE(
        q.first_audit_result_standardized = 'qualita_rejected'
        AND COALESCE(q.first_audit_deliberation_standardized, 'insider_no_deliberation')
            != 'insider_approved',
        FALSE
    )                                                        AS is_blocked

FROM cohort_ops AS co
LEFT JOIN `insider-data-lake.sop_gold.quality_inspection_data` AS q
    ON q.op_code = co.op_code
ORDER BY co.op_code
