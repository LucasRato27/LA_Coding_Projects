-- ============================================================
-- stock_health_abc_icp.sql
-- Correlação: Saúde de Estoque × Curva ABC × ICP por Produto
--
-- Parâmetro obrigatório:
--   {dia_ref}  → data de referência (YYYY-MM-DD) para filtrar
--                o snapshot de sop_gold.stock_health.
--                Exemplo: '2026-06-05'
--
-- Alternativa sem parâmetro (snapshot mais recente):
--   Substitua `sh.dia = '{dia_ref}'` por:
--   `sh.dia = (SELECT MAX(dia) FROM `insider-data-lake.sop_gold.stock_health`)`
--
-- Grão final: sku × dia  (~2.185 SKUs por snapshot)
-- ============================================================

WITH

-- ─── 1. Saúde de estoque + estado e product_name do SKU ─────
-- stock_health tem grão sku × dia. O join com integrated.skus
-- traz sku_state (para filtrar desativados) e product_name
-- (chave canônica para joins com ABC e ICP).
CTE_STOCK_HEALTH AS (
    SELECT
        sh.id_previsao,
        sh.dia,
        sh.isoweek,
        sh.mes,
        sh.sku,
        sh.nome_sku,
        sh.categoria,
        sh.produto_pai,
        sh.variante_cor,
        sh.estoque_passado,
        sh.estoque_projetado,
        sh.estoque_passado_ou_projetado,
        sh.estoque_passado_ou_projetado_d,
        sh.estoque_atual_d_vendas_l7d,
        sh.estoque_seguranca_d,
        sh.estoque_excesso_d,
        sh.stock_classification,
        sh.stock_classification_l7d_sales,
        sh.qtd_a_receber,
        sh.qtd_venda_prevista_diarizada,
        sh.qtd_venda_media_l7d,
        sh.sku_sales_average_price,
        sh.sku_production_cost,
        sh.receita_prevista_diarizada,
        sh.custo_de_estoque,
        sh.data_hora_atualizacao,
        sk.sku_state,
        sk.product_name,    -- chave de join para ABC e ICP
        sk.family,
        sk.gender
    FROM `insider-data-lake.sop_gold.stock_health` AS sh
    LEFT JOIN `insider-data-lake.integrated.skus` AS sk
        ON sk.sku = sh.sku
    WHERE sh.dia = '{dia_ref}'
      AND (sk.sku_state IS NULL OR sk.sku_state != 'desativado')
),

-- ─── 2a. Receita por produto — últimos 3 meses fechados ──────
-- Fonte canônica para curva ABC no projeto.
-- Janela: 3 meses completos anteriores ao mês corrente.
CTE_DPI AS (
    SELECT
        product_name,
        SUM(treated_generated_revenue) AS receita_3m
    FROM `insider-data-lake.sop_silver.demand_prediction_input`
    WHERE DATE(reference_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
      AND DATE(reference_date) <  DATE_TRUNC(CURRENT_DATE(), MONTH)
      AND product_name IS NOT NULL
    GROUP BY product_name
),

-- ─── 2b. Share acumulado de receita ──────────────────────────
CTE_REVENUE_SHARE AS (
    SELECT
        product_name,
        receita_3m,
        SUM(receita_3m) OVER ()                 AS total_receita,
        SUM(receita_3m) OVER (
            ORDER BY receita_3m DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                        AS cum_receita
    FROM CTE_DPI
),

-- ─── 2c. Curva ABC (thresholds: ≤80% = A, ≤95% = B, resto = C) ──
CTE_ABC_CURVE AS (
    SELECT
        product_name,
        receita_3m,
        SAFE_DIVIDE(cum_receita, total_receita)  AS cum_share,
        CASE
            WHEN SAFE_DIVIDE(cum_receita, total_receita) <= 0.80 THEN 'A'
            WHEN SAFE_DIVIDE(cum_receita, total_receita) <= 0.95 THEN 'B'
            ELSE                                                       'C'
        END                                      AS tag_abc
    FROM CTE_REVENUE_SHARE
),

-- ─── 3. OPs ativas — dedup para uma linha por op × product ───
-- supply_chain_efficiency_model_input tem grão op_code × product_sku.
-- planned_quantity_op / received_quantity_op são campos de OP,
-- repetidos para cada SKU. ANY_VALUE elimina duplicatas sem erro.
-- Filtros padrão: exclui flexible/converted, pending, canceled.
CTE_OPS_POR_PRODUTO AS (
    SELECT
        op_code,
        product_name,
        ANY_VALUE(supplier_name)         AS supplier_name,
        ANY_VALUE(supplier_id)           AS supplier_id,
        ANY_VALUE(planned_quantity_op)   AS planned_quantity_op,
        ANY_VALUE(received_quantity_op)  AS received_quantity_op
    FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
    WHERE production_order_type NOT IN ('flexible', 'converted')
      AND cycle_name IS NOT NULL
      AND (supplier_relationship_status IS NULL
           OR supplier_relationship_status NOT IN ('terminated', 'discontinued'))
      AND current_production_stage NOT IN ('pending', 'canceled')
    GROUP BY op_code, product_name
),

-- ─── 4. ICP médio ponderado por produto (carteira ativa) ─────
-- ICP = total_received / total_planned, ponderado por volume.
-- Inclui lista de fornecedores para rastreabilidade.
CTE_ICP_POR_PRODUTO AS (
    SELECT
        product_name,
        SAFE_DIVIDE(
            SUM(received_quantity_op),
            SUM(planned_quantity_op)
        )                                               AS avg_icp,
        SUM(planned_quantity_op)                        AS total_planned_icp,
        SUM(received_quantity_op)                       AS total_received_icp,
        COUNT(DISTINCT op_code)                         AS n_ops_ativas,
        ARRAY_TO_STRING(
            ARRAY_AGG(DISTINCT supplier_name IGNORE NULLS ORDER BY supplier_name),
            ' | '
        )                                               AS fornecedores
    FROM CTE_OPS_POR_PRODUTO
    GROUP BY product_name
)

-- ─── SELECT FINAL ────────────────────────────────────────────
-- Grão: sku × dia (herda de CTE_STOCK_HEALTH).
-- flag_ameaca_estoque e nivel_risco usam threshold padrão de 70%.
-- Para ajustar o threshold, use a função classify_risk() no notebook.
SELECT

    -- Identificação e tempo
    sh.dia,
    sh.isoweek,
    sh.mes,
    sh.id_previsao,
    sh.sku,
    sh.nome_sku,
    sh.product_name,
    sh.produto_pai,
    sh.categoria,
    sh.variante_cor,
    sh.family,
    sh.gender,
    sh.sku_state,

    -- Estoque
    sh.estoque_passado,
    sh.estoque_projetado,
    sh.estoque_passado_ou_projetado,

    -- Cobertura e classificação de saúde
    sh.estoque_passado_ou_projetado_d,
    sh.estoque_atual_d_vendas_l7d,
    sh.estoque_seguranca_d,
    sh.estoque_excesso_d,
    sh.stock_classification,
    sh.stock_classification_l7d_sales,

    -- Demanda e recebimentos
    sh.qtd_a_receber,
    sh.qtd_venda_prevista_diarizada,
    sh.qtd_venda_media_l7d,

    -- Financeiro
    sh.sku_sales_average_price,
    sh.sku_production_cost,
    sh.receita_prevista_diarizada,
    sh.custo_de_estoque,

    -- Curva ABC (nível produto — join via sk.product_name)
    abc.tag_abc,
    abc.cum_share                                        AS abc_cum_share,
    abc.receita_3m                                       AS receita_abc_3m,

    -- ICP por produto (carteira ativa — join via product_name)
    icp.avg_icp,
    icp.total_planned_icp,
    icp.total_received_icp,
    icp.n_ops_ativas,
    icp.fornecedores,

    -- Flag de ameaça de estoque (threshold padrão 70%)
    -- Recomputado no notebook por classify_risk() se necessário.
    CASE
        WHEN icp.avg_icp < 0.70
         AND abc.tag_abc IN ('A', 'B')
         AND (sh.sku_state IS NULL OR sh.sku_state != 'desativado')
        THEN TRUE
        ELSE FALSE
    END                                                  AS flag_ameaca_estoque,

    -- Nível de risco para priorização
    CASE
        WHEN icp.avg_icp IS NULL  AND abc.tag_abc IN ('A', 'B') THEN 'SEM_OP_ATIVA'
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'A'         THEN 'CRITICO'
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'B'         THEN 'ALTO'
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'C'         THEN 'MEDIO'
        WHEN icp.avg_icp >= 0.70  AND abc.tag_abc IN ('A', 'B') THEN 'MONITORAMENTO'
        ELSE 'OK'
    END                                                  AS nivel_risco,

    sh.data_hora_atualizacao

FROM      CTE_STOCK_HEALTH       AS sh
LEFT JOIN CTE_ABC_CURVE          AS abc ON abc.product_name = sh.product_name
LEFT JOIN CTE_ICP_POR_PRODUTO    AS icp ON icp.product_name = sh.product_name

ORDER BY
    CASE
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'A'         THEN 1
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'B'         THEN 2
        WHEN icp.avg_icp IS NULL  AND abc.tag_abc IN ('A', 'B') THEN 3
        WHEN icp.avg_icp < 0.70   AND abc.tag_abc = 'C'         THEN 4
        ELSE 5
    END,
    abc.tag_abc,
    icp.avg_icp
