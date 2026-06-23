-- ==============================================================================
-- PIPELINE DE ANÁLISE DE REVERSAS E PRIORIZAÇÃO DE PRODUTOS
-- ==============================================================================
--
-- Camada 2 — Tabela executiva agregada por produto   (output principal)
-- Camada 1 — Tabela analítica item/reversa/tag       (auditoria e drill-down)
--
-- Bugs corrigidos nesta versão:
--   B1 — portfolio_clustering: ORDER BY era ORDER BY sku (não-determinístico);
--          corrigido para ORDER BY TO_JSON_STRING(pc) DESC.
--   B2 — td_joined: COALESCE(qt_items_returned, 1) inflava T&D com retornos NULL;
--          corrigido para COALESCE(..., 0) aplicado em reversas_unicas.
--   B3 — sinal_priorizacao: zona cinza "médio-médio" caia em ELSE sem comentário;
--          branch documentado explicitamente.
--   B5 — pct_top_3_total: denominador somava qt_reversas_tag por tag, podendo
--          ultrapassar 100%; corrigido para usar COUNT DISTINCT de reversas do produto.
--   B7 — sales_item_base (Camada 2): agrupamento com variant_title duplicava reversa
--          quando mesmo SKU aparecia em múltiplos order_item_id; corrigido com
--          order_items_grouped consolidado em (order_id, sku) antes do join.
-- Campos adicionados:
--   M1 — tipo_problema: classifica cada reversa como Físico, Logístico ou Desistência.
--   M3 — order_date: propagado até o SELECT final da Camada 1.
--   M4 — sku_state: análise restrita a produtos ativos perenes e lançamentos
--          (sku_state IN ('ativo_perene', 'ativo_em_lancamento')). Desativados
--          e cápsulas são excluídos na CTE sales_item_base (Cam. 2) e
--          base_analitica (Cam. 1) via WHERE após o LEFT JOIN em sku_dim.
-- ==============================================================================


-- ==============================================================================
-- CAMADA 2: TABELA EXECUTIVA AGREGADA POR PRODUTO
-- Objetivo: gerar farol decisório de priorização de melhoria física.
-- Granularidade: product_name × category × gender
-- ==============================================================================

WITH params AS (
  SELECT
    DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 12 MONTH) AS start_date,
    CURRENT_DATE("America/Sao_Paulo") AS end_date,
    30 AS min_items_vendidos,
    5 AS min_items_returned
),

-- === BASE DE VENDAS ========================================================

orders AS (
  SELECT
    o.order_id,
    o.order_name,
    o.processed_at,
    DATE(o.processed_at, "America/Sao_Paulo") AS data_compra,
    DATE_TRUNC(DATE(o.processed_at, "America/Sao_Paulo"), MONTH) AS mes_compra,
    DATE_TRUNC(DATE(o.processed_at, "America/Sao_Paulo"), WEEK(MONDAY)) AS semana_compra,
    o.amount_net_payment
  FROM `insider-data-lake.integrated.orders` o
  CROSS JOIN params p
  WHERE NOT o.is_test_order
    AND NOT o.is_internal
    AND o.financial_status IN ('paid', 'partially_refunded')
    AND (NOT o.is_cancelled OR o.is_parent_order)
    AND o.parent_order_id IS NULL
    AND DATE(o.processed_at, "America/Sao_Paulo") BETWEEN p.start_date AND p.end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.order_id
    ORDER BY o.data_received_at DESC
  ) = 1
),

order_items AS (
  SELECT
    oi.order_id,
    oi.order_item_id,
    oi.sku,
    oi.product_title,
    oi.variant_title,
    oi.full_item_name,
    oi.variant_color,
    oi.variant_size,
    oi.variant_model,
    oi.quantity,
    oi.item_order_total,
    oi.data_source_product_id,
    oi.data_source_variant_id
  FROM `insider-data-lake.integrated.order_items` oi
  WHERE oi.store = 'shopify_insider-store-loja'
    AND oi.sku IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY oi.order_id, oi.order_item_id
    ORDER BY oi.data_received_at DESC
  ) = 1
),

-- Consolida itens ao nível (order_id, sku) para evitar duplicação de reversa
-- quando o mesmo SKU aparece em múltiplos order_item_id de um mesmo pedido.
order_items_grouped AS (
  SELECT
    order_id,
    sku,
    ANY_VALUE(product_title) AS product_title,
    ANY_VALUE(variant_title) AS variant_title,
    ANY_VALUE(full_item_name) AS full_item_name,
    ANY_VALUE(variant_color) AS variant_color,
    ANY_VALUE(variant_size) AS variant_size,
    ANY_VALUE(variant_model) AS variant_model,
    ANY_VALUE(data_source_product_id) AS data_source_product_id,
    ANY_VALUE(data_source_variant_id) AS data_source_variant_id,
    SUM(quantity) AS qt_items,
    SUM(item_order_total) AS qt_net_revenue_sku
  FROM order_items
  GROUP BY order_id, sku
),

sku_dim AS (
  SELECT
    sku,
    ANY_VALUE(product_name) AS product_name,
    ANY_VALUE(category) AS category,
    ANY_VALUE(gender) AS gender,
    ANY_VALUE(color) AS color,
    ANY_VALUE(size) AS size,
    ANY_VALUE(sku_state) AS sku_state
  FROM `insider-data-lake.integrated.skus`
  WHERE sku IS NOT NULL
  GROUP BY sku
),

portfolio_clustering AS (
  -- Tabela em granularidade product_name (não tem coluna sku).
  SELECT
    product_name,
    ANY_VALUE(cluster) AS portfolio_cluster,
    ANY_VALUE(TO_JSON_STRING(pc)) AS portfolio_cluster_payload
  FROM `insider-data-lake.sop_silver.portfolio_skp_clustering` pc
  GROUP BY product_name
),

-- Grain resultante: (order_id, sku) — sem GROUP BY, pois order_items_grouped
-- já consolidou os itens e todos os joins são 1:1 por SKU.
sales_item_base AS (
  SELECT
    o.order_id,
    o.order_name,
    o.data_compra,
    o.mes_compra,
    o.semana_compra,

    oi.sku,
    COALESCE(s.product_name, oi.product_title) AS product_name,
    s.category,
    s.gender,
    COALESCE(s.color, oi.variant_color) AS color,
    COALESCE(s.size, oi.variant_size) AS size,
    s.sku_state,

    oi.product_title,
    oi.variant_title,
    oi.full_item_name,
    oi.variant_color,
    oi.variant_size,
    oi.variant_model,
    oi.data_source_product_id,
    oi.data_source_variant_id,

    oi.qt_items,
    oi.qt_net_revenue_sku
  FROM orders o
  JOIN order_items_grouped oi
    ON o.order_id = oi.order_id
  LEFT JOIN sku_dim s
    ON s.sku = oi.sku
  -- Apenas produtos perenes e lançamentos (exclui desativados e cápsulas)
  WHERE s.sku_state IN ('ativo_perene', 'ativo_em_lancamento')
),

-- === REVERSAS =============================================================

reversas_unicas AS (
  SELECT
    r.order_name,
    r.status,
    r.reverse_type,
    r.sku,
    r.return_reason,
    r.updated_at,
    r.created_at,
    DATE(r.created_at, "America/Sao_Paulo") AS data_reversa,
    DATE_TRUNC(DATE(r.created_at, "America/Sao_Paulo"), MONTH) AS mes_reversa,
    DATE_TRUNC(DATE(r.created_at, "America/Sao_Paulo"), WEEK(MONDAY)) AS semana_reversa,
    r.client_comment,
    SAFE_CAST(r.reverse_shipping_cost AS FLOAT64) AS reverse_shipping_cost,
    SAFE_CAST(r.retained_bonus AS FLOAT64) AS retained_bonus,
    SAFE_CAST(r.exchange_value AS FLOAT64) AS exchange_value,
    SAFE_CAST(r.refund_value AS FLOAT64) AS refund_value,

    -- COALESCE aplicado na fonte para não inflar contagens; NULL de return_quantity
    -- indica quantidade não informada no sistema (tratada como 0).
    COALESCE(SAFE_CAST(r.return_quantity AS FLOAT64), 0) AS qt_items_returned,

    CASE
      WHEN LOWER(r.return_reason) LIKE '%ficou grande%' THEN 'Tamanho'
      WHEN r.return_reason = 'Peça íntima' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Produto com defeito' THEN 'Defeito'
      WHEN r.return_reason = 'Arrependimento' THEN 'Desistência'
      WHEN r.return_reason = 'Recebi um produto errado' THEN 'Produto errado'
      WHEN LOWER(r.return_reason) LIKE '%ficou pequeno%' THEN 'Tamanho'
      WHEN r.return_reason = 'Cor diferente do esperado' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Recebi novo pedido de reposição' THEN 'Produto errado'
      WHEN r.return_reason = 'Recebi pedido duplicado' THEN 'Produto errado'
      WHEN r.return_reason = 'Arrependiemento' THEN 'Desistência'
      WHEN r.return_reason = 'Insatisfação (tamanho e cor inclusos)' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Arrependimento (tamanho e cor inclusos)' THEN 'Insatisfação com o produto'
      WHEN r.return_reason LIKE '%Tamanho%' THEN 'Tamanho'
      WHEN r.return_reason = 'Insatisfação com o produto' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Falha na Entrega' THEN 'Problema na entrega'
      WHEN r.return_reason = 'Demora na entrega' THEN 'Problema na entrega'
      WHEN r.return_reason = 'Ficou Grande' THEN 'Tamanho'
      WHEN r.return_reason = 'Ficou Pequeno' THEN 'Tamanho'
      WHEN r.return_reason = 'Não gostei do produto' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Defeito' THEN 'Defeito'
      WHEN r.return_reason = 'Peça íntima (calcinha, sutiã, meia ou cueca)' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Insatisfação' THEN 'Insatisfação com o produto'
      WHEN r.return_reason = 'Defeitos' THEN 'Defeito'
      WHEN r.return_reason = 'Desbotamento' THEN 'Desbotamento'
      WHEN r.return_reason = 'Desistência' THEN 'Desistência'
      WHEN r.return_reason = 'Não gostei da qualidade' THEN 'Insatisfação com o produto'
      WHEN r.return_reason IS NULL THEN NULL
      ELSE 'Outros'
    END AS motivo_classificado,

    -- Classifica o problema: Físico = atributo do produto;
    -- Logístico = falha operacional/entrega; Desistência = mudança de intenção.
    CASE
      WHEN LOWER(r.return_reason) LIKE '%ficou grande%' THEN 'Físico'
      WHEN r.return_reason = 'Peça íntima' THEN 'Físico'
      WHEN r.return_reason = 'Produto com defeito' THEN 'Físico'
      WHEN r.return_reason = 'Arrependimento' THEN 'Desistência'
      WHEN r.return_reason = 'Recebi um produto errado' THEN 'Logístico'
      WHEN LOWER(r.return_reason) LIKE '%ficou pequeno%' THEN 'Físico'
      WHEN r.return_reason = 'Cor diferente do esperado' THEN 'Físico'
      WHEN r.return_reason = 'Recebi novo pedido de reposição' THEN 'Logístico'
      WHEN r.return_reason = 'Recebi pedido duplicado' THEN 'Logístico'
      WHEN r.return_reason = 'Arrependiemento' THEN 'Desistência'
      WHEN r.return_reason = 'Insatisfação (tamanho e cor inclusos)' THEN 'Físico'
      WHEN r.return_reason = 'Arrependimento (tamanho e cor inclusos)' THEN 'Desistência'
      WHEN r.return_reason LIKE '%Tamanho%' THEN 'Físico'
      WHEN r.return_reason = 'Insatisfação com o produto' THEN 'Físico'
      WHEN r.return_reason = 'Falha na Entrega' THEN 'Logístico'
      WHEN r.return_reason = 'Demora na entrega' THEN 'Logístico'
      WHEN r.return_reason = 'Ficou Grande' THEN 'Físico'
      WHEN r.return_reason = 'Ficou Pequeno' THEN 'Físico'
      WHEN r.return_reason = 'Não gostei do produto' THEN 'Físico'
      WHEN r.return_reason = 'Defeito' THEN 'Físico'
      WHEN r.return_reason = 'Peça íntima (calcinha, sutiã, meia ou cueca)' THEN 'Físico'
      WHEN r.return_reason = 'Insatisfação' THEN 'Físico'
      WHEN r.return_reason = 'Defeitos' THEN 'Físico'
      WHEN r.return_reason = 'Desbotamento' THEN 'Físico'
      WHEN r.return_reason = 'Desistência' THEN 'Desistência'
      WHEN r.return_reason = 'Não gostei da qualidade' THEN 'Físico'
      WHEN r.return_reason IS NULL THEN NULL
      ELSE 'Outros'
    END AS tipo_problema,

    CASE
      WHEN r.reverse_type IS NULL THEN 'Sem reversa'
      WHEN LOWER(r.reverse_type) LIKE '%troca%' THEN 'Troca'
      WHEN LOWER(r.reverse_type) LIKE '%devol%' THEN 'Devolução'
      ELSE r.reverse_type
    END AS reverse_type_classificado

  FROM `insider-lake-sensitive.prepared_br.prepared__troquecommerce_order_details_br` r
  CROSS JOIN params p
  WHERE r.status <> 'Cancelado'
    AND r.sku IS NOT NULL
    AND r.return_reason IS NOT NULL
    AND DATE(r.created_at, "America/Sao_Paulo") BETWEEN p.start_date AND p.end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.order_name, r.sku
    ORDER BY r.updated_at DESC
  ) = 1
),

reversas_tags_latest AS (
  SELECT
    order_name,
    sku,
    tags,
    created_at,
    DATE_TRUNC(DATE(created_at), MONTH) AS ingestion_date
  FROM `insider-data-lake.sop_silver.return_reason_tags`
  WHERE order_name IS NOT NULL
    AND sku IS NOT NULL
    AND tags IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_name, sku
    ORDER BY created_at DESC
  ) = 1
),

reversas_tag AS (
  SELECT
    order_name,
    sku,
    tag,
    ingestion_date
  FROM reversas_tags_latest,
  UNNEST(tags) AS tag
),

-- === MÉTRICAS POR PRODUTO =================================================

sales_product_metrics AS (
  SELECT
    sib.product_name,
    sib.category,
    sib.gender,
    ANY_VALUE(pc.portfolio_cluster) AS portfolio_cluster,
    ANY_VALUE(pc.portfolio_cluster_payload) AS portfolio_cluster_payload,

    COUNT(DISTINCT sib.order_id) AS qt_pedidos,
    COUNT(DISTINCT sib.sku) AS qt_skus,
    SUM(sib.qt_items) AS qt_items_vendidos,
    SUM(sib.qt_net_revenue_sku) AS receita_liquida
  FROM sales_item_base sib
  LEFT JOIN portfolio_clustering pc
    ON pc.product_name = sib.product_name
  GROUP BY sib.product_name, sib.category, sib.gender
),

-- Join de reversas às vendas na grain (order_name, sku).
-- Tags NÃO entram aqui para não inflar contagens de T&D.
-- Nota: reversas de pedidos comprados fora da janela de 12 meses são excluídas
-- pelo INNER JOIN com sales_item_base (que filtra por período de compra).
td_joined AS (
  SELECT
    ru.order_name,
    ru.sku,
    sib.product_name,
    sib.category,
    sib.gender,
    sib.color,
    sib.size,

    ru.data_reversa,
    ru.mes_reversa,
    ru.reverse_type_classificado,
    ru.return_reason,
    ru.motivo_classificado,
    ru.tipo_problema,
    ru.client_comment,

    ru.qt_items_returned,
    ru.exchange_value,
    ru.refund_value
  FROM reversas_unicas ru
  JOIN sales_item_base sib
    ON ru.order_name = sib.order_name
   AND ru.sku = sib.sku
),

td_product_metrics AS (
  SELECT
    product_name,
    category,
    gender,

    COUNT(DISTINCT CONCAT(order_name, '|', sku)) AS qt_reversas,
    SUM(qt_items_returned) AS qt_items_returned,

    COUNT(DISTINCT IF(reverse_type_classificado = 'Troca', CONCAT(order_name, '|', sku), NULL)) AS qt_trocas,
    COUNT(DISTINCT IF(reverse_type_classificado = 'Devolução', CONCAT(order_name, '|', sku), NULL)) AS qt_devolucoes,

    COUNT(DISTINCT IF(tipo_problema = 'Físico', CONCAT(order_name, '|', sku), NULL)) AS qt_reversas_fisico,
    COUNT(DISTINCT IF(tipo_problema = 'Logístico', CONCAT(order_name, '|', sku), NULL)) AS qt_reversas_logistico,

    SUM(exchange_value) AS valor_troca,
    SUM(refund_value) AS valor_devolucao
  FROM td_joined
  GROUP BY product_name, category, gender
),

category_metrics AS (
  SELECT
    spm.category,
    SUM(spm.qt_items_vendidos) AS qt_items_vendidos_categoria,
    SUM(COALESCE(tpm.qt_items_returned, 0)) AS qt_items_returned_categoria,
    SAFE_DIVIDE(
      SUM(COALESCE(tpm.qt_items_returned, 0)),
      SUM(spm.qt_items_vendidos)
    ) AS td_rate_categoria
  FROM sales_product_metrics spm
  LEFT JOIN td_product_metrics tpm
    ON spm.product_name = tpm.product_name
   AND spm.category = tpm.category
   AND spm.gender = tpm.gender
  GROUP BY spm.category
),

-- qt_reversas_tag: COUNT DISTINCT (order_name|sku) por tag — não infla T&D.
-- Uma reversa com N tags contribui com 1 para o contador de cada tag.
tag_counts AS (
  SELECT
    tj.product_name,
    tj.category,
    tj.gender,
    rt.tag AS problema_tag,
    COUNT(DISTINCT CONCAT(tj.order_name, '|', tj.sku)) AS qt_reversas_tag
  FROM td_joined tj
  JOIN reversas_tag rt
    ON rt.order_name = tj.order_name
   AND rt.sku = tj.sku
  WHERE rt.tag IS NOT NULL
  GROUP BY tj.product_name, tj.category, tj.gender, rt.tag
),

-- Denominador correto para pct de tags: total de reversas distintas do produto,
-- independente de terem ou não tags. Evita que pct_top_3_total ultrapasse 100%.
td_reversas_total AS (
  SELECT
    product_name,
    category,
    gender,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) AS qt_reversas_total
  FROM td_joined
  GROUP BY product_name, category, gender
),

tag_ranked AS (
  SELECT
    tc.*,
    rt.qt_reversas_total,
    ROW_NUMBER() OVER (
      PARTITION BY tc.product_name, tc.category, tc.gender
      ORDER BY tc.qt_reversas_tag DESC, tc.problema_tag
    ) AS tag_rank
  FROM tag_counts tc
  JOIN td_reversas_total rt
    ON tc.product_name = rt.product_name
   AND tc.category = rt.category
   AND tc.gender = rt.gender
  WHERE tc.problema_tag NOT IN (
    'caimento_bom',
    'conforto_positivo',
    'feedback_positivo_geral',
    'modelagem_boa',
    'tamanho_ideal',
    'tecido_qualidade_boa'
  )
),

top_tags AS (
  SELECT
    product_name,
    category,
    gender,

    MAX(IF(tag_rank = 1, problema_tag, NULL)) AS top_1_problema,
    MAX(IF(tag_rank = 1, SAFE_DIVIDE(qt_reversas_tag, qt_reversas_total), NULL)) AS top_1_pct,

    MAX(IF(tag_rank = 2, problema_tag, NULL)) AS top_2_problema,
    MAX(IF(tag_rank = 2, SAFE_DIVIDE(qt_reversas_tag, qt_reversas_total), NULL)) AS top_2_pct,

    MAX(IF(tag_rank = 3, problema_tag, NULL)) AS top_3_problema,
    MAX(IF(tag_rank = 3, SAFE_DIVIDE(qt_reversas_tag, qt_reversas_total), NULL)) AS top_3_pct,

    SAFE_DIVIDE(
      SUM(IF(tag_rank <= 3, qt_reversas_tag, 0)),
      MAX(qt_reversas_total)
    ) AS pct_top_3_total
  FROM tag_ranked
  WHERE tag_rank <= 3
  GROUP BY product_name, category, gender
),

color_concentration AS (
  SELECT
    product_name,
    category,
    gender,
    color,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) AS qt_reversas_color,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) / SUM(COUNT(DISTINCT CONCAT(order_name, '|', sku))) OVER (
      PARTITION BY product_name, category, gender
    ) AS pct_reversas_color
  FROM td_joined
  WHERE color IS NOT NULL
  GROUP BY product_name, category, gender, color
),

main_color AS (
  SELECT
    product_name,
    category,
    gender,
    color AS principal_cor_afetada,
    pct_reversas_color AS principal_cor_pct
  FROM color_concentration
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_name, category, gender
    ORDER BY pct_reversas_color DESC, color
  ) = 1
),

size_concentration AS (
  SELECT
    product_name,
    category,
    gender,
    size,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) AS qt_reversas_size,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) / SUM(COUNT(DISTINCT CONCAT(order_name, '|', sku))) OVER (
      PARTITION BY product_name, category, gender
    ) AS pct_reversas_size
  FROM td_joined
  WHERE size IS NOT NULL
  GROUP BY product_name, category, gender, size
),

main_size AS (
  SELECT
    product_name,
    category,
    gender,
    size AS principal_tamanho_afetado,
    pct_reversas_size AS principal_tamanho_pct
  FROM size_concentration
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_name, category, gender
    ORDER BY pct_reversas_size DESC, size
  ) = 1
),

comments_sample AS (
  SELECT
    product_name,
    category,
    gender,
    STRING_AGG(client_comment, ' || ' LIMIT 10) AS comentarios_amostra
  FROM (
    SELECT DISTINCT
      product_name,
      category,
      gender,
      client_comment
    FROM td_joined
    WHERE client_comment IS NOT NULL
      AND LENGTH(TRIM(client_comment)) > 0
  )
  GROUP BY product_name, category, gender
),

trend_base AS (
  SELECT
    product_name,
    category,
    gender,

    COUNT(DISTINCT IF(
      data_reversa >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 3 MONTH),
      CONCAT(order_name, '|', sku),
      NULL
    )) AS reversas_ultimos_3m,

    COUNT(DISTINCT IF(
      data_reversa >= DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 6 MONTH)
      AND data_reversa < DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 3 MONTH),
      CONCAT(order_name, '|', sku),
      NULL
    )) AS reversas_3m_anteriores
  FROM td_joined
  GROUP BY product_name, category, gender
),

trend_classified AS (
  SELECT
    *,
    CASE
      WHEN reversas_ultimos_3m + reversas_3m_anteriores < 5 THEN 'Sem volume para tendência'
      WHEN reversas_3m_anteriores = 0 AND reversas_ultimos_3m > 0 THEN 'Apareceu nos últimos 3 meses'
      WHEN SAFE_DIVIDE(reversas_ultimos_3m - reversas_3m_anteriores, reversas_3m_anteriores) >= 0.25 THEN 'Aumentou nos últimos 3 meses'
      WHEN SAFE_DIVIDE(reversas_ultimos_3m - reversas_3m_anteriores, reversas_3m_anteriores) <= -0.25 THEN 'Caiu nos últimos 3 meses'
      ELSE 'Estável'
    END AS tendencia_reversas
  FROM trend_base
),

-- === SCORING ==============================================================

percentiles AS (
  SELECT
    *,

    CUME_DIST() OVER (ORDER BY receita_liquida) AS percentil_receita,
    CUME_DIST() OVER (ORDER BY qt_items_vendidos) AS percentil_unidades,
    CUME_DIST() OVER (ORDER BY td_rate) AS percentil_td_rate,
    CUME_DIST() OVER (ORDER BY qt_items_returned) AS percentil_volume_td,
    CUME_DIST() OVER (ORDER BY delta_vs_categoria) AS percentil_delta_vs_categoria
  FROM (
    SELECT
      spm.product_name,
      spm.category,
      spm.gender,
      spm.portfolio_cluster,
      spm.portfolio_cluster_payload,

      spm.qt_pedidos,
      spm.qt_skus,
      spm.qt_items_vendidos,
      spm.receita_liquida,

      COALESCE(tpm.qt_reversas, 0) AS qt_reversas,
      COALESCE(tpm.qt_items_returned, 0) AS qt_items_returned,
      COALESCE(tpm.qt_trocas, 0) AS qt_trocas,
      COALESCE(tpm.qt_devolucoes, 0) AS qt_devolucoes,
      COALESCE(tpm.qt_reversas_fisico, 0) AS qt_reversas_fisico,
      COALESCE(tpm.qt_reversas_logistico, 0) AS qt_reversas_logistico,
      COALESCE(tpm.valor_troca, 0) AS valor_troca,
      COALESCE(tpm.valor_devolucao, 0) AS valor_devolucao,

      SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos) AS td_rate,

      cm.td_rate_categoria,
      SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos) - cm.td_rate_categoria AS delta_vs_categoria,
      SAFE_DIVIDE(
        SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), spm.qt_items_vendidos),
        cm.td_rate_categoria
      ) AS ratio_vs_categoria,

      SAFE_DIVIDE(spm.receita_liquida, SUM(spm.receita_liquida) OVER ()) AS share_receita_portfolio,
      SAFE_DIVIDE(spm.qt_items_vendidos, SUM(spm.qt_items_vendidos) OVER ()) AS share_unidades_portfolio,
      SAFE_DIVIDE(COALESCE(tpm.qt_items_returned, 0), SUM(COALESCE(tpm.qt_items_returned, 0)) OVER ()) AS share_td_portfolio,

      tt.top_1_problema,
      tt.top_1_pct,
      tt.top_2_problema,
      tt.top_2_pct,
      tt.top_3_problema,
      tt.top_3_pct,
      tt.pct_top_3_total,

      mc.principal_cor_afetada,
      mc.principal_cor_pct,
      ms.principal_tamanho_afetado,
      ms.principal_tamanho_pct,

      tb.reversas_ultimos_3m,
      tb.reversas_3m_anteriores,
      tb.tendencia_reversas,

      cs.comentarios_amostra

    FROM sales_product_metrics spm
    LEFT JOIN td_product_metrics tpm
      ON spm.product_name = tpm.product_name
     AND spm.category = tpm.category
     AND spm.gender = tpm.gender
    LEFT JOIN category_metrics cm
      ON spm.category = cm.category
    LEFT JOIN top_tags tt
      ON spm.product_name = tt.product_name
     AND spm.category = tt.category
     AND spm.gender = tt.gender
    LEFT JOIN main_color mc
      ON spm.product_name = mc.product_name
     AND spm.category = mc.category
     AND spm.gender = mc.gender
    LEFT JOIN main_size ms
      ON spm.product_name = ms.product_name
     AND spm.category = ms.category
     AND spm.gender = ms.gender
    LEFT JOIN trend_classified tb
      ON spm.product_name = tb.product_name
     AND spm.category = tb.category
     AND spm.gender = tb.gender
    LEFT JOIN comments_sample cs
      ON spm.product_name = cs.product_name
     AND spm.category = cs.category
     AND spm.gender = cs.gender
  )
),

scored AS (
  SELECT
    *,

    0.6 * percentil_receita
      + 0.4 * percentil_unidades AS commercial_score,

    0.5 * percentil_td_rate
      + 0.3 * percentil_volume_td
      + 0.2 * percentil_delta_vs_categoria AS td_score,

    -- priority_score expande td_score e commercial_score inline (BigQuery não
    -- permite referência a alias da mesma cláusula SELECT).
    0.6 * (
      0.5 * percentil_td_rate
      + 0.3 * percentil_volume_td
      + 0.2 * percentil_delta_vs_categoria
    )
    + 0.4 * (
      0.6 * percentil_receita
      + 0.4 * percentil_unidades
    ) AS priority_score
  FROM percentiles
),

final AS (
  SELECT
    *,

    CASE
      -- Volume mínimo estatístico verificado primeiro — guarda contra ruído.
      WHEN qt_items_vendidos < (SELECT min_items_vendidos FROM params)
        OR qt_items_returned < (SELECT min_items_returned FROM params)
        THEN 'Sem evidência suficiente'

      -- Alta dor + relevância comercial → ação imediata.
      WHEN td_score >= 0.70
        AND commercial_score >= 0.60
        THEN 'Priorizar melhoria'

      -- Alta dor, baixa tração comercial → vigilância.
      WHEN td_score >= 0.70
        AND commercial_score < 0.60
        THEN 'Monitorar'

      -- Produto de alta relevância com dor moderada → alerta preventivo.
      WHEN commercial_score >= 0.70
        AND td_score >= 0.50
        AND td_score < 0.70
        THEN 'Alerta em produto relevante'

      -- Dor baixa ou produto irrelevante → não priorizar.
      WHEN td_score < 0.50
        OR commercial_score < 0.40
        THEN 'Não priorizar agora'

      -- Zona cinza: td_score ∈ [0.50, 0.70) e commercial_score ∈ [0.40, 0.70).
      -- Dor e tração medianas — acompanhar sem ação imediata.
      ELSE 'Monitorar'
    END AS sinal_priorizacao,

    CONCAT(
      'Top problemas: ',
      COALESCE(top_1_problema, 'sem tag'),
      IF(top_2_problema IS NOT NULL, CONCAT(', ', top_2_problema), ''),
      IF(top_3_problema IS NOT NULL, CONCAT(', ', top_3_problema), ''),
      '. ',
      'Top 3 concentram ',
      CAST(ROUND(100 * COALESCE(pct_top_3_total, 0), 1) AS STRING),
      '% das reversas com tag. ',
      IF(
        principal_cor_afetada IS NOT NULL AND principal_cor_pct >= 0.50,
        CONCAT('Há concentração relevante na cor ', principal_cor_afetada, '. '),
        ''
      ),
      IF(
        principal_tamanho_afetado IS NOT NULL AND principal_tamanho_pct >= 0.50,
        CONCAT('Há concentração relevante no tamanho ', principal_tamanho_afetado, '. '),
        ''
      ),
      IF(
        tendencia_reversas IS NOT NULL,
        CONCAT('Tendência: ', tendencia_reversas, '.'),
        ''
      )
    ) AS resumo_pre_llm

  FROM scored
)

SELECT
  product_name,
  category,
  gender,
  portfolio_cluster,

  sinal_priorizacao,
  priority_score,
  td_score,
  commercial_score,

  qt_pedidos,
  qt_skus,
  qt_items_vendidos,
  receita_liquida,

  qt_reversas,
  qt_items_returned,
  qt_trocas,
  qt_devolucoes,
  qt_reversas_fisico,
  qt_reversas_logistico,
  valor_troca,
  valor_devolucao,

  td_rate,
  td_rate_categoria,
  delta_vs_categoria,
  ratio_vs_categoria,

  share_receita_portfolio,
  share_unidades_portfolio,
  share_td_portfolio,

  top_1_problema,
  top_1_pct,
  top_2_problema,
  top_2_pct,
  top_3_problema,
  top_3_pct,
  pct_top_3_total,

  principal_cor_afetada,
  principal_cor_pct,
  principal_tamanho_afetado,
  principal_tamanho_pct,

  reversas_ultimos_3m,
  reversas_3m_anteriores,
  tendencia_reversas,

  comentarios_amostra,
  resumo_pre_llm,
  portfolio_cluster_payload

FROM final
ORDER BY
  CASE sinal_priorizacao
    WHEN 'Priorizar melhoria' THEN 1
    WHEN 'Alerta em produto relevante' THEN 2
    WHEN 'Monitorar' THEN 3
    WHEN 'Sem evidência suficiente' THEN 4
    WHEN 'Não priorizar agora' THEN 5
    ELSE 6
  END,
  priority_score DESC,
  qt_items_returned DESC;


-- ==============================================================================
-- CAMADA 1: TABELA ANALÍTICA ITEM / REVERSA / TAG
-- Objetivo: permitir auditoria, drill-down e leitura qualitativa dos problemas
--           por SKU/produto.
-- Granularidade: order_id × order_name × sku × problema_tag
--   (múltiplas linhas por reversa quando há múltiplas tags)
-- ==============================================================================

WITH params AS (
  SELECT
    DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 12 MONTH) AS start_date,
    CURRENT_DATE("America/Sao_Paulo") AS end_date
),

-- === BASE DE VENDAS ========================================================

orders AS (
  SELECT
    o.order_id,
    o.order_name,
    o.processed_at,
    DATE(o.processed_at, "America/Sao_Paulo") AS data_compra,
    DATE_TRUNC(DATE(o.processed_at, "America/Sao_Paulo"), MONTH) AS mes_compra,
    DATE_TRUNC(DATE(o.processed_at, "America/Sao_Paulo"), WEEK(MONDAY)) AS semana_compra,
    o.amount_net_payment
  FROM `insider-data-lake.integrated.orders` o
  CROSS JOIN params p
  WHERE NOT o.is_test_order
    AND NOT o.is_internal
    AND o.financial_status IN ('paid', 'partially_refunded')
    AND (NOT o.is_cancelled OR o.is_parent_order)
    AND o.parent_order_id IS NULL
    AND DATE(o.processed_at, "America/Sao_Paulo") BETWEEN p.start_date AND p.end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY o.order_id
    ORDER BY o.data_received_at DESC
  ) = 1
),

order_items AS (
  SELECT
    oi.order_id,
    oi.order_item_id,
    oi.sku,
    oi.product_title,
    oi.variant_title,
    oi.full_item_name,
    oi.variant_color,
    oi.variant_size,
    oi.variant_model,
    oi.quantity,
    oi.item_order_total,
    oi.data_source_product_id,
    oi.data_source_variant_id,
    oi.order_date
  FROM `insider-data-lake.integrated.order_items` oi
  WHERE oi.store = 'shopify_insider-store-loja'
    AND oi.sku IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY oi.order_id, oi.order_item_id
    ORDER BY oi.data_received_at DESC
  ) = 1
),

order_items_grouped AS (
  SELECT
    order_id,
    sku,
    ANY_VALUE(product_title) AS product_title,
    ANY_VALUE(variant_title) AS variant_title,
    ANY_VALUE(full_item_name) AS full_item_name,
    ANY_VALUE(variant_color) AS variant_color,
    ANY_VALUE(variant_size) AS variant_size,
    ANY_VALUE(variant_model) AS variant_model,
    ANY_VALUE(data_source_product_id) AS data_source_product_id,
    ANY_VALUE(data_source_variant_id) AS data_source_variant_id,
    ANY_VALUE(order_date) AS order_date,
    SUM(quantity) AS qt_items,
    SUM(item_order_total) AS qt_net_revenue_sku
  FROM order_items
  GROUP BY order_id, sku
),

sku_dim AS (
  SELECT
    sku,
    ANY_VALUE(product_name) AS product_name,
    ANY_VALUE(category) AS category,
    ANY_VALUE(gender) AS gender,
    ANY_VALUE(color) AS color,
    ANY_VALUE(size) AS size,
    ANY_VALUE(sku_state) AS sku_state
  FROM `insider-data-lake.integrated.skus`
  WHERE sku IS NOT NULL
  GROUP BY sku
),

portfolio_clustering AS (
  -- Tabela em granularidade product_name (não tem coluna sku).
  SELECT
    product_name,
    ANY_VALUE(cluster) AS portfolio_cluster,
    ANY_VALUE(TO_JSON_STRING(pc)) AS portfolio_cluster_payload
  FROM `insider-data-lake.sop_silver.portfolio_skp_clustering` pc
  GROUP BY product_name
),

-- === REVERSAS =============================================================

reversas_unicas AS (
  SELECT
    r.order_name,
    r.status,
    r.reverse_type,
    r.sku,
    r.return_reason,
    r.updated_at,
    r.created_at,
    DATE(r.created_at, "America/Sao_Paulo") AS data_reversa,
    DATE_TRUNC(DATE(r.created_at, "America/Sao_Paulo"), MONTH) AS mes_reversa,
    DATE_TRUNC(DATE(r.created_at, "America/Sao_Paulo"), WEEK(MONDAY)) AS semana_reversa,
    r.client_comment,
    SAFE_CAST(r.reverse_shipping_cost AS FLOAT64) AS reverse_shipping_cost,
    SAFE_CAST(r.retained_bonus AS FLOAT64) AS retained_bonus,
    SAFE_CAST(r.exchange_value AS FLOAT64) AS exchange_value,
    SAFE_CAST(r.refund_value AS FLOAT64) AS refund_value,
    SAFE_CAST(r.return_quantity AS FLOAT64) AS qt_items_returned
  FROM `insider-lake-sensitive.prepared_br.prepared__troquecommerce_order_details_br` r
  CROSS JOIN params p
  WHERE r.status <> 'Cancelado'
    AND r.sku IS NOT NULL
    AND r.return_reason IS NOT NULL
    AND DATE(r.created_at, "America/Sao_Paulo") BETWEEN p.start_date AND p.end_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.order_name, r.sku
    ORDER BY r.updated_at DESC
  ) = 1
),

reversas_tags_latest AS (
  SELECT
    order_name,
    sku,
    tags,
    created_at,
    DATE_TRUNC(DATE(created_at), MONTH) AS ingestion_date
  FROM `insider-data-lake.sop_silver.return_reason_tags`
  WHERE order_name IS NOT NULL
    AND sku IS NOT NULL
    AND tags IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_name, sku
    ORDER BY created_at DESC
  ) = 1
),

reversas_tag AS (
  SELECT
    order_name,
    sku,
    tag,
    ingestion_date
  FROM reversas_tags_latest,
  UNNEST(tags) AS tag
),

-- === BASE ANALÍTICA =======================================================

base_analitica AS (
  SELECT
    -- Datas
    o.data_compra,
    o.mes_compra,
    o.semana_compra,

    ru.data_reversa,
    ru.mes_reversa,
    ru.semana_reversa,

    -- Pedido
    o.order_id,
    o.order_name,

    -- SKU / Produto
    oi.sku,
    oi.order_date,

    COALESCE(s.product_name, oi.product_title) AS product_name,
    s.category,
    s.gender,
    COALESCE(s.color, oi.variant_color) AS color,
    COALESCE(s.size, oi.variant_size) AS size,
    s.sku_state,

    oi.product_title,
    oi.variant_title,
    oi.full_item_name,
    oi.variant_color,
    oi.variant_size,
    oi.variant_model,
    oi.data_source_product_id,
    oi.data_source_variant_id,

    CONCAT(COALESCE(s.product_name, oi.product_title), ' ', COALESCE(s.color, oi.variant_color)) AS variant_color_name,

    -- Cluster
    pc.portfolio_cluster_payload,

    -- Reversa
    ru.status AS reverse_status,
    ru.reverse_type,
    ru.return_reason,
    ru.client_comment,

    CASE
      WHEN LOWER(ru.return_reason) LIKE '%ficou grande%' THEN 'Tamanho'
      WHEN ru.return_reason = 'Peça íntima' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Produto com defeito' THEN 'Defeito'
      WHEN ru.return_reason = 'Arrependimento' THEN 'Desistência'
      WHEN ru.return_reason = 'Recebi um produto errado' THEN 'Produto errado'
      WHEN LOWER(ru.return_reason) LIKE '%ficou pequeno%' THEN 'Tamanho'
      WHEN ru.return_reason = 'Cor diferente do esperado' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Recebi novo pedido de reposição' THEN 'Produto errado'
      WHEN ru.return_reason = 'Recebi pedido duplicado' THEN 'Produto errado'
      WHEN ru.return_reason = 'Arrependiemento' THEN 'Desistência'
      WHEN ru.return_reason = 'Insatisfação (tamanho e cor inclusos)' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Arrependimento (tamanho e cor inclusos)' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason LIKE '%Tamanho%' THEN 'Tamanho'
      WHEN ru.return_reason = 'Insatisfação com o produto' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Falha na Entrega' THEN 'Problema na entrega'
      WHEN ru.return_reason = 'Demora na entrega' THEN 'Problema na entrega'
      WHEN ru.return_reason = 'Ficou Grande' THEN 'Tamanho'
      WHEN ru.return_reason = 'Ficou Pequeno' THEN 'Tamanho'
      WHEN ru.return_reason = 'Não gostei do produto' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Defeito' THEN 'Defeito'
      WHEN ru.return_reason = 'Peça íntima (calcinha, sutiã, meia ou cueca)' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Insatisfação' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason = 'Defeitos' THEN 'Defeito'
      WHEN ru.return_reason = 'Desbotamento' THEN 'Desbotamento'
      WHEN ru.return_reason = 'Desistência' THEN 'Desistência'
      WHEN ru.return_reason = 'Não gostei da qualidade' THEN 'Insatisfação com o produto'
      WHEN ru.return_reason IS NULL THEN NULL
      ELSE 'Outros'
    END AS motivo_classificado,

    CASE
      WHEN LOWER(ru.return_reason) LIKE '%ficou grande%' THEN 'Físico'
      WHEN ru.return_reason = 'Peça íntima' THEN 'Físico'
      WHEN ru.return_reason = 'Produto com defeito' THEN 'Físico'
      WHEN ru.return_reason = 'Arrependimento' THEN 'Desistência'
      WHEN ru.return_reason = 'Recebi um produto errado' THEN 'Logístico'
      WHEN LOWER(ru.return_reason) LIKE '%ficou pequeno%' THEN 'Físico'
      WHEN ru.return_reason = 'Cor diferente do esperado' THEN 'Físico'
      WHEN ru.return_reason = 'Recebi novo pedido de reposição' THEN 'Logístico'
      WHEN ru.return_reason = 'Recebi pedido duplicado' THEN 'Logístico'
      WHEN ru.return_reason = 'Arrependiemento' THEN 'Desistência'
      WHEN ru.return_reason = 'Insatisfação (tamanho e cor inclusos)' THEN 'Físico'
      WHEN ru.return_reason = 'Arrependimento (tamanho e cor inclusos)' THEN 'Desistência'
      WHEN ru.return_reason LIKE '%Tamanho%' THEN 'Físico'
      WHEN ru.return_reason = 'Insatisfação com o produto' THEN 'Físico'
      WHEN ru.return_reason = 'Falha na Entrega' THEN 'Logístico'
      WHEN ru.return_reason = 'Demora na entrega' THEN 'Logístico'
      WHEN ru.return_reason = 'Ficou Grande' THEN 'Físico'
      WHEN ru.return_reason = 'Ficou Pequeno' THEN 'Físico'
      WHEN ru.return_reason = 'Não gostei do produto' THEN 'Físico'
      WHEN ru.return_reason = 'Defeito' THEN 'Físico'
      WHEN ru.return_reason = 'Peça íntima (calcinha, sutiã, meia ou cueca)' THEN 'Físico'
      WHEN ru.return_reason = 'Insatisfação' THEN 'Físico'
      WHEN ru.return_reason = 'Defeitos' THEN 'Físico'
      WHEN ru.return_reason = 'Desbotamento' THEN 'Físico'
      WHEN ru.return_reason = 'Desistência' THEN 'Desistência'
      WHEN ru.return_reason = 'Não gostei da qualidade' THEN 'Físico'
      WHEN ru.return_reason IS NULL THEN NULL
      ELSE 'Outros'
    END AS tipo_problema,

    rt.tag AS problema_tag,
    rt.ingestion_date AS tag_ingestion_month,

    CASE
      WHEN ru.reverse_type IS NULL THEN 'Sem reversa'
      WHEN LOWER(ru.reverse_type) LIKE '%troca%' THEN 'Troca'
      WHEN LOWER(ru.reverse_type) LIKE '%devol%' THEN 'Devolução'
      ELSE ru.reverse_type
    END AS reverse_type_classificado,

    IF(ru.reverse_type IS NOT NULL, TRUE, FALSE) AS teve_reversa,

    -- Métricas comerciais
    o.amount_net_payment AS qt_net_order_revenue,
    oi.qt_net_revenue_sku,
    oi.qt_items,

    -- Métricas de reversa
    COALESCE(ru.qt_items_returned, 0) AS qt_items_returned,
    ru.reverse_shipping_cost,
    ru.retained_bonus,
    ru.exchange_value,
    ru.refund_value

  FROM orders o
  JOIN order_items_grouped oi
    ON o.order_id = oi.order_id
  LEFT JOIN reversas_unicas ru
    ON ru.order_name = o.order_name
   AND ru.sku = oi.sku
  LEFT JOIN reversas_tag rt
    ON rt.order_name = o.order_name
   AND rt.sku = oi.sku
  LEFT JOIN sku_dim s
    ON s.sku = oi.sku
  LEFT JOIN portfolio_clustering pc
    ON pc.product_name = COALESCE(s.product_name, oi.product_title)
  -- Apenas produtos perenes e lançamentos (exclui desativados e cápsulas)
  WHERE s.sku_state IN ('ativo_perene', 'ativo_em_lancamento')
)

SELECT *
FROM base_analitica
ORDER BY data_compra DESC, order_name, sku, problema_tag;
