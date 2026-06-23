-- ============================================================
-- fabric_product_query.sql
-- Mapeamento produto → artigo/tecido para detecção de atraso de MP compartilhada
--
-- Sem parâmetro de data — snapshot estático da relação produto × artigo.
-- Filtro: apenas SKUs em estado ativo relevante.
--
-- Grão de saída: product_name × article_name × article_id
--   (sem duplicatas por produto — um artigo pode aparecer em múltiplos SKUs
--    do mesmo produto, mas o agregado os unifica via COUNT(DISTINCT sku))
--
-- JOIN chain validado contra analytics/lead_time/lead_time_dashboard.ipynb:
--   muninn_product_skus_fabrics.product_sku_id = muninn_product_skus.product_sku_id
--   muninn_product_skus_fabrics.fabric_id       = muninn_fabrics.id
--   muninn_fabrics.article_id                   = muninn_articles.id
--   muninn_product_skus.sku                     = skus.sku
-- ============================================================

WITH

sku_fabric AS (
    SELECT
        s.product_name,
        mf.name          AS fabric_name,
        mf.article_id,
        ma.name          AS article_name,
        mps.sku
    FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS mpsf
    INNER JOIN `insider-data-lake.integrated.muninn_fabrics`      AS mf  ON mf.id              = mpsf.fabric_id
    INNER JOIN `insider-data-lake.integrated.muninn_articles`     AS ma  ON ma.id              = mf.article_id
    INNER JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.product_sku_id = mpsf.product_sku_id
    INNER JOIN `insider-data-lake.integrated.skus`                AS s   ON s.sku              = mps.sku
    WHERE s.sku_state IN ('ativo_perene', 'ativo_em_lancamento', 'ativo_capsula')
      AND ma.name        IS NOT NULL
      AND s.product_name IS NOT NULL
)

SELECT
    product_name,
    article_name,
    CAST(article_id AS INT64)  AS article_id,
    ANY_VALUE(fabric_name)     AS fabric_name,
    COUNT(DISTINCT sku)        AS n_fabric_skus
FROM sku_fabric
GROUP BY
    product_name,
    article_name,
    article_id
ORDER BY
    product_name,
    article_name
