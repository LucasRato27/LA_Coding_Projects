WITH compras_filtradas AS (
  SELECT DISTINCT
    order_id,
    order_name,
    DATE(TIMESTAMP(processed_at), "America/Sao_Paulo") AS data_compra,
    FORMAT_DATE('%Y-%m', DATE(TIMESTAMP(processed_at), "America/Sao_Paulo")) AS mes_compra
  FROM `insider-data-lake.business.insider_orders`
  WHERE 1=1
    AND order_status = 'paid'
    AND is_cancelled = FALSE
    AND (
      coupon_code IS NULL OR (
        NOT STARTS_WITH(coupon_code, 'TF-')
        AND NOT STARTS_WITH(coupon_code, 'TFIN')
        AND NOT STARTS_WITH(coupon_code, 'IR')
        AND NOT (coupon_code LIKE '%Item errado%')
      )
    )
    AND order_name IS NOT NULL
    AND processed_at IS NOT NULL
    AND store IN ('shopify_insider-world', 'shopify_insider-store-loja')
),

itens_comprados AS (
  SELECT
    c.mes_compra,
    c.order_id,
    c.order_name,
    i.sku,
    SUM(i.quantity) AS qt_itens_comprados
  FROM compras_filtradas c
  INNER JOIN `insider-data-lake.business.insider_order_items` i
    ON c.order_id = i.order_id
  WHERE i.sku IS NOT NULL
  GROUP BY 1,2,3,4
),

reversas_deduplicadas AS (
  SELECT DISTINCT
    order_name,
    id_reversa,
    sku,
    SAFE_CAST(return_quantity AS FLOAT64) AS return_quantity
  FROM `insider-lake-sensitive.prepared_br.prepared__troquecommerce_order_details_br`
  WHERE status <> 'Cancelado'
    AND order_name IS NOT NULL
    AND sku IS NOT NULL
    AND id_reversa IS NOT NULL
),

reversas_por_item AS (
  SELECT
    order_name,
    sku,
    SUM(return_quantity) AS qt_itens_revertidos
  FROM reversas_deduplicadas
  GROUP BY 1,2
),

base_final AS (
  SELECT
    ic.mes_compra,
    ic.order_name,
    ic.sku,
    ic.qt_itens_comprados,
    COALESCE(r.qt_itens_revertidos, 0) AS qt_itens_revertidos
  FROM itens_comprados ic
  LEFT JOIN reversas_por_item r
    ON ic.order_name = r.order_name
   AND ic.sku = r.sku
)

SELECT
  mes_compra,
  SUM(qt_itens_comprados) AS qt_itens_comprados,
  SUM(qt_itens_revertidos) AS qt_itens_revertidos,
  ROUND(
    SAFE_DIVIDE(SUM(qt_itens_revertidos), SUM(qt_itens_comprados)) * 100,
    2
  ) AS pct_reversa_itens
FROM base_final
GROUP BY 1
ORDER BY mes_compra DESC;