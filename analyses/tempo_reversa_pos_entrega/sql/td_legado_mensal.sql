-- Source of Truth de Troca & Devolução — visão mensal por fluxo
--
-- Numerador: peças com solicitação de troca/devolução criada no mês.
-- Denominador: peças vendidas no mesmo mês.
-- Não há vínculo de coorte, pedido ou SKU entre vendas e reversas.

WITH params AS (
  SELECT
    DATE '2026-01-01' AS start_date,
    DATE_SUB(
      DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), MONTH),
      INTERVAL 1 MONTH
    ) AS ultimo_mes_fechado
),

compras_filtradas AS (
  SELECT DISTINCT
    order_id,
    DATE(
      TIMESTAMP(processed_at),
      'America/Sao_Paulo'
    ) AS data_compra
  FROM `insider-data-lake.business.insider_orders`
  WHERE order_status = 'paid'
    AND is_cancelled = FALSE
    AND (
      coupon_code IS NULL
      OR (
        NOT STARTS_WITH(coupon_code, 'TF-')
        AND NOT STARTS_WITH(coupon_code, 'TFIN')
        AND NOT STARTS_WITH(coupon_code, 'IR')
        AND NOT coupon_code LIKE '%Item errado%'
      )
    )
    AND order_name IS NOT NULL
    AND processed_at IS NOT NULL
    AND store IN (
      'shopify_insider-store-loja',
      'shopify_insider-world'
    )
),

vendas_mensais AS (
  SELECT
    DATE_TRUNC(c.data_compra, MONTH) AS mes_referencia,
    SUM(COALESCE(i.quantity, 0)) AS qt_itens_vendidos
  FROM compras_filtradas AS c
  INNER JOIN `insider-data-lake.business.insider_order_items` AS i
    ON c.order_id = i.order_id
  CROSS JOIN params AS p
  WHERE i.sku IS NOT NULL
    AND c.data_compra >= p.start_date
    AND c.data_compra < DATE_ADD(p.ultimo_mes_fechado, INTERVAL 1 MONTH)
  GROUP BY 1
),

reversas_deduplicadas AS (
  SELECT DISTINCT
    order_name,
    id_reversa,
    sku,
    DATE(created_at, 'America/Sao_Paulo') AS data_reversa,
    SAFE_CAST(return_quantity AS FLOAT64) AS return_quantity
  FROM `insider-lake-sensitive.prepared_br.prepared__troquecommerce_order_details_br`
  WHERE status <> 'Cancelado'
    AND order_name IS NOT NULL
    AND sku IS NOT NULL
    AND id_reversa IS NOT NULL
    AND created_at IS NOT NULL
),

reversas_mensais AS (
  SELECT
    DATE_TRUNC(data_reversa, MONTH) AS mes_referencia,
    SUM(COALESCE(return_quantity, 0)) AS qt_itens_revertidos
  FROM reversas_deduplicadas
  CROSS JOIN params AS p
  WHERE data_reversa >= p.start_date
    AND data_reversa < DATE_ADD(p.ultimo_mes_fechado, INTERVAL 1 MONTH)
  GROUP BY 1
),

calendar AS (
  SELECT mes_referencia
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(start_date, MONTH),
      ultimo_mes_fechado,
      INTERVAL 1 MONTH
    )
  ) AS mes_referencia
)

SELECT
  FORMAT_DATE('%Y-%m', c.mes_referencia) AS mes_referencia,
  COALESCE(v.qt_itens_vendidos, 0) AS qt_itens_vendidos,
  COALESCE(r.qt_itens_revertidos, 0) AS qt_itens_revertidos,
  ROUND(
    SAFE_DIVIDE(
      COALESCE(r.qt_itens_revertidos, 0),
      COALESCE(v.qt_itens_vendidos, 0)
    ) ,
    4
  ) AS pct_reversas_sobre_vendas
FROM calendar AS c
LEFT JOIN vendas_mensais AS v USING (mes_referencia)
LEFT JOIN reversas_mensais AS r USING (mes_referencia)
ORDER BY c.mes_referencia DESC;