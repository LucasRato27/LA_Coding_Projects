-- =====================================================================================
-- OKR de Troca & Devolução — SOURCE OF TRUTH (COHORT) — visão MENSAL
-- Arquivo espelho estrutural de `td_cohort_semanal.sql` e da definição LEGADA
-- (visão mensal por fluxo). Os nomes e a ordem das CTEs são deliberadamente iguais
-- aos da query legada para que o diff lado a lado mostre apenas PREMISSA, não estilo.
--
-- DEFINIÇÃO
--   Numerador:   SUM(return_quantity) de TODAS as reversas não canceladas do pedido
--                (lifetime), atribuídas ao MÊS DA COMPRA do pedido, via `order_name`.
--   Denominador: SUM(quantity) de itens comprados no mês (filtro de pedido válido).
--   T&D% = numerador / denominador.
--
-- PREMISSAS EXPLÍCITAS (ver AUDITORIA_PREMISSAS_OKR_TD.md para o confronto com o legado)
--   P1  ANCORAGEM: a reversa pertence ao mês da COMPRA (não ao mês de abertura da reversa).
--   P2  NUMERADOR LIFETIME: `created_at` só é validado IS NOT NULL; nunca é filtrado por
--       data. Toda reversa do pedido conta no cohort dele, aconteça quando acontecer.
--   P3  UNIVERSO DE PEDIDO DO NUMERADOR: a data de compra vem de `todos_pedidos` —
--       QUALQUER pedido com `processed_at`, sem exigir paid / loja elegível / cupom limpo.
--       Assimetria DELIBERADA: replica o universo de reversa que o legado aceita
--       implicitamente (o legado nunca valida o pedido de origem da reversa).
--   P4  SEM CASAMENTO DE SKU: não se exige que o SKU revertido conste como item comprado.
--   P5  DENOMINADOR IDÊNTICO AO LEGADO: paid + is_cancelled=FALSE + exclusão de cupons
--       (TF-, TFIN, IR, %Item errado%) + 2 lojas. Deve reconciliar em ZERO com o legado.
--   P6  DEDUP: SELECT DISTINCT (order_name, id_reversa, sku, return_quantity) — atenção:
--       `return_quantity` está DENTRO do DISTINCT, exatamente como no legado. Duas linhas
--       da mesma chave com quantidades diferentes sobrevivem e são ambas somadas.
--       Construto simétrico entre os dois métodos, mas é exposição real a dupla contagem.
--   P7  GRÃO DO NUMERADOR: colapsa para PEDIDO antes de ancorar a data; o SKU não chega
--       ao grão final.
--   P8  TIMEZONE: America/Sao_Paulo em todos os timestamps. `todos_pedidos` usa
--       MIN(processed_at) por order_name.
--   P9  MATURAÇÃO: o cohort é censurado à direita. Um mês só é oficial no DIA 15 DE M+1.
--       Colunas `maduro_em` / `status` materializam essa regra. Não existe no legado.
--
-- JANELA: start_date = 2026-01-01, alinhada à query legada, para reconciliação linha a linha.
-- =====================================================================================

WITH params AS (
  SELECT
    DATE '2026-01-01' AS start_date,
    DATE_SUB(
      DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), MONTH),
      INTERVAL 1 MONTH
    ) AS ultimo_mes_fechado
),

-- [P5] Pedido válido — CTE literalmente idêntica à legada (mesmas colunas, mesmos filtros).
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

-- [P5] Denominador — CTE literalmente idêntica à legada.
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

-- [P3][P8] CTE NOVA vs. legado: âncora de data do numerador.
-- Qualquer pedido com processed_at — sem paid, sem loja, sem cupom. Só serve para
-- descobrir QUANDO o pedido revertido foi comprado.
todos_pedidos AS (
  SELECT
    order_name,
    MIN(
      DATE(
        TIMESTAMP(processed_at),
        'America/Sao_Paulo'
      )
    ) AS data_compra
  FROM `insider-data-lake.business.insider_orders`
  WHERE order_name IS NOT NULL
    AND processed_at IS NOT NULL
  GROUP BY 1
),

-- [P2][P4][P6] Universo de reversa — MESMO do legado, com uma única diferença:
-- `data_reversa` NÃO entra no DISTINCT nem no WHERE, porque o numerador é lifetime.
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
    AND created_at IS NOT NULL
),

-- [P7] CTE NOVA vs. legado: colapsa para o grão de PEDIDO antes de ancorar a data.
reversas_por_pedido AS (
  SELECT
    order_name,
    SUM(COALESCE(return_quantity, 0)) AS qt_revertidos
  FROM reversas_deduplicadas
  GROUP BY 1
),

-- [P1] Numerador. Aqui está a diferença estrutural que sobra vs. o legado:
-- DATE_TRUNC(data_COMPRA, MONTH) em vez de DATE_TRUNC(data_REVERSA, MONTH).
reversas_mensais AS (
  SELECT
    DATE_TRUNC(tp.data_compra, MONTH) AS mes_referencia,
    SUM(r.qt_revertidos) AS qt_itens_revertidos
  FROM reversas_por_pedido AS r
  INNER JOIN todos_pedidos AS tp
    USING (order_name)
  CROSS JOIN params AS p
  WHERE tp.data_compra >= p.start_date
    AND tp.data_compra < DATE_ADD(p.ultimo_mes_fechado, INTERVAL 1 MONTH)
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
    ),
    4
  ) AS pct_reversas_sobre_vendas,
  -- [P9] dia 15 de M+1
  DATE_ADD(
    DATE_ADD(c.mes_referencia, INTERVAL 1 MONTH),
    INTERVAL 14 DAY
  ) AS maduro_em,
  IF(
    CURRENT_DATE('America/Sao_Paulo') >= DATE_ADD(
      DATE_ADD(c.mes_referencia, INTERVAL 1 MONTH),
      INTERVAL 14 DAY
    ),
    'oficial (maduro)',
    'em maturacao'
  ) AS status
FROM calendar AS c
LEFT JOIN vendas_mensais AS v USING (mes_referencia)
LEFT JOIN reversas_mensais AS r USING (mes_referencia)
ORDER BY c.mes_referencia DESC;
