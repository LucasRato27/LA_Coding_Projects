-- =====================================================================================
-- OKR de Troca & Devolução — SOURCE OF TRUTH (COHORT) — visão SEMANAL (HM)
-- Gêmea estrutural de `td_cohort_mensal.sql`: MESMAS CTEs, MESMAS premissas.
-- A única diferença entre os dois arquivos é o eixo de tempo (WEEK(MONDAY) vs. MONTH)
-- e a régua de maturação (W-2 vs. dia 15 de M+1).
--
-- DEFINIÇÃO
--   Numerador:   SUM(return_quantity) de TODAS as reversas não canceladas do pedido
--                (lifetime), atribuídas à SEMANA DA COMPRA do pedido, via `order_name`.
--   Denominador: SUM(quantity) de itens comprados na semana (filtro de pedido válido).
--
-- PREMISSAS: P1–P8 idênticas a `td_cohort_mensal.sql` (ver cabeçalho lá e
--            AUDITORIA_PREMISSAS_OKR_TD.md). Substitui-se apenas:
--   P9' MATURAÇÃO SEMANAL: a semana só é reportável quando é W-2 ou mais antiga, i.e.
--       `semana_ini <= DATE_TRUNC(hoje, WEEK(MONDAY)) - 21 dias`. Equivale, dia a dia,
--       a `semana_ini + 21 dias <= hoje`. Semanas mais recentes estão subestimadas
--       por censura à direita, não por melhora operacional.
--   P10 SEMANA = SEGUNDA A DOMINGO. `DATE_TRUNC(d, WEEK(MONDAY))` é o equivalente exato
--       de `pd.Period(d, "W-SUN").start_time` usado no notebook (Parte C, célula 35).
--       Cuidado: `WEEK` sem argumento no BigQuery começa no DOMINGO — estaria errado.
--   P11' JANELA DIVERGE DA GÊMEA MENSAL — decisão deliberada, revisitada em 2026-08-05.
--       `td_cohort_mensal.sql` usa `start_date = 2026-01-01` e para no último mês
--       fechado porque alimenta uma reconciliação REAL contra a query legada
--       (AUDITORIA_PREMISSAS_OKR_TD.md §7) — a legada só existe em granularidade
--       mensal, então não há nada semanal para reconciliar aqui. Esta query, em vez
--       disso, serve o gráfico de tendência do HM (planning de curto prazo), que
--       precisa de histórico longo para a leitura de médio prazo fazer sentido.
--       Por isso: `start_date = 2025-01-01` (mesma âncora historicamente usada pela
--       célula 31 do notebook) e o calendário vai até a SEMANA ATUAL, mesmo que
--       parcial — a semana em curso aparece como um ponto real (baixo, porque tem
--       poucos dias), não é omitida. O status `em maturacao` / a região sombreada
--       "imaturo" no gráfico já comunicam que essa cauda não é leitura de resultado.
--
-- JANELA: start_date = 2025-01-01, calendário até a semana atual (parcial).
-- Ver P11' acima — divergência deliberada da janela `2026-01-01` da gêmea mensal.
-- =====================================================================================

WITH params AS (
  SELECT
    DATE '2025-01-01' AS start_date,
    -- [P11'] segunda-feira da semana ATUAL (mesmo parcial) — limite superior do
    -- calendário. Diferente da gêmea mensal, que para no último período FECHADO.
    DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), WEEK(MONDAY)) AS semana_atual,
    -- [P9'] segunda-feira da semana W-2 (a semana headline do HM)
    DATE_SUB(
      DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), WEEK(MONDAY)),
      INTERVAL 21 DAY
    ) AS semana_w2
),

-- [P5] Pedido válido — CTE literalmente idêntica à legada e à gêmea mensal.
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

-- [P5] Denominador.
vendas_semanais AS (
  SELECT
    DATE_TRUNC(c.data_compra, WEEK(MONDAY)) AS semana_ini,
    SUM(COALESCE(i.quantity, 0)) AS qt_itens_vendidos
  FROM compras_filtradas AS c
  INNER JOIN `insider-data-lake.business.insider_order_items` AS i
    ON c.order_id = i.order_id
  CROSS JOIN params AS p
  WHERE i.sku IS NOT NULL
    AND c.data_compra >= p.start_date
    AND c.data_compra < DATE_ADD(p.semana_atual, INTERVAL 7 DAY)
  GROUP BY 1
),

-- [P3][P8] Âncora de data do numerador: qualquer pedido com processed_at.
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

-- [P2][P4][P6] Universo de reversa lifetime — sem corte por data de reversa.
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

-- [P7] Grão de pedido.
reversas_por_pedido AS (
  SELECT
    order_name,
    SUM(COALESCE(return_quantity, 0)) AS qt_revertidos
  FROM reversas_deduplicadas
  GROUP BY 1
),

-- [P1] Numerador ancorado na SEMANA DA COMPRA.
reversas_semanais AS (
  SELECT
    DATE_TRUNC(tp.data_compra, WEEK(MONDAY)) AS semana_ini,
    SUM(r.qt_revertidos) AS qt_itens_revertidos
  FROM reversas_por_pedido AS r
  INNER JOIN todos_pedidos AS tp
    USING (order_name)
  CROSS JOIN params AS p
  WHERE tp.data_compra >= p.start_date
    AND tp.data_compra < DATE_ADD(p.semana_atual, INTERVAL 7 DAY)
  GROUP BY 1
),

calendar AS (
  SELECT semana_ini
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(start_date, WEEK(MONDAY)),
      semana_atual,
      INTERVAL 7 DAY
    )
  ) AS semana_ini
),

serie AS (
  SELECT
    c.semana_ini,
    COALESCE(v.qt_itens_vendidos, 0) AS qt_itens_vendidos,
    COALESCE(r.qt_itens_revertidos, 0) AS qt_itens_revertidos,
    ROUND(
      SAFE_DIVIDE(
        COALESCE(r.qt_itens_revertidos, 0),
        COALESCE(v.qt_itens_vendidos, 0)
      ),
      4
    ) AS pct_reversas_sobre_vendas
  FROM calendar AS c
  LEFT JOIN vendas_semanais AS v USING (semana_ini)
  LEFT JOIN reversas_semanais AS r USING (semana_ini)
)

SELECT
  s.semana_ini,
  DATE_ADD(s.semana_ini, INTERVAL 6 DAY) AS semana_fim,
  s.qt_itens_vendidos,
  s.qt_itens_revertidos,
  s.pct_reversas_sobre_vendas,
  -- média móvel de 4 semanas sobre a série COMPLETA (inclui semanas imaturas),
  -- replicando `rolling(4).mean()` do notebook: NULL até haver 4 pontos.
  IF(
    COUNT(*) OVER (
      ORDER BY s.semana_ini
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) = 4,
    ROUND(
      AVG(s.pct_reversas_sobre_vendas) OVER (
        ORDER BY s.semana_ini
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ),
      4
    ),
    NULL
  ) AS media_movel_4s,
  -- [P9'] a semana passa a ser reportável 21 dias depois de começar
  DATE_ADD(s.semana_ini, INTERVAL 21 DAY) AS reportavel_em,
  IF(
    s.semana_ini <= p.semana_w2,
    'reportavel (<=W-2)',
    'em maturacao'
  ) AS status,
  s.semana_ini = p.semana_w2 AS is_w2
FROM serie AS s
CROSS JOIN params AS p
ORDER BY s.semana_ini DESC;
