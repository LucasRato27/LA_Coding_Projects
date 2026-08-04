# Tempo até Reversa & OKR de Troca/Devolução (T&D) — Pipeline e Premissas

## Localização
`analyses/tempo_reversa_pos_entrega/tempo_para_reversa_pos_entrega.ipynb`

Notebook com 3 partes independentes (todas em plotly, `create_bqstorage_client=False` no `.to_dataframe()`):

| Parte | Pergunta | Grão |
|---|---|---|
| **A** | Quanto tempo após a **entrega** o cliente abre reversa? | 1 linha por reversa (`order_name × id_reversa`) |
| **B** | Quanto tempo após a **compra** o cliente abre reversa? (decompõe em envio + decisão) | idem |
| **C** | **OKR oficial de T&D** — KR mensal (MBR) e HM semanal | agregado diário → mensal/semanal |

## Tabelas e schemas (validados em produção, 2026-07-27/08-03)

| Tabela | Projeto | Campo-chave usado |
|---|---|---|
| `business.insider_orders` | `insider-data-lake` | `order_id`, `order_name`, `processed_at`, `order_status`, `is_cancelled`, `coupon_code`, `store` |
| `business.insider_order_items` | `insider-data-lake` | `order_id`, `sku`, `quantity` |
| `integrated_br.shippings_br` | `insider-lake-sensitive` | `order_name`, `delivered_date` (DATETIME) — usar `MIN()` por pedido (multi-volume) |
| `integrated_br.order_exchanges_br` | `insider-lake-sensitive` | `order_id`, `exchange_created_at` — **é a fonte da Troquecommerce redundante**, mesmo min/max timestamp; não usar como alternativa independente |
| `integrated_br.order_refunds_br` | `insider-lake-sensitive` | `order_id`, `refund_created_at` — universo maior (financeiro/Shopify, desde 2021), **não é a fonte oficial de reversa** |
| `prepared_br.prepared__troquecommerce_order_details_br` | `insider-lake-sensitive` | `order_name`, `id_reversa`, `sku`, `created_at`, `status`, `reverse_type`, `return_quantity` — **fonte oficial de reversa (Troquecommerce)** |

**Chave de join universal:** `order_name` (formato `IN-XXXXXXX`) é idêntico em todas as tabelas — validado, não precisa de bridge por `order_id`.

**Filtro padrão T&D (pedidos válidos):** `order_status='paid'`, `is_cancelled=FALSE`, exclusão cupons `TF-`/`TFIN`/`IR`/`Item errado`, `store IN ('shopify_insider-world','shopify_insider-store-loja')`.

**Dedup de reversa:** `SELECT DISTINCT (order_name, id_reversa, sku, ...)` na origem, ou `QUALIFY ROW_NUMBER() OVER (PARTITION BY order_name, id_reversa ORDER BY updated_at DESC) = 1` quando se precisa de 1 linha por reversa. Filtro `status <> 'Cancelado'`.

## ⚠️ Correção crítica de metodologia (histórico do erro)

Nas primeiras iterações da Parte C, implementei o OKR como **métrica de fluxo** (numerador = reversas *criadas* no período, denominador = vendas do período, **sem vínculo** de coorte) — por má leitura de uma query intermediária do usuário. Isso gerou números **~38% mais altos** que a definição real (ver comparação abaixo).

**A fonte da verdade real é COHORT**, confirmada por query verbatim do usuário:
- A reversa é **atribuída ao mês/semana da COMPRA** via join `(order_name, sku)` entre `itens_comprados` e `reversas_por_item`.
- **Numerador = lifetime** (todas as reversas do item até o momento da consulta, sem corte por `created_at`).
- `T&D% = SUM(qt_itens_revertidos) / SUM(qt_itens_comprados)` dentro do mesmo cohort mensal/semanal de compra.

Query canônica (SoT, cohort mensal):
```sql
WITH compras_filtradas AS (...),                 -- pedidos válidos, filtro T&D padrão
itens_comprados AS (                             -- vendas por (mes_compra, order_name, sku)
  SELECT c.mes_compra, c.order_id, c.order_name, i.sku, SUM(i.quantity) AS qt_itens_comprados
  FROM compras_filtradas c JOIN insider_order_items i ON c.order_id = i.order_id
  WHERE i.sku IS NOT NULL GROUP BY 1,2,3,4
),
reversas_deduplicadas AS (SELECT DISTINCT order_name, id_reversa, sku, return_quantity FROM troquecommerce ...),
reversas_por_item AS (SELECT order_name, sku, SUM(return_quantity) AS qt_itens_revertidos FROM reversas_deduplicadas GROUP BY 1,2),
base_final AS (
  SELECT ic.mes_compra, ic.order_name, ic.sku, ic.qt_itens_comprados,
         COALESCE(r.qt_itens_revertidos, 0) AS qt_itens_revertidos
  FROM itens_comprados ic LEFT JOIN reversas_por_item r
    ON ic.order_name = r.order_name AND ic.sku = r.sku
)
SELECT mes_compra, SUM(qt_itens_comprados), SUM(qt_itens_revertidos),
       ROUND(SAFE_DIVIDE(SUM(qt_itens_revertidos), SUM(qt_itens_comprados))*100, 2) AS pct_reversa_itens
FROM base_final GROUP BY 1 ORDER BY 1 DESC
```

## Definições oficiais do OKR (vigentes no notebook, Parte C)

### KR mensal (MBR)
`T&D% = reversas do cohort de compras do mês passado (M-1) ÷ vendas do mesmo cohort (M-1) × 100`
- **Headline = mês passado (M-1)**, não o mês corrente.
- **Calculado no dia 15 do mês seguinte** (M+1) — buffer de 15 dias de maturação do cohort.
- Regra de maturação no notebook: `maduro_em = (mês + 1 mês, dia 1) + 14 dias` (= dia 15 de M+1); `status = "oficial" se HOJE >= maduro_em, senão "em maturação"`.
- O notebook calcula automaticamente o **KR vigente** (último mês oficial) e a **próxima leitura** (mês em maturação + data em que fica oficial).

### HM semanal (planning de curto prazo)
`T&D semanal% = reversas associadas às compras da semana W-2 ÷ vendas da semana W-2 × 100`
- **Cohort de compra: [D-21, D-15]** — a semana ISO (segunda a domingo) que contém esse intervalo é a **W-2 reportável**.
- Semanas mais recentes que W-2 estão "ainda maturando" (poucas reversas capturadas) — sempre plotadas tracejadas/subestimadas no gráfico, nunca usar como leitura de nível.
- Leitura de tendência: **média móvel de 4 semanas** (a série semanal crua é ruidosa).

## Números de referência (execução 2026-08-03, cohort SoT)

| Mês | Vendas (itens) | Reversas (lifetime) | T&D | Status |
|---|---:|---:|---:|---|
| 2026-01 | 217.791 | 11.838 | 5,44% | oficial |
| 2026-02 | 228.536 | 13.164 | 5,76% | oficial |
| 2026-03 | 376.559 | 23.666 | **6,28%** (pico) | oficial |
| 2026-04 | 244.773 | 12.031 | 4,92% | oficial |
| 2026-05 | 195.670 | 9.858 | 5,04% | oficial |
| **2026-06** | 185.589 | 9.240 | **4,98%** | **oficial — KR vigente em 03/ago** |
| 2026-07 | 189.557 | 8.228 | 4,34% (provisório) | em maturação → oficial 15/ago |

**HM W-2 [13–19/jul/2026] = 5,53%**.

### Comparação: definição antiga (flow) vs nova (cohort) — achado relevante
A definição antiga (flow, sem vínculo de coorte) reporta T&D **~38% maior** que a nova (cohort), de forma consistente:

| Mês | Antiga (flow) | Nova (cohort) | Δ p.p. | Δ relativo |
|---|---:|---:|---:|---:|
| jan/26 | 10,59% | 5,44% | -5,15 | -48,6% |
| fev/26 | 7,93% | 5,76% | -2,17 | -27,4% |
| mar/26 | 8,89% | 6,28% | -2,61 | -29,4% |
| abr/26 | 9,35% | 4,92% | -4,43 | -47,4% |
| mai/26 | 7,84% | 5,04% | -2,80 | -35,7% |
| jun/26 | 8,23% | 4,98% | -3,25 | -39,5% |

Média (jan-jun): antiga 8,80% → nova 5,40% → **redução média de 3,40 p.p. (-38%)**.

**Causa raiz:** a definição antiga misturava cohorts (numerador = reversas *criadas* no mês, incluindo devoluções de compras de meses anteriores — ex. Black Friday "vazando" para janeiro — contra vendas apenas daquele mês). A definição cohort elimina essa contaminação cross-safra.

## Resultados da Parte A e B (tempo até reversa, últimos 12 meses)

- **Entrega → reversa:** mediana **2,1 dias**; 48,6% em ≤2d; 78,8% em ≤7d; p90 ≈ 14d (troca) / 9d (devolução).
- **Compra → reversa:** mediana **8-9 dias**; decomposição: compra→entrega (envio) mediana **5d**; entrega→reversa (decisão) mediana **2d**. Gargalo é o frete, não a indecisão do cliente.
- Troca é ~8x mais frequente que devolução no volume Troquecommerce.

## Gotchas técnicos

1. **BigQuery Storage API instável neste ambiente** (`bigquerystorage.googleapis.com` com falha de DNS intermitente). Sempre usar `client.query(sql).to_dataframe(create_bqstorage_client=False)` para forçar fallback REST — muito mais robusto, ainda que um pouco mais lento.
2. **Colisão com sessão Jupyter aberta:** se o usuário está com o notebook aberto no Jupyter/editor, executar `nbconvert --execute --inplace` pode ter a saída sobrescrita pelo autosave da sessão do usuário (ou vice-versa). Sempre confirmar se o arquivo está fechado antes de executar, e verificar após a execução que `"Writing N bytes"` aparece no log **e** os `execution_count`/timestamps das células são recentes (não confiar apenas em "exit code 0").
3. **`DATE_TRUNC(..., WEEK(MONDAY))`** define o início da semana ISO (segunda-feira) — usado consistentemente em todo o pipeline semanal.
4. Export de CSVs em `outputs/` com sufixo `_{YYYYMMDD}` (data de execução): `tempo_reversa_*`, `tempo_compra_reversa_*`, `tempo_decomposicao_compra_entrega_reversa_*`, `okr_td_kr_mensal_*`, `okr_td_hm_semanal_*`, `okr_td_cohort_diario_*`.

## Ver também
- [`relatorio_td/pipeline_reversas.md`](../relatorio_td/pipeline_reversas.md) — pipeline de **priorização de produtos por reversa** (análise diferente: usa `fpa.analytical_dre`, clustering, scorecard — não é o OKR de T&D).
- [`bigquery_tables.md`](../bigquery_tables.md) — catálogo geral de tabelas do projeto.
