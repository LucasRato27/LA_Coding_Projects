# Relatório T&D — Pipeline de Reversas e Priorização de Produtos

## Localização
`analyses/relatorio_td/`

## Arquivos
| Arquivo | Descrição |
|---|---|
| `pipeline_reversas_priorizacao_produtos.sql` | ⚠️ DEPRECATED desde 2026-06-18 — SQL canônico vive nas células do notebook Unificado |
| `td_analysis_functions.py` | Funções Python de análise, scoring, QA, export |
| `TD_Priorizacao_Melhorias_Unificado.ipynb` | **Notebook canônico** — SQL e Python inlined nas células |
| `TD_Priorizacao_Melhorias_v20260611.ipynb` | Versão anterior (referência) |
| `README.md` | Relatório com resultados reais populados via BigQuery |
| `REGRAS_NEGOCIO_TD_PRIORIZACAO.md` | Documentação das regras de negócio e fórmulas |
| `GOVERNANCA_PRIORIZACAO_MELHORIAS.md` | Documentação de governança e sinais de decisão |

## Duas Camadas de Output
| Camada | Granularidade | Uso |
|---|---|---|
| **Camada 2 — Executiva** (query 1 no arquivo) | `product_name × category × gender` | Farol decisório |
| **Camada 1 — Analítica** (query 2 no arquivo) | `order_id × order_name × sku × problema_tag` | Auditoria/drill-down |

## Tabelas Usadas (v2 — 2026-06-18)
1. `business.insider_orders` — pedidos válidos (`order_status = 'paid'`, `is_cancelled = FALSE`, exclusão cupons TF-/TFIN/IR, lojas world + store-loja)
2. `business.insider_order_items` — itens (GROUP BY order_id, sku; campos: sku, quantity, product_title, variant_color, variant_size)
3. `fpa.analytical_dre` — receita e volume por product_name (`revenue_after_discounts`, `non_refunded_quantity`; JOIN com orders + sku_dim)
4. `integrated.skus` — dimensão SKU (fallback color/size, product_name canônico, sku_state)
5. `prepared_br.prepared__troquecommerce_order_details_br` — reversas (dedup por `(order_name, id_reversa, sku)`, project: `insider-lake-sensitive`)
6. `sop_silver.return_reason_tags` — tags qualitativas via UNNEST (⚠️ grão `order_name × sku`, sem `id_reversa`)
7. `sop_silver.portfolio_skp_clustering` — cluster estratégico (**grão: `product_name`, sem coluna `sku`**)
8. `sop_bronze.eval_produto_portfolio` — scorecard de portfólio com 3 pilares

## Validações Executadas via BigQuery (2026-06-19)

Três pontos críticos da migração v2 foram validados em produção:

### Ponto 1 — `order_id` no DRE × JOIN com `business.insider_orders`
- **66.3%** dos `order_id` do DRE (amostra 500k) batem com `insider_orders` filtrado (paid, not cancelled, lojas corretas)
- Os ~33.7% não-macheados são esperados: pedidos de outras lojas, cancelados ou não pagos — exatamente o que os filtros excluem
- ✅ O JOIN `dre.order_id = o.order_id` funciona corretamente no pipeline
- ⚠️ `fpa.analytical_dre` **NÃO tem coluna `date`** — filtro de período deve vir exclusivamente do JOIN com `insider_orders` (via `processed_at`)

### Ponto 2 — `non_refunded_quantity` vs `qt_items` bruto
- **Ratio = 0.9847** — DRE retorna ~98.5% do volume bruto de `business.insider_order_items`
- A diferença de ~1.5% é semanticamente correta: `non_refunded_quantity` já desconta itens efetivamente devolvidos
- Volume total validado: ~2.81M itens brutos → ~2.77M `non_refunded`, receita R$337M
- **Impacto na `td_rate`:** denominador ligeiramente maior que o antigo (bruto), tornando a taxa de T&D levemente mais conservadora — comportamento mais correto
- ✅ CTE `dre_product_metrics` e `COALESCE` em `sales_product_metrics` estão corretos

### Ponto 3 — Múltiplas reversas por `(order_name, sku)` com dedup por `id_reversa`
- **0%** dos pares `(order_name, sku)` têm múltiplas reversas distintas nos últimos 12 meses
- Média = 1.000 reversa por par; máximo = 1
- O `COUNT DISTINCT` downstream é precaução inofensiva — não há risco de inflação com dados atuais
- ✅ Migração segura; nenhum ajuste adicional necessário

## Bugs Críticos já Corrigidos no SQL
- **B1** — `portfolio_clustering` usava `PARTITION BY sku` mas a tabela não tem `sku`; corrigido para `GROUP BY product_name`
- **B2** — `COALESCE(qt_items_returned, 1)` inflava T&D; corrigido para `COALESCE(..., 0)` na fonte
- **B3** — zona cinza no farol documentada no `ELSE 'Monitorar'`
- **B5** — `pct_top_3_total` podia > 100%; corrigido com denominador `COUNT DISTINCT (order_name|sku)` real
- **B7** — duplicação de reversa quando SKU em múltiplos `order_item_id`; corrigido com `order_items_grouped`

## Join Canônico — Scorecard × T&D
```sql
LEFT JOIN `sop_bronze.eval_produto_portfolio` e ON e.product_name = sib.product_name
LEFT JOIN `sop_silver.portfolio_skp_clustering` c ON c.product_name = e.product_name
```

## Resultados Confirmados via BigQuery (2026-06-11, últimos 12 meses)

Fonte: execução real da Camada 2 (`pipeline_reversas_priorizacao_produtos.sql`).

| Farol | Produtos | Itens Retornados | TD Rate Médio |
|---|---|---|---|
| Priorizar melhoria | 44 | 89.078 | 14,1% |
| Alerta em produto relevante | 43 | 133.797 | 6,3% |
| Monitorar | 46 | 9.554 | 13,0% |
| Sem evidência suficiente | 88 | 70 | 3,9% |
| Não priorizar agora | 66 | 9.884 | 4,2% |
| **Total** | **287** | **242.383** | — |

- **Receita total no período:** R$ 461.242.596
- `eval_produto_portfolio` cobre ~126 de 287 produtos; restantes ficam com scores `—`
- `portfolio_skp_clustering` cobre ~205 produtos

## Pipeline Python — Funções em `td_analysis_functions.py`

Arquivo: `analyses/relatorio_td/td_analysis_functions.py`

14 funções públicas confirmadas via smoke test (2026-06-11):

| Função | Descrição |
|---|---|
| `validate_columns` | Verifica colunas obrigatórias; levanta ValueError se faltam |
| `coerce_numeric` | Converte colunas selecionadas para numérico |
| `prepare_executive_df` | Valida colunas, coerce numérico, ordena por `priority_score DESC` |
| `classify_priority` | Replica CASE SQL em Python via `np.select` — QA de labels (espera 100% match) |
| `priority_summary` | Agrega por `sinal_priorizacao`: produtos, unidades, receita, itens retornados, td_rate médio |
| `top_offenders` | Top N produtos por bucket, ordenados por `priority_score, qt_items_returned, td_rate` |
| `category_benchmark` | Benchmark por categoria: produtos, volume, receita, itens retornados, td_rate, produtos_priorizar |
| `problema_tipo_split` | Breakdown por tipo (Físico/Logístico/Outros·Desistência) via `qt_reversas_fisico` + `qt_reversas_logistico` |
| `tag_summary_from_executive` | Agrega top tags das colunas pivotadas `top_1/2/3_problema` |
| `validate_no_tag_duplication` | QA: compara total returned exec vs analítica dedup, tolerance 1% |
| `build_scorecard_product_view` | Merge exec × scorecard_df → coluna `top_3_motivos_td` em HTML `<br>` |
| `build_llm_context_rows` | Dicts compactos por produto para uso em prompts LLM (sem chamar LLM) |
| `create_product_summary_prompt` | Prompt estruturado para resumo qualitativo de 1 produto (≤80 palavras) |
| `export_priority_workbook` | Excel com abas: `farol_executivo`, `resumo_farol`, `benchmark_categoria`, `resumo_tags`, `priorizar_melhoria`, `amostra_analitica` |

**Constantes:**
- `EXECUTIVE_REQUIRED_COLUMNS` — 12 colunas obrigatórias
- `ANALYTICAL_REQUIRED_COLUMNS` — 10 colunas obrigatórias (inclui `tipo_problema`)
- `PrioritizationThresholds` — dataclass frozen com todos os thresholds

## Fórmula de Scoring (valores vigentes — v2, 2026-06-20)

### `td_score` (INALTERADO)
```
td_score = 0.5 × percentil_td_rate
         + 0.3 × percentil_volume_td
         + 0.2 × percentil_delta_vs_categoria
```

### `commercial_score_v2` (SUBSTITUI `commercial_score` — 2026-06-20)
```
commercial_score_v2 = 0.70 × tracao_vendas_score
                    + 0.30 × mc3_score
```

**Bloco 1 — `tracao_vendas_score` (70%)**
```
tracao_vendas_score = 0.30 × percentil_sell_through_30d
                    + 0.30 × percentil_sell_through_60d
                    + 0.25 × percentil_sell_through_90d
                    + 0.15 × percentil_receita_vs_categoria
```
- `sell_through_Nd` = unidades vendidas em N dias após `first_sale_date` / `physical_stock` no dia de primeira venda
- `first_sale_date` vem de `sop_silver.portfolio_skp_clustering`
- Estoque vem de `integrated.stock` (JOIN: `sku × stock_date = first_sale_date`)
- Vendas vem de `business.insider_order_items` + `business.insider_orders` (paid, not cancelled)
- `receita_media_mensal_vs_categoria` = `receita_liquida / AVG(receita_liquida) OVER (PARTITION BY category)` → CUME_DIST particionado por category

**Fallback de sell-through:** quando `sell_through_30d` é NULL (sem cobertura de SKU/estoque), `tracao_vendas_score` cai para o legado: `0.60 × percentil_receita + 0.40 × percentil_unidades`

**Bloco 2 — `mc3_score` (30%)**
```
mc3_score = 0.50 × mc3_vs_categoria_score
           + 0.30 × mc3_vs_portfolio_score
           + 0.20 × representatividade_mc3_score
```
- `mc3_vs_categoria_score`: CASE WHEN mc3_ratio >= mc3_ratio_cat4 THEN 1.00 / >= 0.90×cat4 → 0.75 / >= 0.75×cat4 → 0.50 / ELSE 0.25
- `mc3_vs_portfolio_score`: mesma escala discreta vs `mc3_ratio_portfolio` (mediana do portfólio, pré-computada na tabela)
- `representatividade_mc3_score`: CUME_DIST() OVER (ORDER BY net_profit_after_marketing_costs)
- Fonte: `sop_bronze.eval_produto_portfolio` — campos: `mc3_ratio`, `mc3_ratio_cat4`, `mc3_ratio_portfolio`, `net_profit_after_marketing_costs`
- Cobertura confirmada: **126/126** linhas com `mc3_ratio` preenchido (2026-06-19)

**Fallback de MC3:** quando `mc3_ratio` é NULL (produto fora de `eval_produto_portfolio`), `commercial_score_v2` usa apenas `tracao_vendas_score` com peso total (1.00)

**CASE WHEN canônico no `scored` CTE (BigQuery — sem referência a alias da mesma cláusula SELECT):**
```sql
CASE
  WHEN mc3_vs_categoria_score IS NOT NULL
  THEN
    0.70 * COALESCE(
        0.30 * percentil_sell_through_30d + 0.30 * percentil_sell_through_60d
        + 0.25 * percentil_sell_through_90d + 0.15 * percentil_receita_vs_categoria,
        0.60 * percentil_receita + 0.40 * percentil_unidades
      )
    + 0.30 * (0.50 * mc3_vs_categoria_score + 0.30 * mc3_vs_portfolio_score
              + 0.20 * representatividade_mc3_score)
  ELSE
    COALESCE(
      0.30 * percentil_sell_through_30d + 0.30 * percentil_sell_through_60d
      + 0.25 * percentil_sell_through_90d + 0.15 * percentil_receita_vs_categoria,
      0.60 * percentil_receita + 0.40 * percentil_unidades
    )
END AS commercial_score_v2
```

### `priority_score`
```
priority_score = 0.50 × td_score + 0.50 × commercial_score_v2
```
(era `0.60 × td_score + 0.40 × commercial_score` antes de 2026-06-20)

### Novas CTEs adicionadas ao pipeline (SQL — antes do bloco SCORING)
| CTE | Fonte | O que faz |
|---|---|---|
| `sku_map_st` | `integrated.skus` | Mapeamento product_name → sku (para sell-through) |
| `first_sale` | `sop_silver.portfolio_skp_clustering` | `first_sale_date` por produto |
| `sales_windows` | `business.insider_orders` + `business.insider_order_items` | Unidades vendidas em 30/60/90d após first_sale |
| `launch_stock` | `integrated.stock` | Estoque físico no dia de first_sale (por sku) |
| `sell_through` | `sales_windows` + `launch_stock` | `SAFE_DIVIDE(sold_Nd, initial_stock)` por produto |
| `mc3_base` | `sop_bronze.eval_produto_portfolio` | mc3_ratio, mc3_ratio_cat4, mc3_ratio_portfolio, net_profit |

Os campos de sell-through e MC3 entram no inner SELECT da CTE `percentiles` via LEFT JOIN:
```sql
LEFT JOIN sell_through st ON spm.product_name = st.product_name
LEFT JOIN mc3_base mc ON spm.product_name = mc.product_name
```

## Sistema de Sinais (dois níveis)

**Nível 1 — `sinal_priorizacao` (SQL, 5 buckets):**
| Sinal | Critério |
|---|---|
| Priorizar melhoria | td_score ≥ 0.70 e commercial_score_v2 ≥ 0.60 |
| Alerta em produto relevante | commercial_score_v2 ≥ 0.70 e 0.50 ≤ td_score < 0.70 |
| Monitorar | td_score ≥ 0.70 e commercial_score_v2 < 0.60 (ou zona cinza) |
| Não priorizar agora | td_score < 0.50 ou commercial_score_v2 < 0.40 |
| Sem evidência suficiente | < 30 vendas ou < 5 retornos |

**Nível 2 — `sinal_kill_keep` (Python, regra QA de 2 estados via `np.select`):**
| Sinal | Critério |
|---|---|
| Priorizar melhoria | td_score ≥ 0.70 e commercial_score_v2 ≥ 0.60 → Alta T&D + Alta Tração |
| Não Priorizar (Avaliar Descontinuação/Reformulação) | td_score ≥ 0.70 e commercial_score_v2 < 0.60 → Alta T&D + Baixa Tração |
| Não priorizar agora | demais casos |
| Sem evidência suficiente | < 30 vendas ou < 5 retornos |

**Coluna DataFrame usada:** `commercial_score_v2`
**Constante de nome de coluna:** `COMMERCIAL_SCORE_COL = "commercial_score_v2"` — definida em `td_analysis_functions.py` e na célula Python do notebook; usada em `classify_priority()` e `build_llm_context_rows()`
**Thresholds em `PrioritizationThresholds`:** nomes mantidos sem alteração (`commercial_score_prioritize = 0.60`, `commercial_score_alert = 0.70`, `low_commercial_score = 0.40`) — descrevem semântica dos cortes, não o nome físico da coluna

**`EXECUTIVE_REQUIRED_COLUMNS`** (atualizado): inclui `"commercial_score_v2"` (substituiu `"commercial_score"`)
**`prepare_executive_df` numeric_columns**: inclui `"commercial_score_v2"`

## Scorecard — Fonte Correta e Padrão de Query

Os pilares do scorecard **NÃO estão em `portfolio_cluster_payload`**. A coluna `portfolio_cluster_payload` é `TO_JSON_STRING` de `portfolio_skp_clustering`, que contém apenas campos de cluster/estado/métricas — **sem os score columns**.

**Fonte real:** `sop_bronze.eval_produto_portfolio` — query direta necessária.

```sql
-- Padrão canônico: join scorecard × cluster
SELECT
  e.product_name,
  c.cluster,
  c.skp_state,
  ROUND(e.score_vendas_geral * 100, 1)            AS score_tracao_comercial,
  ROUND(e.score_viabilidade_financeira * 100, 1)  AS score_unit_economics,
  ROUND(e.score_satisf_cliente * 100, 1)          AS score_satisfacao_marca,
  ROUND(e.product_devolution_ratio * 100, 2)      AS td_produto_pct,
  ROUND(e.category4_devolution_ratio * 100, 2)    AS td_categoria_pct
FROM `insider-data-lake.sop_bronze.eval_produto_portfolio` e
LEFT JOIN `insider-data-lake.sop_silver.portfolio_skp_clustering` c
  ON c.product_name = e.product_name
```

Em Python, passar o resultado como `scorecard_df` para `build_scorecard_product_view(executive_df, scorecard_df)`.

## Pilares do Scorecard (`eval_produto_portfolio`)
| Campo | Pilar | Escala |
|---|---|---|
| `score_vendas_geral` | Tração Comercial | 0–1 (×100 para exibição) |
| `score_viabilidade_financeira` | Unit Economics | 0–1 |
| `score_satisf_cliente` | Satisfação e Marca | 0–1 |
| `product_devolution_ratio` | T&D Produto | 0–1 (já em decimal) |
| `category4_devolution_ratio` | T&D Categoria | 0–1 |

## Classificação de Cluster (`portfolio_skp_clustering`)
Valores: KILL · Long tail · Core · Hero · Lancamento · Breakthrough · Outros
- `processed_at` — data de referência mais recente (2026-06-11 confirmado)
- Cobertura: 205 produtos na tabela de clustering; 126 na `eval_produto_portfolio`

## Ambiente Python

- **Virtualenv:** `.venv-1` (Python 3.13)
- **Ativar:** `source /Users/insider/LA_Coding_Projects/.venv-1/bin/activate`
- **Pacotes instalados nesta análise:** `numpy`, `pandas`, `google-cloud-bigquery`, `google-cloud-bigquery[pandas]`, `db-dtypes`, `openpyxl`
- **Credenciais BigQuery:** ADC via `gcloud auth application-default login` (confirmado ativo em 2026-06-11)

## Como Re-rodar e Atualizar o README

O README é populado executando scripts Python que rodam as queries no BigQuery e injetam os resultados nas seções. Padrão de script:

```python
sql_raw = SQL_FILE.read_text(encoding='utf-8')
idx = sql_raw.find('-- CAMADA 1: TABELA ANALÍTICA ITEM / REVERSA / TAG')
SQL_EXEC = sql_raw[sql_raw.find('WITH '):idx].strip()
client = bigquery.Client(project='insider-data-lake')
df_exec = client.query(SQL_EXEC).to_dataframe()
# ... computar diagnósticos com td_analysis_functions
# ... injetar nas seções do README via string replace
```

O arquivo SQL tem **Camada 2 primeiro** (executiva) e **Camada 1 depois** (analítica), separadas pelo header `-- CAMADA 1: TABELA ANALÍTICA ITEM / REVERSA / TAG`.

## Notebook — Estrutura (TD_Priorizacao_Melhorias_v20260611.ipynb)

18 células em `analyses/relatorio_td/`:
1. Setup + imports
2. BigQuery client
3. Carregamento e split das queries do SQL file
4. Execução Camada 2
5. Execução Camada 1 (toggle `LOAD_ANALITICA = True/False`)
6. QA: labels Python vs SQL
7. QA: anti-duplicação de tags
8–15. Diagnósticos (resumo farol, top produtos, alerta, benchmark, tags, split físico/logístico, tendência, concentração cor/tamanho)
16. Export Excel + CSV + contexto LLM

## Padrões SQL Críticos do Pipeline

### CTE `order_items_grouped` (fix B7 — ambas as camadas)
Consolida itens em `(order_id, sku)` **antes** do join com reversas para evitar duplicação:
```sql
order_items_grouped AS (
  SELECT order_id, sku,
    ANY_VALUE(product_title) AS product_title,
    ANY_VALUE(variant_color) AS variant_color,
    ANY_VALUE(variant_size) AS variant_size,
    SUM(quantity) AS qt_items
  FROM order_items
  GROUP BY order_id, sku
)
```
Na Camada 2, `sales_item_base` faz JOIN direto com `order_items_grouped` (sem GROUP BY próprio).
Receita **não** vem mais de `item_order_total` — vem do DRE via `dre_product_metrics`.

### CTE `dre_product_metrics` (v2 — receita do DRE)
```sql
dre_product_metrics AS (
  SELECT s.product_name,
    SUM(dre.non_refunded_quantity) AS qt_items_vendidos,
    SUM(dre.revenue_after_discounts) AS receita_liquida
  FROM `insider-data-lake.fpa.analytical_dre` dre
  JOIN orders o ON dre.order_id = o.order_id
  JOIN sku_dim s ON dre.sku = s.sku
  WHERE s.sku_state IN ('ativo_perene', 'ativo_em_lancamento')
  GROUP BY s.product_name
)
```
`sales_product_metrics` usa `COALESCE(ANY_VALUE(dpm.qt_items_vendidos), SUM(sib.qt_items))`.

### Dedup de reversas (v2 — por id_reversa)
```sql
WHERE r.id_reversa IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY r.order_name, r.id_reversa, r.sku
  ORDER BY r.updated_at DESC
) = 1
```
⚠️ Pode gerar múltiplas linhas por (order_name, sku) — downstream usa COUNT DISTINCT para proteger contagens.

### CTE `td_reversas_total` (fix B5 — denominador correto de tags)
```sql
td_reversas_total AS (
  SELECT product_name, category, gender,
    COUNT(DISTINCT CONCAT(order_name, '|', sku)) AS qt_reversas_total
  FROM td_joined
  GROUP BY product_name, category, gender
)
```
`top_tags` usa `SAFE_DIVIDE(qt_reversas_tag, qt_reversas_total)` — não a soma de qt_reversas_tag por tag.

### `portfolio_clustering` (fix B1 — sem coluna sku)
```sql
portfolio_clustering AS (
  SELECT product_name,
    ANY_VALUE(cluster) AS portfolio_cluster,
    ANY_VALUE(TO_JSON_STRING(pc)) AS portfolio_cluster_payload
  FROM `insider-data-lake.sop_silver.portfolio_skp_clustering` pc
  GROUP BY product_name
)
```
JOIN na Camada 2: `pc.product_name = sib.product_name` (via `sales_product_metrics`).
JOIN na Camada 1: `pc.product_name = COALESCE(s.product_name, oi.product_title)`.

## Classificação `tipo_problema` (adicionada no pipeline)
| Tipo | Exemplos de motivos |
|---|---|
| Físico | Tamanho, Defeito, Desbotamento, Insatisfação, Cor diferente |
| Logístico | Produto errado, Pedido duplicado, Falha/Demora na entrega |
| Desistência | Arrependimento, Desistência |
