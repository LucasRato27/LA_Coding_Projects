# Lead Time Dashboard — Bases de Dados e Premissas

Arquivo: `analytics/lead_time/lead_time_dashboard.ipynb`
Projeto BigQuery: `insider-data-lake` (principal) + `insider-lake-sensitive` (dados de malharia/artigos, prepared_br)

## Tabelas BigQuery utilizadas

### Célula 1 — SQL_CAPACITY (df_capacity): capacidade, lead time teórico e mark-up por fornecedor×produto
- `integrated.muninn_products` (full_price)
- `integrated.muninn_fabric_skus`, `integrated.muninn_fabrics`, `integrated.muninn_articles`,
  `integrated.muninn_knitting_factories`, `integrated.muninn_suppliers` (custo de tecido/malharia)
- `integrated.muninn_product_skus_fabrics`, `integrated.muninn_product_skus`, `integrated.skus` (SKU × tecido)
- `integrated.muninn_apparel_manufacturers_products` (status: available/approved/incubation; `lead_time` cadastrado; `order_minimum_volume`)
- `integrated.muninn_apparel_manufacturer_production_units_products` + `..._production_units` (capacidade semanal/mensal por célula produtiva)
- `sop_silver.demand_prediction_input` → curva ABC por `product_name` (últimos 3 meses fechados; A≤80%, B≤95%, C>95% de receita acumulada)
- Filtro final: `product_state NOT IN ('desativado')`

### Célula 1.1 — SQL_LEAD_TIME_CADASTRO_AUDIT (auditoria de LT cadastrado)
- Reusa `integrated.skus`, `integrated.muninn_products`, `integrated.muninn_apparel_manufacturer_production_units_products`,
  `integrated.muninn_apparel_manufacturers_products`, `integrated.muninn_apparel_manufacturers`, `integrated.muninn_suppliers`
- Classifica `product_status_classificado` (ativo/inativo/status_indeterminado) e `lead_time_status` (ok/zerado/nulo/negativo/invalido)
- **Premissa crítica de saneamento**: linhas com `is_invalid_lead_time=True` são REMOVIDAS de `df_capacity` antes de qualquer análise (evita LT=0/nulo contaminar KPIs). Exportado em `exports/lead_time_cadastrado_auditoria.csv`.

### Célula 2 — SQL_OPS (df_ops_raw): OPs com timestamps de etapas
- Fonte: `sop_silver.supply_chain_efficiency_model_input_history`
- Grão: `(op_code, product_sku, ingestion_date)` — 1 linha por op_code após agregação (`ANY_VALUE`/`MAX`/`MIN` por op_code)
- Stamps extraídos via `MIN(ingestion_date)` por `current_production_stage`:
  `order_request_validation → waiting_fabric_arrival → fabric_validation_and_pre_cutting → cut_fabric_and_sewing_process → quality_inspection → items_delivery_and_invoicing → finished`
- A extração ampla preserva OPs no grão de `op_code`, `dt_planned_original` e
  `is_op_open` mais recente. O Dashboard Geral aplica seu cohort de entrega
  no pré-processamento; Ciclo Produtivo usa a extração ampla com janela própria.
- Filtros estruturais: tipo de OP selecionado, `current_production_stage !=
  'canceled'` e ciclos sem B2B/EPA. Fornecedor encerrado e SKU desativado são
  preservados como histórico válido.
- Todas as leituras da fonte de OPs aplicam `production_order_type = 'committed'` na própria SQL. `ANY_VALUE(production_order_type)` pode ser preservado na agregação e o filtro no dataframe permanece como defesa adicional.
- `stamp_created_production_order = TIMESTAMP(DATE(MIN(created_at)))`. Não há
  fallback para o primeiro `ingestion_date`; OP sem `created_at` fica fora de
  KPIs dependentes de criação.

### Célula 2.5 — SQL_POSTPONEMENT (df_postponement): postergação intencional por OP
- Mesma fonte `sop_silver.supply_chain_efficiency_model_input_history`
- Reconstrói mudanças dia-a-dia em `dt_planned_entry_warehouse` (LAG por op_code, dedup intradia por `ROW_NUMBER`)
- Soma apenas deltas POSITIVOS (postergações reais, não antecipações) = `qt_dias_postergacao_intencional`
- **Distinção de escopo**: mudança em `dt_planned_entry_warehouse` = postergação interna (Insider) → REMOVIDA do lead time limpo.
  Mudança em `dt_reviewed_entry_warehouse` = atraso do fornecedor → NÃO removida (fora do escopo deste ajuste)

### Célula 2.1 — SQL_FABRIC_TEMPO (df_fabric_tempo): tempo de MP para triangulação
- `integrated.muninn_fabric_skus/fabrics/articles/knitting_factories/suppliers` (custo/malharia)
- `fpa.analytical_dre` + `integrated.skus`: filtra produtos com venda nos últimos 8 meses (`skp_with_sales_l8m`)
- `integrated.skus` → status do produto (ativo_perene/ativo_em_lancamento/ativo_capsula/personalizacao/kit/desativado)
- Exclusões manuais por nome de produto: `ziraldo`, `xp`, `maluquinho`, `b2b` (linhas de colab/B2B fora do escopo de triangulação padrão)
- **`insider-lake-sensitive.prepared_br.prepared_muninn_articles_knitting_factories`** (+ `prepared_muninn_knitting_factories`, `prepared_muninn_suppliers`, `prepared_muninn_articles`)
  → única fonte de `tempo_tingimento_dias` (coloring_time) e `tempo_producao_dias` (production_time) por artigo/malharia
- Artigo normalizado via `REGEXP_REPLACE(article_name, r'Modal (\d+)', 'Modal')` para agrupar variantes de Modal
- Tecido principal do produto = artigo com maior `consumo_mediano` (APPROX_QUANTILES mediana) por produto

## Premissas de negócio (parametrizadas em CONFIG, Célula 0)

| Parâmetro | Valor | Significado |
|---|---|---|
| `TARGET_LEAD_TIME` | 120 dias | Padrão atual Insider — usado para `dentro_do_prazo` |
| `janela_meses` | 12 | Janela temporal padrão de análise |
| `janela_meses_curta` | 3 | Janela para tendência recente |
| `percentil_recomendacao` | 0.75 | Percentil usado no prazo recomendado por fornecedor×fluxo (0.5=agressivo, 0.9=conservador) |
| `threshold_desvio_atencao` | 10 dias | Status "atenção" (laranja) |
| `threshold_desvio_critico` | 30 dias | Status "crítico" (vermelho) |
| `buckets_volume` | [0,200,500,1000,2000,5000,∞] | Faixas de volume por OP |
| `min_ops_recomendacao` / `min_ops_grafico` | 5 / 3 | Mínimo de OPs para incluir agregado |

## Premissas de cálculo do lead time

- **Dois fluxos com metodologia diferente**:
  - **PA (produto acabado)**: lead time começa em `stamp_stage_waiting_fabric_arrival` (reserva de MP), exclui `etapa_criacao_agd` (fica NaN)
  - **Tri (triangulação)**: lead time começa em `stamp_created_production_order` (criação da OP), inclui todas as 7 etapas
- Fim do lead time (ambos os fluxos): `dt_largest_entry_warehouse`
- Cohort só inclui OPs com `dt_largest_entry_warehouse <= hoje` (exclui datas planejadas futuras)
- Lead time realizado nulo ou ≤0 é descartado
- Filtro de escopo: apenas `production_order_type == 'committed'`; exclui ciclos com `B2B` ou `EPA` no `cycle_name`
- Etapas negativas (inversão de stamps) são clipadas em 0 via `_diff_days`

## Lead Time Limpo (postergação intencional) — ver também memória `lead_time_dashboard/lead_time_limpo`

- `lead_time_ajustado = (lead_time_realizado_bruto - qt_dias_postergacao_intencional).clip(lower=0)`
- `lead_time_realizado` é SOBRESCRITO pelo valor ajustado — todos os gráficos downstream usam o lead time limpo automaticamente
- Mediana de referência (baseline) por etapa calculada apenas em OPs SEM postergação, agrupado por `(supplier_name, product_names, is_finished_product_order)`
- `etapa_inflada` = etapa com maior desvio POSITIVO vs. mediana baseline (só para OPs com postergação)
- Validação: alerta se baseline < 30 OPs total ou grupo < 5 OPs (medianas instáveis)

## Ajuste de triangulação (Tri) — soma do tempo de MP

- `lead_time_teorico` (Tri) += `tempo_total_dias` (tingimento + produção na malharia) vindo de `df_fabric_tempo`
- Fallback: `FALLBACK_TRI_DIAS = 60` dias quando não há mapeamento de tecido principal para o produto

## Auditoria e saneamento de dados (guard rails)

1. **LT cadastrado inválido** (zerado/nulo/negativo) em produto ATIVO → removido de `df_capacity`, listado em `df_lead_time_cadastro_audit`
2. **Join sem LT teórico após merge** → OP permanece nas análises realizadas e
   de Ciclo Produtivo; é registrada em `df_lead_time_join_audit` e excluída
   somente de consumidores que dependem do LT teórico.
3. Ambas auditorias compartilham o mesmo schema de colunas para consolidação/export
4. OP com `created_at` posterior a `dt_planned_original` ou, quando entregue,
   posterior a `dt_largest_entry_warehouse` é registrada em
   `df_creation_chronology_audit` e excluída dos KPIs.

## Filtros interativos (Célula 1.5 — pensados para Deepnote)

- `supplier_filtro`, `product_filtro` ('(Todos)' = sem filtro)
- `data_inicio` (padrão '2026-01'), `data_fim` (padrão = mês atual) — aplicados em `dt_planned_original`, com o mesmo limite superior exclusivo de `data_fim + 1 mês` nas seções Geral e SLA

## Seção Deepnote — governança de SLA planejado <100 dias

As regras vigentes da seção antes chamada “SLA-90” — incluindo `sla_planejado_dias`, `lead_time_realizado`, threshold, taxonomia Base/Extra, mês-alvo, KPIs, gráficos e exportação — estão consolidadas em `mem:lead_time_dashboard/sla90_deepnote_premissas`.
