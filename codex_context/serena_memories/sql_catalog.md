# SQL Files — Catálogo

## SQLs Principais (usados no notebook)

| Arquivo | Propósito | Parâmetros |
|---------|-----------|------------|
| `receb.sql` | Recebimentos no warehouse: une OPs, NFs, SKUs, fornecedores, células e ciclos. Empilha dados reais (SFTP warehouse) e forecast. | `{day_ini}`, `{day_end}` |
| `ops.sql` | Visão completa das Ordens de Produção: estágio produtivo, datas planejadas/reais/revisadas, quantidades planejadas/cortadas/recebidas, ciclos. | Sem parâmetros |
| `sales.sql` | Vendas históricas por SKU/dia (últimos 24 meses fechados): receita pós-desconto, pós-reembolso e qtd vendida. Fonte: DRE. | Sem parâmetros |
| `stock.sql` | Estoque valorizado por SKU: estoque físico × custo planejado/faturado/preço cheio/PMV. Inclui composição de artigo/tecido. | Sem parâmetros (filtro hardcoded ≥ 2025-07-01) |
| `supplier_info.sql` | Performance consolidada de fornecedores: nota SCALE, qtd entregue, atraso médio, taxa defeito, ICP, markup. | `{day_ini}`, `{day_end}` |

## SQLs — Revisão Capacidades / Farol (`20260424_Revisão capacidades/1_Inputs/1_SQL/`)

| Arquivo | Propósito | Parâmetros |
|---------|-----------|------------|
| `capacity_history.sql` | Histórico mensal de capacidade por célula × produto × mês. Fonte: `sop_gold.supplier_capacity_history`. Dedup `is_finished_product=TRUE` via ROW_NUMBER. | `{day_ini}`, `{day_end}` (aplicados ao `DATE_TRUNC(ingestion_date, MONTH)`) |
| `deliveries_by_cell.sql` | Entregas reais (warehouse inbound) por célula × produto × mês. **cell_number/cell_label vêm do Muninn** via CTE_CELL_MUNINN (ampup→amp→am→ampu), join em `apparel_manufacturer_id + product_name`. Não usa cell da OP. | `{day_ini}`, `{day_end}` (aplicados ao `dt_entry_warehouse`) |
| `planned_load.sql` | Carga planejada (OPs abertas) por célula × produto × mês. Usa COALESCE(dt_reviewed_entry_warehouse, dt_planned_entry_warehouse) como data de entrega. | `{day_ini}`, `{day_end}` |
| `cell_data.sql` | Snapshot atual de células ativas (status IN available/approved/incubation). Traz `label`, `apparel_manufacturer_production_unit_id`, `apparel_manufacturer_cell_number`. Sem parâmetros. | — |

## SQLs do Claudio (análises complementares)

| Arquivo | Propósito |
|---------|-----------|
| `article_use.sql` | Consumo típico de artigo/tecido por produto (mediana) |
| `cell_data.sql` | Base analítica de células produtivas: capacidade, custo, markup, mix de produtos, classificação ABC |
| `mkup_entregue.sql` | Markup e custo real de OPs entregues: CMV planejado vs real, custo MP, qualidade, atraso |
| `mkup_planejado.sql` | Markup planejado da carteira aberta: custos estimados, datas revisadas |
| `receb_futur.sql` | Recebimentos futuros previstos (só FORECAST): OP, SKU, célula, fornecedor |
| `receb_hist.sql` | Recebimentos históricos (REAL + FORECAST empilhados) |
| `scale_monthly.sql` | Tabela SCALE mensal consolidada: notas de atraso, custo, ICP, qualidade |
| `scale_ra.sql` | Base detalhada SCALE para cálculo de ICP médio por fornecedor/produto |
| `stock.sql` (Claudio) | Estoque valorizado por SKU (mesma estrutura do principal) |
| `supplier_cap_hist.sql` | Histórico mensal de capacidade cadastrada por fornecedor e produto |
| `supplier_monthly_info.sql` | Performance mensal do fornecedor: qualidade e atraso |

## SQLs — OKR Estabilidade Planejamento (`20260420_OKR Estabilidade Planejamento/1_Inputs/1_SQL/`)

| Arquivo | Propósito |
|---------|-----------|
| `baseline_por_coorte.sql` | Primeiro snapshot sem pending por `cycle_name`. Classifica `cycle_type` (Base vs Extra via regex `^C\d{2}20\d{2}$`) |
| `plano_vs_atual.sql` | Estado baseline vs estado atual por OP-SKU. Flags: `INT_DATE`, `INT_CANCEL`, `INT_GRADE`, `EXT_DATE_REV`. Fonte: `_history` + `muninn_production_orders` |
| `frequencia_revisoes.sql` | Revisões entre snapshots consecutivos via `LAG()`. Conta mudanças e magnitudes (dias) por tipo INT/EXT |
| `timeline_revisoes.sql` | Drill-down de todos os snapshots de uma OP-SKU (parametrizado: `{op_code}`, `{product_sku}`) |

## SQLs — Relatório T&D (`analyses/relatorio_td/`)

| Arquivo | Propósito |
|---------|-----------|
| `pipeline_reversas_priorizacao_produtos.sql` | Pipeline completo de T&D em dois statements separados por `-- CAMADA 1:` header. **Camada 2 primeiro** (executiva, ~750 linhas), **Camada 1 depois** (analítica, ~400 linhas). Sem parâmetros externos — período hardcoded como `INTERVAL 12 MONTH`. Projetos: `insider-data-lake` + `insider-lake-sensitive`. |

**Camada 2 — Executiva** (grain: `product_name × category × gender`):
- CTEs: `params` → `orders` → `order_items` → `order_items_grouped` → `sku_dim` → `portfolio_clustering` → `sales_item_base` → `reversas_unicas` → `reversas_tags_latest` → `reversas_tag` → `sales_product_metrics` → `td_joined` → `td_product_metrics` → `category_metrics` → `tag_counts` → `td_reversas_total` → `tag_ranked` → `top_tags` → `color_concentration` → `main_color` → `size_concentration` → `main_size` → `comments_sample` → `trend_base` → `trend_classified` → `percentiles` → `scored` → `final`
- Output: 44 colunas, ordenado por `sinal_priorizacao` priority order + `priority_score DESC`

**Camada 1 — Analítica** (grain: `order_id × order_name × sku × problema_tag`):
- Estrutura mais simples; LEFT JOIN em `reversas_tag` expande linhas por tag

## SQLs "Talvez" (uso potencial)

| Arquivo | Propósito |
|---------|-----------|
| `proj_demanda.sql` | Projeção de demanda por SKU (só perenes). Métrica-chave: `final_proj_vendas_sku_mes` |
| `sales.sql` (Talvez) | Vendas históricas por SKU/dia (mesma lógica do principal) |
| `stock_health_hist.sql` | Snapshots mensais do histórico de saúde de estoque |
