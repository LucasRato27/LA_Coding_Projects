# BigQuery — Tabelas e Datasets

## Projetos
- `insider-data-lake` — Data lake principal (público/interno)
- `insider-lake-sensitive` — Dados sensíveis (NFs, custos, CNPJ)

## Datasets Frequentes

### `insider-data-lake.sop_silver` (Supply Chain — Silver)
- `supply_chain_efficiency_model_input` — **Tabela central de OPs**: op_code, supplier, SKU, estágios produtivos, quantidades (planejado/cortado/recebido), todas as datas do fluxo produtivo, ciclo
- `supply_chain_efficiency_model_input_history` — **Histórico diário de snapshots** da tabela de OPs (partição: `ingestion_date`). Usado para calcular Plan Freeze Rate (KR1): comparar estado atual vs baseline por `cycle_name`
- `scale_ra` — **ICP por fornecedor × data de referência.** ⚠️ Grão: `supplier_name × reference_date` — **sem desagregação por produto/SKU.** Campos validados: `supplier_name`, `reference_date`, `total_planned_quantity_icp`, `total_received_quantity_icp`, `icp` (pré-calculado: received/planned). Usar para **tendência histórica de ICP por fornecedor**. Para ICP por produto, calcular a partir de `supply_chain_efficiency_model_input` agrupado por `product_name`.
- `demand_prediction_input` — Input de previsão de demanda. **Fonte canônica da curva ABC.** Grão: `product_name × reference_date`. Padrão ABC: janela últimos 3 meses fechados (`>= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)` e `< DATE_TRUNC(CURRENT_DATE(), MONTH)`), campo `treated_generated_revenue`, limiares: A ≤ 80% | B ≤ 95% | C > 95% (share acumulado). ABC é no nível `product_name` — join com SKUs via `integrated.skus.product_name`.

### `insider-data-lake.sop_gold` (Supply Chain — Gold)
- `predicted_production_order_deliveries` — Forecast de entregas por OP
- `scale_monthly` — Nota SCALE mensal consolidada por fornecedor
- `supplier_monthly_info` — Performance mensal de fornecedores (qualidade, atraso)
- `supplier_capacity_history` — Histórico de capacidade produtiva cadastrada: `supplier_name`, `product_name`, `is_finished_product`, `apparel_manufacturer_production_unit_id`, `ingestion_date`, `weekly_maximum_productive_capacity`, `status`. Particionado por `ingestion_date`
- `demand_prediction` — Projeção de demanda por SKU
- `stock_health` — **Saúde de estoque diária por SKU.** Grão: `sku × dia` (~2.185 SKUs). Validado: 1.258.939 linhas, `sku + dia` é único.
  - Campos de identificação: `id_previsao`, `dia`, `isoweek`, `mes`, `sku`, `nome_sku`, `produto_pai`, `categoria`, `variante_cor`
  - Campos de estoque: `estoque_passado`, `estoque_projetado`, `estoque_passado_ou_projetado`
  - **Campos de cobertura/saúde (principais):** `estoque_passado_ou_projetado_d` (cobertura em dias), `estoque_atual_d_vendas_l7d`, `estoque_seguranca_d`, `estoque_excesso_d`, `stock_classification`, `stock_classification_l7d_sales`
  - Demanda: `qtd_a_receber`, `qtd_venda_prevista_diarizada`, `qtd_venda_media_l7d`
  - Financeiro: `sku_sales_average_price`, `sku_production_cost`, `receita_prevista_diarizada`, `custo_de_estoque`
  - Técnico: `data_hora_atualizacao`
  - **JOIN padrão:** `LEFT JOIN integrated.skus ON sk.sku = sh.sku` → traz `sku_state` e `product_name` (chave de join para ABC e ICP)
  - **⚠️ `produto_pai` em stock_health ≈ `product_name` em integrated.skus**, mas usar sempre `sk.product_name` como chave canônica de join para evitar variação de grafia.
- `stock_health_history` — Saúde do estoque (snapshots mensais) — **versão histórica; preferir `stock_health` para análises diárias**
- `scale_incubation_ra` — SCALE para fornecedores em incubação

### `insider-data-lake.integrated` (Cadastros Integrados)
- `skus` — Cadastro master de SKUs (sku, sku_name, sku_state, product_name, category, family, model, gender, color, size)
- `stock` — Estoque por SKU/data (physical_stock, virtual_stock, wms_stock, damaged_stock)
- `muninn_suppliers` — Fornecedores (id, alias, legal_name)
- `muninn_production_orders` — Ordens de produção (order_code, created_at, author_id)
- `muninn_products` — Produtos (product_name, full_price)
- `muninn_product_skus` — Relação produto → SKU
- `muninn_product_skus_fabrics` — Composição de tecido por SKU (consumption)
- `muninn_fabrics` — Tecidos (fabric_id, article_id)
- `muninn_articles` — Artigos/matéria-prima (name, unit)
- `muninn_knitting_factories` — Malharias (custo de tecido)
- `muninn_fabric_skus` — Relação tecido → SKU
- `muninn_supplier_tax_identification_codes` — CNPJs dos fornecedores
- `muninn_apparel_manufacturers` — Confecções (relação com supplier)
- `muninn_apparel_manufacturers_products` — Confecção × produto (lead_time, capacidade, custo)
- `muninn_apparel_manufacturer_production_units` — Unidades produtivas (células): `id`, `apparel_manufacturer_cell_number` (cell_number), `label` (nome da célula)
- `muninn_apparel_manufacturer_production_units_products` — Célula × produto: `apparel_manufacturer_production_unit_id`, `apparel_manufacturer_product_id`, `weekly_maximum_productive_capacity`
- `order_items` — Itens de pedido (sku, quantity, price)
- `orders` — Pedidos (order_id, processed_at)

### `insider-data-lake.fpa` (FP&A)
- `dre` — DRE operacional: revenue_after_discounts, revenue_after_refunds, gross_profit, net_profit_after_marketing_costs, sku_total_cost, quantity, refunded/exchanged

### `insider-data-lake.business`
- `insider_orders` — Pedidos comerciais. Campos: `order_id`, `order_name`, `processed_at`, `order_status`, `is_cancelled`, `coupon_code`, `store`. Filtros padrão T&D: `order_status = 'paid'`, `is_cancelled = FALSE`, `store IN ('shopify_insider-world', 'shopify_insider-store-loja')`, exclusão cupons TF-/TFIN/IR. Dedup: `SELECT DISTINCT` (não tem `data_received_at` como `integrated.orders`).
- `insider_order_items` — Itens de pedido. Campos: `order_id`, `sku`, `product_title`, `variant_color` (~23.6% null), `variant_size` (~24.6% null), `quantity`. Filtro: `sku IS NOT NULL`. Consolidar com `GROUP BY (order_id, sku)`. **Fonte de verdade de vendas para T&D desde 2026-06-18** (substitui `integrated.orders/order_items`).

### `insider-lake-sensitive.integrated_br` (Sensível — Brasil)
- `supply_production_invoices` — NFs de produção (CNPJ, NF, quantidade, op_codes)
- `supply_warehouse_inbound_analytical_sftp` — Inbound analítico do warehouse (OR, NF, qtd recebida, datas)
- `supply_production_costs` — Custos de produção por OP (cmv planejado/faturado)
- `cmv_model_production_costs_br` — Custo unitário mediano por SKU (planejado vs faturado)
- `supply_production_orders` — Detalhes de OPs com lead time contratual

### `insider-lake-sensitive.prepared_br`
- `prepared_muninn_production_order_payment_agreement` — Condições de pagamento por OP

### `insider-data-lake.z__logistics_cx`
- `qualidade_por_op2` — Auditoria de qualidade por OP (resultado 1ª/última inspeção)


## Tabelas adicionais — Relatório T&D (Análise de Reversas e Priorização de Produtos)

### `insider-data-lake.business` (Fonte principal de vendas — v2 2026-06-18)
- `insider_orders` — Pedidos válidos: `order_id`, `order_name`, `processed_at`, `order_status`, `is_cancelled`, `coupon_code`, `store`. Filtros padrão: `order_status = 'paid'`, `is_cancelled = FALSE`, exclusão cupons (TF-/TFIN/IR/Item errado), `store IN ('shopify_insider-world', 'shopify_insider-store-loja')`. Dedup com `SELECT DISTINCT` (sem `data_received_at`).
- `insider_order_items` — Itens de pedido: `order_id`, `sku`, `product_title`, `variant_color` (~23.6% null), `variant_size` (~24.6% null), `quantity`. Filtro: `sku IS NOT NULL`. Sem dedup — GROUP BY (order_id, sku) consolida.

### `insider-data-lake.fpa` (Receita e Volume — DRE)
- `analytical_dre` — DRE com attribution-weighted rows. Grão real: `order_id × sku × touchpoint` (~30.9 rows/pair em média, mediana 6, máx >10k). `SUM(weight) ≈ 1.0` por (order_id, sku) — confirma estrutura attribution-weighted. Campos para T&D: `non_refunded_quantity`, `revenue_after_discounts`, `order_id`, `sku`. **⚠️ Não tem coluna `date`** — filtro de período deve vir do JOIN com `insider_orders` (via `processed_at`). **Sempre agregar por `product_name`** (via JOIN com `sku_dim`) — nunca fazer join direto por `order_id × sku` no pipeline de T&D (multiplica linhas). Validado (2026-06-19): 66.3% dos `order_id` do DRE batem com `insider_orders` filtrado (lojas + paid); `non_refunded_quantity` = 98.5% do volume bruto de `order_items` (desconta devolvidos — comportamento correto).

### `insider-lake-sensitive.prepared_br`
- `prepared__troquecommerce_order_details_br` — Reversas/trocas/devoluções (Troquecommerce). Grão após dedup: `(order_name, id_reversa, sku)` via `ROW_NUMBER() PARTITION BY order_name, id_reversa, sku ORDER BY updated_at DESC`. Campos: `order_name`, `status`, `reverse_type`, `sku`, `return_reason`, `client_comment`, `created_at`, `updated_at`, `return_quantity`, `reverse_shipping_cost`, `retained_bonus`, `exchange_value`, `refund_value`. Filtros padrão: `status <> 'Cancelado'`, `sku IS NOT NULL`, `return_reason IS NOT NULL`.

### `insider-data-lake.sop_silver`
- `return_reason_tags` — Tags qualitativas de motivos de reversa, geradas por LLM. Grão: `(order_name, sku)` após dedup por `created_at DESC`. Campo `tags` é ARRAY — fazer `UNNEST(tags) AS tag`. Campos: `order_name`, `sku`, `tags`, `created_at`.
- `portfolio_skp_clustering` — Clusterização estratégica de produtos. **⚠️ Grão: `product_name` (NÃO tem coluna `sku`)**. JOIN sempre por `product_name`. Campos relevantes: `product_name`, `cluster` (KILL · Long tail · Core · Hero · Lancamento · Breakthrough · Outros), `skp_state`, `processed_at`. Não usar `pc.*` com QUALIFY por sku — não existe essa coluna.

### `insider-data-lake.sop_bronze`
- `eval_produto_portfolio` — Scorecard de avaliação de portfólio por produto. Grão: `product_name`. Campos principais:
  - **Pilares de scorecard:** `score_satisf_cliente` (satisfação e marca), `score_vendas_geral` (tração comercial), `score_viabilidade_financeira` (unit economics) — todos em escala 0–1, multiplicar por 100 para exibição.
  - **T&D:** `product_devolution_ratio` (% T&D produto), `category4_devolution_ratio` (% T&D categoria nível 4), `product_exchange_ratio` (% troca).
  - **Financeiro:** `revenue_after_discounts`, `gross_profit`, `net_profit_after_marketing_costs`, `mc3_ratio`.
  - **Avaliação:** `rating_product`, `product_review_score`.
  - Não tem campo de cluster — JOIN com `portfolio_skp_clustering` em `product_name` para obter `cluster` (KILL/Keep).
  - **⚠️ Query padrão:** sempre usar `GROUP BY product_name` + `ANY_VALUE()` nos scores (evitar duplicatas por versões de avaliação).
  - **⚠️ NÃO está em `portfolio_cluster_payload`:** o campo `portfolio_cluster_payload` em `df_exec` é `TO_JSON_STRING` de `portfolio_skp_clustering`, que **não contém os score columns**. Para obter os pilares, fazer query direta a `eval_produto_portfolio`.

## Tabelas adicionais — Projeto 20260422 (NRP Simulador)

### `insider-data-lake.sop_bronze`
- `capacity_use` — Uso de capacidade por célula e mês; usado para construir `cap_use`/`available_capacity` no simulador.
  - Campos relevantes: `apparel_manufacturer_production_unit_id`, `month_dt`, `monthly_capacity`, `total_quant_planned`, `prop_monthly_capacity`, `dt_reference`
  - Regra no notebook: filtrar `dt_reference = MAX(dt_reference)` via SQL de entrada.

### `insider-data-lake.integrated` (uso reforçado no simulador)
- `muninn_apparel_manufacturer_production_units` + `muninn_apparel_manufacturer_production_units_products` — base para mapear célula × produto e capacidade semanal/mensal no `cell_data.sql`.
