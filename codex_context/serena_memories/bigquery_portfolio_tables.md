# BigQuery — Catálogo Portfolio (Vendas, Financeiro, Produtos)

> Fonte: escaneamento de arquivos SQL e notebooks em `Portfolio/`
> Última atualização: 2026-06-11

---

## Datasets consumidos pelo projeto Portfolio

| Dataset | Tabelas | Descrição |
|---------|---------|-----------|
| `insider-data-lake.fpa` | `analytical_dre` | DRE financeiro por pedido/sku |
| `insider-data-lake.integrated` | `skus`, `stock`, `order_items`, `product_reviews`, muninn_* | Dados integrados |
| `insider-data-lake.sop_silver` | `supply_chain_efficiency_model_input`, `dim_color_names` | Supply chain silver |

---

## `insider-data-lake.fpa.analytical_dre`

**Tabela central de toda a análise financeira do portfólio.** Grão: linha de pedido × SKU.

> ⚠️ Diferente de `fpa.dre` (DRE operacional). Esta é a versão analítica por pedido/sku.

**Notebooks que usam:**
- `Hero_Core/exploration/Analise_Sensibilidade_Hero_Core_CLEAN.ipynb`
- `Hero_Core/artifacts/Classificacao_Produtos_Completo.ipynb`
- `exploration/Score de Produtos.ipynb`
- `Scorecard_Lancamentos/inputs/SQL/base.sql`, `base_unificada.sql`, `base scorecard.sql`

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `sku` | STRING | Join com integrated.skus |
| `order_date` | DATE/TIMESTAMP | Filtros temporais, agrupamento mensal |
| `order_status` | STRING | Filtro: `!= 'Not authorized'` |
| `quantity` | INT64 | Volume vendido; filtro relevância (`>= 100/mês`) |
| `sold_quantity` | INT64 | Quantidade efetivamente vendida (para CMV) |
| `refunded_quantity` | INT64 | Devoluções, taxa de devolução |
| `non_refunded_quantity` | INT64 | Vendas líquidas (sell-through 30/60/90d) |
| `revenue_after_taxes` | FLOAT64 | Receita líquida de impostos |
| `revenue_after_discounts` | FLOAT64 | Receita pós-desconto (markup, representatividade) |
| `revenue_after_refunds` | FLOAT64 | Receita pós-devoluções (markup de entrada) |
| `items_potential_revenue` | FLOAT64 | Receita potencial a preço cheio (full price) |
| `sku_total_cost` | FLOAT64 | CMV total do SKU |
| `net_profit_after_marketing_costs` | FLOAT64 | Lucro líquido pós marketing (MC3) |

**Filtros padrão:**
```sql
-- Exclui meses de baixo volume
QUALIFY SUM(quantity) OVER (PARTITION BY DATE_TRUNC(order_date, MONTH)) >= 100
-- Exclui pedidos não autorizados
WHERE order_status != 'Not authorized'
```

---

## `insider-data-lake.integrated.skus` (Portfolio — colunas validadas)

Cadastro mestre de SKUs. Além das colunas já documentadas em `bigquery_tables.md`, no contexto Portfolio são críticas:

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `sku` | STRING | Chave de join universal |
| `product_name` | STRING | Chave de agrupamento por produto (join com sop_silver) |
| `category_4` | STRING | Subcategoria nível 4 (benchmarks) |
| `sku_state` | STRING | `ativo_perene`, `ativo_capsula`, `ativo_em_lancamento`, `desativado` |

**Filtros Portfolio:**
- `sku_state = 'ativo_em_lancamento'` → lançamentos
- `sku_state IN ('ativo_perene', 'ativo_capsula', 'ativo_em_lancamento', 'desativado')` → base geral

---

## `insider-data-lake.integrated.stock` (Portfolio — colunas validadas)

Posição de estoque diária por SKU.

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `sku` | STRING | Join com skus |
| `stock_date` | DATE | Filtro `= CURRENT_DATE` para posição atual |
| `virtual_stock` | INT64 | Estoque virtual (cobertura, valor em estoque) |

---

## `insider-data-lake.integrated.order_items` (Portfolio — colunas validadas)

Itens de pedido com dados de produto do e-commerce.

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `sku` | STRING | Join com skus |
| `data_source_product_id` | STRING | ID do produto na fonte (corrigir product_id para reviews) |
| `product_title` | STRING | Filtro `IS NOT NULL` para garantir match |

**Uso principal:** correção de `product_id` para cruzar reviews com `product_name` correto.

---

## `insider-data-lake.integrated.product_reviews`

Avaliações de produtos no site.

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `product_id` | STRING | Join com correção via `order_items.data_source_product_id` |
| `rating` | FLOAT64 | Nota média de avaliação |

**Filtro padrão:** `HAVING COUNT(1) > 10` (mínimo de reviews para robustez)

---

## Cluster `insider-data-lake.integrated.muninn_*` (Portfolio)

Tabelas do sistema Muninn usadas no contexto Portfolio (via `Score de Produtos.ipynb`):

| Tabela | Campos-chave | Relação |
|--------|-------------|---------|
| `muninn_apparel_manufacturer_production_units_products` | `apparel_manufacturer_product_id` | join com manufacturers products |
| `muninn_apparel_manufacturers_products` | `id`, `product_id`, `apparel_manufacturer_id` | produto ↔ fornecedor |
| `muninn_products` | `product_id` | cadastro de produtos Muninn |
| `muninn_apparel_manufacturers` | `id`, `supplier_id` | fabricante ↔ fornecedor |
| `muninn_suppliers` | `id` | cadastro de fornecedores |
| `muninn_articles_knitting_factories` | — | dados de malharias |
| `muninn_products_articles` | — | produto ↔ artigo (matéria-prima) |

---

## `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` (Portfolio — colunas validadas)

Input do modelo de eficiência da cadeia. Dados de OPs — colunas usadas no Scorecard de Lançamentos:

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `product_name` | STRING | Agrupamento por produto |
| `planned_quantity` | INT64 | Quantidade planejada de produção |
| `received_quantity` | INT64 | Quantidade recebida (base do sell-through) |
| `dt_min_entry_warehouse` | DATE | Data mínima de entrada no CD (filtro 120d) |
| `current_production_stage` | STRING | Filtro: exclui `pending` e `canceled` |

---

## `insider-data-lake.sop_silver.dim_color_names`

Dimensão de cores genéricas. Usada em EDA de `integrated.skus`.

| Coluna | Tipo | Contexto |
|--------|------|---------|
| `cor_generica` | STRING | Mapeamento de cores |

---

## Diagrama de relacionamentos (Portfolio)

```
fpa.analytical_dre ──(sku)──> integrated.skus ──(sku)──> integrated.stock
         │
         ├──(product_name)──> sop_silver.supply_chain_efficiency_model_input
         │
         └──(sku)──> integrated.order_items ──(data_source_product_id)──> integrated.product_reviews

integrated.skus ──(product_name)──> muninn_products
                                      ──> muninn_apparel_manufacturers_products
                                           ──> muninn_suppliers
```
