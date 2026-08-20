# Governança de Matéria-Prima — Documentação Técnica Consolidada

### Referência completa de tabelas, campos, queries e decisões

Este documento consolida **tudo** que é necessário para continuar, auditar ou reconstruir o
notebook `fabric_governance` sem depender de contexto de conversa anterior: schema real
verificado no BigQuery, o SQL exato de cada bloco, o contrato de saída, as decisões tomadas e
por quê, e os números de baseline do último run validado.

> **Sincronização:** 2026-08-07. Fonte de verdade: notebook Deepnote
> [`fabric_governance`](https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/fabricgovernance-62e6d7b178ac492a837fba610b5590c6),
> ID `62e6d7b178ac492a837fba610b5590c6`, projeto `Governanca-Lead-Time`
> (`62b1671d-dc82-4747-9bbb-b134a69a3491`), integração BigQuery
> `5088268a-9640-458a-83d1-46b07b955fae`.
> Último run validado: `4843b844-2d43-4e06-bfc1-ff9d998f642d`, status `success`,
> 26 checks de governança (0 FALHA, 2 ATENÇÃO).

## Sumário

1. Papel do notebook e o que NÃO é
2. Decisões vigentes (não re-discutir sem motivo novo)
3. Hierarquia de cadastro real (corrigida)
4. Catálogo de tabelas fonte — schema completo verificado
5. Modelo de saída — as 5 tabelas governadas + derivadas
6. SQL de cada bloco (texto exato, executável)
7. Contrato de schema (`SCHEMA_CONTRATO`) e regras de tipagem
8. Bloco de auditoria — checks e como interpretá-los
9. Achados que corrigem documentação anterior
10. Gotchas de dado confirmados
11. Baseline e resultado do último run
12. Limitações conhecidas e próximos passos
13. Referências cruzadas

---

## 1. Papel do notebook e o que NÃO é

**Este notebook é a camada de estruturação da base, não um dashboard nem uma análise.**
Produz tabelas planas com contrato de schema estável, para consumo por outras plataformas:
extração para Google Sheets primeiro, base governável para o Lovable depois.

Consequências de design, na ordem em que aparecem no notebook:

- **Contrato de schema acima de tudo.** `SCHEMA_CONTRATO` (bloco 6) declara coluna, tipo e
  ordem para cada tabela. `aplicar_contrato()` levanta erro se algo divergir — não tenta
  "consertar" silenciosamente.
- **Saída plana.** Nenhuma coluna pode ser `ARRAY`/`STRUCT`. Listas viram string `;`-separada
  (`malharias_nomes`) com a contagem ao lado (`n_malharias`).
- **Chaves estáveis.** Todo `*_id` é preservado, mesmo quando já existe o nome denormalizado,
  para permitir modelagem relacional no destino.
- **Sem gráfico.** Qualidade de dado sai como coluna/flag e como tabela `df_governanca`.
- **NULL é ausência de cadastro, não zero.** Gramatura, largura e LT sem cadastro ficam vazios
  de propósito — nunca preenchidos com `0` ou `"N/A"`.

---

## 2. Decisões vigentes (não re-discutir sem motivo novo)

Decididas com Lucas Sampaio em 2026-08-07:

| Tema | Decisão | Por quê |
|---|---|---|
| Escopo | **Fabric governance puro: MP, tecido, artigo, malharia.** Confecção fora de tudo — nem LT cadastrado, nem realizado, nem recebimento de MP na OP. | Escopo pedido explicitamente; confecção já é coberta por outro notebook (`lead_time_dashboard`). |
| Lead time | **Cadastrado e realizado, ambos da malharia.** Onde não há cadastro, fica em branco (NULL), nunca inferido. | Evitar inventar dado onde o cadastro é incompleto. |
| Gramatura / largura | Colunas existem no contrato, hoje `NULL` em 100% das linhas, com check de cobertura. | Não existe fonte no data lake (verificado, seção 9). Contrato já pronto para quando a fonte aparecer — nenhum consumidor externo precisa mudar depois. |
| Universo | Tabelas governadas com universo COMPLETO de cadastro + uma tabela derivada (`mart_produto_mp`) com o recorte comercial (venda L8M) em uso na query de produção. | A governança não pode perder linha por causa de um filtro comercial; a leitura comercial precisa ser reproduzível à parte. |
| Papel do notebook | Estruturação de base para consumo externo (Sheets → Lovable), não dashboard/análise. | Definido explicitamente pelo usuário — mudou o desenho de 3 tabelas analíticas para 5 tabelas + contrato + dicionário + auditoria. |

---

## 3. Hierarquia de cadastro real (corrigida)

A documentação anterior (`documentacao_materia_prima.md`) descrevia:

```
Artigo (MP genérica) → Tecido (variação de artigo) → Fabric SKU → Product SKU
```

**Isso está incompleto.** Medido em 2026-08-07: `muninn_fabrics.product_color_id` está
preenchido em **100% dos 602 tecidos** (0 nulos). Ou seja, tecido não é uma entidade genérica
de cadastro — **é sempre amarrado a uma cor de produto**. A hierarquia real é:

```
Artigo (MP genérica: "Modal", "Bi Stretch")
  └── Tecido = Artigo × Cor de produto (product_color_id sempre preenchido)
        └── Fabric SKU = Tecido cotado numa malharia específica, com preço
              └── aplicado a um Product SKU (via ficha técnica, com consumo)
```

```
muninn_articles (id, name, unit)
      ▲ article_id
muninn_fabrics (id, name, article_id, product_color_id)   ← product_color_id SEMPRE preenchido
      ▲ fabric_id
muninn_fabric_skus (id, fabric_id, knitting_factory_id, sku, unit_price, status, ...)
      │ fabric_id                              │ knitting_factory_id
      ▼                                         ▼
muninn_product_skus_fabrics                muninn_knitting_factories (id, supplier_id)
(product_sku_id, fabric_id, consumption)         │ supplier_id  [SEMPRE papel 'knitting']
      ▲ product_sku_id                           ▼
muninn_product_skus (product_sku_id, sku, ...)  muninn_suppliers (id, alias, legal_name, type)

muninn_articles_knitting_factories (article_id, knitting_factory_id, coloring_time, production_time)
      → LT cadastrado da malharia (cobre 383 de 401 fabric_skus = 95,5%; 44 de 96 artigos)

muninn_fabric_orders (fabric_sku_id, knitting_factory_id, created_at, estimated_invoicing_date,
                       updated_invoicing_date, real_invoicing_date)
      → LT realizado da malharia
```

**Gotcha do `supplier_id` compartilhado:** `muninn_knitting_factories.supplier_id` e
`muninn_apparel_manufacturers.supplier_id` apontam para a **mesma** `muninn_suppliers`. São
dois papéis diferentes do mesmo cadastro de fornecedor. `muninn_suppliers.type` só assume
`manufacturer` ou `knitting` — a base de MP filtra sempre pelo papel `knitting` e o bloco de
auditoria testa isso explicitamente (`papel_fornecedor__somente_knitting`).

---

## 4. Catálogo de tabelas fonte — schema completo verificado

Projeto `insider-data-lake`, dataset `integrated` salvo indicação contrária. Todas verificadas
via `INFORMATION_SCHEMA.COLUMNS` em 2026-08-07. Contagem de `ingestion_date` distinto = 1 em
todas as tabelas Muninn: são **snapshot do estado atual**, sem histórico versionado nativo.

### `muninn_articles` — dimensão, 96 linhas
`id` (PK) · `name` · `unit` (kg, metro) · `created_at` · `updated_at` · `ingestion_date`

### `muninn_fabrics` — dimensão, 602 linhas
`id` (PK, = `fabric_id`) · `name` · `article_id` (FK) · `product_color_id`
(**preenchido em 100% dos casos** — ver seção 3) · `created_at` · `updated_at` · `ingestion_date`

### `muninn_fabric_skus` — fato de cotação, 401 linhas
`id` (PK, = `fabric_sku_id`) · `fabric_id` (FK) · `knitting_factory_id` (FK) · `sku` ·
`factory_color_code` · `invoice_fabric_code` · `invoice_fabric_name` · `unit_price` ·
`volume_per_roll` · `minimum_volume_per_order` · `multiple_volume_per_order` ·
`status` (**filtrar `= 'available'`** para custo vigente; 318 de 401 são `available`) ·
`payment_agreement_id` (FK) · `update_source` · `update_author_id` · `created_at` ·
`updated_at` · `ingestion_date`

### `muninn_knitting_factories` — dimensão, 22 linhas
`id` (PK) · `supplier_id` (FK → `muninn_suppliers.id`, **sempre papel malharia**) ·
`freight_type` · `current_supplier_agreement_id` (FK) · `stock_id` · `created_at` ·
`updated_at` · `ingestion_date`

### `muninn_suppliers` — dimensão, 124 linhas
`id` (PK) · `alias` · `legal_name` · `tax_identification_code` (CNPJ) · `address_line_1/2` ·
`city` · `state` · `postal_code` · `country` · `address_district` · `address_number` ·
`address_complement` · `type` (**só `manufacturer` ou `knitting`**) · `stock_id` ·
`current_supplier_agreement_id` (FK) · `state_registration_code` ·
`field_errors` (**JSON string; `'{}'` = cadastro OK — 108 de 124 fornecedores**) ·
`created_at` · `updated_at` · `ingestion_date`

### `muninn_articles_knitting_factories` — LT cadastrado da malharia, 50 linhas
`id` (PK) · `article_id` (FK) · `knitting_factory_id` (FK) · `coloring_time` (dias) ·
`production_time` (dias) · `mininimum_order_volume` [sic] · `volume_per_roll` ·
`multiple_volume_to_coloring` · `bought_by_supplier` (BOOL) · `created_at` · `updated_at` ·
`ingestion_date`.
**Cobertura: 44 de 96 artigos, mas 383 de 401 fabric_skus (95,5%)** — os artigos cobertos
concentram a maior parte das cotações; medir cobertura por artigo subestima muito.

### `muninn_fabric_orders` — LT realizado da malharia, 6.990 linhas
`id` (PK, = `fabric_order_id`) · `fabric_sku_id` (FK) · `knitting_factory_id` (FK) ·
`author_id` · `price` · `estimated_invoicing_date` (**compromisso ORIGINAL**) ·
`updated_invoicing_date` (**compromisso REPACTUADO**) · `real_invoicing_date` · `status` ·
`volume` · `invoiced_volume` · `available_volume` · `notes` · `order_code` ·
`article_order_id` (FK) · `invoiced_price` · `update_author_id` · `update_source` ·
`created_at` · `updated_at` · `deleted_at` (**filtrar `IS NULL`**) · `ingestion_date`

### `muninn_payment_agreement` — 135 linhas
`id` (PK) · `supplier_agreement_id` (FK) · `label` · `payment_method` · `payment_channel` ·
`first_payment_due_days` · `fixed_month_due_day` · `installments` · `installment_days_gap` ·
`penalty_type` · `penalty_value` · `update_author_id` · `update_source` · `created_at` ·
`updated_at` · `ingestion_date`

### `muninn_supplier_agreement` — 137 linhas, **`supplier_agreement_id` duplicado (122 ids únicos) — exige dedupe**
`supplier_agreement_id` · `supplier_id` (FK) · `agreement_start_date` ·
`agreement_expiration_date` · `agreement_status` · `payment_agreement_label` ·
`payment_method` · `payment_channel` · `first_payment_due_days` · `fixed_month_due_day` ·
`installments` · `installment_days_gap` · `penalty_type` · `penalty_value`

### `muninn_product_skus_fabrics` — BOM grão SKU (sistema de registro), 24.388 linhas
`id` (PK) · `product_sku_id` (FK) · `fabric_id` (FK) · `consumption` (consumo por peça)

### `muninn_product_skus` — dimensão, 32.824 linhas
`product_sku_id` (PK) · `sku` · `sku_name` · `product_id` (FK) · `product_color_id` ·
`product_size_id` · `sku_price` · `sku_cost` · `sku_gtin_ean` · `equivalent_sku_id` ·
`product_state_id` · `created_at` · `updated_at` · `ingestion_date`

### `muninn_products` — dimensão, 4.196 linhas
`product_id` (PK) · `product_name` · `gender` · `parent_product_id` ·
`product_category_id` · `product_family_id` · `product_model_id` · `ncm` · `cest` · `gpc` ·
`quality_type` · `default_product_grade_id` · `full_price` · `product_stock_goal` ·
`created_at` · `updated_at` · `ingestion_date`

### `integrated.skus` — dimensão comercial, 32.824 linhas
`sku` (PK) · `sku_name` · `sku_gtin_ean` · `ncm` · `gpc` · `cest` · `sku_state` · `gender` ·
`color` · `size` · `fabric_cost` (**custo de MP por SKU já calculado por outro processo —
reconciliação, ver seção 9**) · `industrialization_cost` · `industrialized_sku_cost` ·
`manufacturer_finished_product_cost` · `sku_price` · `full_price` · `product_id` ·
`product_name` · `category` · `family` · `model` · `equivalent_sku_id` ·
`days_of_stock_goal` · `ingestion_date`

### `integrated.sku_bill_of_materials` — camada de leitura derivada, 36.139 linhas
`sku` · `sku_name` · `fabric_id` · `article_name` · `color` · `fabric_name` · `consumption` ·
`unit`. **Superset de `muninn_product_skus_fabrics`** (ver seção 9) — não é fonte primária,
entra só como reconciliação.

### `integrated.cmv_model` — CMV real por SKU/dia, fresco até 2026-08-06 (39.930 SKUs distintos)
Colunas relevantes: `date` · `sku` · `product_name` · `opening_stock_quantity` ·
`invoiced_quantity` · `fiscal_unitary_cost` · `tax_free_unitary_cost` · `unitary_cost` ·
`unitary_cost_tax_free` · `sold_quantity` · `closing_stock_quantity` · `closing_stock_value`

### `integrated.markup_prices` — 69.302 linhas / 4.806 SKUs distintos, **exige dedupe por vigência**
`store` · `sku` · `markup_price` · `valid_from` · `valid_to` (`NULL` ou futuro = vigente)

### `integrated.product_cost` — 6.719 linhas
`sku` · `sku_name` · `unit_variant_cost` · `variant_line` · `variant_gender` · `variant_type` ·
`class_closing` · `variant_category` · `variant_color` · `variant_size` · `variant_fabric` ·
`rn`

### Fora de escopo (existem, não entram no notebook)
- `product_production_parameters` — LT de **confecção** cadastrado (`lead_time`,
  `days_to_deliver_to_supplier`, `manufacturer_cost`, `full_price`, `reliability`,
  capacidade semanal). 704 linhas, 237 produtos. Fora por decisão de escopo (seção 2).
- `muninn_production_orders` — recebimento físico de MP na OP
  (`expected_fabric_receiving_date`, `real_fabric_receiving_date`). Fora por decisão de escopo.
- `muninn_products_articles` — BOM grão **produto** (planejamento), 920 linhas. Fora do v1;
  se entrar, o campo de consumo deve se chamar `consumo_produto_estimado`, nunca
  `consumption`, para não ser somado com `consumo_sku` (grão SKU).

---

## 5. Modelo de saída — as 5 tabelas governadas + derivadas

| # | Tabela | Grão (PK) | Linhas × colunas (run 2026-08-07) |
|---|---|---|---|
| 1 | `dim_mp_fornecimento` | `fabric_sku_id` | 401 × 51 |
| 2 | `fact_sku_bom` | `sku × fabric_id` | 24.388 × 27 |
| 3 | `fact_mp_lt_realizado` | `fabric_order_id` | 6.990 × 31 |
| 4 | `fact_sku_economics` | `sku` | 21.073 × 31 |
| 5 | `mart_produto_mp` | `product_name` (derivada comercial) | 166 × 28 |
| 6 | `dicionario_dados` | `tabela × coluna` (gerado do contrato) | 163 |
| 7 | `df_governanca` / `governanca` | `check` (auditoria) | 26 |

### Como as tabelas se ligam

```
dim_mp_fornecimento (fabric_sku_id)
   │ fabric_id                          │ fabric_sku_id
   ▼                                    ▼
fact_sku_bom (sku × fabric_id)     fact_mp_lt_realizado (fabric_order_id)
   │ sku
   ▼
fact_sku_economics (sku)

mart_produto_mp (product_name) ← derivada agregada, NÃO expõe article_id, não junte por id
```

Regras de junção:

- `dim_mp_fornecimento` → `fact_sku_bom` por `fabric_id` (1 tecido : N cotações).
- `dim_mp_fornecimento` → `fact_mp_lt_realizado` por `fabric_sku_id`.
- `fact_sku_bom` → `fact_sku_economics` por `sku`. `fact_sku_bom` tem N linhas por SKU (uma
  por tecido); `fact_sku_economics` tem 1. **Agregar antes de juntar**, senão custo duplica.
- `mart_produto_mp` não expõe `article_id`: a normalização `Modal (\d+)` → `Modal` funde
  vários `article_id` num nome só, então nenhum representaria o grupo. Junta-se por
  `product_name`.

---

## 6. SQL de cada bloco (texto exato, executável)

Todos os blocos SQL rodam contra `insider-data-lake` via a integração BigQuery
`5088268a-9640-458a-83d1-46b07b955fae`, com `deepnote_variable_name` definindo o nome do
DataFrame Python resultante (chave de metadata correta — ver seção 12, nota de implementação).

### 6.1 `dim_mp_fornecimento` (bloco SQL, grão `fabric_sku_id`)

```sql
-- dim_mp_fornecimento — GRÃO: fabric_sku_id (1 linha por cotação de tecido numa malharia)
-- Fonte de verdade de: MP/tecido/artigo, malharia, status, custo unitário, LT malharia cadastrado.
-- NÃO filtra status='available': mantém a cotação desativada como histórico auditável.
-- Só a agregação de custo vigente (custo_min/max_fabric) considera 'available'.
WITH acordo_fornecedor AS (
    -- muninn_supplier_agreement tem supplier_agreement_id duplicado (137 linhas / 122 ids) -> dedupe
    SELECT
        supplier_agreement_id,
        ANY_VALUE(agreement_status)          AS agreement_status,
        ANY_VALUE(agreement_start_date)      AS agreement_start_date,
        ANY_VALUE(agreement_expiration_date) AS agreement_expiration_date
    FROM `insider-data-lake.integrated.muninn_supplier_agreement`
    GROUP BY supplier_agreement_id
),

lt_malharia_cadastrado AS (
    -- LT cadastrado da malharia por artigo x malharia. Cobertura parcial:
    -- LEFT JOIN proposital lá embaixo -> onde não há cadastro, fica NULL (em branco).
    SELECT
        article_id,
        knitting_factory_id,
        coloring_time                   AS lt_tingimento_cadastrado_dias,
        production_time                 AS lt_producao_cadastrado_dias,
        coloring_time + production_time AS lt_malharia_cadastrado_dias,
        mininimum_order_volume          AS volume_minimo_pedido_artigo,
        multiple_volume_to_coloring     AS multiplo_volume_tingimento,
        bought_by_supplier              AS comprado_pelo_fornecedor
    FROM `insider-data-lake.integrated.muninn_articles_knitting_factories`
),

base AS (
    SELECT
        mfs.id                        AS fabric_sku_id,
        mfs.sku                       AS fabric_sku,
        mfs.fabric_id,
        mf.name                       AS fabric_name,
        mf.article_id,
        ma.name                       AS article_name,
        ma.unit                       AS article_unit,

        -- Papel de fornecedor: SEMPRE malharia (muninn_knitting_factories).
        -- Nunca muninn_apparel_manufacturers, que aponta para a mesma muninn_suppliers.
        mfs.knitting_factory_id,
        mkf.supplier_id               AS malharia_supplier_id,
        ms.alias                      AS malharia_nome,
        ms.legal_name                 AS malharia_razao_social,
        ms.tax_identification_code    AS malharia_cnpj,
        ms.type                       AS malharia_tipo,
        ms.city                       AS malharia_cidade,
        ms.state                      AS malharia_uf,
        mkf.freight_type              AS tipo_frete,

        mfs.status                    AS status_cotacao,
        mfs.unit_price                AS custo_unitario,
        mfs.minimum_volume_per_order  AS volume_minimo_pedido,
        mfs.multiple_volume_per_order AS multiplo_volume_pedido,
        mfs.volume_per_roll           AS volume_por_rolo,
        mfs.factory_color_code        AS codigo_cor_fabrica,
        mfs.invoice_fabric_code       AS codigo_tecido_nf,
        mfs.invoice_fabric_name       AS nome_tecido_nf,

        mfs.payment_agreement_id,
        mpa.label                     AS acordo_pagamento_label,
        mpa.payment_method            AS forma_pagamento,
        mpa.installments              AS parcelas,
        mpa.first_payment_due_days    AS dias_primeiro_vencimento,
        af.agreement_status           AS status_acordo_fornecedor,

        lt.lt_tingimento_cadastrado_dias,
        lt.lt_producao_cadastrado_dias,
        lt.lt_malharia_cadastrado_dias,
        lt.volume_minimo_pedido_artigo,
        lt.multiplo_volume_tingimento,
        lt.comprado_pelo_fornecedor,

        -- GAP DECLARADO: não existe fonte para gramatura/largura no data lake (verificado em
        -- INFORMATION_SCHEMA de integrated + sop_silver em 2026-08-07). A coluna existe no
        -- contrato para o consumidor não quebrar quando a fonte aparecer.
        CAST(NULL AS FLOAT64)         AS gramatura_g_m2,
        CAST(NULL AS FLOAT64)         AS largura_cm,

        -- product_color_id está preenchido em 100% dos 602 tecidos (medido em 2026-08-07).
        -- Ou seja: tecido NÃO é entidade genérica de cadastro — é sempre amarrado a uma cor de
        -- produto. A hierarquia "Artigo -> Tecido -> Fabric SKU" da documentação anterior está
        -- incompleta: falta a dimensão de cor no meio.
        mf.product_color_id,
        mf.product_color_id IS NOT NULL AS fabric_amarrado_a_cor,

        -- field_errors vem como JSON string; '{}' significa cadastro OK (108 de 124 fornecedores).
        ms.field_errors               AS malharia_field_errors,
        (ms.field_errors IS NOT NULL AND ms.field_errors != '{}') AS malharia_cadastro_incompleto,

        mfs.update_source             AS origem_ultima_alteracao,
        mfs.update_author_id          AS autor_ultima_alteracao,
        DATE(mfs.created_at)          AS cotacao_criada_em,
        DATE(mfs.updated_at)          AS cotacao_atualizada_em
    FROM `insider-data-lake.integrated.muninn_fabric_skus`               AS mfs
    LEFT JOIN `insider-data-lake.integrated.muninn_fabrics`              AS mf  ON mf.id  = mfs.fabric_id
    LEFT JOIN `insider-data-lake.integrated.muninn_articles`             AS ma  ON ma.id  = mf.article_id
    LEFT JOIN `insider-data-lake.integrated.muninn_knitting_factories`   AS mkf ON mkf.id = mfs.knitting_factory_id
    LEFT JOIN `insider-data-lake.integrated.muninn_suppliers`            AS ms  ON ms.id  = mkf.supplier_id
    LEFT JOIN `insider-data-lake.integrated.muninn_payment_agreement`    AS mpa ON mpa.id = mfs.payment_agreement_id
    LEFT JOIN acordo_fornecedor      AS af ON af.supplier_agreement_id = ms.current_supplier_agreement_id
    LEFT JOIN lt_malharia_cadastrado AS lt ON lt.article_id = mf.article_id
                                          AND lt.knitting_factory_id = mfs.knitting_factory_id
),

-- Faixa de custo e concentração de fornecimento por tecido, só sobre cotação vigente.
-- CTE agregada (não função de janela): COUNT(DISTINCT) não é permitido com OVER no BigQuery.
faixa_custo_por_fabric AS (
    SELECT
        fabric_id,
        MIN(custo_unitario)                 AS custo_min_fabric,
        MAX(custo_unitario)                 AS custo_max_fabric,
        COUNT(DISTINCT knitting_factory_id) AS n_malharias,
        -- Saída plana: string ';'-separada, nunca ARRAY (Sheets/Lovable não ingerem ARRAY).
        STRING_AGG(DISTINCT malharia_nome, '; ' ORDER BY malharia_nome) AS malharias_nomes
    FROM base
    WHERE status_cotacao = 'available'
    GROUP BY fabric_id
)

SELECT
    b.*,
    f.custo_min_fabric,
    f.custo_max_fabric,
    IFNULL(f.n_malharias, 0) AS n_malharias,
    f.malharias_nomes
FROM base AS b
LEFT JOIN faixa_custo_por_fabric AS f ON f.fabric_id = b.fabric_id
ORDER BY b.article_name, b.fabric_name, b.fabric_sku_id
```

### 6.2 `fact_sku_bom` (bloco SQL, grão `sku × fabric_id`)

```sql
-- fact_sku_bom — GRÃO: sku x fabric_id (ficha técnica real, 1 linha por tecido do SKU)
-- Sistema de registro: muninn_product_skus_fabrics.
-- sku_bill_of_materials entra SÓ como flag de reconciliação (é superset, não fonte concorrente).
WITH faixa_custo_por_fabric AS (
    SELECT
        mfs.fabric_id,
        MIN(mfs.unit_price)                     AS custo_min_fabric,
        MAX(mfs.unit_price)                     AS custo_max_fabric,
        COUNT(DISTINCT mfs.knitting_factory_id) AS n_malharias,
        STRING_AGG(DISTINCT ms.alias, '; ' ORDER BY ms.alias) AS malharias_nomes
    FROM `insider-data-lake.integrated.muninn_fabric_skus`              AS mfs
    LEFT JOIN `insider-data-lake.integrated.muninn_knitting_factories`  AS mkf ON mkf.id = mfs.knitting_factory_id
    LEFT JOIN `insider-data-lake.integrated.muninn_suppliers`           AS ms  ON ms.id  = mkf.supplier_id
    WHERE mfs.status = 'available'
    GROUP BY mfs.fabric_id
),

-- Reconciliação: chave (sku, fabric_id) presente na tabela derivada sku_bill_of_materials.
bom_derivado AS (
    SELECT DISTINCT sku, fabric_id
    FROM `insider-data-lake.integrated.sku_bill_of_materials`
)

SELECT
    mps.sku,
    mps.sku_name,
    mpsf.product_sku_id,
    mps.product_id,
    mp.product_name,

    s.sku_state,
    s.gender,
    s.color,
    s.size,
    s.category,
    s.family,
    mps.product_state_id,

    mpsf.fabric_id,
    mf.name                       AS fabric_name,
    mf.article_id,
    ma.name                       AS article_name,
    ma.unit                       AS article_unit,

    -- NOME DELIBERADO: consumo_sku, nunca 'consumption'.
    -- Impede que alguém some isto com o consumo estimado de muninn_products_articles,
    -- que vive em grão de PRODUTO e usa o mesmo nome de campo na origem.
    mpsf.consumption              AS consumo_sku,

    fc.custo_min_fabric           AS custo_unitario_min_fabric,
    fc.custo_max_fabric           AS custo_unitario_max_fabric,
    fc.n_malharias,
    fc.malharias_nomes,

    -- Custo de MP deste tecido no SKU = consumo x custo unitário do tecido.
    fc.custo_min_fabric * mpsf.consumption AS custo_mp_min,
    fc.custo_max_fabric * mpsf.consumption AS custo_mp_max,

    -- Custo de MP do SKU já calculado por outro processo (integrated.skus.fabric_cost).
    -- Fica lado a lado para reconciliação — o escopo pode não ser idêntico ao calculado aqui.
    s.fabric_cost                 AS custo_mp_referencia_skus,

    bd.sku IS NOT NULL            AS presente_em_sku_bom
FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS mpsf
LEFT JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.product_sku_id = mpsf.product_sku_id
LEFT JOIN `insider-data-lake.integrated.muninn_products`     AS mp  ON mp.product_id = mps.product_id
LEFT JOIN `insider-data-lake.integrated.muninn_fabrics`      AS mf  ON mf.id = mpsf.fabric_id
LEFT JOIN `insider-data-lake.integrated.muninn_articles`     AS ma  ON ma.id = mf.article_id
LEFT JOIN `insider-data-lake.integrated.skus`                AS s   ON s.sku = mps.sku
LEFT JOIN faixa_custo_por_fabric AS fc ON fc.fabric_id = mpsf.fabric_id
LEFT JOIN bom_derivado           AS bd ON bd.sku = mps.sku AND bd.fabric_id = mpsf.fabric_id
ORDER BY mps.sku, mpsf.fabric_id
```

### 6.3 `fact_mp_lt_realizado` (bloco SQL, grão `fabric_order_id`)

```sql
-- fact_mp_lt_realizado — GRÃO: fabric_order_id (1 linha por pedido de tecido à malharia)
-- Único "realizado" do escopo. Confecção está fora por decisão.
--
-- Padrão original vs repactuado (espelha a correção de postergação do lead time produtivo):
--   estimated_invoicing_date = compromisso ORIGINAL   -> métrica de contrato
--   updated_invoicing_date   = compromisso REPACTUADO -> diagnóstico
-- Medir só contra o repactuado esconde atraso, porque a data se move junto com o atraso.
WITH lt_malharia_cadastrado AS (
    SELECT
        article_id,
        knitting_factory_id,
        coloring_time + production_time AS lt_malharia_cadastrado_dias
    FROM `insider-data-lake.integrated.muninn_articles_knitting_factories`
),

pedidos AS (
    SELECT
        mfo.id                        AS fabric_order_id,
        mfo.order_code                AS fabric_order_code,
        mfo.fabric_sku_id,
        mfo.knitting_factory_id,
        mfo.article_order_id,
        mfo.status                    AS status_pedido,
        mfo.volume                    AS volume_pedido,
        mfo.invoiced_volume           AS volume_faturado,
        mfo.available_volume          AS volume_disponivel,
        mfo.price                     AS preco_pedido,
        mfo.invoiced_price            AS preco_faturado,
        DATE(mfo.created_at)          AS data_pedido,
        mfo.estimated_invoicing_date  AS data_faturamento_estimada_original,
        mfo.updated_invoicing_date    AS data_faturamento_repactuada,
        mfo.real_invoicing_date       AS data_faturamento_real
    FROM `insider-data-lake.integrated.muninn_fabric_orders` AS mfo
    WHERE mfo.deleted_at IS NULL
)

SELECT
    p.fabric_order_id,
    p.fabric_order_code,
    p.fabric_sku_id,
    mfs.sku                       AS fabric_sku,
    mfs.fabric_id,
    mf.name                       AS fabric_name,
    mf.article_id,
    ma.name                       AS article_name,
    ma.unit                       AS article_unit,

    p.knitting_factory_id,
    ms.alias                      AS malharia_nome,
    ms.legal_name                 AS malharia_razao_social,

    p.status_pedido,
    p.volume_pedido,
    p.volume_faturado,
    p.volume_disponivel,
    p.preco_pedido,
    p.preco_faturado,

    p.data_pedido,
    p.data_faturamento_estimada_original,
    p.data_faturamento_repactuada,
    p.data_faturamento_real,

    -- LT realizado: do pedido até o faturamento real.
    DATE_DIFF(p.data_faturamento_real, p.data_pedido, DAY)                        AS lt_malharia_realizado_dias,
    -- LT prometido originalmente, no mesmo eixo.
    DATE_DIFF(p.data_faturamento_estimada_original, p.data_pedido, DAY)           AS lt_malharia_estimado_original_dias,
    -- MÉTRICA DE CONTRATO: aderência contra o compromisso original.
    DATE_DIFF(p.data_faturamento_real, p.data_faturamento_estimada_original, DAY) AS atraso_vs_original_dias,
    -- Diagnóstico: aderência contra o compromisso já repactuado.
    DATE_DIFF(p.data_faturamento_real, p.data_faturamento_repactuada, DAY)        AS atraso_vs_repactuado_dias,
    -- Diagnóstico: quanto a data foi empurrada (equivalente à postergação do LT produtivo).
    DATE_DIFF(p.data_faturamento_repactuada, p.data_faturamento_estimada_original, DAY) AS dias_repactuacao,

    -- LT cadastrado da malharia para o artigo, no mesmo grão -> cadastrado vs realizado.
    lt.lt_malharia_cadastrado_dias,
    DATE_DIFF(p.data_faturamento_real, p.data_pedido, DAY) - lt.lt_malharia_cadastrado_dias
                                                                                 AS desvio_vs_cadastrado_dias,

    CASE
        WHEN p.data_faturamento_estimada_original IS NULL THEN 'MISSING_ESTIMATED'
        WHEN p.data_faturamento_real IS NULL              THEN 'PENDENTE'
        WHEN p.data_faturamento_real > p.data_faturamento_estimada_original THEN 'LATE'
        WHEN p.data_faturamento_real = p.data_faturamento_estimada_original THEN 'ON_TIME'
        ELSE 'EARLY'
    END AS status_faturamento_mp
FROM pedidos AS p
LEFT JOIN `insider-data-lake.integrated.muninn_fabric_skus`        AS mfs ON mfs.id = p.fabric_sku_id
LEFT JOIN `insider-data-lake.integrated.muninn_fabrics`            AS mf  ON mf.id  = mfs.fabric_id
LEFT JOIN `insider-data-lake.integrated.muninn_articles`           AS ma  ON ma.id  = mf.article_id
LEFT JOIN `insider-data-lake.integrated.muninn_knitting_factories` AS mkf ON mkf.id = p.knitting_factory_id
LEFT JOIN `insider-data-lake.integrated.muninn_suppliers`          AS ms  ON ms.id  = mkf.supplier_id
LEFT JOIN lt_malharia_cadastrado AS lt ON lt.article_id = mf.article_id
                                      AND lt.knitting_factory_id = p.knitting_factory_id
ORDER BY p.data_pedido DESC, p.fabric_order_id
```

### 6.4 `fact_sku_economics` (bloco SQL, grão `sku`)

```sql
-- fact_sku_economics — GRÃO: sku (1 linha por SKU com ficha técnica de tecido)
--
-- AVISO DE ESCOPO (crítico para quem consome só o número):
-- custo_mp_* é MATÉRIA-PRIMA ISOLADA. cmv_unitario é CUSTO TOTAL do SKU.
-- Divergência entre os dois NÃO é erro — são escopos diferentes. Por isso a base expõe
-- as fontes alternativas de custo lado a lado em vez de eleger uma só.
WITH custo_mp_por_sku AS (
    -- Soma o custo de MP de TODOS os tecidos do SKU (um SKU pode ter vários).
    SELECT
        mps.sku,
        COUNT(DISTINCT mpsf.fabric_id)              AS n_tecidos_no_sku,
        SUM(mpsf.consumption)                       AS consumo_total_sku,
        SUM(fc.custo_min_fabric * mpsf.consumption) AS custo_mp_min,
        SUM(fc.custo_max_fabric * mpsf.consumption) AS custo_mp_max
    FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS mpsf
    JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.product_sku_id = mpsf.product_sku_id
    LEFT JOIN (
        SELECT fabric_id, MIN(unit_price) AS custo_min_fabric, MAX(unit_price) AS custo_max_fabric
        FROM `insider-data-lake.integrated.muninn_fabric_skus`
        WHERE status = 'available'
        GROUP BY fabric_id
    ) AS fc ON fc.fabric_id = mpsf.fabric_id
    GROUP BY mps.sku
),

cmv_vigente AS (
    -- cmv_model é diário; pega a última data disponível por SKU.
    SELECT
        sku,
        date                  AS cmv_data_ref,
        unitary_cost          AS cmv_unitario,
        unitary_cost_tax_free AS cmv_unitario_sem_imposto,
        sold_quantity         AS cmv_qtd_vendida_ref
    FROM `insider-data-lake.integrated.cmv_model`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY date DESC) = 1
),

markup_vigente AS (
    -- markup_prices tem 69.302 linhas para 4.806 SKUs (histórico de vigência) -> dedupe
    -- para a linha vigente mais recente.
    SELECT
        sku,
        markup_price,
        DATE(valid_from) AS markup_valid_from
    FROM `insider-data-lake.integrated.markup_prices`
    WHERE valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP()
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY valid_from DESC) = 1
),

custo_variante AS (
    SELECT sku, ANY_VALUE(unit_variant_cost) AS custo_variante_unitario
    FROM `insider-data-lake.integrated.product_cost`
    GROUP BY sku
)

SELECT
    s.sku,
    s.sku_name,
    s.product_id,
    s.product_name,
    s.sku_state,
    s.gender,
    s.color,
    s.size,
    s.category,
    s.family,

    -- Matéria-prima (escopo: MP isolada)
    mp.n_tecidos_no_sku,
    mp.consumo_total_sku,
    mp.custo_mp_min,
    mp.custo_mp_max,
    s.fabric_cost                 AS custo_mp_referencia_skus,

    -- CMV (escopo: custo total)
    c.cmv_data_ref,
    c.cmv_unitario,
    c.cmv_unitario_sem_imposto,
    c.cmv_qtd_vendida_ref,

    -- Preço e markup
    mk.markup_price,
    mk.markup_valid_from,
    s.sku_price,
    s.full_price,
    SAFE_DIVIDE(mk.markup_price, c.cmv_unitario_sem_imposto) AS markup_ratio,

    -- Peso da MP dentro do custo total. Fora de (0, 1] indica escopo de custo trocado.
    SAFE_DIVIDE(mp.custo_mp_min, c.cmv_unitario) AS pct_mp_no_cmv,

    -- Fontes alternativas de custo, para auditoria de escopo (não são a mesma coisa).
    cv.custo_variante_unitario,
    mps.sku_cost                         AS custo_sku_muninn,
    s.industrialization_cost             AS custo_industrializacao,
    s.industrialized_sku_cost            AS custo_sku_industrializado,
    s.manufacturer_finished_product_cost AS custo_produto_acabado_fornecedor
FROM `insider-data-lake.integrated.skus` AS s
LEFT JOIN custo_mp_por_sku AS mp ON mp.sku = s.sku
LEFT JOIN cmv_vigente      AS c  ON c.sku  = s.sku
LEFT JOIN markup_vigente   AS mk ON mk.sku = s.sku
LEFT JOIN custo_variante   AS cv ON cv.sku = s.sku
LEFT JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.sku = s.sku
-- Base de MP: só SKUs que têm ficha técnica de tecido.
WHERE mp.sku IS NOT NULL
ORDER BY s.product_name, s.sku
```

### 6.5 `mart_produto_mp` (bloco SQL, grão `product_name`)

```sql
-- mart_produto_mp — GRÃO: product_name (1 linha por produto)
-- Camada COMERCIAL derivada. Reproduz fielmente o recorte da query de produção
-- (venda nos últimos 8 meses, exclusões de linha, tecido principal por consumo mediano)
-- e acrescenta as colunas de governança que a query original não trazia.
--
-- As tabelas governadas (dim_mp_fornecimento, fact_sku_bom, fact_sku_economics) mantêm o
-- universo COMPLETO de cadastro. Este mart é o recorte comercial, não a base.
WITH fabric_costs AS (
    SELECT
        mfs.id AS fabric_sku_id,
        mfs.fabric_id,
        mfs.knitting_factory_id,
        mfs.unit_price,
        mfs.minimum_volume_per_order,
        mf.name  AS fabric_name,
        mf.article_id,
        ma.name  AS article_name,
        ma.unit  AS article_unit,
        ms.alias AS knitting_factory_name
    FROM `insider-data-lake.integrated.muninn_fabric_skus` AS mfs
    LEFT JOIN `insider-data-lake.integrated.muninn_fabrics`            AS mf  ON mf.id  = mfs.fabric_id
    LEFT JOIN `insider-data-lake.integrated.muninn_articles`           AS ma  ON ma.id  = mf.article_id
    LEFT JOIN `insider-data-lake.integrated.muninn_knitting_factories` AS mkf ON mkf.id = mfs.knitting_factory_id
    LEFT JOIN `insider-data-lake.integrated.muninn_suppliers`          AS ms  ON ms.id  = mkf.supplier_id
    WHERE mfs.status = 'available'
),

fabric_min_max_cost AS (
    SELECT
        fc.fabric_id,
        fc.fabric_name,
        MIN(fc.unit_price)                     AS min_fabric_cost,
        MAX(fc.unit_price)                     AS max_fabric_cost,
        MIN(fc.minimum_volume_per_order)       AS minimum_volume_per_order,
        COUNT(DISTINCT fc.knitting_factory_id) AS number_knitting_factories,
        -- Saída plana: STRING_AGG em vez do ARRAY_AGG original (Sheets/Lovable não ingerem ARRAY).
        STRING_AGG(DISTINCT fc.knitting_factory_name, '; ' ORDER BY fc.knitting_factory_name) AS knitting_factories_names
    FROM fabric_costs AS fc
    GROUP BY fc.fabric_id, fc.fabric_name
),

-- Nome de artigo normalizado. A query original funde "Modal (5651)", "Modal (5590)" e "Modal"
-- num único artigo. Como isso agrupa VÁRIOS article_id sob o mesmo nome, o lead time precisa
-- ser chaveado pelo nome normalizado — chavear por article_id pegaria um id arbitrário do grupo.
artigo_normalizado AS (
    SELECT
        id AS article_id,
        REGEXP_REPLACE(name, r'Modal \(\d+\)', 'Modal') AS article_name_norm
    FROM `insider-data-lake.integrated.muninn_articles`
),

lt_cadastrado_por_artigo AS (
    SELECT
        an.article_name_norm,
        MIN(akf.coloring_time + akf.production_time) AS lt_malharia_cadastrado_min_dias,
        MAX(akf.coloring_time + akf.production_time) AS lt_malharia_cadastrado_max_dias
    FROM `insider-data-lake.integrated.muninn_articles_knitting_factories` AS akf
    JOIN artigo_normalizado AS an ON an.article_id = akf.article_id
    GROUP BY an.article_name_norm
),

lt_realizado_por_artigo AS (
    SELECT
        an.article_name_norm,
        COUNT(*) AS n_pedidos_mp_faturados,
        APPROX_QUANTILES(DATE_DIFF(mfo.real_invoicing_date, DATE(mfo.created_at), DAY), 2)[OFFSET(1)]
            AS lt_malharia_realizado_mediano_dias,
        COUNTIF(mfo.real_invoicing_date > mfo.estimated_invoicing_date) AS n_pedidos_mp_atrasados
    FROM `insider-data-lake.integrated.muninn_fabric_orders` AS mfo
    JOIN `insider-data-lake.integrated.muninn_fabric_skus`   AS mfs ON mfs.id = mfo.fabric_sku_id
    JOIN `insider-data-lake.integrated.muninn_fabrics`       AS mf  ON mf.id  = mfs.fabric_id
    JOIN artigo_normalizado AS an ON an.article_id = mf.article_id
    WHERE mfo.deleted_at IS NULL
      AND mfo.real_invoicing_date IS NOT NULL
    GROUP BY an.article_name_norm
),

skp_with_sales_l8m AS (
    SELECT DISTINCT s.product_name
    FROM `insider-data-lake.fpa.analytical_dre` d
    LEFT JOIN `insider-data-lake.integrated.skus` s USING(sku)
    WHERE DATE(d.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 8 MONTH)
      AND d.order_status != 'Not authorized'
      AND d.quantity > 0
      AND s.product_name IS NOT NULL
),

skp_status AS (
    SELECT
        s.product_name,
        CASE
            WHEN COUNTIF(s.sku_state = 'ativo_perene') > 0        THEN 'ativo_perene'
            WHEN COUNTIF(s.sku_state = 'ativo_em_lancamento') > 0 THEN 'ativo_em_lancamento'
            WHEN COUNTIF(s.sku_state = 'ativo_capsula') > 0       THEN 'ativo_capsula'
            WHEN COUNTIF(s.sku_state = 'personalizacao') > 0      THEN 'personalizacao'
            WHEN COUNTIF(s.sku_state = 'kit') > 0                 THEN 'kit'
            ELSE 'desativado'
        END AS product_status
    FROM `insider-data-lake.integrated.skus` s
    INNER JOIN skp_with_sales_l8m l8m USING(product_name)
    GROUP BY s.product_name
),

-- CMV e markup médios por produto (colunas novas em relação à query original).
economics_por_produto AS (
    SELECT
        s.product_name,
        AVG(c.unitary_cost)          AS cmv_unitario_medio,
        AVG(c.unitary_cost_tax_free) AS cmv_unitario_medio_sem_imposto,
        AVG(mk.markup_price)         AS markup_price_medio,
        MAX(c.date)                  AS cmv_data_ref
    FROM `insider-data-lake.integrated.skus` s
    LEFT JOIN (
        SELECT sku, date, unitary_cost, unitary_cost_tax_free
        FROM `insider-data-lake.integrated.cmv_model`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY date DESC) = 1
    ) c ON c.sku = s.sku
    LEFT JOIN (
        SELECT sku, markup_price
        FROM `insider-data-lake.integrated.markup_prices`
        WHERE valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP()
        QUALIFY ROW_NUMBER() OVER (PARTITION BY sku ORDER BY valid_from DESC) = 1
    ) mk ON mk.sku = s.sku
    GROUP BY s.product_name
),

sku_fabrics AS (
    SELECT
        mps.sku,
        s.product_name,
        mpsf.fabric_id,
        mpsf.consumption,
        fc.min_fabric_cost AS min_fabric_unitary_cost,
        fc.max_fabric_cost AS max_fabric_unitary_cost,
        fc.minimum_volume_per_order,
        ma.unit AS article_unit,
        ma.name AS article_name,
        mf.article_id,
        fc.number_knitting_factories,
        fc.knitting_factories_names
    FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS mpsf
    LEFT JOIN `insider-data-lake.integrated.muninn_fabrics`      AS mf  ON mf.id = mpsf.fabric_id
    LEFT JOIN `insider-data-lake.integrated.muninn_articles`     AS ma  ON ma.id = mf.article_id
    LEFT JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.product_sku_id = mpsf.product_sku_id
    LEFT JOIN `insider-data-lake.integrated.skus`                AS s   ON mps.sku = s.sku
    LEFT JOIN fabric_min_max_cost AS fc ON fc.fabric_id = mpsf.fabric_id
    INNER JOIN skp_with_sales_l8m l8m ON l8m.product_name = s.product_name
),

product_article_agg AS (
    SELECT
        sf.product_name,
        ss.product_status,
        REGEXP_REPLACE(sf.article_name, r'Modal \(\d+\)', 'Modal') AS article_name,
        sf.article_unit,
        -- article_id NÃO é exposto: a normalização funde vários ids num grupo, então qualquer
        -- id escolhido seria arbitrário. O join de lead time usa o nome normalizado.
        MIN(sf.minimum_volume_per_order)                       AS minimum_volume_per_order,
        APPROX_QUANTILES(sf.consumption, 2)[OFFSET(1)]         AS median_article_consumption,
        MIN(sf.min_fabric_unitary_cost)                        AS custo_unitario_min,
        MAX(sf.max_fabric_unitary_cost)                        AS custo_unitario_max,
        MAX(sf.number_knitting_factories)                      AS n_malharias,
        STRING_AGG(DISTINCT sf.knitting_factories_names, '; ') AS malharias_nomes
    FROM sku_fabrics AS sf
    INNER JOIN skp_status AS ss ON ss.product_name = sf.product_name
    WHERE ss.product_status IN ('ativo_perene', 'ativo_em_lancamento', 'desativado')
      AND LOWER(sf.product_name) NOT LIKE '%ziraldo%'
      AND LOWER(sf.product_name) NOT LIKE '% xp%'
      AND LOWER(sf.product_name) NOT LIKE '%maluquinho%'
      AND LOWER(sf.product_name) NOT LIKE '% b2b %'
    GROUP BY sf.product_name, ss.product_status, article_name, sf.article_unit
),

principal AS (
    SELECT
        product_name,
        product_status,
        article_name AS tecido_principal,
        article_unit,
        ROUND(CAST(median_article_consumption AS FLOAT64), 4) AS consumo_mediano,
        minimum_volume_per_order,
        custo_unitario_min,
        custo_unitario_max,
        n_malharias,
        malharias_nomes,
        COUNT(*) OVER (PARTITION BY product_name) AS qtd_tecidos_total
    FROM product_article_agg
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY product_name
        ORDER BY median_article_consumption DESC
    ) = 1
)

SELECT
    p.product_name,
    p.product_status,
    p.tecido_principal,
    p.article_unit,
    p.consumo_mediano,
    p.minimum_volume_per_order,
    p.qtd_tecidos_total,

    -- Colunas de governança acrescentadas à query original
    CAST(p.custo_unitario_min AS FLOAT64) AS custo_unitario_min,
    CAST(p.custo_unitario_max AS FLOAT64) AS custo_unitario_max,
    ROUND(CAST(p.custo_unitario_min AS FLOAT64) * p.consumo_mediano, 4) AS custo_mp_min_por_peca,
    ROUND(CAST(p.custo_unitario_max AS FLOAT64) * p.consumo_mediano, 4) AS custo_mp_max_por_peca,
    p.n_malharias,
    p.malharias_nomes,

    ltc.lt_malharia_cadastrado_min_dias,
    ltc.lt_malharia_cadastrado_max_dias,
    ltr.lt_malharia_realizado_mediano_dias,
    ltr.n_pedidos_mp_faturados,
    ltr.n_pedidos_mp_atrasados,
    SAFE_DIVIDE(ltr.n_pedidos_mp_atrasados, ltr.n_pedidos_mp_faturados) AS pct_pedidos_mp_atrasados,

    ec.cmv_unitario_medio,
    ec.cmv_unitario_medio_sem_imposto,
    ec.cmv_data_ref,
    ec.markup_price_medio,
    SAFE_DIVIDE(ec.markup_price_medio, ec.cmv_unitario_medio_sem_imposto) AS markup_ratio_medio,
    SAFE_DIVIDE(CAST(p.custo_unitario_min AS FLOAT64) * p.consumo_mediano, ec.cmv_unitario_medio) AS pct_mp_no_cmv,

    -- GAP DECLARADO: sem fonte no data lake.
    CAST(NULL AS FLOAT64) AS gramatura_g_m2,
    CAST(NULL AS FLOAT64) AS largura_cm
FROM principal AS p
LEFT JOIN lt_cadastrado_por_artigo AS ltc ON ltc.article_name_norm = p.tecido_principal
LEFT JOIN lt_realizado_por_artigo  AS ltr ON ltr.article_name_norm = p.tecido_principal
LEFT JOIN economics_por_produto    AS ec  ON ec.product_name = p.product_name
ORDER BY p.product_name ASC
```

### 6.6 Reconciliação (bloco SQL, `recon_bom`)

```sql
-- Reconciliação entre as duas fontes de ficha técnica.
-- Baseline 2026-08-07: 24.388 casando, 0 divergentes, 11.751 só no sku_bill_of_materials.
-- Qualquer linha em n_consumo_divergente é FALHA: significa que as fontes se separaram.
WITH bom AS (
    SELECT sku, fabric_id, consumption
    FROM `insider-data-lake.integrated.sku_bill_of_materials`
),
psf AS (
    SELECT mps.sku, f.fabric_id, f.consumption
    FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS f
    JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps USING (product_sku_id)
)
SELECT
    (SELECT COUNT(*) FROM psf)                                                     AS n_psf,
    (SELECT COUNT(*) FROM bom)                                                     AS n_bom_derivado,
    (SELECT COUNT(*) FROM bom JOIN psf USING (sku, fabric_id)
      WHERE bom.consumption = psf.consumption)                                     AS n_consumo_igual,
    (SELECT COUNT(*) FROM bom JOIN psf USING (sku, fabric_id)
      WHERE bom.consumption != psf.consumption)                                    AS n_consumo_divergente,
    (SELECT COUNT(*) FROM bom LEFT JOIN psf USING (sku, fabric_id)
      WHERE psf.sku IS NULL)                                                       AS n_so_no_bom_derivado,
    (SELECT COUNT(*) FROM `insider-data-lake.integrated.muninn_fabrics`)           AS n_fabrics,
    (SELECT COUNTIF(product_color_id IS NOT NULL)
       FROM `insider-data-lake.integrated.muninn_fabrics`)                         AS n_fabrics_com_cor,
    (SELECT COUNT(*) FROM `insider-data-lake.integrated.muninn_articles`)          AS n_artigos,
    (SELECT COUNT(DISTINCT article_id)
       FROM `insider-data-lake.integrated.muninn_articles_knitting_factories`)     AS n_artigos_com_lt_cadastrado,
    (SELECT MAX(date) FROM `insider-data-lake.integrated.cmv_model`)               AS cmv_data_mais_recente
```

---

## 7. Contrato de schema (`SCHEMA_CONTRATO`) e regras de tipagem

Bloco Python (bloco 6 do notebook) declara `SCHEMA_CONTRATO`: um dict
`{tabela: {"grao": ..., "descricao": ..., "colunas": [(coluna, tipo, descricao), ...]}}`,
na ordem oficial de saída. Tipos aceitos: `STRING | INT64 | FLOAT64 | DATE | BOOL`.

`aplicar_contrato(df, nome)`:
1. Verifica que o conjunto de colunas do DataFrame bate exatamente com o contrato — **levanta
   erro se faltar ou sobrar coluna**.
2. Coage cada coluna para o tipo declarado (`_coagir`): `STRING`→`pandas string`,
   `INT64`→`Int64` nullable, `FLOAT64`→`float64`, `BOOL`→`boolean` nullable,
   `DATE`→string ISO `YYYY-MM-DD` (formato que Sheets/Lovable ingerem sem ambiguidade).
3. Rejeita qualquer célula que seja `list`/`dict`/`set`/`tuple`/`ndarray` — saída tem que ser
   plana.
4. Acrescenta `atualizado_em` (timestamp do run) como última coluna.

`dicionario_dados` é gerado a partir do **mesmo** `SCHEMA_CONTRATO` — não há duas fontes de
verdade sobre schema que possam divergir com o tempo.

**Lista completa das 5 tabelas com suas colunas** está no próprio `SCHEMA_CONTRATO` no bloco
6 do notebook (163 linhas no `dicionario_dados` exportado). Resumo por tabela:

- `dim_mp_fornecimento`: 51 colunas — identificação de tecido/artigo/malharia, custo, LT
  cadastrado, condição comercial, gaps declarados (`gramatura_g_m2`, `largura_cm`), flags de
  qualidade de cadastro.
- `fact_sku_bom`: 27 colunas — identificação de SKU/produto, tecido, `consumo_sku`, custo de
  MP, flag de reconciliação `presente_em_sku_bom`.
- `fact_mp_lt_realizado`: 31 colunas — identificação de pedido/tecido/malharia, datas
  original/repactuada/real, LT realizado e estimado, `status_faturamento_mp`.
- `fact_sku_economics`: 31 colunas — CMV, markup, custo de MP, fontes alternativas de custo
  (`custo_sku_muninn`, `custo_industrializacao`, `custo_sku_industrializado`,
  `custo_produto_acabado_fornecedor`).
- `mart_produto_mp`: 28 colunas — agregado comercial por produto com custo, LT e economia.

Para o dicionário completo campo a campo, ler `SCHEMA_CONTRATO` no bloco 6 do notebook ou
exportar `governanca_mp_dicionario_dados.csv`.

---

## 8. Bloco de auditoria — checks e como interpretá-los

Bloco Python (bloco 8 do notebook) monta `df_governanca` com uma linha por check, 3 níveis:

- **`OK`** — dentro do esperado.
- **`ATENCAO`** — anomalia real, não bloqueia, mas deve ser investigada.
- **`GAP_CONHECIDO`** — ausência de dado já declarada (gramatura/largura), não é regressão.
- **`FALHA`** — quebra o contrato ou a integridade do grão. **O notebook levanta
  `AssertionError` e para** se houver qualquer `FALHA`.

Categorias de check:
1. Unicidade de PK em cada uma das 5 tabelas.
2. Volume de linhas (vs baseline).
3. Reconciliação `sku_bill_of_materials` vs `muninn_product_skus_fabrics` (crítico — ver
   seção 9, achado 1).
4. Papel de fornecedor: `malharia_tipo` deve ser só `knitting`.
5. Cobertura de cadastro: gramatura/largura (gap), LT malharia cadastrado, `field_errors`.
6. Frescor de `cmv_model` e cobertura de CMV/markup.
7. Sanidade de valor: custo/consumo/LT negativo ou inválido, `pct_mp_no_cmv` fora de (0,1].
8. Aderência de lead time: % de pedidos atrasados vs compromisso original, contagem de
   repactuações.

---

## 9. Achados que corrigem documentação anterior

1. **`sku_bill_of_materials` não é fonte concorrente — é superset.** Existe em
   `insider-data-lake.integrated` (a documentação anterior só conhecia uma versão `_br` em
   `insider-lake-sensitive`, inacessível). Comparação executada: as 24.388 linhas de
   `muninn_product_skus_fabrics` casam **100%** em `(sku, fabric_id, consumption)` — **zero
   divergência de consumo**. `sku_bill_of_materials` tem 36.139 linhas (11.751 a mais).
   `muninn_product_skus_fabrics` segue como sistema de registro; a outra tabela entra só
   como flag `presente_em_sku_bom` de reconciliação, testada a cada run.

2. **Gramatura e largura não existem no data lake.** Varredura de
   `INFORMATION_SCHEMA.COLUMNS` em `integrated` e `sop_silver` por
   `gramatura|largura|grammage|width|composic|yield` não retorna nenhuma coluna de MP. Os
   nomes de artigo ("Modal", "Bi Stretch", "Ponto de Roma") não codificam a informação. Gap
   real, não gap de busca.

3. **Tecido não é entidade genérica de cadastro** — ver seção 3. `product_color_id` está
   preenchido em 100% dos 602 tecidos.

4. **`muninn_fabric_orders` tem repactuação de data**, no mesmo padrão do lead time
   produtivo: `estimated_invoicing_date` é o compromisso original, `updated_invoicing_date`
   o repactuado. A métrica de contrato usa a original (`atraso_vs_original_dias`); medir
   contra a repactuada esconderia atraso — confirmado no run: **59,7% dos pedidos são
   faturados após o compromisso original**, e **557 pedidos tiveram repactuação**.

5. **Tabelas resolvidas que a documentação anterior não conhecia:**
   `muninn_articles_knitting_factories` (LT malharia cadastrado), `muninn_fabric_orders`
   (LT malharia realizado), `cmv_model` (CMV real), `markup_prices` (markup vigente),
   `muninn_supplier_agreement` / `muninn_payment_agreement` (condição comercial),
   `integrated.skus.fabric_cost` (custo de MP de referência para reconciliação).

---

## 10. Gotchas de dado confirmados

- **`muninn_supplier_agreement` tem `supplier_agreement_id` duplicado** (137 linhas / 122
  ids únicos) — sempre `GROUP BY` + `ANY_VALUE` antes de juntar.
- **`muninn_suppliers.field_errors` é JSON string; `'{}'` = cadastro OK** (108 de 124). Testar
  só `IS NOT NULL` classificaria 100% como incompleto, errado — precisa também checar
  `!= '{}'`.
- **`muninn_suppliers.type` só tem `manufacturer` e `knitting`.** Os dois papéis (malharia e
  confecção) apontam para a mesma tabela de fornecedores — o join de MP tem que passar por
  `muninn_knitting_factories`, nunca por `muninn_apparel_manufacturers`.
- **Todas as tabelas Muninn têm um único `ingestion_date`** e nenhum id duplicado nas
  dimensões — são snapshot do estado atual, sem risco de fan-out em joins simples, mas
  **sem histórico versionado nativo**. Não dá para calcular variação de custo de MP no tempo
  com estas tabelas; só `cmv_model` tem eixo temporal real (`date`).
- **`COUNT(DISTINCT)` não funciona como função analítica (`OVER`) no BigQuery** — exige CTE
  agregada com `GROUP BY` (ver `faixa_custo_por_fabric`).
- **A normalização `Modal (\d+)` → `Modal` funde vários `article_id` num nome só.** Qualquer
  lead time agregado por artigo tem que ser chaveado pelo **nome normalizado**
  (`article_name_norm`), nunca por `article_id` — pegar um id do grupo seria arbitrário. Por
  isso `mart_produto_mp` não expõe `article_id`.
- **`markup_prices` tem histórico de vigência** (69.302 linhas / 4.806 SKUs) — sempre filtrar
  `valid_to IS NULL OR valid_to > CURRENT_TIMESTAMP()` e desempatar por `valid_from` mais
  recente (`QUALIFY ROW_NUMBER()`).
- **`cmv_model` é diário** — sempre pegar a última `date` por SKU antes de usar como "o CMV
  do SKU".

---

## 11. Baseline e resultado do último run

Run `4843b844-2d43-4e06-bfc1-ff9d998f642d`, status `success`, 2026-08-07.

**Contrato aplicado com sucesso** — 5 tabelas + `dicionario_dados` (163 linhas):

| Tabela | Linhas | Colunas |
|---|---:|---:|
| `dim_mp_fornecimento` | 401 | 51 |
| `fact_sku_bom` | 24.388 | 27 |
| `fact_mp_lt_realizado` | 6.990 | 31 |
| `fact_sku_economics` | 21.073 | 31 |
| `mart_produto_mp` | 166 | 28 |

**26 checks de governança: 0 FALHA, 2 ATENÇÃO.**

```
pk_unica__dim_mp_fornecimento              OK       0
pk_unica__fact_sku_bom                     OK       0
pk_unica__fact_mp_lt_realizado             OK       0
pk_unica__fact_sku_economics               OK       0
pk_unica__mart_produto_mp                  OK       0
linhas__dim_mp_fornecimento                OK       401
linhas__fact_sku_bom                       OK       24388
linhas__mart_produto_mp                    OK       166
reconciliacao__consumo_divergente          OK       0
reconciliacao__so_no_bom_derivado          OK       11751
papel_fornecedor__somente_knitting         OK       knitting
cobertura__gramatura_g_m2                  GAP      0
cobertura__largura_cm                      GAP      0
cobertura__sem_lt_malharia_cadastrado      OK       18 (4.5%)
cobertura__artigos_com_lt_cadastrado       OK       44 de 96
semantica__fabric_amarrado_a_cor           OK       100.0%
qualidade__malharia_cadastro_incompleto    ATENCAO  143
frescor__cmv_dias_atraso                   OK       1
cobertura__skus_com_cmv                    OK       20203 de 21073
cobertura__skus_com_markup                 OK       4665 de 21073
sanidade__custo_unitario_invalido          OK       0
sanidade__consumo_sku_invalido             OK       0
sanidade__lt_realizado_negativo            OK       0
sanidade__pct_mp_no_cmv_fora_faixa         ATENCAO  238
lead_time__pct_pedidos_mp_atrasados        OK       59.7%
lead_time__pedidos_com_repactuacao         OK       557
```

**Achado operacional do primeiro run** (não é erro de dado — é resultado):
**59,7% dos pedidos de tecido são faturados depois do compromisso original**, com 557
pedidos repactuados. Confirma que medir contra a data repactuada esconderia a maior parte
do atraso.

**Exportação:** 7 CSVs em `exports/`, convenção `governanca_mp_<tabela>.csv`, atrás da flag
`EXPORTAR_CSV`. `ESCREVER_SHEETS = False` — ponto de plugue para a próxima camada, ainda não
implementado (`escrever_sheets()` levanta `NotImplementedError` de propósito).

**Custo de execução:** o bloco `mart_produto_mp` lê `fpa.analytical_dre` e `cmv_model` —
~5 GB processados por execução. Os demais blocos varrem poucos MB. Relevante para decidir
frequência de agendamento.

---

## 12. Limitações conhecidas e próximos passos

1. **Gramatura e largura não são rastreáveis hoje.** Destravar exige fonte nova: cadastro
   Muninn ou planilha do time de desenvolvimento de produto.
2. **LT malharia cadastrado cobre 95,5% das cotações, mas só 44 de 96 artigos.** Comparação
   cadastrado vs realizado só é estatisticamente robusta no subconjunto coberto.
3. **Base é snapshot, sem série histórica de custo de MP.** Quem precisar de tendência de
   preço no tempo tem que acumular snapshots deste notebook por fora, ou aguardar uma tabela
   `_history` para `fabric_skus` (análoga à que já existe para
   `supply_chain_efficiency_model_input_history`).
4. **Confecção está inteiramente fora de escopo** (decisão registrada na seção 2).
   `product_production_parameters` e `muninn_production_orders` seguem disponíveis para uma
   v2, se o escopo mudar.
5. **`muninn_products_articles` fica fora do v1** — BOM em grão de produto, para
   planejamento vs execução real.
6. **Camada de extração para Google Sheets ainda não implementada** — o ponto de plugue
   (`escrever_sheets()`, flag `ESCREVER_SHEETS`) já existe no bloco 8 do notebook.

### Nota de implementação Deepnote (para quem for editar blocos SQL)

A chave de metadata que nomeia o DataFrame Python resultante de um bloco SQL é
**`deepnote_variable_name`**, não `sqlVariableName`. Usar a chave errada faz o SQL rodar com
sucesso (sem erro visível no bloco), mas nenhuma variável é criada — o erro só aparece depois,
como `NameError` no primeiro bloco Python que tentar referenciar o resultado.

---

## 13. Referências cruzadas

- `analytics/lead_time/documentacao_materia_prima.md` — documentação original (parcialmente
  superada por este documento; mantida para histórico da hierarquia de cadastro pré-correção).
- `analytics/lead_time/documentacao_governanca_lead_time.md` — governança do
  `lead_time_dashboard` (confecção, fora de escopo deste notebook).
- `codex_context/serena_memories/lead_time_dashboard/premissas_fundamentais_calculo.md` —
  padrão de correção de postergação (original vs repactuado), reaproveitado em
  `fact_mp_lt_realizado`.
- `analyses/ddal/mp_atrasada/details.sql` — forma do `CASE` de status de recebimento,
  reaproveitada em `status_faturamento_mp`.
- Memória Serena: `bigquery_tables_materia_prima` (schema condensado, aponta para este
  documento como fonte de verdade).
- Memória Serena: `lead_time_dashboard/governanca_materia_prima_20260807.md` (decisões e
  achados, versão de handoff de conversa).
