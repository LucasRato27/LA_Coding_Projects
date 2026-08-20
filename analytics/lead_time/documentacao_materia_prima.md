# Documentação — Base de Dados de Matéria-Prima (Tecido/Artigo/Malharia)

> **Parcialmente superada em 2026-08-07.** A hierarquia de cadastro descrita neste documento
> (seção "Visão geral") está incompleta — falta a dimensão de cor, confirmada em 100% dos
> tecidos. Para schema verificado campo a campo, SQL de produção e contrato de saída, ver
> `analytics/lead_time/documentacao_governanca_materia_prima.md`, referente ao notebook
> `fabric_governance`. Este documento permanece válido para o fluxo de recebimento físico de
> MP na OP (seção 3) e o cruzamento com qualidade (seção 5), que estão fora do escopo daquele
> notebook.
>
> Consolidação de todas as tabelas, schemas, campos e padrões de query relacionados
> a matéria-prima (MP) usados nas análises de lead time, ICP e atraso de produção.
> Fontes: `analyses/ddal/mp_atrasada/`, `analytics/lead_time/lead_time_dashboard.ipynb`,
> `analytics/icp_diagnostics/references/icp_root_cause_functions.py`.
>
> Projeto BigQuery: `insider-data-lake` · Região: `southamerica-east1`

---

## Visão geral — Hierarquia de cadastro

A cadeia de matéria-prima segue esta hierarquia, do mais genérico ao mais específico:

```
Artigo (matéria-prima genérica: "malha", "tricot", etc.)
  └── Tecido (variação de artigo: cor/composição específica)
        └── Fabric SKU (tecido cotado/comprado de UMA malharia específica)
              └── aplicado a um Product SKU (via ficha técnica/consumo)
```

```
muninn_articles (id, name, unit)
      ▲ article_id
muninn_fabrics (id, name, article_id)
      ▲ fabric_id
muninn_fabric_skus (id, fabric_id, knitting_factory_id, sku, unit_price, ...)
      │ fabric_id                              │ knitting_factory_id
      ▼                                         ▼
muninn_product_skus_fabrics                muninn_knitting_factories (id, supplier_id)
(product_sku_id, fabric_id, consumption)         │ supplier_id
      ▲ product_sku_id                           ▼
muninn_product_skus (product_sku_id, sku, ...)  muninn_suppliers (id, alias, legal_name)
```

Para o **fluxo de recebimento físico de MP na OP** (datas, atraso), a fonte é outra:
`muninn_production_orders` (campos `expected_fabric_receiving_date` /
`real_fabric_receiving_date`), cruzada com `supply_chain_efficiency_model_input[_history]`
(estágio produtivo `raw_material_receiving` / `raw_material_pending`).

---

## 1. Tabelas de cadastro de tecido/artigo (`insider-data-lake.integrated`)

### `muninn_articles`
Artigo = matéria-prima genérica (categoria do tecido, ex.: "Malha PV", "Ribana").

| Campo | Descrição |
|---|---|
| `id` | PK do artigo |
| `name` | Nome do artigo |
| `unit` | Unidade de medida (ex.: kg, metro) |

### `muninn_fabrics`
Tecido = variação específica de um artigo (cor, composição, gramatura).

| Campo | Descrição |
|---|---|
| `id` | PK do tecido (`fabric_id`) |
| `name` | Nome do tecido |
| `article_id` | FK → `muninn_articles.id` |

### `muninn_knitting_factories`
Malharia = fábrica que produz/fornece o tecido. **Modelada como um fornecedor.**

| Campo | Descrição |
|---|---|
| `id` | PK da malharia (`knitting_factory_id`) |
| `supplier_id` | FK → `muninn_suppliers.id` — ⚠️ a malharia é um `supplier_id`, **não confundir** com o `supplier_id` do fornecedor de confecção (`muninn_apparel_manufacturers.supplier_id`) |

### `muninn_fabric_skus`
Tecido específico cotado/comprado de uma malharia (fabric × knitting factory × preço).

| Campo | Descrição |
|---|---|
| `id` | PK — `fabric_sku_id` |
| `fabric_id` | FK → `muninn_fabrics.id` |
| `knitting_factory_id` | FK → `muninn_knitting_factories.id` |
| `sku` | Código do fabric SKU |
| `invoice_fabric_name` | Nome do tecido na nota fiscal |
| `unit_price` | Preço unitário cotado nesta malharia |
| `minimum_volume_per_order` | Volume mínimo de pedido |
| `multiple_volume_per_order` | Múltiplo de pedido (lote) |
| `status` | ⚠️ **Filtrar sempre `= 'available'`** para custos vigentes |

⚠️ **Um mesmo `fabric_id` pode ter várias `fabric_sku`** (cotações de malharias
diferentes para o mesmo tecido). Por isso, o padrão de análise usa `MIN`/`MAX` de
`unit_price` agrupado por `fabric_id` para capturar a faixa de custo, e
`COUNT(DISTINCT knitting_factory_id)` para medir concentração de fornecimento
(quantas malharias fornecem aquele tecido).

### `muninn_product_skus_fabrics`
Ficha técnica: relação Product SKU × Tecido, com consumo.

| Campo | Descrição |
|---|---|
| `product_sku_id` | FK → `muninn_product_skus.product_sku_id` |
| `fabric_id` | FK → `muninn_fabrics.id` |
| `consumption` | Consumo de tecido por peça (usado para custo de MP por SKU: `consumption × unit_price`) |

### `muninn_product_skus`
Cadastro de Product SKU.

| Campo | Descrição |
|---|---|
| `product_sku_id` | PK |
| `sku` | Código do SKU |
| `sku_name` | Nome do SKU |
| `product_id` | FK para produto (`muninn_products`) |

---

## 2. Padrão de query — Custo de matéria-prima por SKU

Baseado em `analytics/lead_time/lead_time_dashboard.ipynb` (Célula 1 — base de capacidade):

```sql
fabric_costs AS (
  SELECT
    mfs.id AS fabric_sku_id,
    mfs.fabric_id,
    mfs.knitting_factory_id,
    mfs.sku AS fabric_sku,
    mfs.invoice_fabric_name AS factory_fabric_name,
    mfs.unit_price,
    mfs.minimum_volume_per_order,
    mfs.multiple_volume_per_order,
    mf.name AS fabric_name,
    mf.article_id,
    ma.name AS article_name,
    ma.unit AS article_unit,
    mkf.supplier_id,
    ms.alias AS knitting_factory_name
  FROM `insider-data-lake.integrated.muninn_fabric_skus` AS mfs
  LEFT JOIN `insider-data-lake.integrated.muninn_fabrics` AS mf
    ON mf.id = mfs.fabric_id
  LEFT JOIN `insider-data-lake.integrated.muninn_articles` AS ma
    ON ma.id = mf.article_id
  LEFT JOIN `insider-data-lake.integrated.muninn_knitting_factories` AS mkf
    ON mkf.id = mfs.knitting_factory_id
  LEFT JOIN `insider-data-lake.integrated.muninn_suppliers` AS ms
    ON ms.id = mkf.supplier_id
  WHERE mfs.status = 'available'
),

fabric_min_max_cost AS (
  SELECT
    fc.fabric_id,
    fc.fabric_name,
    MIN(fc.unit_price) AS min_fabric_cost,
    MAX(fc.unit_price) AS max_fabric_cost,
    COUNT(DISTINCT fc.knitting_factory_id) AS number_knitting_factories,
    ARRAY_AGG(DISTINCT fc.knitting_factory_name) AS knitting_factories_names
  FROM fabric_costs AS fc
  GROUP BY fc.fabric_id, fc.fabric_name
),

article_sku AS (
  SELECT
    mpsf.product_sku_id,
    mps.sku,
    mpsf.fabric_id,
    mf.name AS fabric_name,
    mpsf.consumption,
    fc.min_fabric_cost AS min_fabric_unitary_cost,
    fc.max_fabric_cost AS max_fabric_unitary_cost,
    fc.min_fabric_cost * mpsf.consumption AS min_fabric_cost,   -- custo total de MP mínimo por SKU
    fc.max_fabric_cost * mpsf.consumption AS max_fabric_cost,   -- custo total de MP máximo por SKU
    ma.unit AS article_unit,
    ma.name AS article_name,
    mf.article_id
  FROM `insider-data-lake.integrated.muninn_product_skus_fabrics` AS mpsf
  LEFT JOIN `insider-data-lake.integrated.muninn_fabrics` AS mf ON mf.id = mpsf.fabric_id
  LEFT JOIN `insider-data-lake.integrated.muninn_articles` AS ma ON ma.id = mf.article_id
  LEFT JOIN `insider-data-lake.integrated.muninn_product_skus` AS mps ON mps.product_sku_id = mpsf.product_sku_id
  LEFT JOIN fabric_min_max_cost AS fc ON fc.fabric_id = mpsf.fabric_id
)
```

A partir de `article_sku`, o dashboard agrega por SKU (`sku_fabric_costs`) e por
produto (`avg_fabric_cost_prod`), somando/tirando média do custo de MP e usando
`STRING_AGG`/`ARRAY_AGG ORDER BY freq DESC` para identificar o artigo mais comum
por produto (`top_article`).

---

## 3. Datas de recebimento de MP na OP — `muninn_production_orders`

Fonte das datas reais de recebimento de tecido/MP por Ordem de Produção. **JOIN
obrigatório:** `po.order_code = sc.op_code` (campo `op_code` vem de
`supply_chain_efficiency_model_input` / `_history`).

| Campo | Descrição |
|---|---|
| `order_code` | Código da OP — chave de JOIN com `op_code` |
| `expected_fabric_receiving_date` | Data **esperada** de recebimento de tecido/MP |
| `real_fabric_receiving_date` | Data **real** de recebimento de tecido/MP |
| `expected_production_delivery_date` | Data esperada de entrega da produção (pós-MP) |
| `planned_production_delivery_date` | Data planejada de entrega da produção |
| `production_stage` | Estágio produtivo (Muninn) |
| `status` | Status da OP |
| `canceled_production_reason` | Motivo de cancelamento — único campo que separa `INT_CANCEL` (Revisão de Demanda em Season) de `EXT_CANCEL` (qualquer outro motivo) |
| `apparel_manufacturer_id` | FK → `muninn_apparel_manufacturers.id` → traz `supplier_id` → `muninn_suppliers` (`id`, `alias`, `legal_name`) |

### Regra de atraso de MP (validada em `analyses/ddal/mp_atrasada/`)

```sql
is_mp_late := real_fabric_receiving_date > expected_fabric_receiving_date
```

Classificação completa de status de recebimento (usada em `details.sql`):

```sql
CASE
  WHEN expected_fabric_receiving_date IS NULL THEN 'MISSING_EXPECTED_DATE'
  WHEN real_fabric_receiving_date IS NULL THEN 'MISSING_REAL_DATE'
  WHEN real_fabric_receiving_date > expected_fabric_receiving_date THEN 'LATE'
  WHEN real_fabric_receiving_date = expected_fabric_receiving_date THEN 'ON_TIME'
  WHEN real_fabric_receiving_date < expected_fabric_receiving_date THEN 'EARLY'
END AS fabric_receiving_status
```

### Lead time produtivo pós-MP

Tempo entre a chegada real da MP e a entrega esperada da produção — usado para
distinguir "atraso de MP" de "compressão do lead time produtivo":

```sql
DATE_DIFF(
  expected_production_delivery_date,
  real_fabric_receiving_date,
  DAY
) AS mp_to_expected_production_end_lead_time_days
```

⚠️ **Regra de referência:** se esse valor for **< 45 dias**, o atraso de entrega
final **não pode ser atribuído só à MP** — há também compressão do lead time
produtivo padrão. Ver causa raiz "Mp entregue dentro de um Lead Time menor que 45
dias" no `README.md` da análise DDAL (`analyses/ddal/mp_atrasada/README.md`).

### Padrão de JOIN completo (referência: `analyses/ddal/mp_atrasada/details.sql`)

```sql
FROM `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` AS sc  -- estado atual da OP, agregado por op_code
INNER JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
  ON po.order_code = sc.op_code
INNER JOIN `insider-data-lake.integrated.muninn_apparel_manufacturers` AS am
  ON am.id = po.apparel_manufacturer_id
INNER JOIN `insider-data-lake.integrated.muninn_suppliers` AS s
  ON s.id = am.supplier_id
WHERE sc.production_order_type NOT IN ('flexible', 'converted')
  AND sc.dt_planned_entry_warehouse BETWEEN <inicio_janela> AND <analysis_date>  -- janela do cohort
  AND (sc.dt_min_entry_warehouse IS NULL OR sc.dt_min_entry_warehouse > <analysis_date>)  -- ainda não recebida no CD
  AND sc.current_production_stage NOT IN ('pending', 'canceled')  -- exclui estágios inválidos p/ análise de detratora
```

---

## 4. Estágio produtivo relacionado a MP — `supply_chain_efficiency_model_input[_history]`

Dataset: `insider-data-lake.sop_silver`. Campo `current_production_stage` sinaliza
o estágio em que a OP se encontra. Os valores relacionados a matéria-prima são:

| Valor de `current_production_stage` | Significado |
|---|---|
| `raw_material_receiving` | Recebendo matéria-prima |
| `raw_material_pending` | Matéria-prima pendente de recebimento |

Constante de referência (`analytics/icp_diagnostics/references/icp_root_cause_functions.py`):

```python
_STAGES_MP: frozenset[str] = frozenset({"raw_material_receiving", "raw_material_pending"})
```

Esses estágios são usados como **sinal** de causa raiz `ATRASO_MP_CONFIRMADO` na
classificação de detratores de ICP — mas **não confirmam** o atraso por si só. A
confirmação definitiva do atraso vem do cruzamento de datas em
`muninn_production_orders` (`expected_fabric_receiving_date` vs
`real_fabric_receiving_date`, seção 3).

Outros estágios usados na hierarquia de causa raiz (para contexto, não são MP):
- `quality_inspection` — reprovação de qualidade (`_STAGE_QA`)
- `cut_fabric_and_sewing_process` — corte e costura (`_STAGE_SEW`)
- `finished`, `items_delivery_and_invoicing` — atraso de faturamento/NF (`_STAGES_NF`)
- `pending`, `canceled` — **sempre excluídos** de qualquer análise de OP detratora

---

## 5. Qualidade — cruzamento com atraso de MP (não é MP, mas frequentemente relacionado)

`insider-data-lake.sop_gold.quality_inspection_data` — Auditoria de qualidade por OP.

| Campo | Descrição |
|---|---|
| `op_code` | Chave de JOIN com a OP |
| `supplier_name` | Nome do fornecedor (na tabela de qualidade) |
| `audit_count` | Quantidade de auditorias realizadas |
| `dt_first_audit_completed` | Data da primeira auditoria concluída |
| `first_audit_result_standardized` | Resultado padronizado da 1ª auditoria (ex.: `qualita_rejected`) |
| `first_audit_deliberation_standardized` | Deliberação: `insider_approved` (reprovado mas liberado) ou `insider_rejected` (corrigir/reauditar) |
| `first_audit_defective_rate` | Taxa de defeito da 1ª auditoria |
| `pieces_rejected_in_first_audit_insider` | Peças rejeitadas na 1ª auditoria |
| `query_execution_date` | Data de execução da query (metadado) |

⚠️ **Gap de dado confirmado:** não há coluna de **tipo de defeito** (ex.: problema
de ribana/cor/tom) em `quality_inspection_data`, `quality_summary` nem
`quality_reaudit_data`. Validado em `analyses/ddal/mp_atrasada/README.md`. Se uma
análise precisar dessa granularidade (ex.: "quantas OPs tiveram atraso por defeito
de cor de ribana"), é necessário sinalizar esse gap — não é rastreável hoje.

---

## 6. Gotchas / limitações confirmadas

1. **`fabric_id` não é 1:1 com custo.** Um tecido pode ser cotado por várias
   malharias com preços diferentes — sempre usar `MIN`/`MAX(unit_price)` e
   `COUNT(DISTINCT knitting_factory_id)` para expor a faixa e a concentração de
   fornecimento, nunca pegar um valor único.
2. **Malharia é um `supplier_id`.** `muninn_knitting_factories.supplier_id` aponta
   para `muninn_suppliers`, assim como o fornecedor de confecção
   (`muninn_apparel_manufacturers.supplier_id`) — são dois papéis diferentes
   apontando para a mesma tabela de fornecedores. Não confundir os dois ao filtrar
   por fornecedor.
3. **Datas de MP vivem em outro dataset.** `expected_fabric_receiving_date` e
   `real_fabric_receiving_date` estão em `integrated.muninn_production_orders`, não
   em `sop_silver.supply_chain_efficiency_model_input` — é necessário JOIN por
   `order_code = op_code` para juntar as duas visões (estado agregado da OP × datas
   detalhadas de MP).
4. **Sem coluna de tipo de defeito de qualidade** ligada a matéria-prima (seção 5)
   — gap de dado a ser sinalizado em qualquer análise que precise dessa granularidade.
5. **Estágio de MP (`raw_material_*`) é sinal, não confirmação.** Usar sempre em
   conjunto com as datas de `muninn_production_orders` para confirmar o atraso real.
6. **Filtrar `muninn_fabric_skus.status = 'available'`** para custos vigentes —
   registros com outros status representam cotações desativadas/históricas.

---

## Referências

- Análise original de atraso de MP (fornecedor DDAL):
  `analyses/ddal/mp_atrasada/README.md`, `details.sql`, `extended_details.sql`
- Dashboard de lead time (padrão de custo de MP por SKU):
  `analytics/lead_time/lead_time_dashboard.ipynb` (Célula 1)
- Funções de causa raiz de ICP (estágios de MP):
  `analytics/icp_diagnostics/references/icp_root_cause_functions.py`
- Memória Serena consolidada: `bigquery_tables_materia_prima`
- Catálogo geral de tabelas: `base.md`, memória Serena `bigquery_tables`
