# Supplier [IN] — Governança de Matéria-Prima e Publicação (2026-08-14)

> Consultar junto de `governanca_materia_prima_20260807.md`. Esta memória é a atualização
> operacional da base e da publicação; não substitui o baseline histórico de 2026-08-07.

## Estado confirmado

- Notebook Deepnote vigente: `bot_fabric_governance`
  (`dbf42a43d57241dbb4bf0727cedd371b`), no projeto `Bot Supplier In`
  (`5846542d-63c1-48fb-8d2c-3f8b33253048`). É agendado e o último run visível foi
  `5ea6e2db-495b-43e4-8bdc-9c85795db255` em 2026-08-14 17:34 UTC.
- A referência anterior a `fabric_governance` / `Governança Lead Time` é histórica e não deve
  ser usada para localizar ou alterar a base vigente.
- Todas as seis células SQL consultam BigQuery pelo projeto `insider-data-lake`: dataset
  `integrated` e, no recorte comercial, dataset `fpa`. A integração BigQuery conhecida no
  workspace é `bigquery-integration` (`5088268a-9640-458a-83d1-46b07b955fae`).
- A planilha de consumo é `1XB9cZztzBziergCs7AzaeUMUzlM-Tg9vooakHSqaamk`; BigQuery/Deepnote
  seguem como fonte de verdade.
- Dry-run `be58215a-2ead-4321-ab15-5eefedc2c8e6`: sucesso, sem escrita externa.
- Publicação `1ee0c894-6a2a-4d21-9dc3-859be96388c8`: sucesso, 10 blocos executados e 0 falhas.

## Contrato de publicação

As sete saídas são resolvidas por `gid`, não por título, e são sobrescritas somente depois do
preflight global. O preflight bloqueia `SAIDAS` incompleta, dataframe vazio, cabeçalho inválido
ou duplicado e valores `ARRAY`/`STRUCT` antes da primeira limpeza. Escrita em lotes de 2.000
linhas, seguida de validação de cabeçalho, dimensão e última linha.

| Saída | Aba | Resultado da publicação 2026-08-10 |
|---|---|---|
| `dim_mp_fornecimento` | `dim_mp_fornecimento` (0) | 401 × 51 |
| `fact_sku_bom` | `fact_sku_bom` (1263944509) | 24.406 × 27 |
| `fact_mp_lt_realizado` | `fact_mp_leadtime_realizado` (784021430) | 6.990 × 31 |
| `fact_sku_economics` | `fact_sku_economics` (1858110063) | 21.091 × 31 |
| `mart_produto_mp` | `mart_produto_mp` (2006224731) | 167 × 28 |
| `dicionario_dados` | `dicionario_dados` (1605547778) | 163 × 7 |
| `governanca` | `df_governanca` (1425809795) | 26 × 6 |

`ESCREVER_SHEETS=True` é produtivo; definir como `False` faz dry-run. Não há scheduler
configurável pela superfície do Deepnote disponível: configurar manualmente a agenda diária às
03:00 BRT na interface e acompanhar o primeiro run agendado.

## Inventário de bases e tabelas do notebook vigente

O inventário abaixo foi extraído das seis consultas SQL do notebook em 2026-08-14. São **18
tabelas físicas**: 17 em `insider-data-lake.integrated` e uma em
`insider-data-lake.fpa`. Não há leitura de tabelas `sop_silver` no SQL atual.

| Dataset | Tabela | Papel no modelo | Saídas que a consomem |
|---|---|---|---|
| `integrated` | `muninn_fabric_skus` | Cotação de tecido: preço, status, MOQ e malharia | dimensão, BOM, LT, economics, mart, auditoria |
| `integrated` | `muninn_fabrics` | Tecido, artigo e `product_color_id` | dimensão, BOM, LT, mart, auditoria |
| `integrated` | `muninn_articles` | Cadastro de artigo | dimensão, BOM, LT, mart, auditoria |
| `integrated` | `muninn_knitting_factories` | Papel de malharia e vínculo com fornecedor | dimensão, BOM, LT, mart |
| `integrated` | `muninn_suppliers` | Nome, localização, tipo e qualidade cadastral do fornecedor | dimensão, BOM, LT, mart |
| `integrated` | `muninn_articles_knitting_factories` | LT cadastrado e parâmetros por artigo × malharia | dimensão, LT, mart, auditoria |
| `integrated` | `muninn_supplier_agreement` | Vigência do acordo do fornecedor | dimensão |
| `integrated` | `muninn_payment_agreement` | Forma, parcelas e vencimento de pagamento | dimensão |
| `integrated` | `muninn_product_skus_fabrics` | Sistema de registro da BOM por SKU × tecido | BOM, economics, mart, auditoria |
| `integrated` | `muninn_product_skus` | Relação SKU de produto, produto e nome do SKU | BOM, economics, mart, auditoria |
| `integrated` | `muninn_products` | Nome do produto | BOM |
| `integrated` | `skus` | Atributos comerciais e custo de MP de referência | BOM, economics, mart |
| `integrated` | `sku_bill_of_materials` | BOM derivada, apenas reconciliação | BOM, auditoria |
| `integrated` | `muninn_fabric_orders` | Pedido de tecido e datas de faturamento | LT, mart |
| `integrated` | `cmv_model` | CMV histórico por SKU/data | economics, mart, auditoria |
| `integrated` | `markup_prices` | Markup; exige deduplicação para selecionar o vigente | economics, mart |
| `integrated` | `product_cost` | Fonte alternativa de custo por SKU | economics |
| `fpa` | `analytical_dre` | Venda L8M para o recorte comercial | mart |

### Mapeamento de tabelas publicadas

| Saída | Grão | Fontes primárias |
|---|---|---|
| `dim_mp_fornecimento` | `fabric_sku_id` | `muninn_fabric_skus` + cadastros de tecido/artigo/malharia/fornecedor + acordos |
| `fact_sku_bom` | `sku` × `fabric_id` | `muninn_product_skus_fabrics`; `sku_bill_of_materials` apenas como flag |
| `fact_mp_lt_realizado` | `fabric_order_id` | `muninn_fabric_orders` + `muninn_articles_knitting_factories` |
| `fact_sku_economics` | `sku` | BOM agregada + `cmv_model` + `markup_prices` + `product_cost` |
| `mart_produto_mp` | `product_name` | recorte L8M de `fpa.analytical_dre`, com enriquecimento de MP/economics |
| `dicionario_dados` / `df_governanca` | metadados / check | gerados no notebook; a auditoria relê as fontes de BOM, LT e CMV |

### Regras de uso que não podem ser alteradas por acidente

- `muninn_product_skus_fabrics` é o sistema de registro; `sku_bill_of_materials` é superset e
  só reconcilia. Divergência de consumo bloqueia publicação.
- O fornecedor é sempre `muninn_knitting_factories → muninn_suppliers`; não usar o papel de
  confecção.
- `muninn_supplier_agreement` precisa ser deduplicada por `supplier_agreement_id` antes do join.
- A métrica de atraso usa `muninn_fabric_orders.estimated_invoicing_date` (compromisso original).
  `updated_invoicing_date` é apenas diagnóstico de repactuação.
- Tabelas Muninn são snapshots: não inferir histórico de preço ou status entre execuções.
- `fpa.analytical_dre` e `cmv_model` concentram o custo de leitura do mart (cerca de 5 GB no
  baseline histórico); evitar reexecuções sem necessidade.

## Supplier [IN]: cobertura e gaps

**Disponível agora:** cadastro/localização de malharia, artigo, tecido/cor, `fabric_sku`,
cotações, custo, MOQ, múltiplos, rolo, frete, condições comerciais, consumo por SKU, CMV,
markup, LT cadastrado/realizado, atraso versus compromisso original, vendas L8M e concentração
por número de malharias.

**Parcial:** dual sourcing (preço/faixa e número de alternativas, sem equivalência técnica) e
simulador de CMV/markup (tem custo histórico, mas não recebe PV/meta/volume de briefing).

**Não disponível no lake atual:** composição estruturada, gramatura, largura, peeling/pilling,
capacidade de malharia, moeda de cotação, claims, certificados, laudos, anexos e histórico de
preço/status. Não existem também intake, Kanban/SLA de sourcing, portal/self-onboarding ou
repositório documental governado.

## Decisão de ingestão

Fase seguinte: formulário/template CSV padronizado por fornecedor, com validação de unidade,
composição, gramatura, largura, pilling, capacidade, moeda, claims e referência de documento.
A ingestão deve cair primeiro em staging no BigQuery, ter validação/rejeição por linha e só então
ser promovida ao contrato de consumo. Não escrever diretamente nas tabelas Muninn nem substituir
campos ausentes por zero.

Relacionado: `governanca_materia_prima_20260807.md`,
`analytics/governanca_mp/COBERTURA_ATUAL_SUPPLIER_IN.md`.
