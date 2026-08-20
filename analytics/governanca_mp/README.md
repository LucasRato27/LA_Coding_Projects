# Governança de Matéria-Prima

## Objetivo

Este diretório contém a camada governada de dados de matéria-prima (MP) para
tecido, artigo e malharia. O produto do notebook são tabelas planas, com grão e
contrato de colunas explícitos, para consumo posterior em Google Sheets, Lovable
ou outra camada analítica. Ele **não é um dashboard** e não calcula indicadores
de confecção.

## Ativo principal

- Deepnote: [fabric_governance](https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/fabricgovernance-62e6d7b178ac492a837fba610b5590c6?utm_content=62b1671d-dc82-4747-9bbb-b134a69a3491)
- Projeto Deepnote: `Governança Lead Time` (`62b1671d-dc82-4747-9bbb-b134a69a3491`)
- Notebook ID: `62e6d7b178ac492a837fba610b5590c6`
- Espelho local: [`fabric_governance.ipynb`](fabric_governance.ipynb)

O Deepnote é a referência operacional. O notebook local é o artefato versionável
para revisão e manutenção; antes de editar qualquer um, conferir se ambos ainda
estão alinhados.

## Escopo e fronteiras

Inclui:

- cadastro de artigo, tecido, `fabric_sku`, malharia, cotações e condições comerciais;
- ficha técnica de SKU e consumo de tecido;
- lead time de malharia cadastrado e realizado;
- custo de MP, CMV e markup por SKU;
- recorte comercial por produto com vendas nos últimos oito meses;
- auditoria de qualidade e exportação CSV.

Fica fora do escopo:

- confecção: LT cadastrado, LT realizado e recebimento de MP na OP;
- `muninn_production_orders` e `product_production_parameters`;
- tendência histórica de preço/status de MP, pois as tabelas Muninn usadas são snapshots;
- gramatura e largura, que não possuem fonte disponível no data lake.

## Modelo de dados publicado

| Tabela | Grão / chave | Finalidade |
|---|---|---|
| `dim_mp_fornecimento` | `fabric_sku_id` | Cadastro da cotação de tecido em uma malharia, custo, status, MOQ, frete, condição comercial e LT cadastrado. |
| `fact_sku_bom` | `sku` × `fabric_id` | Ficha técnica real, consumo por peça e faixa de custo de MP. |
| `fact_mp_lt_realizado` | `fabric_order_id` | Pedido de tecido, LT realizado e aderência ao compromisso original/repactuado. |
| `fact_sku_economics` | `sku` | CMV, markup, custo de MP e participação de MP no CMV. |
| `mart_produto_mp` | `product_name` | Visão comercial de produtos com venda L8M e tecido principal. |
| `dicionario_dados` | `tabela` × `coluna` | Dicionário gerado a partir do contrato de schema. |
| `df_governanca` | `check` | Resultado dos controles de qualidade. |

Relações principais:

```text
dim_mp_fornecimento --fabric_id--> fact_sku_bom --sku--> fact_sku_economics
          |
          +--fabric_sku_id--> fact_mp_lt_realizado

mart_produto_mp: derivada comercial; não usar como dimensão relacional por ID.
```

`fact_sku_bom` tem múltiplas linhas por SKU. Agregue-a antes de juntá-la a
`fact_sku_economics`, para não duplicar custos ou métricas por SKU.

## Premissas de negócio críticas

1. O fornecedor do modelo é a **malharia**, obtida por
   `muninn_knitting_factories → muninn_suppliers`; nunca usar o fornecedor de
   confecção.
2. A fonte de registro da BOM é `muninn_product_skus_fabrics`.
   `sku_bill_of_materials` é um superset usado apenas para reconciliação.
3. A métrica contratual de atraso usa `estimated_invoicing_date` (compromisso
   original). `updated_invoicing_date` é diagnóstico de repactuação; substituí-la
   pela data original mascara atrasos.
4. `custo_mp_*` representa somente matéria-prima; `cmv_unitario` representa o
   custo total. Divergência entre eles não é, por si só, erro.
5. `NULL` significa ausência real de cadastro. Não substituir por `0`, `N/A` ou
   outro valor sintético.
6. Um tecido é associado a uma cor (`product_color_id`); a hierarquia correta é
   artigo → tecido/cor → `fabric_sku` → SKU de produto.

## Fontes BigQuery relevantes

As consultas usam principalmente `insider-data-lake.integrated`:

- `muninn_fabric_skus`, `muninn_fabrics`, `muninn_articles`;
- `muninn_knitting_factories`, `muninn_suppliers`;
- `muninn_product_skus_fabrics`, `muninn_product_skus`, `muninn_products`, `skus`;
- `muninn_fabric_orders`, `muninn_articles_knitting_factories`;
- `muninn_supplier_agreement`, `muninn_payment_agreement`;
- `sku_bill_of_materials`, `cmv_model`, `markup_prices` e a fonte comercial de vendas.

Cuidados de modelagem:

- deduplicar `muninn_supplier_agreement` por `supplier_agreement_id` antes do join;
- `muninn_suppliers.field_errors = '{}'` indica cadastro OK; testar apenas `IS NOT NULL`
  produz falso positivo;
- aplicar `status = 'available'` apenas às faixas de custo vigente. A dimensão mantém
  também cotações inativas para auditoria;
- `COUNT(DISTINCT ...) OVER (...)` não é permitido no BigQuery: usar CTE agregada;
- a normalização de nomes de artigo pode fundir IDs; não expor um `article_id` arbitrário
  no mart agregado.

## Contrato de publicação

- colunas em `snake_case`, sem acentos ou espaços;
- valores escalares somente: sem `ARRAY` e `STRUCT`;
- listas são serializadas com `;` e acompanhadas de uma contagem quando aplicável;
- datas no padrão ISO `YYYY-MM-DD`;
- todos os IDs disponíveis são preservados;
- ordem e coerção de tipos são aplicadas pelo `SCHEMA_CONTRATO` do notebook;
- todas as tabelas carregam `atualizado_em`.

O dicionário de dados é derivado do mesmo contrato. Ao incluir, remover ou renomear
uma coluna, atualizar o contrato — não editar apenas a documentação.

## Execução e saídas

O notebook tem dois controles no primeiro bloco de código:

- `EXPORTAR_CSV = True`: publica os CSVs em `exports/`;
- `ESCREVER_SHEETS = True`: publica as sete saídas no destino produtivo após o
  preflight completo de dados, autenticação e abas. Use `False` somente para dry-run.

As saídas seguem o padrão `exports/governanca_mp_<tabela>.csv`. O único ponto de
publicação é o dicionário `SAIDAS`; uma nova tabela só deve ser exposta depois de
entrar no contrato, passar na auditoria e ser adicionada nele. A publicação no
Sheets resolve cada aba pelo `gid`, escreve em lotes de 2.000 linhas e valida
cabeçalho e dimensão depois do overwrite.

A primeira publicação foi concluída com sucesso em 2026-08-10. Configurar o
Deepnote para rodar diariamente às 03:00 BRT; a agenda ainda depende da interface
do Deepnote. Antes de rodar, considerar que o bloco de `mart_produto_mp` lê
`fpa.analytical_dre` e `cmv_model` e tem custo aproximado de 5 GB; os demais
blocos leem poucos MB.

## Auditoria e critérios de publicação

`df_governanca` registra controles de chave, volume, reconciliação, papel de
fornecedor, cobertura de cadastro, frescor, sanidade e aderência de LT.

| Status | Ação |
|---|---|
| `FALHA` | Não publicar. O notebook interrompe a execução. |
| `ATENCAO` | Publicável com investigação e registro do impacto. |
| `GAP_CONHECIDO` | Lacuna já documentada; não é regressão por si só. |
| `OK` | Dentro da regra de controle. |

O controle crítico é `reconciliacao__consumo_divergente`: qualquer valor acima de
zero significa que as duas fontes de BOM se separaram e deve bloquear a publicação.

## Baseline conhecido

Baseline medido em 07/08/2026, para detectar degradação — não é uma meta imutável:

- `dim_mp_fornecimento`: 401 linhas (318 cotações `available`);
- `fact_sku_bom`: 24.388 linhas;
- `fact_mp_lt_realizado`: 6.990 linhas;
- `fact_sku_economics`: 21.073 linhas;
- `mart_produto_mp`: 166 produtos;
- 602 tecidos, todos associados a cor; 96 artigos;
- 0 divergências de consumo entre as fontes de BOM.

No último run validado (`4843b844-2d43-4e06-bfc1-ff9d998f642d`), houve 26 checks,
0 falhas e 2 atenções: cadastro incompleto de malharia e participação de MP no CMV
fora da faixa esperada em parte dos SKUs.

## Alterações seguras e próximas evoluções

Antes de alterar regra, schema ou fonte:

1. preservar o grão das tabelas já publicadas;
2. explicitar impacto no contrato e no consumidor;
3. executar a auditoria e comparar com o baseline;
4. registrar uma nova memória em [`PROJECT_MEMORY.md`](PROJECT_MEMORY.md).

Quick win: implementar a camada de exportação para Google Sheets consumindo apenas
`SAIDAS`, sem replicar consultas ou regras de negócio.

Evolução 10x: criar uma tabela de snapshots versionados de `fabric_skus` e das
dimensões Muninn. Isso habilita tendência de preço, mudanças de fornecedor/status e
alertas de risco sem confundir retrato atual com histórico.
