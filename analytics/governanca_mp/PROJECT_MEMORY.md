# Memória do Projeto — Governança de Matéria-Prima

Atualizada em: 2026-08-14  
Status: inventário de fontes reconciliado com o notebook Deepnote vigente; base estruturada e publicada no Google Sheets.

## Referências canônicas

- Deepnote vigente: `bot_fabric_governance` — ID `dbf42a43d57241dbb4bf0727cedd371b`.
- Projeto vigente: `Bot Supplier In` — ID `5846542d-63c1-48fb-8d2c-3f8b33253048`.
- URL: https://deepnote.com/workspace/INSIDER-Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Bot-Supplier-In-5846542d-63c1-48fb-8d2c-3f8b33253048/notebook/botfabricgovernance-dbf42a43d57241dbb4bf0727cedd371b
- O notebook tem seis blocos SQL, é agendado e usou por último o run
  `5ea6e2db-495b-43e4-8bdc-9c85795db255` em 2026-08-14 17:34 UTC.
- A referência anterior `fabric_governance` / `Governança Lead Time` é histórica; não alterar
  aquele ativo quando a demanda se referir a Supplier [IN].
- Último dry-run: `be58215a-2ead-4321-ab15-5eefedc2c8e6`, sucesso em 2026-08-10, sem escrita no Sheets.
- Última publicação: `1ee0c894-6a2a-4d21-9dc3-859be96388c8`, sucesso em 2026-08-10, com 0 blocos falhos.
- Documentação operacional: `README.md`.
- Material para stakeholder: `COBERTURA_ATUAL_SUPPLIER_IN.md`.

## Decisões já tomadas

- O notebook é uma camada de dados com contrato estável, não dashboard.
- Escopo estrito: MP, artigo, tecido e malharia. Confecção fica fora.
- `muninn_product_skus_fabrics` é o sistema de registro da BOM de SKU.
- `sku_bill_of_materials` é superset e é usado para reconciliação, não como fonte concorrente.
- Aderência de pedido de tecido é medida contra o compromisso original
  (`estimated_invoicing_date`); a data repactuada serve ao diagnóstico.
- Saídas precisam ser planas e com tipos escalares, para compatibilidade com Sheets e Lovable.
- `NULL` preserva ausência real de cadastro; nunca normalizar para zero.
- A publicação no Sheets usa os sete `gid`s fixos, preflight global e overwrite em lotes de 2.000 linhas.
- O agendamento definido é diário às 03:00 BRT; a configuração ainda depende da interface do Deepnote.

## Inventário de dados do notebook vigente

**Conexão.** BigQuery, projeto `insider-data-lake`; integração conhecida no workspace:
`bigquery-integration` (`5088268a-9640-458a-83d1-46b07b955fae`). As fontes estão em dois
datasets: `integrated` (17 tabelas) e `fpa` (1 tabela). Não há SQL atual em `sop_silver`.

| Dataset | Tabelas |
|---|---|
| `integrated` | `muninn_fabric_skus`, `muninn_fabrics`, `muninn_articles`, `muninn_knitting_factories`, `muninn_suppliers`, `muninn_articles_knitting_factories`, `muninn_supplier_agreement`, `muninn_payment_agreement`, `muninn_product_skus_fabrics`, `muninn_product_skus`, `muninn_products`, `skus`, `sku_bill_of_materials`, `muninn_fabric_orders`, `cmv_model`, `markup_prices`, `product_cost` |
| `fpa` | `analytical_dre` |

| Saída | Fontes de negócio |
|---|---|
| `dim_mp_fornecimento` | Cotação (`muninn_fabric_skus`), artigo/tecido, malharia/fornecedor, acordos e LT cadastrado. |
| `fact_sku_bom` | `muninn_product_skus_fabrics` como fonte de registro; `sku_bill_of_materials` só para reconciliação. |
| `fact_mp_lt_realizado` | `muninn_fabric_orders`, enriquecida com o LT cadastrado. |
| `fact_sku_economics` | BOM agregada, `cmv_model`, `markup_prices`, `product_cost` e `skus`. |
| `mart_produto_mp` | Venda L8M de `fpa.analytical_dre`, com MP/economics e cadastro de malharia. |
| `dicionario_dados` / `df_governanca` | Gerados no notebook; a auditoria consulta BOM, LT e CMV. |

### Guardrails de fontes

- Usar `muninn_knitting_factories → muninn_suppliers` para o fornecedor de malharia.
- Deduplicar `muninn_supplier_agreement` por `supplier_agreement_id` antes do join.
- Medir atraso contra `estimated_invoicing_date`; `updated_invoicing_date` é diagnóstico.
- Tratar tabelas Muninn como snapshots, sem série histórica de preço/status.
- `markup_prices` requer deduplicação para selecionar o registro vigente.
- O mart lê `fpa.analytical_dre` e `cmv_model`; custo histórico aproximado: 5 GB por execução.

## Fatos validados

- As 24.388 linhas de `muninn_product_skus_fabrics` conciliavam 100% em
  `(sku, fabric_id, consumption)` com `sku_bill_of_materials`; zero divergência.
- `sku_bill_of_materials` tinha 36.139 linhas e é superset da BOM de SKU.
- `product_color_id` estava preenchido nos 602 tecidos: tecido é associado a cor,
  não uma entidade genérica.
- Gramatura e largura não foram encontradas no data lake e permanecem como campos nulos
  declarados no contrato.
- `muninn_supplier_agreement` tem duplicidade em `supplier_agreement_id`: dedupe é obrigatório.
- `field_errors = '{}'` em fornecedor significa cadastro sem erro.
- As tabelas Muninn consultadas são snapshots; não suportam análise histórica de custo/status.

## Baseline operacional (2026-08-07)

- `dim_mp_fornecimento`: 401 linhas, 51 colunas; 318 cotações vigentes.
- `fact_sku_bom`: 24.388 linhas, 27 colunas.
- `fact_mp_lt_realizado`: 6.990 linhas, 31 colunas.
- `fact_sku_economics`: 21.073 linhas, 31 colunas.
- `mart_produto_mp`: 166 linhas, 28 colunas.
- 26 checks: 0 `FALHA`, 2 `ATENCAO`.
- Aderência observada: 59,7% dos pedidos faturados após o compromisso original;
  557 pedidos tiveram repactuação.

## Publicação no Sheets (2026-08-10)

- Destino: planilha `1XB9cZztzBziergCs7AzaeUMUzlM-Tg9vooakHSqaamk`.
- `dim_mp_fornecimento`: 401 × 51; `fact_sku_bom`: 24.406 × 27;
  `fact_mp_lt_realizado`: 6.990 × 31; `fact_sku_economics`: 21.091 × 31.
- `mart_produto_mp`: 167 × 28; `dicionario_dados`: 163 × 7;
  `df_governanca`: 26 × 6.
- Preflight bloqueia saídas ausentes, vazias, com colunas duplicadas ou valores aninhados
  antes de limpar qualquer aba. A terceira aba mantém o título
  `fact_mp_leadtime_realizado`, resolvida pelo `gid`.

## Riscos e limitações permanentes

- Não inferir tendência de custo de MP de execuções isoladas; é necessário persistir snapshots.
- Não juntar `fact_sku_bom` com `fact_sku_economics` sem agregação prévia por SKU.
- Não usar fornecedor de confecção em joins de malharia.
- Cobertura de LT cadastrado de malharia é parcial; comparativos cadastrado × realizado
  só são válidos no subconjunto coberto.
- O bloco comercial pode processar aproximadamente 5 GB. Não agendar/rodar sem necessidade.

## Próxima ação recomendada

Configurar o agendamento diário às 03:00 BRT na interface do Deepnote e acompanhar
o primeiro run agendado. Antes de qualquer mudança de schema ou fonte, revisar
`README.md`, executar os checks e registrar aqui o impacto e o novo baseline.
