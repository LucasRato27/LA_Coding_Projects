# Governança de Matéria-Prima — Base de Dados (2026-08-07)

> Atualização operacional em [`supplier_in_governanca_mp_20260810.md`](supplier_in_governanca_mp_20260810.md):
> exportação para Sheets publicada, cobertura atual do Supplier [IN] e decisão de ingestão.

> **Documentação técnica completa** (schema campo a campo, SQL exato de cada bloco, contrato
> de saída, dicionário de dados): `analytics/lead_time/documentacao_governanca_materia_prima.md`.
> Este arquivo registra decisões e achados de handoff de conversa — não repete o SQL.

Notebook Deepnote `fabric_governance` (`62e6d7b178ac492a837fba610b5590c6`), projeto
`Governança Lead Time` (`62b1671d-dc82-4747-9bbb-b134a69a3491`), integração BigQuery
`5088268a-9640-458a-83d1-46b07b955fae`.

## Papel do notebook

**Camada de estruturação de base, não dashboard.** Produz tabelas planas com contrato de
schema estável, para consumo externo: extração para Google Sheets primeiro, Lovable depois.
Sem gráficos. Qualidade sai como coluna/flag e como tabela de auditoria.

## Escopo (decidido com Lucas em 2026-08-07)

- **Fabric governance puro: MP, tecido, artigo, malharia.**
- **Confecção fora de tudo** — nem LT cadastrado, nem realizado, nem recebimento de MP na OP.
- **Lead time cadastrado e realizado, ambos da malharia.** Onde não há cadastro, fica em branco.
- Universo completo de cadastro nas tabelas governadas + uma derivada com o recorte comercial.

## Modelo

| Tabela | Grão | Fonte principal |
|---|---|---|
| `dim_mp_fornecimento` | `fabric_sku_id` | `muninn_fabric_skus` + fabrics/articles/knitting_factories/suppliers |
| `fact_sku_bom` | `sku × fabric_id` | `muninn_product_skus_fabrics` |
| `fact_mp_lt_realizado` | `fabric_order_id` | `muninn_fabric_orders` |
| `fact_sku_economics` | `sku` | `cmv_model` + `markup_prices` + custo de MP agregado |
| `mart_produto_mp` | `product_name` | derivada comercial (venda L8M, tecido principal) |
| `dicionario_dados` | `tabela × coluna` | gerado do próprio `SCHEMA_CONTRATO` |

## Achados verificados no BigQuery (medidos, não inferidos)

1. **`sku_bill_of_materials` existe em `insider-data-lake.integrated`** — a documentação
   anterior só conhecia a versão `_br` em `insider-lake-sensitive`, inacessível. Comparação
   executada: 24.388 linhas de `muninn_product_skus_fabrics` casam 100% em
   `(sku, fabric_id, consumption)`, **zero divergência de consumo**; `sku_bill_of_materials`
   tem 36.139 linhas (11.751 extras). **É superset, não fonte concorrente.** Risco 6.1 da
   documentação de handoff está resolvido. `muninn_product_skus_fabrics` segue como sistema
   de registro; a outra entra só como flag de reconciliação.

2. **Gramatura e largura NÃO existem no data lake.** Varredura de
   `INFORMATION_SCHEMA.COLUMNS` em `integrated` + `sop_silver` por
   `gramatura|largura|grammage|width|composic|yield` não retorna nada de MP. Os nomes de artigo
   ("Modal", "Bi Stretch", "Ponto de Roma") também não codificam. Gap real. As colunas existem
   no contrato com valor nulo para o consumidor não quebrar quando a fonte aparecer.

3. **`product_color_id` está preenchido em 100% dos 602 tecidos** (0 nulos; 242 cores
   distintas; 111 cores com mais de um tecido). Ou seja: **tecido não é entidade genérica de
   cadastro — é sempre amarrado a uma cor de produto.** A hierarquia
   "Artigo → Tecido → Fabric SKU → Product SKU" da documentação anterior está **incompleta**:
   falta a dimensão de cor no meio. Risco 6.2 resolvido com dado.

4. **Tabelas que resolvem os campos pedidos e não estavam documentadas:**
   - `muninn_articles_knitting_factories` (50 linhas) → LT cadastrado da malharia
     (`coloring_time` + `production_time`). **Cobre 44 de 96 artigos, mas 383 de 401
     fabric_skus (95,5%)** — a contagem de artigos engana, porque os artigos cobertos são
     justamente os que concentram as cotações. Medir cobertura por artigo subestima muito.
   - `muninn_fabric_orders` (6.990 linhas) → LT realizado da malharia.
   - `cmv_model` → CMV real por SKU/dia, 39.930 SKUs, fresco até 2026-08-06.
   - `markup_prices` → 69.302 linhas / 4.806 SKUs; 4.806 vigentes. **Precisa dedupe.**
   - `integrated.skus.fabric_cost` → custo de MP por SKU já calculado por outro processo.
     Serve de reconciliação contra o custo calculado na base.
   - `muninn_supplier_agreement` / `muninn_payment_agreement` → condição comercial vigente.
   - `product_production_parameters` → LT de confecção. **Fora de escopo por decisão.**

5. **`muninn_fabric_orders` repete o padrão de repactuação de data** do lead time produtivo:
   `estimated_invoicing_date` (original) vs `updated_invoicing_date` (repactuado). A métrica de
   contrato usa a **original**; medir contra a repactuada esconde atraso. Mesma lógica da
   correção de postergação em [[premissas_fundamentais_calculo]].

## Armadilhas de dado confirmadas

- **`muninn_supplier_agreement` tem `supplier_agreement_id` duplicado** (137 linhas / 122 ids)
  → exige dedupe antes de qualquer join.
- **`muninn_suppliers.field_errors` é JSON string e vale `'{}'` quando o cadastro está OK**
  (108 de 124). Testar `IS NOT NULL` classifica 100% como incompleto — errado.
- **`muninn_suppliers.type` tem só `manufacturer` e `knitting`.** Os dois papéis apontam para a
  mesma tabela. A base testa explicitamente que só entra `knitting`.
- Todas as tabelas Muninn têm **um único `ingestion_date`** e nenhum id duplicado — são
  snapshot do estado atual, sem risco de fan-out e **sem histórico versionado**.
- `COUNT(DISTINCT)` **não funciona como função analítica** no BigQuery — exige CTE agregada.
- Na normalização `Modal (\d+)` → `Modal`, **vários `article_id` se fundem num nome só**.
  Chavear lead time por `article_id` pegaria um id arbitrário — o join tem que usar o nome
  normalizado, e `article_id` não deve ser exposto no mart.

## Baseline de auditoria (2026-08-07)

- `dim_mp_fornecimento`: 401 linhas (318 `available`), 401 PK únicos
- `fact_sku_bom`: 24.388 linhas
- `sku_bill_of_materials`: 36.139 linhas, 0 consumo divergente
- `mart_produto_mp`: 166 produtos, checksum de `consumo_mediano` = 64,9265
- `muninn_fabrics`: 602, sendo 602 com cor
- `muninn_articles`: 96

O `mart_produto_mp` reproduz a query de produção em uso; os dois números acima são a
referência para conferir que a derivada não mudou o recorte.

## Resultado do run de validação (`4843b844-2d43-4e06-bfc1-ff9d998f642d`, status `success`)

26 checks de governança: **0 FALHA, 2 ATENÇÃO**. Tabelas geradas:
`dim_mp_fornecimento` 401×51, `fact_sku_bom` 24.388×27, `fact_mp_lt_realizado` 6.990×31,
`fact_sku_economics` 21.073×31, `mart_produto_mp` 166×28, `dicionario_dados` 163,
`df_governanca` 26. Sete CSVs em `exports/`.

**Achados operacionais do primeiro run (não são erro de dado — são resultado):**

- **59,7% dos pedidos de tecido são faturados depois do compromisso ORIGINAL.**
- **557 pedidos tiveram a data de faturamento repactuada.** Isso confirma que medir aderência
  contra `updated_invoicing_date` esconderia a maior parte do atraso — a decisão de usar a
  data original como métrica de contrato não é preferência de estilo, é o que preserva o sinal.
- CMV cobre 20.203 de 21.073 SKUs da base; markup vigente cobre 4.665.

**As 2 atenções (esperadas, não bloqueiam):**

- 143 cotações cuja malharia tem `field_errors` preenchido (cadastro de fornecedor incompleto).
- 238 SKUs com `pct_mp_no_cmv` fora de (0,1] — indica escopo de custo trocado nesses casos,
  que é exatamente o que o aviso de escopo da tabela existe para sinalizar.

## Custo de execução

O bloco `mart_produto_mp` lê `fpa.analytical_dre` e `cmv_model` — ~5 GB por execução. Os demais
varrem poucos MB. Considerar isso antes de agendar o notebook.

Relacionado: [[premissas_fundamentais_calculo]], [[bases_e_premissas]], [[source_inventory]].
