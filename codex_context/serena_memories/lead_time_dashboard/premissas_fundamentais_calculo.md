# Lead Time Dashboard — Premissas Fundamentais de Cálculo

Atualizado em 2026-08-04 a partir das decisões de negócio desta conversa e do
notebook Deepnote validado.

## Autoridade e escopo

- Notebook: `lead_time_dashboard`.
- Notebook ID: `0b56034555ea4a43a7d80e0ede768923`.
- Projeto Deepnote: `Governanca Lead Time`
  (`62b1671d-dc82-4747-9bbb-b134a69a3491`).
- Fonte principal:
  `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`.
- Este documento é a referência canônica para comparar o Dashboard Geral e a
  seção de SLA planejado menor que 100 dias.
- Em caso de conflito, prevalecem: pedido atual do usuário, `AGENTS.md`,
  notebook/dados atuais e, por último, esta memória.
- O histórico completo da troca de primeira aparição por `ingestion_date` para
  criação transacional por `created_at` está em
  `transicao_data_criacao_ingestion_created_at.md`.

## Princípio de paridade entre as seções

Dashboard Geral e seção SLA devem usar as mesmas premissas fundamentais:

- mesma fonte e grão de OP;
- mesma formação do universo elegível;
- mesma definição de data planejada original;
- mesmos filtros interativos;
- mesma definição de início e fim do lead time realizado;
- mesma correção de postergação;
- mesmas exclusões estruturais.

As diferenças deliberadas são de objetivo e apresentação:

- Dashboard Geral representa o modelo legado, com referência de prazo realizado
  de 120 dias;
- seção SLA representa o novo modelo: entra somente OP com
  `0 < sla_planejado_dias < 100` e o desempenho realizado é avaliado contra
  90 dias;
- gráficos podem ser diferentes porque respondem perguntas distintas;
- a seção SLA pode manter o recorte visual a partir de agosto de 2025.

## Universo de OPs

Todas as queries que leem OPs devem restringir o universo na própria leitura da
fonte:

```sql
WHERE production_order_type = 'committed'
```

A agregação por `op_code` pode manter:

```sql
ANY_VALUE(production_order_type) AS production_order_type
```

O filtro pós-query pode permanecer como defesa adicional, mas não substitui o
predicado SQL. Nenhuma CTE de data original, postergação, baseline, lookup,
diagnóstico ou dashboard pode ler OP `flexible`, `converted` ou outro tipo.

Exclusões compartilhadas:

- OP cancelada;
- ciclo contendo `B2B` ou `EPA`, case-insensitive;
- OP sem os campos obrigatórios da métrica correspondente.

Fornecedor atualmente `terminated`/`discontinued` e SKU atualmente
`desativado` **não** invalidam OP histórica: ambos permanecem no universo,
nos filtros e nas métricas realizadas. A ausência de LT teórico cadastral
deve permanecer auditável, sem remover a OP de métrica realizada.

## Data planejada original e cohort

`dt_planned_original` é a primeira `dt_planned_entry_warehouse` não nula
observada cronologicamente, depois de filtrar o histórico para `committed` e
deduplicá-lo por OP e dia.

Sequência obrigatória:

1. Particionar por `op_code` e `DATE(ingestion_date)`.
2. Ordenar por `ingestion_date ASC`.
3. Manter o primeiro snapshot de cada dia.
4. Entre os snapshots diários, selecionar a primeira data planejada não nula.

```sql
ROW_NUMBER() OVER (
  PARTITION BY op_code, DATE(ingestion_date)
  ORDER BY ingestion_date ASC
) AS rn
```

```sql
ARRAY_AGG(
  dt_planned_entry_warehouse IGNORE NULLS
  ORDER BY ingestion_date ASC
  LIMIT 1
)[SAFE_OFFSET(0)] AS dt_planned_original
```

Regras:

- não usar `MIN(dt_planned_entry_warehouse)`;
- não usar a data planejada corrente para definir o cohort;
- não usar fallback para outra data planejada;
- OP sem `dt_planned_original` não entra no cohort;
- Dashboard Geral, SLA e `df_postponement` precisam reconciliar essa data sem
  divergências.

## Filtros interativos

Todos os gráficos e tabelas das duas seções devem herdar os mesmos filtros antes
das agregações:

- `supplier_filtro`: igualdade exata, salvo `'(Todos)'`;
- `product_filtro`: correspondência parcial literal em `product_names`, salvo
  `'(Todos)'`;
- `data_inicio`;
- `data_fim`.

O filtro temporal usa exclusivamente `dt_planned_original`:

```python
data_fim_exclusivo = pd.Timestamp(data_fim) + pd.DateOffset(months=1)

(dt_planned_original >= data_inicio)
& (dt_planned_original < data_fim_exclusivo)
```

Essa semântica de `data_fim + 1 mês` é herdada e deve permanecer igual nas duas
seções até nova decisão explícita.

## SLA planejado

A criação da OP usa exclusivamente o campo transacional nativo `created_at`.
Como o campo é `DATETIME`, ele é normalizado para data antes da conversão em
timestamp. Quando há mais de um valor por OP, prevalece o menor. Não há fallback
para `ingestion_date`.

```sql
TIMESTAMP(DATE(MIN(created_at))) AS stamp_created_production_order
```

`creation_date_source` registra `created_at` ou `missing_created_at`.
`created_at_distinct_count` permite auditar múltiplos valores sem alterar o
alias downstream `stamp_created_production_order`. OP sem `created_at` não entra
em KPI dependente de criação.

```python
sla_planejado_dias = (
    dt_planned_original - stamp_created_production_order
).dt.days
```

Na seção SLA:

```python
0 < sla_planejado_dias < 100
```

SLA de 99 dias entra; SLA de 100 dias não entra.

Validação retroativa de 2026-08-04:

- histórico committed disponível desde `2025-11-10`;
- OPs criadas antes do piso: gap snapshot − `created_at` mediano de 200 dias;
- OPs criadas no ou após o piso: gap mediano de 2 dias e P90 de 5 dias;
- 140 OPs com `created_at` posterior ao planejado e 91 posteriores à entrega
  foram mantidas em auditoria e excluídas dos KPIs;
- notebook de auditoria: `0626482742d24c1b87ebf20cf411bb4d`;
- run de corte de cobertura: `9156ebae-f761-4663-bf08-93530ff72d0c`, status
  `success`.

## Lead time realizado

Fim para PA e Tri:

- `dt_largest_entry_warehouse`.

Início:

- PA: primeiro ingresso em `waiting_fabric_arrival`;
- Tri: `stamp_created_production_order`.

```python
lead_time_realizado_bruto = (
    dt_largest_entry_warehouse - inicio_lead_time
).dt.days
```

OP não entregue permanece pendente. Datas realizadas futuras não devem compor o
cohort de entregas realizadas.

Guard rails de criação:

- `created_at > dt_planned_original`: excluir de todos os KPIs e auditar;
- OP entregue com `created_at > dt_largest_entry_warehouse`: excluir de todos
  os KPIs e auditar;
- `created_at` ausente: não imputar e excluir dos KPIs dependentes de criação.

## Correção de postergação

A postergação é calculada no histórico `committed`, após deduplicação diária.
Somar somente deslocamentos positivos em transições committed→committed quando:

- `prev_planned` não é nulo;
- `prev_reviewed` não é nulo;
- `prev_planned == prev_reviewed`;
- a nova data planejada é posterior à anterior.

Mudança positiva de `dt_planned_entry_warehouse` nessas condições é postergação
interna da Insider e deve ser removida. Mudança de
`dt_reviewed_entry_warehouse` representa atraso do fornecedor e não deve ser
descontada por esta regra.

```python
lead_time_ajustado = (
    lead_time_realizado_bruto
    - qt_dias_postergacao_intencional
).clip(lower=0)

lead_time_realizado = lead_time_ajustado
```

KPIs, status, gráficos e exportações usam o lead time ajustado. O bruto permanece
disponível apenas para auditoria.

## Meta realizada

- Dashboard Geral: referência operacional de 120 dias.
- Seção SLA: `META_LEAD_TIME_REALIZADO_DIAS = 90`.

Na seção SLA:

```python
atingiu_meta_90d = (
    entregue
    & lead_time_realizado.notna()
    & (lead_time_realizado <= 90)
)
```

O denominador de `% Dentro do Prazo (90d)` e dos heatmaps é o total de OPs
entregues no respectivo recorte.

## Ciclos e visualizações

- Base: `cycle_name` exatamente no padrão `CMMYYYY`.
- Extra: demais formatos.
- A seção Ciclo Produtivo possui base dedicada no grão de `op_code`, separada
  do cohort de entrega do Dashboard Geral: jan/2025 até o mês corrente; Base
  pelo código do ciclo e Extra por `dt_planned_original`.
- Nessa seção, `is_op_open` do snapshot mais recente define pendência:
  `TRUE` é pendente; `FALSE` ou nulo é OP fechada. Normalizar para `bool`
  antes de `np.select`.
- A pontualidade de Ciclo Produtivo é volume: peças recebidas no prazo / peças
  recebidas de OPs fechadas; `received_quantity_op` já está no grão de OP.
- Gráficos podem ter propósitos diferentes entre Geral e SLA.
- Manter o corte visual da seção SLA a partir de agosto de 2025.
- Manter somente o gráfico de matéria-prima executado depois da aplicação do
  lead time ajustado.
- Os heatmaps da seção SLA mostram `% dentro de 90d` por fornecedor×mês e
  produto×mês, separados em Base e Extra.

## Capacidade e Single Source

A visão Single Source deve usar a capacidade cadastral atual. Não transformar
essa visão em reconstrução histórica de capacidade sem nova decisão explícita.

## Exportação para Google Sheets

A tabela exportada deve seguir as premissas do Dashboard Geral:

- mesmo cohort por `dt_planned_original`;
- mesmas exclusões e filtros;
- somente OPs elegíveis após a agregação;
- lead time realizado ajustado por postergação;
- unicidade de `op_code`;
- preservação do contrato de colunas existente.

Runs metodológicos devem desativar temporariamente a escrita e restaurar a flag
do notebook ao final.

## Controles obrigatórios

- `op_code` único nas bases finais;
- zero OPs canceladas;
- zero `dt_planned_original` nula no cohort;
- datas dentro da janela selecionada;
- aderência aos filtros de produto e fornecedor;
- zero divergências de `dt_planned_original` entre Geral, SLA e postergação;
- zero divergências de dias de postergação no universo comparável;
- zero uso de `first_snapshot_fallback` nas bases principal e trimestral;
- OPs com `missing_created_at` fora dos KPIs dependentes de criação;
- zero OPs com cronologia inválida de `created_at` nas bases de KPI;
- `lead_time_realizado` igual ao bruto menos postergação, com limite inferior
  zero;
- todos os consumidores de cada seção derivados do respectivo cohort canônico.

## Última validação conhecida

A decisão final foi validada no notebook de auditoria
`0626482742d24c1b87ebf20cf411bb4d`, run
`9156ebae-f761-4663-bf08-93530ff72d0c` (`success`): piso do histórico em
2025-11-10, gap recente mediano de 2 dias e P90 de 5 dias. A inspeção do
notebook vigente confirmou ausência de `first_snapshot_fallback` nas bases
principal e trimestral.

O notebook principal não foi executado depois dessa edição porque a exportação
para Google Sheets estava habilitada. Último run completo anterior à decisão
final:

Run Deepnote: `7a59976c-b09a-4da1-be1c-7f1adf976344`.

- status `success`;
- 58 blocos executados e 0 falhas;
- auditoria estrutural: 11 leituras do histórico e todas com filtro SQL
  `committed`;
- Geral: 663 OPs após filtros;
- Geral enriquecido: 633 OPs, 156 com postergação;
- SLA: 1.791 OPs elegíveis antes do limite;
- cohort SLA final: 449 OPs;
- 1.791 OPs reconciliadas e zero divergências de data original;
- 1.791 OPs reconciliadas e zero divergências de postergação;
- escrita no Sheets desativada no run e restaurada no notebook vivo.

## Uso em conversas futuras

Antes de alterar o dashboard:

1. consultar este documento;
2. consultar `sla90_deepnote_premissas.md`, `bases_e_premissas.md` e
   `lead_time_limpo.md` somente quando for necessário maior detalhe;
3. validar as premissas contra o notebook atual;
4. sinalizar qualquer divergência antes de mudar lógica crítica.
