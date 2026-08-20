# Lead Time Dashboard — transição da data de criação das OPs

Atualizado em 2026-08-04. Este registro preserva explicitamente a definição
anterior e a definição vigente de `stamp_created_production_order`.

## Definição anterior — primeira aparição no histórico

Até 2026-08-04, a data de criação usada pelo dashboard não era uma criação
transacional. Ela correspondia ao primeiro snapshot em que a OP aparecia na
tabela histórica:

```sql
MIN(CAST(ingestion_date AS TIMESTAMP))
  AS stamp_created_production_order
```

Semântica: `ingestion_date` informava quando a OP foi observada pela primeira
vez em `supply_chain_efficiency_model_input_history`. Portanto, a métrica era
limitada pela cobertura temporal dos snapshots e podia ser posterior à criação
real da OP.

Consequência: o SLA planejado era calculado como:

```python
sla_planejado_dias = (
    dt_planned_original - first_ingestion_date
).dt.days
```

Quando uma OP antiga só passou a aparecer depois no histórico disponível, o
SLA podia ficar igual a zero ou negativo. Isso eliminava indevidamente parte do
histórico, especialmente T1–T3/2025, pelo filtro `sla_planejado_dias > 0`.

## Definição intermediária — `created_at` com fallback

Na primeira alteração de 2026-08-04, `created_at` passou a ser a fonte principal,
mas OPs sem valor ainda recebiam a primeira aparição no histórico como fallback.
Essa regra foi usada apenas como etapa intermediária e foi substituída no mesmo
dia após a análise retroativa.

```sql
COALESCE(
  TIMESTAMP(DATE(MIN(created_at))),
  MIN(CAST(ingestion_date AS TIMESTAMP))
) AS stamp_created_production_order
```

Regras complementares:

- `created_at` preenchido: `creation_date_source = 'created_at'`;
- `created_at` ausente: usar a primeira aparição no histórico completo e
  registrar `creation_date_source = 'first_snapshot_fallback'`;
- `created_at_distinct_count` audita OPs com múltiplos valores;
- o alias downstream permanece `stamp_created_production_order`;
- a mesma definição é aplicada nas bases principal e trimestral.

O SLA vigente continua:

```python
sla_planejado_dias = (
    dt_planned_original - stamp_created_production_order
).dt.days
```

Logo, para OPs com `created_at`, o filtro `sla_planejado_dias > 0` equivale a
`dt_planned_original > DATE(created_at)`. Para OPs sem `created_at`, a comparação
usa o fallback de primeira aparição.

## Definição vigente — `created_at` canônico e exclusivo

Após a validação retroativa, o fallback foi removido das bases principal e
trimestral. Havendo mais de um valor por OP, prevalece o menor. O campo
`DATETIME` é normalizado para data antes da conversão em timestamp:

```sql
TIMESTAMP(DATE(MIN(created_at))) AS stamp_created_production_order
```

Regras complementares vigentes:

- `created_at` preenchido: `creation_date_source = 'created_at'`;
- `created_at` ausente: `creation_date_source = 'missing_created_at'`, sem data
  sintética e fora dos KPIs dependentes de criação;
- não usar `ingestion_date` como substituto de criação;
- `created_at_distinct_count` continua auditando múltiplos valores;
- o alias downstream permanece `stamp_created_production_order`;
- a mesma definição é aplicada nas bases principal e trimestral.

O SLA continua sendo calculado por:

```python
sla_planejado_dias = (
    dt_planned_original - stamp_created_production_order
).dt.days
```

OP sem `created_at` produz SLA nulo e é removida pelo requisito já existente de
`sla_planejado_dias.notna() & sla_planejado_dias.gt(0)`.

## Impacto metodológico

- Triangulação inicia o lead time realizado em
  `stamp_created_production_order`; portanto, a troca também altera seu lead
  time realizado.
- Produto acabado continua iniciando em `waiting_fabric_arrival`.
- `dt_planned_original`, postergação, filtros de universo e regras de prazo não
  foram alterados por esta transição.
- Guard rails adicionais excluem de todos os KPIs OPs com `created_at` posterior
  ao prazo original ou, quando entregue, posterior à entrega. Os registros
  permanecem disponíveis nas tabelas de auditoria.

## Validação da regra intermediária

Run Deepnote `a178ed9e-ac77-4136-b0b4-6768bea72fc3`, executado com fornecedor e
produto em `(Todos)`, faixa `Todos` e período de `2025-01-01` a `2026-07-01`:

- sucesso, sem escrita no Google Sheets durante a validação;
- 3.136 OPs (82,29%) usaram `created_at`;
- 675 OPs (17,71%) usaram o fallback;
- zero OPs com múltiplos `created_at` distintos;
- 2.418 OPs reconciliadas entre as bases principal e histórica, sem
  divergências de data ou fonte;
- após `sla_planejado_dias > 0`: T1/2025 com 370 OPs, T2/2025 com 999 e
  T3/2025 com 1.205;
- esse run é histórico e anterior à remoção do fallback e aos guard rails
  finais; não deve ser usado como descrição da regra vigente.

## Validação retroativa e corte de cobertura

Detalhes completos em
`validacao_retroativa_created_at_vs_snapshot_20260804.md`.

- O histórico committed começa em `2025-11-10`.
- Antes desse piso, o primeiro snapshot representa a primeira carga disponível,
  não a criação da OP.
- Para `created_at < 2025-11-10`: 1.659 OPs, gap mediano de 200 dias e P90 de
  277 dias; 98,5% aparecem pela primeira vez no piso do histórico.
- Para `created_at >= 2025-11-10`: 592 OPs, gap mediano de 2 dias, P90 de 5
  dias e 100% com gap de até 7 dias.
- No recorte operacional auditado, 140 OPs tinham `created_at` posterior ao
  planejado e 91 posterior à entrega; elas não entram nos KPIs.

## Decisão vigente

Usar `created_at` como fonte canônica e exclusiva de criação. Não voltar a
interpretar o primeiro snapshot como criação transacional nem aplicar fallback
por `ingestion_date` sem uma nova decisão explícita de negócio.
