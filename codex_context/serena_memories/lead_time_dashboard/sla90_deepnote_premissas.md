# Deepnote — Governança de OPs com SLA planejado <100 dias

> Este snapshot histórico foi substituído em 2026-07-29 para eliminar premissas
> obsoletas sobre pré-filtro committed, fallback de data planejada e lead time
> realizado sem correção de postergação.

## Referência canônica

Consulte primeiro:

- [`premissas_fundamentais_calculo.md`](premissas_fundamentais_calculo.md)

Fontes complementares:

- [`bases_e_premissas.md`](bases_e_premissas.md)
- [`lead_time_limpo.md`](lead_time_limpo.md)
- [`source_inventory.md`](source_inventory.md)
- [`transicao_data_criacao_ingestion_created_at.md`](transicao_data_criacao_ingestion_created_at.md)

Na memória Serena ativa, o documento detalhado permanece disponível como:

```text
lead_time_dashboard/sla90_deepnote_premissas
```

## Definições que não podem regredir

- Todas as queries de OPs filtram `production_order_type = 'committed'` na
  própria leitura da fonte.
- Geral e SLA podem preservar `ANY_VALUE(production_order_type)` e repetir o
  filtro pós-query como defesa adicional.
- O cohort usa exclusivamente `dt_planned_original`, sem fallback.
- `dt_planned_original` é obtida após deduplicação diária do histórico completo.
- Geral, SLA e postergação devem reconciliar a data original sem divergências.
- KPIs e gráficos usam `lead_time_ajustado`, descontando somente postergação
  interna válida.
- A seção SLA usa `0 < sla_planejado_dias < 100` para entrada e 90 dias como
  meta do realizado.
- Desde 2026-08-04, `stamp_created_production_order` usa exclusivamente
  `MIN(created_at)` normalizado para data. OP sem `created_at` não recebe
  fallback e fica fora dos KPIs dependentes de criação.
- O primeiro `ingestion_date` é somente diagnóstico de cobertura. O histórico
  committed começa em `2025-11-10` e não representa criação de OPs legadas.
- OP com `created_at` posterior ao planejado ou à entrega é auditada e excluída
  antes de qualquer KPI.
- Filtros de produto, fornecedor e período devem atingir todos os consumidores
  do cohort.

Último run validado: `7a59976c-b09a-4da1-be1c-7f1adf976344`, com 58 blocos
executados e zero falhas.
