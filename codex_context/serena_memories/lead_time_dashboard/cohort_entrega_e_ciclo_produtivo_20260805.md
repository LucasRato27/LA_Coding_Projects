# Lead Time Dashboard — Cohort de Entrega e Ciclo Produtivo

Decisão implementada em 2026-08-05 no notebook Deepnote `lead_time_dashboard`
(`ba6032bb1f0843f2ba934db8b66d0728`). Esta memória substitui somente a
semântica temporal do cohort principal e dos visuais de Ciclo Produtivo.

## Cohort temporal do dashboard

- Os inputs `data_inicio_analise` e `data_fim_analise` filtram
  `dt_largest_entry_warehouse`, e não `dt_planned_original`.
- O limite superior continua exclusivo e mensal: `data_fim + 1 mês`.
- O cohort exige `dt_largest_entry_warehouse <= CURRENT_DATE()`; portanto,
  inclui apenas OPs efetivamente entregues dentro do período.
- A SQL de `df_postponement` usa o mesmo escopo de entrega. A data planejada
  original permanece necessária para calcular SLA planejado e postergação, mas
  não define mais o recorte temporal.

## Ciclo Produtivo — regra superada

A definição anterior que posicionava Base e Extra pelo mês de entrega foi
**superada em 2026-08-06**. O Dashboard Geral continua sendo de entrega, mas
Ciclo Produtivo usa base dedicada no grão de OP e deixa de herdar seu cohort:

- `cycle_type = Base`: mês e ano extraídos de `cycle_name` no padrão
  `CMMYYYY`; por exemplo, `C022025` corresponde a fevereiro de 2025;
- `cycle_type = Extra`: primeiro dia do mês de `dt_planned_original`;
- o recorte é jan/2025 até o mês corrente, tanto para Base quanto para Extra;
- `is_op_open` do snapshot mais recente define pendência (`TRUE`) ou OP
  fechada (`FALSE`/nulo), independentemente de `dt_largest_entry_warehouse`;
- eixos e tooltips usam o rótulo **Mês do ciclo**.

## Evolução trimestral

- A base histórica trimestral continua independente dos inputs de período do
  dashboard.
- Cada OP é atribuída ao trimestre de `dt_largest_entry_warehouse`; o
  denominador é o total de OPs entregues no trimestre, após os demais filtros
  de elegibilidade e qualidade.
- `dt_planned_original` continua servindo ao cálculo de SLA, mas não à
  formação do trimestre exibido.

## Validação e operação

- Validar ausência de predicados temporais por `dt_planned_original` em
  `df_ops_raw` e `df_postponement`.
- Validar `op_code` único, OPs entregues + pendentes = OPs alocadas, e a regra
  híbrida de `mes_alvo` para Base e Extra.
- A exportação produtiva para Google Sheets é permitida somente para o filtro
  exclusivo `production_order_type = committed`.
