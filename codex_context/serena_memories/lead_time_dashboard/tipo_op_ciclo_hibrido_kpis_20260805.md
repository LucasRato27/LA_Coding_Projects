# Lead Time Dashboard — tipo de OP, ciclo híbrido e KPIs reconciliados

Decisão implementada em 2026-08-05 no notebook Deepnote `lead_time_dashboard`
(`ba6032bb1f0843f2ba934db8b66d0728`). Esta memória complementa as memórias de
cohort de entrega e de criação por `created_at`.

## Filtro exclusivo de tipo de OP

- O input obrigatório `production_order_type_filtro` aceita apenas
  `committed` (OPs definitivas, padrão) ou `incubation` (OPs de incubação).
- Não existe opção `Todos`; os dois tipos nunca podem ser combinados.
- OPs, datas originais, stamps, postergação, dropdowns, série trimestral,
  diagnósticos e validações derivam do histórico filtrado pelo tipo escolhido.
- O Python confirma que 100% das linhas possuem o tipo selecionado.
- Execuções `incubation` não escrevem na planilha Google Sheets produtiva.

## Cohort e eixos temporais

- O cohort principal do Dashboard Geral continua sendo definido por `dt_largest_entry_warehouse`,
  com início inclusivo, fim mensal exclusivo (`data_fim + 1 mês`) e somente
  entregas até a data da execução.
- Séries realizadas usam mês/trimestre de entrega.
- Ciclo Produtivo Base usa o mês/ano de `cycle_name` (`CMMYYYY`).
- Ciclo Produtivo Extra usa o mês de `dt_planned_original`.
- A regra anterior que usava mês de entrega no eixo do Ciclo Produtivo foi
  superada; somente essa parte da memória anterior deixa de valer.
- Atualização 2026-08-06: Ciclo Produtivo não usa mais o cohort principal;
  aplica janela própria de jan/2025 ao mês corrente e classifica pendência por
  `is_op_open` mais recente. A métrica de prazo dessa seção é em peças
  recebidas, não em contagem de OPs.

## KPIs de quantidade reconciliados

- Data de entrega e atendimento integral em quantidade são conceitos
  diferentes.
- Quantidade válida exige `planned_quantity_op > 0`, recebida preenchida e
  não negativa.
- `pecas_atendidas = min(received_quantity_op, planned_quantity_op)` por OP.
- `saldo_nao_atendido = max(planned_quantity_op - received_quantity_op, 0)`.
- `% atendimento = sum(pecas_atendidas) / sum(pecas_planejadas_validas)`.
- Cards exibem OPs entregues, completas, parciais e inválidas, além de peças
  planejadas, atendidas e saldo.
- Guard rails: completas + parciais = OPs com quantidade válida; atendidas +
  saldo = planejadas.

## Auditoria mensal da criação

- Notebook: `Auditoria mensal — criação da OP: snapshot vs created_at`
  (`0fc8465ceffc4558a59cb81257cf3e3a`).
- Método antigo: primeira ocorrência já `committed`, por
  `MIN(ingestion_date)` no histórico committed.
- Método novo: `TIMESTAMP(DATE(MIN(created_at)))`, sem fallback.
- População pareada de OPs entregues e elegíveis nos dois métodos.
- Eixo: mês de `dt_largest_entry_warehouse`, jan/2025 a jul/2026.
- Deltas: mediana da diferença por OP (`novo - velho`).

## Validação executada

- Auditoria mensal: run `3db64093-ba2a-49ff-8a5d-d8379f4f8765`, concluído
  com 19 meses únicos, zero duplicidade mensal, 793 OPs no cohort pareado e
  10 meses sem OP elegível (preservados na saída com contagem zero).
- Dashboard `committed`, exportação desativada: run
  `ffda9615-94b6-4e18-991c-7d46abde66ab`, 63/63 blocos aprovados.
- Dashboard `incubation`, exportação desativada: run
  `4fab971c-4b72-4bf8-8088-b5e4d2df1c37`, concluído sem falhas.
- Run final sem overrides, com default `committed` e exportação produtiva:
  `44fab58c-4897-455e-91ba-24c2a059d843`, 63/63 blocos aprovados e 1.539 OPs
  exportadas em 38 colunas para a aba configurada.
