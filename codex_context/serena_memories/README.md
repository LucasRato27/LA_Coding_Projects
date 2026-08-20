# Memórias do Serena — índice para o Codex

Este diretório é um snapshot das memórias do Serena do projeto
`LA_Coding_Projects`, exportado em 2026-07-27 e reconciliado em 2026-08-06
com as premissas vigentes do dashboard de Lead Time.

## Regra de uso

- Consulte somente as memórias relevantes à tarefa atual.
- Trate conteúdo datado como contexto histórico e valide-o contra o código e os
  dados atuais antes de tomar decisões.
- Em caso de conflito, prevalecem, nesta ordem: pedido atual do usuário,
  `AGENTS.md`, código/dados atuais e, por último, estas memórias.
- Não exponha credenciais ou conteúdo sensível eventualmente referenciado.

## Catálogo

### Contexto geral e padrões

- [`project_overview.md`](project_overview.md)
- [`project_structure.md`](project_structure.md)
- [`style_and_conventions.md`](style_and_conventions.md)
- [`suggested_commands.md`](suggested_commands.md)
- [`task_completion_checklist.md`](task_completion_checklist.md)

### Projeto e arquitetura

- [`project/overview.md`](project/overview.md)
- [`project/databases.md`](project/databases.md)
- [`project/quality_inspection_data.md`](project/quality_inspection_data.md)
- [`project/refactor_plan.md`](project/refactor_plan.md)

### BigQuery, SQL e métricas

- [`bigquery_tables.md`](bigquery_tables.md)
- [`bigquery_portfolio_tables.md`](bigquery_portfolio_tables.md)
- [`sql_catalog.md`](sql_catalog.md)
- [`business_metrics.md`](business_metrics.md)

### Domínio

- [`domain/cycle_name_classification.md`](domain/cycle_name_classification.md)
- [`okr_estabilidade_planejamento.md`](okr_estabilidade_planejamento.md)

### Lead Time Dashboard

- [`lead_time_dashboard/handoff_governanca_lead_time_20260806.md`](lead_time_dashboard/handoff_governanca_lead_time_20260806.md) — **consultar primeiro**; referência vigente do notebook Deepnote, Ciclo Produtivo e último run validado.
- [`lead_time_dashboard/handoff_governanca_lead_time_20260805.md`](lead_time_dashboard/handoff_governanca_lead_time_20260805.md) — handoff histórico, supersedido na referência do notebook pelo de 2026-08-06.
- [`lead_time_dashboard/premissas_fundamentais_calculo.md`](lead_time_dashboard/premissas_fundamentais_calculo.md)
- [`lead_time_dashboard/source_inventory.md`](lead_time_dashboard/source_inventory.md)
- [`lead_time_dashboard/bases_e_premissas.md`](lead_time_dashboard/bases_e_premissas.md)
- [`lead_time_dashboard/lead_time_limpo.md`](lead_time_dashboard/lead_time_limpo.md) — histórico de implementação; validar contra premissas vigentes.
- [`lead_time_dashboard/sla90_deepnote_premissas.md`](lead_time_dashboard/sla90_deepnote_premissas.md)
- [`lead_time_dashboard/v2_deepnote_diferencas.md`](lead_time_dashboard/v2_deepnote_diferencas.md) — histórico.
- [`lead_time_dashboard/reformulacao_faixa_sla_e_memorial_calculo.md`](lead_time_dashboard/reformulacao_faixa_sla_e_memorial_calculo.md)
- [`lead_time_dashboard/transicao_data_criacao_ingestion_created_at.md`](lead_time_dashboard/transicao_data_criacao_ingestion_created_at.md)
- [`lead_time_dashboard/validacao_retroativa_created_at_vs_snapshot_20260804.md`](lead_time_dashboard/validacao_retroativa_created_at_vs_snapshot_20260804.md)
- [`lead_time_dashboard/cohort_entrega_e_ciclo_produtivo_20260805.md`](lead_time_dashboard/cohort_entrega_e_ciclo_produtivo_20260805.md)
- [`lead_time_dashboard/tipo_op_ciclo_hibrido_kpis_20260805.md`](lead_time_dashboard/tipo_op_ciclo_hibrido_kpis_20260805.md)
- [`lead_time_dashboard/governanca_materia_prima_20260807.md`](lead_time_dashboard/governanca_materia_prima_20260807.md) — notebook `fabric_governance` (base de MP: tecido/artigo/malharia); documentação técnica completa em `analytics/lead_time/documentacao_governanca_materia_prima.md`.
- [`lead_time_dashboard/supplier_in_governanca_mp_20260810.md`](lead_time_dashboard/supplier_in_governanca_mp_20260810.md) — **consultar primeiro para Supplier [IN]**; registra a primeira publicação no Sheets, contrato de exportação, cobertura atual do lake e decisão de ingestão.

### Projeção de necessidade de produção

- [`20260422_projecao_nec_prod/overview.md`](20260422_projecao_nec_prod/overview.md)
- [`20260422_projecao_nec_prod/modulo_simulacao_atendimento.md`](20260422_projecao_nec_prod/modulo_simulacao_atendimento.md)
- [`20260422_projecao_nec_prod/visualizacao_e_regras.md`](20260422_projecao_nec_prod/visualizacao_e_regras.md)

### Relatório T&D (priorização de produtos)

- [`relatorio_td/pipeline_reversas.md`](relatorio_td/pipeline_reversas.md)

### Tempo até Reversa & OKR de T&D (entrega/compra → reversa, KR mensal, HM semanal)

- [`tempo_reversa_okr_td/pipeline_e_premissas.md`](tempo_reversa_okr_td/pipeline_e_premissas.md)

## Sincronização

Este snapshot não é sincronizado automaticamente. Quando as memórias do Serena
mudarem, copie novamente o conteúdo de `.serena/memories/` e valide a paridade
entre origem e destino.
