# Governança de Lead Time — mapa documental

## Fonte de verdade operacional

- Notebook Deepnote canônico: `lead_time_dashboard` (`0b56034555ea4a43a7d80e0ede768923`).
- Handoff vigente: `codex_context/serena_memories/lead_time_dashboard/handoff_governanca_lead_time_20260806.md`.
- Última validação completa: run `69f7ded9-c91e-494f-ba33-5d31d12b9de6` — sucesso, 62 blocos e zero falhas.

## Documentação para leitura e autoria

| Documento | Uso | Status |
| --- | --- | --- |
| `documentacao_governanca_lead_time.md` | Documento metodológico vigente, sem números de execução | Vigente; validar contra o handoff antes de publicar |
| `documentacao_lead_time_produtivo.md` | Narrativa executiva com números de maio/2026 | Histórico; não usar como fonte de regras ou números atuais |
| `codex_context/serena_memories/lead_time_dashboard/handoff_governanca_lead_time_20260806.md` | Decisões, IDs e estado operacional mais recente | Fonte de reconciliação |
| `codex_context/serena_memories/lead_time_dashboard/premissas_fundamentais_calculo.md` | Regras normativas de cálculo | Vigente, sujeito ao notebook |
| `codex_context/serena_memories/lead_time_dashboard/source_inventory.md` | Fontes, confiança e runs | Vigente |

## Regras que precisam permanecer consistentes

- Grão: uma linha por `op_code`; `received_quantity_op` é quantidade por OP.
- Universo: tipo de OP selecionado; excluir apenas OP cancelada e B2B/EPA. Não excluir fornecedor/SKU desativado por status atual.
- Dashboard Geral: cohort por `dt_largest_entry_warehouse`.
- Ciclo Produtivo: base dedicada; jan/2025 ao mês atual; Base por `CMMYYYY`, Extra por `dt_planned_original`; `is_op_open=TRUE` significa pendente.
- SLA individual: `<100` dias → 90d; `>=100` dias → 120d.
- Pontualidade do Ciclo Produtivo: peças recebidas no prazo / peças recebidas de OPs fechadas.

## Documentos históricos

Arquivos datados em `codex_context/serena_memories/lead_time_dashboard/` preservam decisões e validações de suas datas. Não devem substituir o handoff de 2026-08-06 nem as premissas fundamentais quando houver divergência.
