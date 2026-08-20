# Handoff — Governança de Lead Time — 2026-08-06

## Referência vigente no Deepnote

- Projeto: `Governanca-Lead-Time` (`62b1671d-dc82-4747-9bbb-b134a69a3491`).
- Notebook canônico: `lead_time_dashboard` (`0b56034555ea4a43a7d80e0ede768923`).
- Link: <https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/leadtimedashboard-0b56034555ea4a43a7d80e0ede768923?utm_content=62b1671d-dc82-4747-9bbb-b134a69a3491>
- O ID anterior `ba6032bb1f0843f2ba934db8b66d0728` não deve mais ser usado como referência para novas edições.

## Decisões e alterações discutidas

1. **KPIs de Ciclo Produtivo**
   - Exibir card de `OPs planejadas`: OPs alocadas no recorte, no grão de `op_code`.
   - `OPs entregues` usa OP fechada no snapshot mais recente (`is_op_open != TRUE`); `is_op_open = TRUE` é pendente.
   - `% dentro do prazo` é calculado em peças: peças recebidas no prazo / todas as peças recebidas de OPs fechadas. Não usa todas as OPs ou peças alocadas como denominador.
   - A regra por OP é: SLA planejado `<100` dias -> prazo operacional de 90 dias; SLA `>=100` dias -> 120 dias. Em `Todos`, cada OP mantém sua regra individual.

2. **Status por mês do ciclo**
   - Os gráficos empilhados Base e Extra devem sempre conter `Pendente`, `Entregue dentro do prazo` e `Entregue acima do prazo`.
   - A soma dos três status deve reconciliar com as OPs alocadas no mês/tipo de ciclo.

3. **Evolução trimestral da pontualidade**
   - Deve respeitar todos os filtros globais: tipo de OP, fornecedor, produto, faixa de SLA e período (`data_inicio_analise` a `data_fim_analise`, fim inclusivo por mês).
   - O filtro de faixa SLA mantém a regra individual 90/120 dias.

4. **Base dedicada de Ciclo Produtivo — implementada**
   - A seção não reutiliza mais o cohort por `dt_largest_entry_warehouse` do Dashboard Geral. A base dedicada está no grão de `op_code` e usa o histórico completo do tipo de OP selecionado.
   - Recorte temporal da seção: jan/2025 até o mês corrente. Ciclos `Base` usam o mês extraído de `cycle_name` no padrão `CMMYYYY`; ciclos `Extra` usam o mês de `dt_planned_original`.
   - Pendência é determinada pelo snapshot mais recente de `is_op_open`: `TRUE` significa pendente; `FALSE` ou nulo significa OP fechada/entregue. A normalização para `bool` é obrigatória antes de usar `np.select`, pois o dtype anulável do pandas produz `NA` inválido para suas condições.
   - SLA é individual por OP: `sla_planejado_dias < 100` usa 90 dias; `>=100` usa 120 dias. A comparação usa o lead time ajustado por postergação.
   - `% dentro do prazo` no card e na série é métrica de volume: peças recebidas de OPs fechadas dentro do prazo / peças recebidas de todas as OPs fechadas. `received_quantity_op` é quantidade agregada no grão de OP, não quantidade por SKU.
   - A tabela/CSV de governança deriva de `df_ciclo_produtivo`, e não mais de `df_ops_enriched_all`.

5. **Universo histórico ampliado**
   - Fornecedores `terminated`/`discontinued` e SKUs `desativado` deixaram de ser exclusões do universo canônico, dropdowns e Ciclo Produtivo; entregas históricas permanecem válidas.
   - Permanecem excluídas somente OPs canceladas, ciclos B2B/EPA e OPs fora do tipo selecionado.
   - Falta de lead time teórico cadastral não remove OPs das métricas realizadas; permanece apenas na auditoria e limita consumidores que dependem de comparação teórica.

6. **Diagnóstico de meses Base**
   - O gráfico Base não possui corte visual em setembro/2025. Ele filtra somente `cycle_type = 'Base'`; meses sem linha Base agregada não são desenhados pelo Plotly.
   - Run de 2026-08-06: `governanca_graficos` cobre jan/2025 a ago/2026; a primeira linha Base disponível é set/2025. A ausência anterior decorre da taxonomia/dados de `cycle_name`, não de filtro por entrega. Foi confirmado que não existem ciclos `C08` anteriores esperados.

## Estado de validação

- Run controlado em modo somente leitura: `c87f04c0-0d09-4596-a6e7-305d28331c77` falhou exclusivamente porque uma célula preexistente tentou gravar `exports/lead_time_cadastrado_auditoria.csv` em filesystem read-only.
- Run controlado gravável e sem escrita na planilha Google Sheets: `69f7ded9-c91e-494f-ba33-5d31d12b9de6`, status `success`, 62 blocos executados, 0 falhas, em 2026-08-06. Após a validação, a flag padrão de exportação produtiva foi restaurada.
