# Lead Time Limpo — Metodologia e Implementação

## Contexto
OPs da Insider têm `dt_planned_entry_warehouse` alterada intencionalmente pelo planejamento ao longo do ciclo. Isso infla o lead time realizado, escondendo o gargalo produtivo real. O objetivo é remover esse tempo de postergação do cálculo.

## Arquivo
`notebooks/lead_time_dashboard.ipynb`

## Estrutura de células após implementação (2026-05-26)
| Idx | Célula |
|-----|--------|
| 0 | Imports + config |
| 1 | SQL_CAPACITY |
| 2 | SQL_OPS → df_ops_raw |
| 3 | SQL_POSTPONEMENT → df_postponement ← **novo** |
| 4 | SQL_FABRIC_TEMPO |
| 5 | Preprocessing → df_ops + df_ops_enriched |
| 6 | df_lead_time_clean computation ← **novo** |
| 7 | Validação (prints) ← **novo** |
| 8+ | Visualizações existentes |

## Tabela fonte para postergação
`insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`
- Snapshot diário por op_code
- Campos-chave: `op_code`, `ingestion_date`, `dt_planned_entry_warehouse`, `dt_reviewed_entry_warehouse`

## Lógica SQL (SQL_POSTPONEMENT)
1. **ops_in_scope**: filtra por `dt_planned_entry_warehouse >= '2026-01-01'` e status de fornecedor — garante mesmo universo do SQL_OPS
2. **daily_dedup**: `ROW_NUMBER() OVER (PARTITION BY op_code, DATE(ingestion_date) ORDER BY ingestion_date ASC) = 1` — evita double-count intra-dia
3. **with_lag**: `LAG(dt_planned_entry_warehouse) OVER (PARTITION BY op_code ORDER BY ingestion_date)` — detecta mudanças dia a dia
4. **original_planned**: primeiro valor de `dt_planned_entry_warehouse` por op_code (QUALIFY ROW_NUMBER = 1)
5. **postponement_totals**: `SUM(DATE_DIFF(..., DAY))` apenas para deltas positivos (postergações, não antecipações)
6. Output: `op_code`, `dt_planned_original`, `qt_dias_postergacao_intencional`, `flag_teve_postergacao`

## Distinção importante
- **Postergação interna (Insider)**: mudança em `dt_planned_entry_warehouse` → removida no lead time limpo
- **Atraso do fornecedor**: mudança em `dt_reviewed_entry_warehouse` → NÃO removida (fora do escopo)

## DataFrames gerados
- `df_postponement`: 1 linha por op_code, com as 4 colunas de postergação
- `df_ops_enriched`: df_ops + colunas de postergação (left join por op_code). OPs sem match recebem `qt_dias_postergacao_intencional=0`, `flag_teve_postergacao=False`
- `df_lead_time_clean`: colunas por OP — op_code, supplier_name, product_names, is_finished_product_order, lead_time_realizado, qt_dias_postergacao_intencional, lead_time_ajustado, flag_teve_postergacao, etapa_inflada, duracao_etapa_inflada_original, duracao_etapa_inflada_ajustada

## Lógica Python — lead time ajustado
```python
lead_time_ajustado = (lead_time_realizado - qt_dias_postergacao_intencional).clip(lower=0)
```

## Lógica Python — etapa inflada
1. Mediana por (supplier_name, is_finished_product_order) calculada APENAS em OPs com `flag_teve_postergacao=False` (baseline limpa)
2. Para OPs com postergação: desvio_etapa = duração_real − mediana_baseline
3. etapa_inflada = `idxmax(dev_cols)` — etapa com maior desvio positivo
4. `duracao_etapa_inflada_ajustada = max(0, duracao_original - qt_dias_postergacao_intencional)`

## Etapas disponíveis (ETAPAS_COLS, definido em Cell 5)
```
etapa_criacao_agd, etapa_agd_validmp, etapa_valid_corte,
etapa_valid_corte_exec, etapa_costura, etapa_inspecao, etapa_fat_estoque
```
PA: etapa_criacao_agd = NaN (lead time PA começa em stamp_stage_waiting_fabric_arrival)

## Riscos conhecidos
- Se baseline limpa for pequena (<30 OPs), medianas de referência podem não ser representativas (validação sinaliza)
- OPs com todos os stamps NaN: etapa_inflada = None (tratado)
- Lead time PA vs Tri: fluxo diferenciado já estava implementado antes dessa feature
