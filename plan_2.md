# Plan: KR1 Time Evolution Tracking

## TL;DR
Add a "movie" view of KR1 — showing how o OKR evoluiu semana a semana — via nova SQL que compara cada snapshot semanal contra o baseline, e novos gráficos de curva de degradação no notebook.

## Approach: Weekly Snapshot Comparison

**Ideia central**: Reusar a lógica do `plano_vs_atual.sql` mas substituir o "snapshot mais recente" por um sampling semanal de todos os snapshots. Para cada (semana, op_code, product_sku), computar as mesmas flags INT comparando contra o baseline.

**Por que não usar as datas do `frequencia_revisoes.sql`?**
- Mudanças podem reverter (dt_planned mudou e voltou = NÃO alterado no snapshot atual)
- KR1 é "baseline vs estado no tempo T", não "já foi alterado alguma vez"
- Precisa de comparação real snapshot-vs-baseline em cada ponto no tempo

## Steps

### Phase 1: New SQL (`kr1_evolucao.sql`)
1. Criar `1_Inputs/1_SQL/kr1_evolucao.sql`
   - Reusar `CTE_BASELINE_CORRECTED` e `CTE_PLANO_ORIGINAL` (idênticos ao plano_vs_atual)
   - Substituir `CTE_LATEST_SNAPSHOT` por `CTE_WEEKLY_SNAPSHOTS`: sample via `DATE_TRUNC(ingestion_date, WEEK(MONDAY))`
   - Computar mesmas flags (`is_int_date`, `is_int_cancel`, `is_int_grade`, `is_int_any`)
   - GROUP BY `snapshot_date, cycle_name` → retorna volumes agregados por ciclo por semana
   - Output: `snapshot_date, cycle_name, cycle_type, vol_original, vol_int_date, vol_int_cancel, vol_int_grade, vol_int_any, vol_ext_any`
   - Join com `muninn_production_orders` para cancel reason (igual ao existente)

### Phase 2: Notebook — Load & Compute (2 novas células)
2. Markdown cell: "# 3b. Evolução do KR1 ao Longo do Tempo"
3. Code cell: carregar `kr1_evolucao.sql`, merge com mes_alvo, computar KR1% por snapshot_date

### Phase 3: Notebook — Visualizações (2-3 novos gráficos)
4. **Gráfico A — Curva de degradação por mes_alvo**: line chart, x=snapshot_date, y=KR1%, color=mes_alvo
5. **Gráfico B — KR1 consolidado over time**: linha Base vs Extra ao longo das semanas
6. **Gráfico C — Heatmap semana × mes_alvo**: color=KR1%

## Important: preservar notebook existente
- Todos os gráficos 1-9 existentes ficam intactos
- Nova seção "8. Evolução Temporal do KR1" adicionada APÓS seção 7 (Export) ou como seção separada antes do Export
- Gráficos A, B, C ficam na nova seção
- User quer separar "deep dive do estado atual" da "evolução temporal"

## Relevant files
- `1_Inputs/1_SQL/plano_vs_atual.sql` — reusar CTEs de baseline e plano original
- `2_Códigos/KR1_Plan_Freeze_Rate_v20260420.ipynb` — adicionar células após seção 3

## Verification
1. No snapshot mais recente, KR1 da evolução deve bater com KR1 atual do `plano_vs_atual.sql`
2. KR1 deve ser monotonicamente não-crescente (só piora ou mantém, nunca melhora — exceto se mudança reverte)
3. Primeiro snapshot após baseline ≈ 100% para cada ciclo

## Decisions
- Sampling semanal (não diário) para controlar custo BigQuery
- Agregação no SQL para reduzir transferência de dados
- Mesmos filtros: `production_order_type NOT IN ('flexible', 'converted')`
