# OKR Plan Freeze Rate (KR1) — Base de dados e lógica de cálculo

Fonte de definição de negócio: `analyses/definicao_okr_plan_freeze_rate.md` (validado com João/Giulia, pendente alinhamento Raí sobre coorte de cálculo, doc de 2026-04-27).
Notebook operacional: `analytics/plan_freeze_rate/KR1_Plan_Freeze_Rate_v20260420.ipynb` + SQLs em `analytics/plan_freeze_rate/sql/`.
Documentação de negócio (gráficos, glossário): `analytics/plan_freeze_rate/README.md`.

## O que é
Plan Freeze Rate mede quanto do plano produtivo original sobrevive sem alteração INTERNA até a entrega. Fórmula:
`KR1 = 1 − (volume_alterado_internamente / volume_original)`
- Volume original = `planned_quantity` por SKU no snapshot de **baseline** do ciclo.
- Cada SKU conta uma única vez no numerador mesmo com múltiplos motivos internos (sem double-count).

## Tabelas BigQuery usadas

### `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`
Tabela central. Histórico diário de snapshots por `(op_code, product_sku, ingestion_date)`. Partição: `ingestion_date`. ~12.3M linhas.
Filtro obrigatório em toda query do KR1:
```sql
WHERE production_order_type NOT IN ('flexible', 'converted')
  AND cycle_name IS NOT NULL
```
Campos-chave usados: `cycle_name`, `ingestion_date`, `current_production_stage` (para detectar 'pending'), `dt_planned_entry_warehouse`, `dt_reviewed_entry_warehouse`, `planned_quantity`, `op_code`, `product_sku`.

### `insider-data-lake.integrated.muninn_production_orders`
JOIN via `order_code = op_code`. Única fonte de `canceled_production_reason` — campo que distingue INT_CANCEL de EXT_CANCEL.

### `insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
Snapshot mais recente (sem histórico). Usar apenas para testes ad-hoc/validação pontual — nunca em produção do KR1.

## Lógica de Ciclos (`cycle_name`)

- **Ciclo Base**: regex `^C\d{2}20\d{2}` (prefixo), aceitando sufixos como `C012026B` (ex.: `C062026`, `C012026B`). Meta KR1a ≥ 85%.
- **Ciclo Extra**: qualquer `cycle_name` que não bate no regex acima (reforços, coleções pontuais, alocações fora do calendário). Meta KR1b ≥ 70%, acompanhado em paralelo, **não entra no peso do OKR oficial** — é naturalmente mais volátil.
- Classificação SQL:
```sql
CASE WHEN REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}') THEN 'Base' ELSE 'Extra' END AS cycle_type
```
- **Ciclos excluídos manualmente**: `C012026` (anomalia conhecida na formação do baseline).

### Baseline por ciclo
Primeiro `ingestion_date` do ciclo em que `COUNTIF(current_production_stage = 'pending') = 0` — dia em que o plano "congela" e vira compromisso. É o denominador do KR1 (volume original) e a referência para todos os deltas.

### Atribuição de mês-alvo (`mes_alvo`)
Cada coorte (`cycle_name`) recebe o mês em que concentra o maior volume planejado no baseline (`idxmax` da soma mensal de `baseline_planned_qty`). Permite comparar por mês de entrada de mercadoria em vez do mês em que o ciclo foi rodado.

## Reason codes

**Internos (entram no KR1 — numerador):**
| Flag | Condição SQL |
|---|---|
| `is_int_date` | `current_dt_planned != baseline_dt_planned` |
| `is_int_grade` | `current_planned_qty != baseline_planned_qty` |
| `is_int_cancel` | `current_stage = 'canceled' AND canceled_production_reason = 'Revisão de Demanda (In Season)'` |
| `is_int_any` | união dos 3 acima, sem double-count por SKU |

**Externos (fora do KR1, acompanhamento paralelo de risco de cadeia):**
| Flag | Condição |
|---|---|
| `is_ext_cancel` | cancelada com motivo ≠ 'Revisão de Demanda (In Season)' |
| `is_ext_date_rev` | `dt_reviewed` mudou SEM que `dt_planned` tenha mudado (supplier mexeu sozinho) |
| `is_ext_any` | união dos 2 acima; **não conta INT_DATE como externo** |

**Não conta como alteração interna nem externa (é outro KR):**
- Atraso na entrega (`dt_max_entry > dt_reviewed`) → é KR2, fora do escopo do Plan Freeze Rate.

## Consolidação / OKR ponderado

1. Cálculo do KR1 por coorte (`cycle_name`) individualmente.
2. Agregação por `mes_alvo × cycle_type`.
3. **Ponderação por proximidade** (janela Cn..Cn+3, pesos alinhados com Giulia em 24/04):
```
OKR = (35·Cn + 35·Cn+1 + 20·Cn+2 + 10·Cn+3) / 100
```
- Cn/Cn+1 (35% cada): janela de atuação ainda existe, resultado materializa em produção real no curto prazo.
- Cn+2 (20%): já influenciável, mais buffer.
- Cn+3 (10%): recém-baselineado, sinal pouco maturado (mais forecast que medição).
- Só reportado quando os 4 meses têm dado (`min_weight_coverage = 100`).
- Mês corrente = base de cálculo oficial reportável; meses futuros aparecem como FM (forecast) no mesmo gráfico — decisão alinhada com João em 23/04.

## Particularidades / pegadinhas already validadas
- KR1 de uma coorte só cai ou fica estável ao longo do tempo, nunca sobe (alterações acumulam). Coortes recém-baselineadas começam ~100% e degradam.
- Parte da queda em ciclos recentes vem de Insider sincronizando datas com supplier que mexeu primeiro — classificado como interno (INT_DATE) mas origem é externa. Ainda não decomposto (próximo passo do roadmap do indicador).
- `kr1_evolucao.sql` usa carry-forward semanal por OP-SKU para manter `vol_original` constante dentro do ciclo ao longo da série temporal.
- Acompanhamento externo temporal usa `vol_ext_any` = união de EXT_CANCEL + EXT_DATE_REV; usa peak/high-water mark ao longo do ciclo (não estado final) para não mascarar cascatas revertidas.

## Roadmap / pendências (do doc de definição)
1. Validar com Raí o mês-base de cálculo (mês corrente vs ciclos abertos ponderados).
2. Decompor a parcela de "mudança interna reativa a sinal externo".
3. Cruzar Plan Freeze Rate com ICP por supplier (`sop_silver.scale_ra`, grão supplier×data).
4. Gráfico de evolução temporal como leitura recorrente em plannings.
