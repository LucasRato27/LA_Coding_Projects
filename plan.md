# Plan: KR1 — Plan Freeze Rate (Estabilidade do Planejamento)

## TL;DR
KR1 mede **que % do plano original de cada ciclo sobreviveu sem alteração interna da Insider**. Usa snapshots da tabela `_history` para detectar 3 tipos de mudança interna: alteração de `dt_planned`, cancelamento por "Revisão de Demanda (In Season)", e mudança de grade (planned_quantity por SKU). Coortes são definidas por `cycle_name`, com baseline = primeiro snapshot sem OPs "pending". Reportado semanalmente, com visões separadas para ciclos base e extras.

---

## 1. Definição do KR1

### Fórmula

```
Plan Freeze Rate = 1 - (Σ volume_alterado_interno / Σ volume_original)
```

- **volume_original**: `planned_quantity` por SKU de cada OP no snapshot de baseline do ciclo
- **volume_alterado_interno**: volume de SKUs que sofreram pelo menos 1 tipo de mudança interna (binário por SKU-OP para evitar double-counting)
- **Granularidade**: por coorte (cycle_name), agregável por mês-alvo e consolidado
- **Frequência**: semanal
- **Meta sugerida**: ≥85% para ciclos base, ≥70% para extras (ajustável após 1 trimestre de dados)

### Baseline (Snapshot de Referência)

Para cada `cycle_name`, o baseline é o **primeiro `ingestion_date`** na tabela `_history` em que **nenhuma OP do ciclo tenha `current_production_stage = 'pending'`**. Esse é o momento em que o ciclo foi oficialmente emitido.

- Tabela: `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`
- Partição: `ingestion_date`
- Lógica: `MIN(ingestion_date) WHERE cycle_name = X AND current_production_stage ≠ 'pending'` — validar se TODAS as OPs do ciclo saíram de pending no mesmo snapshot, ou se basta que a maioria tenha saído

### 3 Tipos de Mudança Interna (Reason Codes INT)

| Código | Descrição | Detecção |
|--------|-----------|----------|
| `INT_DATE` | Alteração de `dt_planned_entry_warehouse` | Comparar `dt_planned_entry_warehouse` no snapshot atual vs baseline. Se diferente → alterado |
| `INT_CANCEL` | Cancelamento por decisão interna | JOIN com `muninn_production_orders` onde `canceled_production_reason = 'Revisão de Demanda (In Season)'` |
| `INT_GRADE` | Mudança de grade (qtd por SKU) | Comparar `planned_quantity` no nível OP-SKU no snapshot atual vs baseline. Se diferente → alterado |

**Regra de contagem**: Se um SKU-OP sofreu qualquer combinação dos 3 tipos, conta como "alterado" **uma única vez** no numerador do KR1. O breakdown por reason code é separado (pode somar >100% do alterado).

### O que NÃO é mudança interna

| Evento | Classificação | Motivo |
|--------|--------------|--------|
| Mudança de `dt_reviewed_entry_warehouse` | EXTERNO | Ajuste do/com fornecedor |
| Cancelamento com outro motivo (não "In Season") | EXTERNO | Decisão motivada por fator externo |
| Atraso na entrega (`dt_max_entry > dt_reviewed`) | EXTERNO (KR2) | Performance do fornecedor |
| Entrega parcial | EXTERNO (KR3) | Performance do fornecedor |

---

## 2. Denominador Consolidado — Como resolver múltiplas coortes

### Problema
Cada `cycle_name` é uma coorte independente. Como consolidar em um número único semanal?

### Solução: Mês-Alvo + Ponderação por Volume

**Passo 1 — Atribuir mês-alvo a cada coorte**:
Para cada coorte, o "mês-alvo" é o mês que concentra a maior parte do `volume_original` por `dt_planned_entry_warehouse` no baseline (moda ponderada por volume).

Exemplo: C012026 tem 80% do volume com `dt_planned` em março → mês-alvo = março.

**Passo 2 — Consolidar por mês-alvo**:
```
KR1_consolidado_mes = 1 - (Σ vol_alterado de todas as coortes do mês / Σ vol_original de todas as coortes do mês)
```

Isso é equivalente a uma média ponderada por volume dos KR1s individuais de cada coorte.

**Passo 3 — Visão semanal**:
A cada semana, comparar o snapshot mais recente contra o baseline de cada coorte. O KR1 de cada coorte evolui ao longo do tempo (só pode cair ou ficar estável — é cumulativo).

### Métricas complementares

| Métrica | O que mostra |
|---------|-------------|
| **KR1 (cumulativo)** | % do plano original intacto desde o baseline |
| **Taxa de Mudança Semanal** | % do plano que mudou ESTA semana especificamente (alerta precoce) |
| **Breakdown por Reason Code** | Qual tipo de mudança está puxando o KR1 para baixo |

---

## 3. Tratamento de Extras — 3 Opções

### Opção A — Tudo junto, meta única
- Extras entram no consolidado com a mesma meta dos ciclos base
- **Pro**: simplicidade
- **Contra**: extras são naturalmente mais voláteis → puxa o indicador para baixo injustamente

### Opção B — Visão separada
- KR1 mede apenas ciclos base. Extras têm dashboard separado, sem meta de OKR
- **Pro**: KR1 limpo, reflete o processo formal de planejamento
- **Contra**: ignora ~50% do volume

### Opção C — Sub-indicadores (Recomendado)
- **KR1a** (ciclos base): meta ≥85%
- **KR1b** (extras): meta ≥70%
- **KR1 consolidado** = média ponderada por volume de KR1a e KR1b
- **Pro**: visão completa, metas justas, incentiva estabilidade em ambos sem penalizar a flexibilidade dos extras
- **Contra**: ligeiramente mais complexo de comunicar

---

## 4. Visualização (Dashboard KR1)

### Visão Principal — Weekly Scorecard
1. **Gauge KR1 consolidado** com trend (vs semana anterior)
2. **KR1a vs KR1b** — dois mini-gauges lado a lado
3. **Tabela de coortes ativas** — cycle_name | vol_original | vol_alterado | KR1 | pior reason code

### Visão de Evolução
4. **Line chart semanal** — KR1 por coorte ao longo do tempo (cada coorte é uma linha que nasce em 100% e vai caindo)
5. **Stacked area da Taxa de Mudança Semanal** — por reason code (INT_DATE, INT_CANCEL, INT_GRADE), mostrando o "ritmo" de degradação

### Visão de Diagnóstico
6. **Waterfall por coorte** — vol_original → INT_DATE → INT_CANCEL → INT_GRADE → vol_inalterado
7. **Treemap ou bar**: quais produtos/SKUs mais mudaram em cada coorte
8. **Scatter**: vol_alterado vs mês-alvo, para ver se coortes mais distantes (D+4) são mais estáveis que coortes próximas

---

## 5. Implementação — Steps

### Fase 1 — SQL de Baseline e Detecção de Mudanças

**Step 1**: Query para determinar o baseline de cada coorte
- Fonte: `supply_chain_efficiency_model_input_history`
- Lógica: Para cada `cycle_name`, achar o `MIN(ingestion_date)` onde nenhuma OP do ciclo está em `pending`
- Output: tabela `baseline_por_coorte` (cycle_name, ingestion_date_baseline)

**Step 2**: Query para extrair estado no baseline
- Fonte: `_history` filtrado por `ingestion_date = baseline`
- Output: tabela `plano_original` (op_code, sku, cycle_name, dt_planned, planned_qty, ...)

**Step 3**: Query para extrair estado atual (snapshot mais recente)
- Fonte: `_history` no `MAX(ingestion_date)` + JOIN com `muninn_production_orders` para cancelamentos
- Output: tabela `plano_atual` (op_code, sku, cycle_name, dt_planned_atual, planned_qty_atual, canceled_reason, ...)

**Step 4**: Query de comparação — *depende de steps 2-3*
- LEFT JOIN `plano_original` com `plano_atual` por (op_code, sku)
- Criar flags: `is_int_date`, `is_int_cancel`, `is_int_grade`, `is_altered`
- Calcular `volume_alterado` e `volume_original` por coorte

### Fase 2 — Cálculo e Agregação

**Step 5**: Calcular KR1 por coorte — *depende de step 4*
- KR1 = 1 - (sum(planned_qty WHERE is_altered) / sum(planned_qty_original))
- Separar KR1a (base) e KR1b (extras)

**Step 6**: Calcular mês-alvo por coorte — *depende de step 2*
- Moda ponderada de `dt_planned` no baseline

**Step 7**: Consolidar KR1 por mês-alvo — *depende de steps 5-6*
- Weighted average

**Step 8**: Calcular taxa de mudança semanal — *depende de step 4 com múltiplos snapshots*
- Comparar snapshot da semana atual vs semana anterior (não vs baseline)

### Fase 3 — Dashboard

**Step 9**: Implementar gauges e tabela (visão principal) — *depende de steps 5, 7*
**Step 10**: Implementar line charts de evolução — *depende de step 5 com série temporal*
**Step 11**: Implementar waterfall e diagnóstico — *depende de steps 4, 5*

---

## 6. Arquivos Relevantes

### Fontes de dados
- `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` — snapshots históricos (CENTRAL para o KR1)
- `insider-data-lake.integrated.muninn_production_orders` — `canceled_production_reason` para INT_CANCEL
- `insider-data-lake.sop_silver.supply_chain_efficiency_model_input` — estado atual das OPs

### SQLs existentes como referência
- `20260415_Lookback cadeia/1_Inputs/1_SQL/ops.sql` — lógica de extração de OPs, cycle_type regex
- `20260415_Lookback cadeia/1_Inputs/1_SQL/1_SQLs para Claudio/mkup_entregue.sql` — delay_days, quality join (referência para EXT)

### Notebook base
- `20260415_Lookback cadeia/2_Códigos/Análises Lookback_v20260414.ipynb` — para implementar análise

---

## 7. Verificação

1. Validar baseline: para um cycle_name conhecido (e.g., C012026), verificar manualmente que o snapshot de baseline faz sentido (OPs não estão mais pending, volumes batem com o que se espera)
2. Conferir que `SUM(volume_original)` de todas as coortes de um mês bate com o volume total de OPs planejadas para aquele mês
3. Testar INT_CANCEL: filtrar OPs canceladas com motivo "Revisão de Demanda (In Season)" e confirmar que são classificadas corretamente
4. Testar double-counting: uma OP que teve INT_DATE + INT_GRADE deve contar só 1x no KR1 consolidado, mas 2x no breakdown
5. Comparar KR1 de coortes mais antigas vs mais recentes — coortes mais antigas devem ter KR1 mais degradado (mais tempo para mudanças acumularem)
6. Sanity check: KR1 nunca pode subir de uma semana para outra (é cumulativo)

---

## 8. Decisões Confirmadas

- dt_planned = controle Insider (INT), dt_reviewed = ajuste fornecedor (EXT)
- Baseline = primeiro snapshot sem OPs pending por ciclo
- Cancelamento interno = `canceled_production_reason = 'Revisão de Demanda (In Season)'`
- Mudança de grade = mudança em `planned_quantity` no nível SKU-OP
- Frequência: semanal
- cycle_name é fixo por OP (não migra)

---

## 9. Acompanhamento Externo (fora do OKR, mas no dashboard)

### Reason Codes Externos

| Código | Descrição | Detecção na _history |
|--------|-----------|----------------------|
| `EXT_CANCEL` | Cancelamento por motivo externo | `canceled_production_reason` ≠ `'Revisão de Demanda (In Season)'` AND OP cancelada |
| `EXT_DATE_REV` | Fornecedor mudou `dt_reviewed` | `dt_reviewed` mudou entre snapshots **E** `dt_planned` **NÃO** mudou no mesmo intervalo |

### Regra-chave para EXT_DATE_REV vs INT_DATE
Quando a Insider muda `dt_planned`, o `dt_reviewed` muda junto. Portanto:
- **Se `dt_planned` mudou → tudo é INT_DATE** (a mudança de `dt_reviewed` é consequência)
- **Se `dt_reviewed` mudou MAS `dt_planned` ficou igual → EXT_DATE_REV** (fornecedor causou)

Na prática, para cada par de snapshots consecutivos (dia D e dia D+1) de uma mesma OP:
```
IF dt_planned[D+1] ≠ dt_planned[D] → INT_DATE
IF dt_reviewed[D+1] ≠ dt_reviewed[D] AND dt_planned[D+1] = dt_planned[D] → EXT_DATE_REV
```

---

## 10. Análise de Frequência de Revisões

### Por que medir
Uma OP que mudou 1x de data pode ser um ajuste razoável. Uma que mudou 5x é sinal de instabilidade — seja interna ou do fornecedor. A frequência de revisões é uma métrica de "saúde do processo" independente do KR1.

### Métricas de Frequência

| Métrica | Definição | Granularidade |
|---------|-----------|---------------|
| **n_rev_planned** | Nº de vezes que `dt_planned` mudou entre snapshots consecutivos | Por OP |
| **n_rev_reviewed** | Nº de vezes que `dt_reviewed` mudou (sem mudança simultânea de `dt_planned`) | Por OP |
| **n_rev_grade** | Nº de vezes que `planned_quantity` no nível SKU mudou | Por OP-SKU |
| **n_rev_total** | `n_rev_planned + n_rev_reviewed + n_rev_grade` | Por OP |
| **magnitude_date** | Soma dos |Δ dias| em cada revisão de data | Por OP (separa planned vs reviewed) |

### Como calcular com snapshots diários
Para cada OP, comparar `snapshot[D]` vs `snapshot[D-1]`:
- Se `dt_planned[D] ≠ dt_planned[D-1]` → incrementa `n_rev_planned`, acumula `|dt_planned[D] - dt_planned[D-1]|` em `magnitude_date_planned`
- Se `dt_reviewed[D] ≠ dt_reviewed[D-1]` AND `dt_planned[D] = dt_planned[D-1]` → incrementa `n_rev_reviewed`, acumula magnitude
- Se `planned_quantity[D] ≠ planned_quantity[D-1]` para qualquer SKU → incrementa `n_rev_grade`

### Visualização de Frequência (adicional ao dashboard)

**Gráfico A — Distribuição de Revisões por OP** (histogram)
- Eixo X: número de revisões (0, 1, 2, 3, 4, 5+)
- Eixo Y: % de OPs
- Cor: tipo (INT vs EXT)
- Insight: "70% das OPs nunca mudaram" ou "30% mudaram 3+ vezes"

**Gráfico B — Heatmap Revisões × Fornecedor** (heatmap)
- Eixo X: fornecedor
- Eixo Y: faixa de n_rev_reviewed (0, 1, 2, 3+)
- Cor: volume de OPs
- Insight: quais fornecedores geram mais re-revisões

**Gráfico C — Timeline de Revisões** (gantt-like, drill-down)
- Para uma OP específica: mostrar dt_planned e dt_reviewed ao longo do tempo (cada snapshot = um ponto)
- Visualiza a "dança" das datas — quando a data planejada deu um salto, quando o fornecedor revisou, etc.

**Gráfico D — Scatter de Instabilidade**
- Eixo X: n_rev_total da OP
- Eixo Y: magnitude_date total (soma dos deltas)
- Tamanho do ponto: volume da OP
- Cor: INT-dominado vs EXT-dominado
- Quadrante superior-direito = OPs mais instáveis (muitas revisões de grande magnitude)

---

## 11. Steps de Implementação Atualizados

### Fase 0 — Setup do Projeto
0. Criar pasta `20260420_OKR Estabilidade Planejamento/` com estrutura padrão: `1_Inputs/1_SQL/`, `2_Códigos/`, `3_Outputs/`, `4_Analysis/`

### Fase 1 — SQL de Baseline e Detecção (steps 1-4, sequenciais)
1. Query de baseline por coorte (primeiro ingestion_date sem OPs pending)
2. Extração do plano original (estado no baseline)
3. Extração do estado atual + JOIN com `muninn_production_orders` para cancelamentos
4. Comparação baseline vs atual → flags INT_DATE, INT_CANCEL, INT_GRADE, EXT_CANCEL, EXT_DATE_REV

### Fase 2 — Análise de Frequência (step 5, paralelo com Fase 2-KR1)
5. Query de pares de snapshots consecutivos para calcular n_rev_planned, n_rev_reviewed, n_rev_grade, magnitude_date por OP — usa window function LAG() sobre ingestion_date

### Fase 3 — Cálculo KR1 (steps 6-9, depende de Fase 1)
6. KR1 por coorte (apenas mudanças INT)
7. Mês-alvo por coorte
8. KR1a (base) + KR1b (extras) + consolidado
9. Taxa de mudança semanal

### Fase 4 — Dashboard (steps 10-14, depende de Fases 2-3)
10. Scorecard KR1 + tabela de coortes
11. Evolução temporal por coorte
12. Waterfall + reason code breakdown (INT e EXT lado a lado)
13. Distribuição + heatmap de frequência de revisões
14. Timeline drill-down de OP específica + scatter de instabilidade

### Verificação
1. Baseline: validar manualmente para um ciclo conhecido
2. Somas: vol_original fecha com volume total planejado
3. INT_CANCEL: filtrar por 'Revisão de Demanda (In Season)' e conferir
4. EXT_DATE_REV: verificar que não há overlap com INT_DATE (quando dt_planned muda, a mudança de dt_reviewed NÃO conta como EXT)
5. Frequência: OP com n_rev_total = 0 deve ter KR1 = 100% para essa OP
6. KR1 nunca sobe entre semanas
7. Spot-check: pegar 3-5 OPs com muitas revisões e validar manualmente na timeline de snapshots

## 12. Decisões Confirmadas

- Opção C para extras (sub-indicadores KR1a + KR1b)
- Cancelamento externo = tudo que não é 'Revisão de Demanda (In Season)'
- dt_reviewed muda junto com dt_planned quando Insider altera → só conta como EXT_DATE_REV se dt_planned ficou igual
- Snapshot diário na _history
- Acompanhamento externo + frequência de revisões fora do OKR formal, mas no dashboard
