# KR1 — Plan Freeze Rate (Estabilidade do Planejamento)

Documentação de negócio do notebook [KR1_Plan_Freeze_Rate_v20260420.ipynb](KR1_Plan_Freeze_Rate_v20260420.ipynb).

> **Escopo desta documentação:** apenas o **OKR de Plan Freeze Rate** e as visões diretamente relacionadas — ou seja, **somente as alterações internas** (mexidas feitas pela Insider no plano após o congelamento). O notebook também produz visões de alterações externas (fornecedor), que ficam **fora deste documento** por não entrarem no KR1.
>
> Público-alvo: S&OP, Supply, Planejamento e Liderança. Analistas encontrarão um apêndice técnico ao final.

---

## 1. Objetivo do notebook

O KR1 mede **quanto do plano original de cada ciclo de compra sobreviveu sem alteração interna da Insider**, entre o momento em que o ciclo foi congelado (*baseline*) e o snapshot mais recente.

Em linguagem de negócio, ele responde:

> *"Do plano que a Insider assumiu com os fornecedores no início do ciclo, quanto continua de pé hoje?"*

- **Alto KR1 → planejamento estável.** Fornecedores conseguem trabalhar com previsibilidade, lead time cai, ICP sobe.
- **Baixo KR1 → replanejamento constante.** Cada alteração interna vira retrabalho para o fornecedor e ruído na cadeia.

Referência conceitual completa: [definicao_okr_plan_freeze_rate.md](../../analyses/definicao_okr_plan_freeze_rate.md).

---

## 2. Universo de OPs considerado

Todas as análises partem do mesmo recorte:

| Filtro | Regra | Motivo |
|---|---|---|
| Fonte | `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` | Tabela oficial com histórico diário de cada OP-SKU |
| Tipo de OP | Exclui `flexible` e `converted` | Não representam plano firme; distorcem a base |
| Ciclo | `cycle_name` não pode ser nulo | OP sem ciclo não pertence a nenhum plano formal |
| Ciclos excluídos | `C012026` removido manualmente | Ciclo com anomalia conhecida na formação do baseline |
| Janela de análise | `mes_alvo ≥ 2025-11` | Início da vigência do OKR atual |

### Tipos de ciclo

- **Base** — ciclos regulares mensais, no formato `C{MM}{AAAA}` (ex.: `C062026`, `C072026`). Meta KR1 ≥ **85%**.
- **Extra** — qualquer ciclo fora desse padrão (reforços, ciclos especiais, coleções pontuais). Meta KR1 ≥ **70%**.

O KR1 é reportado separadamente para Base e Extra (KR1a e KR1b) porque a natureza de estabilidade esperada é diferente.

---

## 3. Como o KR1 é calculado

### Passo 1 — Definir o baseline de cada ciclo

O **baseline** é o primeiro dia (`ingestion_date`) em que **nenhuma OP do ciclo ainda está em stage `pending`**. Ou seja: o dia em que o plano finalmente "congelou" e virou compromisso.

A partir dele, todo delta interno observado é atribuído a um reason code.

### Passo 2 — Comparar baseline vs. estado atual

Para cada par `OP × SKU`, comparamos os valores do baseline com o snapshot mais recente e marcamos flags **internas** (as únicas que entram no KR1):

- Mudou a **data planejada** de entrada em CD → **INT_DATE**
- Mudou a **quantidade planejada** do SKU → **INT_GRADE**
- OP foi **cancelada por decisão de demanda** ("Revisão de Demanda (In Season)") → **INT_CANCEL**

### Passo 3 — Aplicar a fórmula

$$KR1 = 1 - \frac{\text{volume alterado internamente}}{\text{volume original do baseline}}$$

Regra importante: **cada SKU conta uma única vez no numerador**, mesmo que tenha sido tocado por mais de um motivo interno. Não há double-count.

### Passo 4 — Atribuir cada ciclo a um mês-alvo

Cada ciclo é classificado no **mês em que concentra o maior volume planejado**. Isso permite comparar KR1 por mês de entrada de mercadoria, e não pelo mês em que o ciclo foi rodado.

### Passo 5 — OKR ponderado (janela Cn..Cn+3)

O OKR reportado semanalmente é uma **média ponderada** dos KR1 dos 4 meses seguintes ao snapshot.

$$OKR_{semana} = \frac{35 \cdot KR1_{Cn} + 35 \cdot KR1_{Cn+1} + 20 \cdot KR1_{Cn+2} + 10 \cdot KR1_{Cn+3}}{100}$$

Onde `Cn` é o **mês corrente** do snapshot, `Cn+1` o mês seguinte, e assim por diante.

#### Por que esses pesos?

Os pesos refletem **o nível de controle real que temos sobre cada mês da janela** — quanto mais próximo do "agora", mais caro é replanejar e mais rigor exigimos:

| Mês da janela | Peso | Racional de negócio |
|---|---|---|
| **Cn (mês corrente)** | **35%** | Mês que **está acontecendo agora**. Boa parte das OPs já saiu da fábrica ou está em transporte — mexer aqui gera o maior impacto na cadeia (retrabalho, custo de reprogramação, ICP). Deve ser o mais "congelado". |
| **Cn+1** | **35%** | Mês **imediatamente seguinte**. Fornecedores já estão produzindo. Alterações ainda são muito custosas. Peso alto para reforçar que também precisa estar firme. |
| **Cn+2** | **20%** | Mês com **alguma flexibilidade**. Fornecedores estão programando matéria-prima; ajustes são possíveis, mas ainda pesam na relação. Peso intermediário. |
| **Cn+3** | **10%** | Mês **mais distante da janela**. Ainda há espaço para revisão sem grande impacto operacional. Peso menor porque cobra-se menos estabilidade. |
| **Soma** | **100%** | |

**Leitura:** os pesos criam um **gradiente de rigor**. O custo de replanejamento não é linear no tempo — mexer no mês corrente é dramaticamente mais caro do que mexer em Cn+3. Os pesos concentram a cobrança onde ela precisa doer.

**Reporte:** o OKR ponderado só é calculado quando **todos os 4 meses da janela têm dado** (`min_weight_coverage = 100`). Se faltar qualquer um, a semana fica em branco — evita induzir leitura errada com base incompleta.

---

## 4. Glossário — Reason codes internos

| Código | O que é | Entra no KR1? |
|---|---|---|
| **INT_DATE** | Insider mudou a data planejada de entrada da OP em CD | ✅ Sim |
| **INT_GRADE** | Insider mudou a quantidade planejada de um SKU | ✅ Sim |
| **INT_CANCEL** | OP cancelada com motivo "Revisão de Demanda (In Season)" | ✅ Sim |
| **INT_ANY** | União dos três acima — numerador do KR1, sem double-count | ✅ Sim |

Outros termos:

| Termo | Definição |
|---|---|
| **Baseline** | Primeiro dia do ciclo em que nenhuma OP está mais em `pending`. Fotografia do plano congelado. |
| **cycle_name** | Identificador do ciclo. Base = `C{MM}{AAAA}`; Extra = qualquer outro rótulo. |
| **mes_alvo** | Mês em que o ciclo concentra o maior volume planejado no baseline. |
| **KR1a** | KR1 dos ciclos Base. Meta ≥ 85%. |
| **KR1b** | KR1 dos ciclos Extra. Meta ≥ 70%. |
| **OKR Ponderado** | Média ponderada Cn..Cn+3 (pesos 35/35/20/10). |
| **Volume original** | Soma de `planned_quantity` no baseline. Denominador do KR1. |
| **Waterfall** | Visualização em cascata que decompõe o volume original nos motivos de alteração até chegar ao "inalterado". |

---

## 5. Guia de leitura dos gráficos (apenas alterações internas)

Abaixo, os blocos do notebook relevantes para o OKR de Plan Freeze Rate. Gráficos de alterações externas e gráficos de dispersão (scatter) **não fazem parte deste escopo** e foram omitidos.

### Seção 4 — Visão de Planejamento (snapshot atual)

**4.1 · KR1 por Mês-Alvo**
> *"Qual foi o KR1 de cada mês, separado por tipo de ciclo?"*

Barras agrupadas: para cada `mes_alvo`, duas barras (Base e Extra). Eixo Y = KR1 em %, escala 0–105%. Linhas horizontais tracejadas nas metas (85% Base, 70% Extra).

**Como ler:** barra acima da linha da meta → mês dentro do compromisso. Barra abaixo → mês crítico, exige investigação de causas.

**4.2 · Waterfall de Alterações Internas**
> *"De onde vieram as perdas de KR1 no mês selecionado?"*

Decomposição em cascata do volume original de um mês específico (default: `2026-06`, ciclos Base). Começa em 100% do volume original, subtrai em ordem exclusiva: **INT_CANCEL → INT_DATE → INT_GRADE**, e termina em "Inalterado".

**Como ler:** o tamanho de cada degrau negativo indica qual reason code mais destruiu KR1 naquele mês. A prioridade `CANCEL > DATE > GRADE` garante que cada SKU aparece em apenas um bucket (sem overlap).

**4.3 · Breakdown de Reason Codes Internos por Mês**
> *"Como as alterações internas se distribuem ao longo dos meses (só Base)?"*

Barras empilhadas por mês, painel **Interno** (INT_CANCEL / INT_DATE / INT_GRADE). Linha pontilhada preta = % total de SKUs alterados internamente (sem double-count).

**Como ler:** compare a composição mês a mês — é INT_DATE que domina? INT_GRADE está crescendo? A linha total mostra a magnitude combinada.

> *Obs.: o notebook também exibe um painel "Externo" ao lado desse gráfico — ignorar para fins deste OKR.*

**4.4 · KR1 por Coorte (Top 20)**
> *"Quais ciclos individuais estão puxando o KR1 para cima ou para baixo?"*

Barras por `cycle_name` (top 20 por volume original), coloridas por tipo (Base/Extra). Hover mostra `n_ops` e `vol_int_any`.

**Como ler:** identifica ciclos-outlier (ex.: um ciclo Base específico com KR1 muito abaixo dos demais) que merecem drill-down.

---

### Seção 5 — Frequência de Revisões

**5.1 · Distribuição de Frequência de Revisões por OP**
> *"Quantas vezes uma OP típica é revisada durante o ciclo?"*

Barras agrupadas Base/Extra por faixas: `0 / 1 / 2 / 3 / 4-5 / 6+` revisões. Imprime também: `% sem revisão` e `% com 3+ revisões`.

**Como ler:** quanto mais concentrado à esquerda (0-1 revisões), melhor. Cauda longa (6+) sinaliza OPs "em turbilhão" — candidatas naturais a drill-down.

---

### Seção 6 — Drill-Down: Timeline de uma OP específica

O notebook seleciona automaticamente a OP-SKU **mais instável** (maior número total de revisões).

**6.1 · Timeline de Datas**
Linha de `dt_planned` (vermelho) ao longo dos snapshots diários, com marker cheio nos dias em que houve **INT_DATE**. Diamantes laranjas marcam dias com **INT_GRADE**.

**Como ler:** conta a história do plano dessa OP dia a dia. Útil para investigar casos específicos em reuniões de causa raiz — ver quantas vezes a Insider mexeu, em qual direção, com que intervalo.

> *Obs.: o gráfico original também mostra `dt_reviewed` (linha roxa) marcando alterações externas do fornecedor — ignorar para este OKR.*

**6.2 · Timeline de Grade**
Série de `planned_quantity` ao longo dos snapshots, com markers destacados nos **INT_GRADE**.

**Como ler:** mostra a magnitude e a direção das mudanças de quantidade (subiu, desceu, quantas vezes).

---

### Seção 7 — Evolução Temporal do KR1

**7.1 · KR1 por Mês-Alvo ao longo das semanas** (facetado Base | Extra)
> *"Como o KR1 de cada mês evoluiu semana a semana?"*

Uma linha por `mes_alvo`. Eixo X = semana do snapshot; Eixo Y = KR1 %.

**Como ler:** conta a história da degradação do plano. Mês que começa em 100% e vai caindo → replanejamento contínuo. Mês estável → plano firme.

**7.2 · KR1 Consolidado Base vs Extra**
Linhas semanais dos KR1a e KR1b agregados, com as metas (85% / 70%) como referência.

**Como ler:** visão macro para leadership. Comparação direta com as metas.

**7.3 · Heatmap Semana × Mês-Alvo (Base)**
Matriz colorida em escala verde-amarelo-vermelho (50–100%).

**Como ler:** identificação visual rápida de quais combinações semana × mês estão em zona crítica. Verde = plano firme; vermelho = plano derretendo.

---

### Seção 7a — OKR Ponderado e Visões Internas

Esta é a **seção-chave do OKR**.

**7a.1 · Evolução do OKR Ponderado Cn..Cn+3** (Base vs Extra)
Métrica **oficial** de acompanhamento semanal do OKR, aplicando os pesos 35/35/20/10 descritos na seção 3.

**Como ler:** é o número que vai para o report executivo. Cada ponto = OKR daquela semana calculado com os 4 meses seguintes.

**7a.2 · Volume Alocado por Ciclo (stack)**
Barras empilhadas por ciclo com 4 classes: **Cancelamento / Alteração de Data / Alteração de Grade / Inalterado**. Barras de ciclos Base opacas; Extra em opacidade 38% para distinção visual.

**Como ler:** mostra o volume absoluto em cada bucket. Ciclo com barra grande e altura verde ("Inalterado") = plano estável e relevante. Barras com muita fatia vermelha/laranja = ciclos que puxaram o OKR para baixo.

**7a.3 · % de Alteração Interna por Mês** (facetado Base/Extra)
Barras empilhadas mês a mês com % em cada reason code interno (INT_CANCEL / INT_DATE / INT_GRADE) + linha "% Total INT".

**Como ler:** decomposição percentual da erosão do KR1, mês a mês. Complementa o waterfall (4.2) numa visão temporal.

**7a.4 · OKR Ponderado por Fornecedor (Top 10, Base)**
Ranking dos 10 fornecedores com maior peso pelo OKR ponderado dos ciclos Base.

**Como ler:** onde o replanejamento interno mais afeta parceiros específicos. Fornecedor no topo com OKR baixo = maior alavanca de melhoria de relacionamento.

**7a.5 · OKR Ponderado por Produto (Top 10, Base)**
Mesma lógica no nível de produto.

**Como ler:** que categorias/produtos concentram a instabilidade interna. Insumo para conversar com o time de Produto sobre disciplina de forecast.

**7a.6 · Volume Alocado + OKR Atual por Fornecedor**
Bar + line (eixo secundário): barras = volume alocado por fornecedor; linha = KR1 atual. Top 10, com distinção Base vs Extra.

**Como ler:** cruza **relevância (volume)** com **estabilidade (KR1)**. Volume alto + KR1 baixo = **maior alavanca de melhoria** (impacto × facilidade de mover o número).

**7a.7 · Volume Alocado + OKR Atual por Produto**
Mesma leitura, no nível de produto.

---

## 6. Outputs gerados

Todos gravados em [outputs/plan_freeze_rate/](../../outputs/plan_freeze_rate/) com sufixo `_{AAAAMMDD}` da data de execução.

| Arquivo | Grão | Uso sugerido |
|---|---|---|
| `kr1_por_coorte_{today}.csv` | 1 linha por `cycle_name` | Ranking de ciclos, volumes INT decompostos, insumo para dashboards |
| `kr1_por_mes_{today}.csv` | 1 linha por `mes_alvo × cycle_type` | Reporte executivo mensal |
| `plano_vs_atual_{today}.csv` | 1 linha por OP-SKU | Base analítica completa para drill-down ad-hoc |
| `frequencia_revisoes_{today}.csv` | 1 linha por OP-SKU | Investigação de OPs mais revisadas |
| `kr1_evolucao_{today}.csv` | Série semanal por ciclo × supplier × produto | Insumo do painel de evolução temporal |

---

## 7. Parâmetros configuráveis

Na segunda célula do notebook:

| Parâmetro | Default | Quando alterar |
|---|---|---|
| `WATERFALL_MES_ESCOLHIDO` | `'2026-06'` | Para gerar o waterfall de outro mês em reuniões |
| `WATERFALL_CYCLE_TYPE` | `'Base'` | Trocar para `'Extra'` ou `None` |
| `CICLOS_EXCLUIDOS` | `['C012026']` | Adicionar ciclos com problemas de baseline conhecidos |
| `OKR_WEIGHTS` | `{0:35, 1:35, 2:20, 3:10}` | **Somente** se a definição oficial do OKR for revisada |

---

## 8. Perguntas frequentes e limitações

**Por que só alterações internas entram no KR1?**
Porque o KR1 mede **compromisso interno da Insider** — só penaliza alterações que a própria empresa fez no plano após o congelamento. Mexidas do fornecedor são acompanhadas em outro indicador (risco de cadeia), fora deste documento.

**Por que o ciclo `C012026` foi excluído?**
Anomalia conhecida no baseline (formação incompleta na virada do ano). Voltará quando reprocessado.

**Por que o OKR ponderado às vezes não aparece em algumas semanas?**
Só é reportado quando os 4 meses da janela Cn..Cn+3 têm dados. Se faltar qualquer um, a semana fica em branco para não induzir leitura errada.

**Por que os pesos são 35/35/20/10 e não iguais (25 cada)?**
Porque o custo de replanejamento **não é linear no tempo**. Mexer no mês corrente ou no próximo é dramaticamente mais caro para o fornecedor do que mexer em Cn+3. Os pesos reforçam onde a estabilidade precisa ser mais rigorosa — mesmo racional que sustenta a existência do baseline.

**A análise semanal usa qual dia como corte?**
Segunda-feira. Cada snapshot semanal pega o último dado disponível **até o domingo anterior**.

**O KR1 pode ficar acima de 100%?**
Não. Se um SKU volta a ter os mesmos valores do baseline após uma alteração, ele deixa de contar no numerador. Mas alterações que se revertem no meio do caminho ainda são visíveis na frequência de revisões (5.1).

**Novas OPs criadas depois do baseline entram na conta?**
Não. O denominador (`volume original`) é fixado no baseline. Adições posteriores são registradas separadamente para diagnóstico, mas não afetam o KR1 do ciclo.

---

## Apêndice técnico

### Tabelas fonte

| Tabela | Uso |
|---|---|
| `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` | Histórico diário de OP-SKU (base de tudo) |
| `insider-data-lake.integrated.muninn_production_orders` | Metadados da OP; único lugar com `canceled_production_reason` |

### Filtros obrigatórios (aplicados em toda query do KR1)

```sql
WHERE production_order_type NOT IN ('flexible', 'converted')
  AND cycle_name IS NOT NULL
```

### Definição do baseline (SQL)

```sql
-- Primeiro ingestion_date por ciclo onde ninguém está mais em 'pending'
SELECT cycle_name, MIN(ingestion_date) AS baseline_date
FROM (
  SELECT cycle_name, ingestion_date
  FROM supply_chain_efficiency_model_input_history
  WHERE production_order_type NOT IN ('flexible','converted')
    AND cycle_name IS NOT NULL
  GROUP BY cycle_name, ingestion_date
  HAVING COUNTIF(current_production_stage = 'pending') = 0
)
GROUP BY cycle_name
```

### Reason codes internos (lógica SQL simplificada)

| Flag | Condição |
|---|---|
| `is_int_date` | `current_dt_planned != baseline_dt_planned` |
| `is_int_grade` | `current_planned_qty != baseline_planned_qty` |
| `is_int_cancel` | `current_stage = 'canceled'` AND `canceled_production_reason = 'Revisão de Demanda (In Season)'` |
| `is_int_any` | `is_int_date OR is_int_grade OR is_int_cancel` |

### Fórmulas

$$KR1_{ciclo} = 1 - \frac{\sum_{SKU} \text{baseline\_planned\_qty} \cdot \mathbb{1}[is\_int\_any]}{\sum_{SKU} \text{baseline\_planned\_qty}}$$

$$OKR_{semana} = \frac{35 \cdot KR1_{Cn} + 35 \cdot KR1_{Cn+1} + 20 \cdot KR1_{Cn+2} + 10 \cdot KR1_{Cn+3}}{100}$$

### Classificação de ciclo

```sql
CASE
  WHEN REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}$') THEN 'Base'
  ELSE 'Extra'
END AS cycle_type
```

### Arquivos SQL na pasta [sql/](sql/)

| Arquivo | Grão | Papel |
|---|---|---|
| `01_baseline.sql` | `cycle_name` | Define baseline e classifica Base/Extra |
| `02_plano_vs_atual.sql` | `op_code × product_sku` | Query mestra do KR1 (baseline vs snapshot atual + flags) |
| `03_frequencia_revisoes.sql` | `op_code × product_sku` | Contagem e magnitude de revisões via `LAG` |
| `04_kr1_evolucao.sql` | `semana × ciclo × supplier × produto` | Reconstrói KR1 semana a semana |
| `05_drill_down_op.sql` | Snapshots diários de 1 OP-SKU | Parametrizada; alimenta a Seção 6 |
