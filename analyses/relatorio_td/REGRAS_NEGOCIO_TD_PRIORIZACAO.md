# Regras de Negócio — Pipeline de T&D e Priorização de Melhorias

**Documento:** Auditoria de lógica analítica  
**Versão:** 2.0 · **Data:** 20/06/2026  
**Fonte do código:** `analyses/relatorio_td/TD_Priorizacao_Melhorias_Unificado.ipynb` (SQL canônico em célula `c0007`) · `td_analysis_functions.py` (utilidades Python)  
⚠️ `pipeline_reversas_priorizacao_produtos.sql` — **DEPRECATED desde 2026-06-18** (SQL migrado para dentro do notebook)  
**Audiência:** Time de Negócio / Produto Físico / Produto / Comercial

> Este documento descreve, em linguagem de negócio, **cada regra de decisão** embutida no pipeline de análise de Trocas e Devoluções (T&D). O objetivo é permitir que uma pessoa de negócio verifique se a lógica faz sentido antes de agir sobre os resultados.

---

## Sumário

1. [O que o pipeline faz](#1-o-que-o-pipeline-faz)
2. [Pré-requisito: volume mínimo para diagnóstico](#2-pré-requisito-volume-mínimo-para-diagnóstico)
3. [Os três scores e como são calculados](#3-os-três-scores-e-como-são-calculados)
4. [O farol de priorização — 5 buckets](#4-o-farol-de-priorização--5-buckets)
5. [Ordem de avaliação das regras (importante)](#5-ordem-de-avaliação-das-regras-importante)
6. [Decisão estratégica — kill or keep](#6-decisão-estratégica--kill-or-keep)
7. [Classificação do tipo de problema](#7-classificação-do-tipo-de-problema)
8. [Mapeamento de tags para tipo de ação](#8-mapeamento-de-tags-para-tipo-de-ação)
9. [Geração automática do tweet analítico](#9-geração-automática-do-tweet-analítico)
10. [Como o scorecard de portfólio entra no output](#10-como-o-scorecard-de-portfólio-entra-no-output)
11. [Ordenação dos produtos nos outputs](#11-ordenação-dos-produtos-nos-outputs)
12. [Perguntas para validação de negócio](#12-perguntas-para-validação-de-negócio)

---

## 1. O que o pipeline faz

O pipeline recebe a base consolidada de pedidos, reversas e tags de motivo dos últimos **12 meses** e, para cada produto do portfólio, responde duas perguntas:

1. **Qual a urgência de melhorar esse produto?** → sinal `sinal_priorizacao` (5 categorias, calculado em SQL)
2. **Vale melhorar ou é melhor descontinuar/reformular?** → sinal `sinal_kill_keep` (2 estados, calculado em Python)

O output final é um workbook Excel com 7 abas, sendo a aba `relatorio_pf` a principal entrega ao time de Produto Físico.

---

## 2. Pré-requisito: volume mínimo para diagnóstico

Antes de qualquer classificação, o produto precisa ter volume suficiente para que os scores sejam estatisticamente confiáveis.

| Critério | Limiar mínimo |
|---|---|
| Itens vendidos no período | **≥ 30 unidades** |
| Itens retornados (T&D) no período | **≥ 5 reversas** |

**Regra:** Se o produto não atingir **ambos** os critérios ao mesmo tempo, ele recebe automaticamente o sinal `Sem evidência suficiente` e não entra em nenhum outro bucket. Nenhum score é calculado para esses produtos.

> **Ponto de atenção para negócio:** Produtos em lançamento recente ou com baixo volume histórico serão sempre classificados como "Sem evidência suficiente", mesmo que a taxa de T&D seja alta. Eles devem ser acompanhados em ciclos futuros.

---

## 3. Os três scores e como são calculados

Todos os scores são **percentis relativos ao portfólio** calculados com a função `CUME_DIST()` do BigQuery. Isso significa que um score de `0.80` não é uma nota absoluta — significa que o produto está melhor (ou pior) do que 80% do portfólio naquela dimensão.

### Score 1 — Dor do cliente (`td_score`)

Mede o quanto o produto concentra problema físico em comparação ao restante do portfólio.

```
td_score = 0,50 × percentil_td_rate
         + 0,30 × percentil_volume_absoluto_de_reversas
         + 0,20 × percentil_delta_vs_categoria
```

| Componente | Peso | O que mede |
|---|---|---|
| `percentil_td_rate` | 50% | % de itens devolvidos sobre itens vendidos — taxa bruta |
| `percentil_volume_td` | 30% | Volume absoluto de reversas — impacto operacional |
| `percentil_delta_vs_categoria` | 20% | Quanto a taxa do produto supera a média da sua categoria |

**Interpretação:** Um `td_score = 0.70` significa que o produto tem mais dor de cliente que 70% do portfólio.

---

### Score 2 — Tração Comercial + Margem (`commercial_score_v2`)

Mede a força de venda e a qualidade econômica do produto em dois blocos:

```
commercial_score_v2 = 0,70 × tracao_vendas_score
                    + 0,30 × mc3_score
```

#### Bloco 1 — Velocidade comercial (`tracao_vendas_score`, 70%)

```
tracao_vendas_score = 0,30 × percentil_sell_through_30d
                    + 0,30 × percentil_sell_through_60d
                    + 0,25 × percentil_sell_through_90d
                    + 0,15 × percentil_receita_media_mensal_vs_categoria
```

| Componente | Peso | O que mede |
|---|---|---|
| `percentil_sell_through_30d` | 30% | Percentual do estoque inicial vendido em 30 dias após lançamento |
| `percentil_sell_through_60d` | 30% | Percentual do estoque inicial vendido em 60 dias |
| `percentil_sell_through_90d` | 25% | Percentual do estoque inicial vendido em 90 dias |
| `percentil_receita_media_mensal_vs_categoria` | 15% | Receita média mensal do produto ÷ média da categoria (CUME_DIST por categoria) |

**Fallback de sell-through:** quando dados de estoque/SKU não estão disponíveis (~7/126 produtos), o componente `tracao_vendas_score` é substituído pelo score legado: `0,60 × percentil_receita + 0,40 × percentil_unidades`.

#### Bloco 2 — Qualidade econômica via MC3 (`mc3_score`, 30%)

```
mc3_score = 0,50 × mc3_vs_categoria_score
           + 0,30 × mc3_vs_portfolio_score
           + 0,20 × representatividade_mc3_score
```

| Componente | Peso | O que mede | Escala |
|---|---|---|---|
| `mc3_vs_categoria_score` | 50% | MC3 do produto vs top quartil da sua categoria | Discreta: 1,00 / 0,75 / 0,50 / 0,25 |
| `mc3_vs_portfolio_score` | 30% | MC3 do produto vs mediana do portfólio | Discreta: 1,00 / 0,75 / 0,50 / 0,25 |
| `representatividade_mc3_score` | 20% | Peso absoluto do produto em contribuição MC3 | CUME_DIST sobre `net_profit_after_marketing_costs` |

Fonte: `sop_bronze.eval_produto_portfolio` — cobertura de ~126 de 287 produtos.

**Fallback de MC3:** quando `mc3_ratio` é NULL (produto não está em `eval_produto_portfolio`), o `commercial_score_v2` é calculado usando apenas `tracao_vendas_score` com peso total (1,00).

**Interpretação:** Um `commercial_score_v2 = 0.60` significa que o produto tem velocidade comercial e contribuição econômica melhores que 60% do portfólio.

---

### Score 3 — Prioridade global (`priority_score`)

Combina dor de cliente e tração comercial com pesos iguais.

```
priority_score = 0,50 × td_score + 0,50 × commercial_score_v2
```

A lógica é: **o produto que mais machuca o cliente E tem relevância comercial/econômica deve ser atacado primeiro**.

---

## 4. O farol de priorização — 5 buckets

O `sinal_priorizacao` classifica cada produto em um dos cinco buckets abaixo. As condições são verificadas **em ordem** (ver seção 5).

| Cor | Sinal | Interpretação |
|---|---|---|
| 🔴 | **Priorizar melhoria** | Alta dor de cliente + alta tração comercial → ação imediata justificada |
| 🟡 | **Alerta em produto relevante** | Produto comercialmente relevante com T&D em escalada → monitorar de perto |
| 🟠 | **Monitorar** | Alta dor, mas tração insuficiente para priorizar investimento agora |
| ⚪ | **Não priorizar agora** | Sem urgência — dor baixa ou produto pequeno |
| ⬜ | **Sem evidência suficiente** | Volume insuficiente para diagnóstico confiável |

### Critérios exatos de cada bucket

| Sinal | Critério exato |
|---|---|
| **Sem evidência suficiente** | `itens_vendidos < 30` **OU** `itens_retornados < 5` |
| **Priorizar melhoria** | `td_score ≥ 0,70` **E** `commercial_score_v2 ≥ 0,60` |
| **Monitorar** | `td_score ≥ 0,70` **E** `commercial_score_v2 < 0,60` |
| **Alerta em produto relevante** | `commercial_score_v2 ≥ 0,70` **E** `td_score ≥ 0,50` **E** `td_score < 0,70` |
| **Não priorizar agora** | `td_score < 0,50` **OU** `commercial_score_v2 < 0,40` |
| **Monitorar** *(zona cinza)* | Todos os demais casos não enquadrados acima |

> ⚠️ **Atenção:** O bucket "Monitorar" é usado em **duas situações distintas**: (a) produto com td_score alto mas sem tração; e (b) zona cinza com scores intermediários que não se encaixam em nenhuma regra. Os scores brutos são necessários para distingui-los.

---

## 5. Ordem de avaliação das regras (importante)

As regras são avaliadas **sequencialmente** como um if-else encadeado. O produto cai no **primeiro bucket cuja condição for satisfeita** e não é reavaliado pelos seguintes.

```
┌─────────────────────────────────────────────────┐
│  1. Vendas < 30 OU retornos < 5?                │
│     SIM → "Sem evidência suficiente" (para aqui)│
└────────────────────────┬────────────────────────┘
                         │ NÃO
                         ▼
┌─────────────────────────────────────────────────┐
│  2. td_score ≥ 0,70 E commercial_score_v2 ≥ 0,60? │
│     SIM → "Priorizar melhoria" (para aqui)      │
└────────────────────────┬────────────────────────┘
                         │ NÃO
                         ▼
┌─────────────────────────────────────────────────┐
│  3. td_score ≥ 0,70 E commercial_score_v2 < 0,60? │
│     SIM → "Monitorar" (para aqui)               │
└────────────────────────┬────────────────────────┘
                         │ NÃO
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│  4. commercial_score_v2 ≥ 0,70 E td_score ≥ 0,50 E td_score < 0,70?│
│     SIM → "Alerta em produto relevante" (para aqui)              │
└────────────────────────┬─────────────────────────────────────────┘
                         │ NÃO
                         ▼
┌─────────────────────────────────────────────────┐
│  5. td_score < 0,50 OU commercial_score_v2 < 0,40? │
│     SIM → "Não priorizar agora" (para aqui)     │
└────────────────────────┬────────────────────────┘
                         │ NÃO
                         ▼
              "Monitorar" (zona cinza — padrão)
```

> **Por que a ordem importa?** Um produto com `td_score = 0.71` e `commercial_score_v2 = 0.65` satisfaz a regra 2 ("Priorizar melhoria") e seria descartado pelas regras seguintes. Se a ordem fosse diferente, o mesmo produto poderia cair em outro bucket.

---

## 6. Decisão estratégica — kill or keep

O `sinal_kill_keep` é uma segunda camada de decisão, calculada em Python **apenas para produtos com evidência suficiente**. Responde a pergunta: **"Vale investir em melhoria ou é melhor descontinuar?"**

| Condição | `sinal_kill_keep` | Decisão de negócio |
|---|---|---|
| `td_score ≥ 0,70` E `commercial_score_v2 ≥ 0,60` | **Priorizar melhoria** | Alta dor + alta tração → investir na correção |
| `td_score ≥ 0,70` E `commercial_score_v2 < 0,60` | **Não Priorizar (Avaliar Descontinuação/Reformulação)** | Alta dor + baixa tração → avaliar saída do portfólio |
| Demais casos | **Não priorizar agora** | Sem urgência de decisão estrutural |

**Matriz de decisão completa:**

|  | **Alta Tração** (`commercial_score_v2 ≥ 0,60`) | **Baixa Tração** (`commercial_score_v2 < 0,60`) |
|---|---|---|
| **Alta T&D** (`td_score ≥ 0,70`) | 🔴 Investir em melhoria | 🟠 Avaliar descontinuação / reformulação |
| **T&D moderada** (`0,50 ≤ td_score < 0,70`) | 🟡 Alerta — monitorar | ⚪ Não priorizar |
| **Baixa T&D** (`td_score < 0,50`) | ⚪ Não priorizar | ⚪ Não priorizar |

---

## 7. Classificação do tipo de problema

As reversas de cada produto são separadas em três tipos:

| Tipo | Como é calculado | Responsável |
|---|---|---|
| **Físico** | Campo `qt_reversas_fisico` — pré-calculado e tagueado no SQL | Time PF |
| **Logístico** | Campo `qt_reversas_logistico` — pré-calculado e tagueado no SQL | Logística |
| **Outros / Desistência** | `total_reversas − qt_reversas_fisico − qt_reversas_logistico` | — |

> **Regra de negócio:** O campo "Outros / Desistência" é **inferido por subtração** — não existe uma tag explícita para ele. Qualquer reversa que não foi classificada como Físico ou Logístico cai nessa categoria. Isso inclui arrependimentos de compra, presentes devolvidos e problemas de tamanho sem tag específica.

---

## 8. Mapeamento de tags para tipo de ação

Quando o sistema identifica o principal motivo de retorno de um produto, ele mapeia automaticamente para um **tipo de ação recomendada** para o time PF:

### Tags de problema físico → ação recomendada

| Tipo de ação | Tags que levam a esse tipo |
|---|---|
| **Modelagem** | `caimento_ruim`, `modelagem_ruim`, `sustentacao_ruim` |
| **Grade** | `comprimento_curto`, `comprimento_longo`, `tamanho_grande`, `tamanho_pequeno`, `tamanho_pepequeno` |
| **Tecido** | `tecido_fino`, `tecido_grosso`, `tecido_transparente`, `tecido_qualidade_ruim`, `tecido_marca_corpo`, `tecido_quente`, `tecido_amassa`, `pilling_bolinhas`, `encolhimento` |
| **PDP / Comunicação** | `cor_diferente_site` |
| **Investigação adicional** | `defeito_costura`, `defeito_aviamento`, `defeito_fio_puxado`, `defeito_furo_rasgo`, `defeito_gola`, `defeito_mancha`, `conforto_negativo` |

### Tags que indicam que o problema não é do time PF

| Situação | Tags | Tipo de ação |
|---|---|---|
| Tags positivas dominando | `caimento_bom`, `conforto_positivo`, `feedback_positivo_geral`, `modelagem_boa`, `tamanho_ideal`, `tecido_qualidade_boa` | **Investigar tags negativas** ⚠️ |
| Tags de logística | `atendimento_ineficiente`, `logistica_adiantamento`, `logistica_atraso`, `logistica_embalagem`, `logistica_item_errado`, `logistica_item_faltando`, `provador_virtual_impreciso` | **Logístico — fora do escopo PF** |

> **Regra de tag positiva:** Se a tag mais frequente do produto for uma tag positiva, o sistema **não gera diagnóstico de ofensores**. Em vez disso, exibe um aviso: *"⚠️ Tags predominantes são positivas. Recomenda-se investigar as tags negativas para diagnóstico completo."*
>
> **Regra padrão:** Se uma tag não está mapeada em nenhuma das categorias acima, o tipo de ação é **"Investigação adicional"**.

---

## 9. Geração automática do tweet analítico

O campo `tweet_analitico` (máximo 80 palavras) é gerado automaticamente por regras heurísticas — **sem uso de IA generativa**. A lógica é a seguinte, na ordem em que os blocos são montados:

### Bloco 1 — Ofensores principais (ou aviso de tag positiva)

- **Se** top-1 tag é positiva → exibe apenas o aviso sobre tags positivas
- **Caso contrário** → lista top 3 tags com seus percentuais: `caimento_ruim (45%), tamanho_grande (23%), tecido_fino (15%)`

### Bloco 2 — Concentração por cor (condicional)

- **Regra:** Só aparece se **≥ 50%** das reversas do produto estão concentradas em uma única cor
- Exemplo: *"Cor crítica: Preto (62% das reversas)."*

### Bloco 3 — Concentração por tamanho (condicional)

- **Regra:** Só aparece se **≥ 50%** das reversas do produto estão concentradas em um único tamanho
- Exemplo: *"Tamanho crítico: M (58% das reversas)."*

### Bloco 4 — Comparativo T&D vs categoria (sempre presente)

- Formato: `"T&D: 18,3% produto vs 9,1% categoria (9,2pp acima)."`
- O sinal "acima" ou "abaixo" é determinado automaticamente pelo delta

### Bloco 5 — Alavanca do Top 3 (sempre presente)

- Mostra o percentual das reversas que os 3 principais motivos representam
- Exemplo: *"Top 3 concentra 83% das reversas tagueadas."*

### Bloco 6 — Tendência (condicional)

- **Regra:** Só aparece se a tendência **não** for `"Estável"` nem `"Sem volume para tendência"`
- Exemplo: *"Tendência recente: Aumentou."*

### Bloco 7 — Ação sugerida (sempre presente)

- Gerado a partir do mapeamento da seção 8
- Exemplo: *"Ação sugerida: Modelagem."*

> **Truncamento:** Se o texto resultante ultrapassar 80 palavras, as palavras excedentes são cortadas e substituídas por `…`

---

## 10. Como o scorecard de portfólio entra no output

Os três pilares do scorecard (`score_tracao_comercial`, `score_unit_economics`, `score_satisfacao_marca`) vêm da tabela `eval_produto_portfolio` e são exibidos na aba `relatorio_pf` como **contexto de decisão** — eles **não determinam** o `sinal_priorizacao` nem o `sinal_kill_keep`.

| Pilar | O que mede | Uso no output |
|---|---|---|
| **Tração Comercial** | Se o produto vende bem (score 0–100) | Referência de posicionamento no portfólio |
| **Unit Economics** | Se o produto gera margem (score 0–100) | Apoia a decisão de descontinuação |
| **Satisfação e Marca** | Satisfação de quem ficou com o produto (score 0–100) | Complementa o diagnóstico de dor do cliente |

### Combinações relevantes para decisão

| Combinação observada | Interpretação sugerida |
|---|---|
| Tração alta + Satisfação baixa + T&D alta | Produto vendido por força de marketing, mas estruturalmente problemático — alta urgência |
| Unit Economics baixo + td_score alto | Duplo impacto financeiro (margem apertada + custo de reversa) — candidato prioritário à descontinuação |
| Tração alta + Satisfação alta + T&D alta | Problema provavelmente concentrado em SKU/cor específica — correção cirúrgica |
| Cluster KILL + td_score alto | Descontinuação já prevista estrategicamente, com dor de cliente confirmada |

> ⚠️ **Cobertura parcial:** Aproximadamente 126 dos 287 produtos ativos têm scorecard preenchido. Os demais aparecem com campos em branco na aba `relatorio_pf`, mas o `sinal_priorizacao` e o `sinal_kill_keep` permanecem válidos.

---

## 11. Ordenação dos produtos nos outputs

A ordenação influencia diretamente quais produtos aparecem no topo de cada aba.

| Aba | Critério de ordenação | Lógica |
|---|---|---|
| `farol_executivo` | `priority_score DESC` → `qt_items_returned DESC` → `receita_liquida DESC` | Produto com maior urgência global primeiro |
| `priorizar_melhoria` | `priority_score DESC` → `qt_items_returned DESC` → `td_rate DESC` | Top 50 do bucket mais crítico |
| `benchmark_categoria` | `produtos_priorizar DESC` → `itens_retornados DESC` | Categorias com mais produtos no bucket crítico |
| `resumo_tags` | `produtos_priorizar DESC` → `produtos DESC` | Tags que mais aparecem em produtos críticos |
| `tweets_analiticos` | Bucket (Priorizar melhoria antes de Alerta) → `priority_score DESC` | Ação mais urgente sempre no topo |
| `relatorio_pf` | `priority_score DESC` | Ranking global de prioridade |

---

## 12. Perguntas para validação de negócio

Use as perguntas abaixo para verificar se as regras fazem sentido para o seu contexto:

### Sobre os limiares dos scores

- [ ] O corte de `td_score ≥ 0,70` para "Priorizar melhoria" está correto? Um produto no 70º percentil de dor já deve entrar como urgente?
- [ ] O peso de receita (60%) sobre unidades (40%) no `commercial_score` legado reflete a prioridade da empresa? Ou unidades deveriam pesar mais?
- [ ] O peso 50/50 entre `td_score` e `commercial_score_v2` no `priority_score` faz sentido? Dor do cliente e tração comercial com peso igual equilibra a decisão?

### Sobre o volume mínimo

- [ ] O limiar de **30 vendas e 5 retornos** é adequado para o porte atual do portfólio? Produtos menores ficam de fora, mas alguns podem ser estratégicos.

### Sobre o bucket "Monitorar"

- [ ] Produtos no bucket "Monitorar" têm `td_score ≥ 0,70` mas `commercial_score_v2 < 0,60`. Existe algum produto nesse bucket que deveria ser tratado como urgente mesmo assim? (Ex: cluster Hero em ramp-up com commercial_score_v2 temporariamente baixo)

### Sobre as tags

- [ ] O mapeamento de `defeito_costura` e `defeito_aviamento` para "Investigação adicional" (e não para uma ação direta) está correto? Ou deveriam ter um fluxo de ação definido?
- [ ] A tag `conforto_negativo` foi classificada como ambígua e vai para "Investigação adicional". Isso está de acordo com o entendimento do time PF?

### Sobre o tweet analítico

- [ ] O corte de **≥ 50%** de concentração em cor/tamanho para mencionar no tweet é adequado? Concentrações de 40% já seriam relevantes?
- [ ] A lógica de truncar em 80 palavras pode cortar informações importantes? O limite de 80 palavras é suficiente?

### Sobre o scorecard

- [ ] Os produtos sem scorecard (≈161 de 287) devem ter tratamento diferente? O `sinal_priorizacao` deles é igualmente confiável?

---

## Glossário

| Termo | Definição |
|---|---|
| **T&D** | Trocas e Devoluções — evento de reversa gerado pelo cliente |
| **`td_rate`** | `itens_retornados ÷ itens_vendidos` — taxa bruta de T&D do produto |
| **`delta_vs_categoria`** | Diferença entre a `td_rate` do produto e a média da sua categoria |
| **`td_score`** | Score composto de dor do cliente (0 a 1, onde 1 = pior do portfólio) |
| **`commercial_score_v2`** | Score composto de tração comercial + margem (0 a 1, onde 1 = maior no portfólio) |
| **`priority_score`** | `0,5 × td_score + 0,5 × commercial_score_v2` — ranking global de urgência |
| **`sinal_priorizacao`** | Farol operacional de 5 buckets gerado em SQL |
| **`sinal_kill_keep`** | Decisão estratégica de 2 estados gerada em Python |
| **`pct_top_3_total`** | % das reversas tagueadas que os 3 principais motivos representam |
| **Cluster** | Posicionamento estratégico do produto: Hero · Core · Long tail · KILL · Lancamento · Breakthrough |
| **CUME_DIST** | Função estatística que calcula o percentil relativo de um valor dentro de um conjunto |
