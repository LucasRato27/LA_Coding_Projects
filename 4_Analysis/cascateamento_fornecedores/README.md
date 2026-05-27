# Diagnóstico: Grau de Antecipação e Cascateamento da Cadeia Produtiva

**Data:** 2026-04-28
**Autor:** Lucas Alencar Sampaio
**Fonte:** snapshots até 2026-04-24, ciclos Base

---

## Resumo executivo

Há falta de visibilidade sobre se a cadeia tem horizonte de longo prazo — se os fornecedores alteram com antecedência ou de forma reativa, se alteram muito ou pouco, e se um problema em um ciclo contamina os ciclos seguintes. Esta análise quantifica esses três eixos e identifica quais fornecedores concentram os problemas.

---

## Diagnóstico do problema atual

Atualmente, há reatividade presente no planejamento da cadeia e pouca visão de longo prazo. Quando existe algum problema — atraso de matéria-prima, gargalo produtivo, reprovação por qualidade — o fornecedor faz a alteração externa (cancelamento ou revisão de data) com pouca antecedência. Esse é um comportamento reativo e pouco voltado ao longo prazo.

Esta análise foi construída para responder três perguntas objetivas:

1. **A cadeia consegue frear altas taxas de alteração ao longo do tempo, ou o problema se perpetua nos ciclos seguintes?** → Cascateamento
2. **A cadeia altera com antecedência razoável ou de forma reativa?** → Grau de antecipação (referência: lead time produtivo médio de 45 dias)
3. **Em média, os fornecedores alteram muito ou pouco do plano alocado?** → Taxa de alteração média

---

## Definições e métricas

> **Nota metodológica:** todas as métricas usam `vol_ext_any` — a soma de cancelamentos externos (`ext_cancel`) e revisões de data revisada pelo supplier (`ext_date_rev`). Cobre todas as alterações externas ao plano original, independentemente do tipo.
>
> Para cada par (supplier, mes_alvo), a referência é o **pico (high-water mark)** do percentual ao longo da vida do ciclo, não o estado final. Isso evita que reversões pontuais ou ajustes de baseline mascarem cascades que efetivamente ocorreram.

### Taxa de alteração média (% EXT médio)

**O que é:** a porcentagem do volume original de um ciclo que teve alteração externa após o baseline — após o plano ter sido alocado.

**Como é calculada:**
```
% EXT_supplier = média ponderada (por volume) do peak pct_ext_any
                 ao longo de todos os mes_alvo do supplier
```

**Como ler:** um supplier com % EXT médio de 84% (BAE) alterou, em média, mais de 4/5 do volume alocado em algum momento após o plano fechado. Quanto maior o número, maior a instabilidade do fornecedor em relação ao plano que foi acordado.

---

### Cascateamento (%)

**O que é:** a probabilidade de que, dado que um ciclo (mes_alvo M) teve taxa de alteração alta, o ciclo seguinte (M+1) também tenha taxa alta. Mede se os problemas de um ciclo se propagam para o próximo.

**Como é calculado:**
```
Cascade = P(peak_M+1 > 30% | peak_M > 30%)
```

Para identificar o par (M, M+1), usa-se uma janela de ±14 dias em torno do início esperado de M+1 — o que torna a medição robusta mesmo quando os ciclos não alinham perfeitamente ao calendário. O threshold de 30% opera como proxy de "ciclo problemático".

**Como ler:** um supplier com cascade de 88% (BAE BRASIL) significa que em quase todos os ciclos onde M foi problemático, M+1 também foi. O problema não ficou contido — se propagou para o mês seguinte na maioria das vezes.

---

### Grau de antecipação (mediana em dias antes do mes_alvo)

**O que é:** com quantos dias de antecedência em relação ao início do mes_alvo o sinal de problema ficou visível — definido como o momento em que `pct_ext_any` cruzou 10%.

**Como é calculado:**
```
dias_antes = data de início do mes_alvo − snapshot_week em que pct_ext_any ≥ 10%

Grau de antecipação = mediana de dias_antes ao longo de todos os ciclos do supplier
```

**Como ler:**
- **Valor positivo alto (ex: 84 dias — RIZLLEP):** o sinal apareceu 84 dias antes do ciclo começar. Há janela de atuação, mesmo se o supplier altera muito.
- **Valor próximo de zero (ex: 7 dias — NOVA FORMULA):** o sinal aparece quando o ciclo já está prestes a começar. Sem tempo de reação.
- **Valor negativo (ex: −4 dias — ARTIGO X):** o sinal chegou depois que o mes_alvo já começou. Informação tardia demais para qualquer ação preventiva.

O lead time produtivo de referência é 45 dias. Todo supplier com grau de antecipação abaixo de 45 dias opera fora da janela de ação produtiva.

---

## 1. A cadeia como um todo

### Taxa de alteração global

**% EXT médio ponderado por volume na carteira: 67%.** Em média, mais de dois terços do volume alocado em ciclos Base sofre algum tipo de alteração externa após o baseline. Não é exceção — é a regra atual da operação.

### Cascateamento global

Considerando todos os pares consecutivos (M, M+1) onde M teve pico > 30%, **o ciclo seguinte também ultrapassou 30% em 73% dos casos** (70 de 96 pares). A correlação entre o pico de um mes_alvo e o do seguinte é r = 0,32.

A propagação de problemas é a regra, não exceção. Quando um ciclo está problemático, há ~3/4 de chance do próximo também ficar.

### Grau de antecipação global

**Mediana global: 31 dias antes do mes_alvo.** Para uma cadeia com lead time produtivo de 45 dias, isso significa que o sinal médio chega depois do ponto de não-retorno da produção. A cadeia opera predominantemente em modo reativo — só consegue ajustar quando já está dentro da janela em que o lead time não permite mais reorganização.

---

## 2. Desagregação por fornecedor

A leitura abaixo identifica os principais detratores por cada um dos três crivos e, em seguida, posiciona-os no quadrante combinado.

### Detratores por taxa de alteração média

Os fornecedores que mais alteram o plano após o baseline, ordenados por volume na carteira:

| Supplier | Vol. total (pcs) | % EXT médio |
|---|---:|---:|
| **BAE BRASIL** | 1.178.608 | **84%** |
| LUTESTIL | 129.126 | **79%** |
| MAURA | 58.245 | **76%** |
| RIZLLEP | 197.419 | 71% |
| ART LIVRE | 307.105 | 68% |
| BY COTTON | 308.826 | 68% |
| PIXIE | 54.006 | 68% |
| NOVA FORMULA | 29.231 | 60% |
| DALOP | 53.961 | 54% |
| FABIO | 79.930 | 45% |
| DDAL | 115.190 | 41% |
| MALHAS D'STEFANO | 244.948 | 19% |
| Lunelli Nordeste | 104.953 | 20% |

BAE BRASIL é o detrator dominante: 84% de taxa sobre 1,18 milhão de peças significa que cerca de 1 milhão de peças do plano original foram alteradas em algum momento. MALHAS D'STEFANO e Lunelli Nordeste são os contraexemplos — alta participação na carteira com taxa baixa.

### Detratores por cascateamento

Suppliers com cascade alto e número relevante de pares avaliados:

| Supplier | Cascade (%) | Pares avaliados |
|---|---:|---:|
| **BAE BRASIL** | **88%** | 8 |
| **RIZLLEP** | **86%** | 7 |
| **ART LIVRE** | **83%** | 6 |
| DALOP | 83% | 6 |
| PIXIE | 83% | 6 |
| MAURA | 71% | 7 |
| BY COTTON | 67% | 6 |
| DDAL | 60% | 5 |
| Lunelli Nordeste | **0%** | 1 |

BAE BRASIL e RIZLLEP encadeiam quase 9 em cada 10 ciclos problemáticos. ART LIVRE, DALOP e PIXIE seguem perto. Quando um destes suppliers tem ciclo ruim, o time precisa antecipar atuação no ciclo seguinte — a probabilidade de contaminação é dominante.

### Detratores por grau de antecipação

Suppliers com sinal mais tardio (mediana abaixo do lead time de 45 dias):

| Supplier | Antecipação mediana (dias) | % ciclos com ≥45d antecipação |
|---|---:|---:|
| ARTIGO X | **−4 d** | 0% |
| MC & MC | **−5 d** | 17% |
| MAC CLEM | 6 d | 0% |
| NOVA FORMULA | 7 d | 33% |
| MALHAS D'STEFANO | 10 d | 17% |
| DALOP | 20 d | 43% |
| CENTRAL DA MODELAGEM | 21 d | 33% |
| AZZURRA | 26 d | 25% |
| PIXIE | 27 d | 29% |
| DDAL | 40 d | 50% |

ARTIGO X e MC & MC têm mediana negativa — o sinal chega depois que o mes_alvo já começou. DALOP, AZZURRA, PIXIE concentram volume relevante operando muito abaixo do lead time produtivo.

No outro extremo, **RIZLLEP** (84d), **MAURA** (58d), **BY COTTON** (54d), **BAE BRASIL** (52d), **ART LIVRE** (52d) e **FABIO** (52d) operam acima do lead time — ou seja, o sinal chega cedo o suficiente para haver janela de ação, mesmo se a taxa de alteração for alta.

### Quadrante: cascateamento × grau de antecipação

A tabela combina os três crivos para os principais suppliers:

| Supplier | Vol. total (pcs) | Cascade (%) | Antecipação (dias) | % EXT médio |
|---|---:|---:|---:|---:|
| **BAE BRASIL** | 1.178.608 | **88%** | 52 d | **84%** |
| BY COTTON | 308.826 | 67% | 54 d | 68% |
| ART LIVRE | 307.105 | **83%** | 52 d | 68% |
| MALHAS D'STEFANO | 244.948 | n/d | **10 d** | **19%** |
| **RIZLLEP** | 197.419 | **86%** | **84 d** | 71% |
| LUTESTIL | 129.126 | **100%** | 33 d | **79%** |
| DDAL | 115.190 | 60% | 40 d | 41% |
| Lunelli Nordeste | 104.953 | **0%** | 66 d | **20%** |
| FABIO | 79.930 | **100%** | 52 d | 45% |
| MAURA | 58.245 | 71% | 58 d | **76%** |
| PIXIE | 54.006 | **83%** | 27 d | 68% |
| DALOP | 53.961 | **83%** | **20 d** | 54% |
| CENTRAL DA MODELAGEM | 34.651 | **100%** | **21 d** | **86%** |
| NOVA FORMULA | 29.231 | 50% | **7 d** | 60% |
| AZZURRA | 14.429 | **100%** | **26 d** | 56% |
| ARTIGO X | 12.840 | 50% | **−4 d** | 47% |

Tabela completa: [quadrante_cascade_previsibilidade.csv](quadrante_cascade_previsibilidade.csv)

Posicionando os principais detratores no quadrante cascade × antecipação:

#### Quadrante 1 — Alto cascade + baixa antecipação (crítico)

> Quando um ciclo vai mal, o seguinte também vai. E o sinal chega tarde demais para reagir.

**DALOP, PIXIE, CENTRAL DA MODELAGEM, AZZURRA**

Cascade ≥ 80% combinado com antecipação ≤ 27 dias. São suppliers que cascateiam de forma quase determinística e cujo sinal aparece dentro da janela de não-retorno. Volume individual menor, mas comportamento estruturalmente problemático.

#### Quadrante 2 — Alto cascade + antecipação razoável (gerenciável apesar de instável)

> Quando um ciclo vai mal, o seguinte também vai — mas há tempo para atuar se houver processo de monitoramento.

**BAE BRASIL, ART LIVRE, RIZLLEP, MAURA, FABIO, BY COTTON**

Cascade alto (67%-88%) com antecipação ≥ 52 dias. Concentra a maior parte do volume estratégico da carteira. RIZLLEP merece destaque: cascade de 86% e taxa média de 71%, mas avisa com 84 dias de antecedência — é instável, mas previsível na sua instabilidade.

#### Quadrante 3 — Baixo cascade + boa antecipação (controlado)

> Problemas pontuais, sem propagação, com tempo para reagir.

**Lunelli Nordeste**

Único supplier com volume relevante (105 mil peças) que combina cascade nulo (0% sobre 1 par) com antecipação de 66 dias e taxa média de 20%. É o benchmark operacional — mostra que a combinação "estável + previsível" existe na cadeia.

#### Quadrante 4 — Sinal tardio + cascade moderado (vigilância)

> Sinal muito próximo ou depois do mes_alvo, sem padrão claro.

**NOVA FORMULA, ARTIGO X, MC & MC**

Antecipação ≤ 7 dias (ARTIGO X e MC & MC chegam a sinalizar depois do mes_alvo começar). Volume individual baixo, mas padrão grave: nenhuma janela operacional para reação.

#### Caso à parte — MALHAS D'STEFANO

Volume alto (245 mil peças) com taxa de alteração baixa (19%), mas antecipação mediana de apenas 10 dias. Não cascateia (poucos pares com M > 30%), mas quando altera, altera tarde. Comportamento misto que merece leitura específica.

---

## 3. Evidência do cascateamento

Os exemplos mais claros de cascade confirmado nos dados (ambos M e M+1 com pico > 30%):

| Supplier | mes_alvo M | Pico M | mes_alvo M+1 | Pico M+1 |
|---|---|---:|---|---:|
| BAE BRASIL | 2025-12 | 88% | 2026-01 | 92% |
| BAE BRASIL | 2026-01 | 92% | 2026-02 | 76% |
| BAE BRASIL | 2026-02 | 76% | 2026-03 | 81% |
| RIZLLEP | 2026-03 | 100% | 2026-04 | 100% |
| RIZLLEP | 2026-04 | 100% | 2026-05 | 100% |
| RIZLLEP | 2026-05 | 100% | 2026-06 | 100% |
| ART LIVRE | 2026-02 | 74% | 2026-03 | 80% |
| BY COTTON | 2025-11 | 61% | 2025-12 | 64% |

BAE BRASIL encadeou **4 ciclos consecutivos** (Dez/Jan/Fev/Mar) com pico ≥ 76%. RIZLLEP encadeou **5 ciclos consecutivos** (Mar/Abr/Mai/Jun/Jul) com pico ≥ 99% — o caso mais extremo de cascade detectado na carteira.

Série completa de pares: [pares_cascade_m_m1.csv](pares_cascade_m_m1.csv)

---

## Arquivos desta análise

| Arquivo | Conteúdo |
|---|---|
| [quadrante_cascade_previsibilidade.csv](quadrante_cascade_previsibilidade.csv) | Supplier × cascade × antecipação × % EXT médio (peak ponderado) |
| [pares_cascade_m_m1.csv](pares_cascade_m_m1.csv) | Todos os pares consecutivos M → M+1 com pico de pct_ext_any |
| [antecedencia_por_supplier.csv](antecedencia_por_supplier.csv) | Distribuição do grau de antecipação por supplier |
| [pareto_supplier_okr_ativo.csv](pareto_supplier_okr_ativo.csv) | Pareto de volume EXT na carteira OKR ativa |
| [delta_semanal_ext_base.csv](delta_semanal_ext_base.csv) | Série semanal de variação da taxa EXT por mes_alvo |
