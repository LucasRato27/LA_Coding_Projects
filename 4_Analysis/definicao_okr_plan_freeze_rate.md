# OKR Plan Freeze Rate — Definição para a Planning de Eficiência Produtiva

**Data:** 2026-04-27
**Responsável:** Lucas Alencar Sampaio
**Status:** definição validada com João/Giulia, **pendente alinhamento com Raí** quanto à coorte de cálculo

---

## O que é

Plan Freeze Rate é o indicador que mede **quanto do plano produtivo original sobrevive sem alteração interna até a entrega**. É a métrica de estabilidade do planejamento — uma proxy direta de previsibilidade da cadeia, retrabalho e capacidade de cumprimento dos compromissos assumidos no fechamento do ciclo.

Em uma frase: **se a Insider fecha um plano de produção, quanto desse plano realmente vira execução sem precisar ser reaberto?**

---

## Por que essa métrica

Hoje a área de Eficiência Produtiva acompanha bem o **resultado** da produção (ICP, lead time, atraso). O que não temos é uma métrica que mostra **quanto o plano em si é estável** — antes mesmo de virar produção.

O Plan Freeze Rate fecha esse gap. Permite distinguir três cenários que hoje se confundem:
1. Planejamento estável e supplier confiável → indicador alto.
2. Planejamento estável mas supplier instável → indicador alto, ICP baixo.
3. Planejamento instável (replanejamento constante) → indicador baixo, mesmo se ICP alto.

Sem essa decomposição, qualquer queda de performance acaba sendo atribuída ao supplier por padrão. O Plan Freeze Rate força a conversa sobre **a parcela de instabilidade que é interna**.

---

## Como é calculado

### Fórmula

```
Plan Freeze Rate = 1 − (Volume alterado internamente / Volume original)
```

- **Volume original** = `planned_quantity` por SKU no snapshot de baseline do ciclo (primeiro dia em que nenhuma OP do ciclo está em `pending`).
- **Volume alterado internamente** = volume de SKUs que sofreram pelo menos uma das 3 mudanças internas listadas abaixo.
- Granularidade: por coorte (`cycle_name`), agregada por mês-alvo, e consolidada via ponderação.

### As 3 mudanças que contam como "alteração interna"

| Tipo | Definição | Detecção |
|---|---|---|
| **Cancelamento In Season** | OP cancelada por decisão da Insider (revisão de demanda) | `canceled_production_reason = 'Revisão de Demanda (In Season)'` |
| **Mudança de data planejada** | Insider mexeu em `dt_planned_entry_warehouse` | Comparação baseline vs atual |
| **Mudança de grade** | `planned_quantity` por SKU mudou | Comparação baseline vs atual no nível OP-SKU |

Se um SKU teve mais de um tipo, conta uma única vez no numerador (sem double-counting).

### O que **NÃO** conta como alteração interna

- Cancelamento por motivo externo (qualidade, capacidade do supplier, matéria-prima, etc.) → vai para o acompanhamento externo.
- Mudança de `dt_reviewed` pelo supplier sem que `dt_planned` tenha mudado → vai para o acompanhamento externo.
- Atraso na entrega (`dt_max_entry > dt_reviewed`) → é KR2, não entra aqui.

Essa fronteira é o que garante que a métrica reflete responsabilidade interna, não comportamento de fornecedor.

---

## Como o número consolidado é construído

Cada ciclo (cycle_name) é uma coorte independente. Para chegar a um número único reportável, seguimos 3 passos:

1. **Atribuição de mês-alvo:** cada coorte recebe o mês onde concentra a maior parte do volume planejado no baseline.
2. **Cálculo por coorte:** Plan Freeze Rate de cada coorte individualmente.
3. **Consolidação ponderada por proximidade:** os ciclos abertos próximos pesam mais que os distantes.

### Ponderação dos ciclos abertos

Decisão alinhada com Giulia (1o1 de 24/04):

```
OKR = (35·C04 + 35·C05 + 20·C06 + 10·C07) / 100
```

- **C04 e C05** carregam o peso maior (35% cada): são os ciclos onde a janela de atuação ainda existe e onde o resultado se materializa em produção real no curto prazo.
- **C06** entra com peso 20%: já é influenciável, mas com mais buffer.
- **C07** entra com peso 10%: ainda recém-baselineado, sinal pouco maturado — mais previsão (FM) que medição.

Essa ponderação concentra o foco nos ciclos onde a área tem mais condição de atuar, sem perder visibilidade dos meses mais distantes.

### Tratamento dos ciclos extras

Ciclos extras (alocações fora do calendário base) são naturalmente mais voláteis e não entram no OKR principal. São acompanhados em paralelo com meta separada.

- **KR1a (Base):** meta 85%
- **KR1b (Extras):** meta 70%, acompanhamento sem entrar no peso oficial

---

## Como ler o resultado

### Leitura cumulativa por coorte

O Plan Freeze Rate de uma coorte só pode cair ou ficar estável ao longo do tempo — nunca subir. Toda alteração registrada permanece contada. Isso significa que:

- Coortes recém-baselineadas começam em ~100% e degradam com o tempo.
- Coortes maduras refletem o resultado final do plano.
- Comparação entre coortes precisa considerar a idade — coorte com 5 meses não compara diretamente com coorte de 1 mês.

### Mês corrente como base de cálculo, futuros como previsão

Decisão alinhada com João (1o1 de 23/04): o número reportado oficialmente é o do **mês-alvo corrente**, com os meses futuros mostrados como **FM (forecast/previsão)** no mesmo gráfico.

Isso preserva visibilidade do horizonte sem reportar como "medido" algo que ainda vai mudar muitas vezes.

> **Pendência:** validar com o Raí se essa base de cálculo (mês corrente como referência reportável) está alinhada com a expectativa da liderança. É o último ponto antes de fechar a metodologia.

---

## Acompanhamento externo (fora do OKR, mas dentro da Planning)

Dois reason codes externos são acompanhados em paralelo, com dashboard separado. Não puxam o KR1 para baixo, mas alimentam diagnósticos de cadeia:

- **Cancelamento externo:** qualquer cancelamento que não seja "Revisão de Demanda (In Season)".
- **Mudança de data revisada (dt_reviewed) pelo supplier:** supplier mexeu na data sem a Insider ter mexido junto.

Esse acompanhamento é o que sustenta as conversas com OPMs em MBAR sobre comportamento de fornecedores específicos. **É também o que está mostrando, hoje, padrão claro de cascateamento de revisões pelos principais fornecedores** — assunto que vai virar análise dedicada para o pré-mortem da Black Friday.

---

## O que esperar do indicador na primeira leitura

Os primeiros números vão refletir o estado atual da carteira, não a performance da área no formato novo. Pontos importantes para a leitura inicial:

1. **Ciclos mais maduros (C03, C04) tendem a aparecer com Plan Freeze Rate baixo.** Não é piora — é o reflexo cumulativo de meses de replanejamento.
2. **Parte da queda em C04/C05/C06 vem de Insider sincronizando datas com supplier que mexeu primeiro.** O dado classifica como mudança interna, mas a origem é externa. Estamos trabalhando para decompor essa parcela em uma evolução futura do indicador.
3. **O número absoluto vai ser desconfortável no primeiro report.** Isso é informação — não falha. A meta de 85% é referência de longo prazo; a primeira função do indicador é estabelecer baseline.

---

## Próximos passos

| # | Próximo passo | Quando |
|---|---|---|
| 1 | Validar com o Raí o mês-base de cálculo (mês corrente vs ciclos abertos ponderados) | Esta semana |
| 2 | Apresentar o indicador ponderado (35-35-20-10) na planning de Eficiência Produtiva | Próxima planning |
| 3 | Decompor a parcela de "mudança interna reativa a sinal externo" no notebook | Próximas 2 semanas |
| 4 | Cruzar Plan Freeze Rate com ICP por supplier para entender a relação entre estabilidade do plano e cumprimento da entrega | Plano de evolução do indicador |
| 5 | Levar o gráfico de evolução temporal (deepnote) como leitura recorrente das plannings de início e final de mês | A partir da próxima planning |
