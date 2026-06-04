# Governança de Lead Time Produtivo — Insider Store
### Documentação Executiva e Metodológica

> **Versão de referência:** cohort jan/2026–mai/2026 · 880 OPs analisadas · Última execução: maio/2026

---

## Sumário

1. [TL;DR — Objetivo da governança](#1-tldr--objetivo-da-governança-de-lead-time)
2. [Resumo executivo](#2-resumo-executivo)
3. [Metodologia](#3-metodologia)
4. [Principais insights de negócio](#4-principais-insights-de-negócio)
5. [Como usar esta governança na rotina](#5-como-usar-esta-governança-na-rotina)
6. [Limitações e cuidados de interpretação](#6-limitações-e-cuidados-de-interpretação)

---

## 1. TL;DR — Objetivo da governança de lead time

A **governança de Lead Time Produtivo** mede, de forma sistemática, quanto tempo a Insider leva, na prática, para trazer um produto da origem até o armazém — e compara esse tempo realizado com o prazo teórico cadastrado para cada fornecedor e produto.

### Por que isso importa

O prazo de entrega impacta diretamente o capital de giro, o nível de serviço e a capacidade de planejamento. Quando o prazo cadastrado está errado — muito curto ou muito folgado — o planejamento da coleção, a programação de compras e o cálculo de cobertura de estoque ficam comprometidos. Um prazo subestimado gera falta de produto; um prazo superestimado imobiliza caixa desnecessariamente.

Hoje, a Insider opera com um **prazo padrão de 120 dias** como referência geral. Esse número é um ponto de partida, não um reflexo da realidade de cada fornecedor, produto ou fluxo produtivo. A governança existe justamente para substituir esse padrão único por uma visão granular e baseada em dados.

### O que a governança resolve

- **Prazos cadastrados defasados:** identifica onde o prazo teórico está muito abaixo (gerador de atraso percebido) ou muito acima (gerador de folga falsa) do realizado.
- **Gargalos operacionais reais:** separa atraso genuíno de postergação intencional pelo planejamento, permitindo cobranças e ações cirúrgicas.
- **Risco de concentração:** mapeia produtos com fornecedor único e cruza com performance de entrega.
- **Oportunidades de realocação:** identifica onde diferentes fornecedores produzem o mesmo produto com spreads relevantes de prazo.

### A quem se destina

- **Planejamento de Coleção e PCP:** para calibrar datas de pedido e cobertura de estoque.
- **Sourcing e Relacionamento com Fornecedores:** para embasar conversas de melhoria e decisões de alocação.
- **Operações e Supply Chain:** para priorizar acompanhamento e identificar etapas-gargalo.
- **Produto:** para entender restrições de prazo que afetam lançamentos.
- **Liderança:** para acompanhar a evolução mensal do índice de pontualidade e o estado da governança.

---

## 2. Resumo executivo

> Os números abaixo são extraídos dos outputs da última execução do notebook (jan/2026–mai/2026).

### 2.1 KPIs de Nível 1

| Indicador | Valor | Variação MoM |
|---|---|---|
| **Mediana Geral de Lead Time** | **99 dias** | ▼ 17 dias (melhora) |
| Mediana Triangulação | 116 dias | ▲ 2 dias (piora leve) |
| Mediana Produto Acabado | 94 dias | ▼ 22 dias (melhora expressiva) |
| **% dentro do prazo (≤120d)** | **77,0%** | — |
| OPs analisadas | 880 | — |

A mediana geral de 99 dias fica **abaixo do target de 120 dias**, o que, à primeira vista, parece confortável. Contudo, o número é fortemente puxado pelo fluxo de Produto Acabado (PA), que tem mediana de 94 dias. Triangulação permanece mais próxima do limite, com 116 dias — e apresentou leve piora no último mês.

### 2.2 Perfil do universo analisado

- **880 OPs** passaram pelos filtros metodológicos no cohort jan–mai/2026.
- **707 OPs de Produto Acabado (PA)** · **173 OPs de Triangulação (Tri)**.
- **43 fornecedores ativos** e **144 produtos distintos** cobertos.
- Cohort baseado na data planejada de entrada no armazém.

### 2.3 Postergações intencionais

| Indicador | Valor |
|---|---|
| OPs com postergação intencional | 393 (44,7%) |
| Mediana de dias postergados (somente OPs postergadas) | 35 dias |
| Lead time bruto médio | 117,2 dias |
| Lead time ajustado médio | 94,9 dias |

Quase **metade das OPs** sofreu alguma postergação intencional da data planejada de entrega. A mediana de 35 dias por OP postergada é significativa: quando removida essa postergação, o lead time médio cai de 117 para 95 dias. Isso indica que **parte relevante do atraso percebido tem origem no planejamento, não na execução produtiva**.

### 2.4 Gargalos por etapa produtiva

As etapas que mais concentraram postergações nas OPs (com base na etapa inflada identificada):

| Etapa | OPs postergadas com inflação nessa etapa |
|---|---|
| Corte Executado | 64 |
| Costura | 61 |
| Validação → Corte | 54 |
| Agendamento → Valid. Matéria-Prima | 38 |
| Faturamento → Entrada em Estoque | 33 |
| Inspeção | 12 |
| Indeterminado (nenhuma etapa inflada) | 131 |

As etapas de **corte e costura** concentram a maior parte das postergações materializadas em atraso de execução.

### 2.5 Matéria-prima e Triangulação

- A base de mapeamento de tecidos cobriu **82,3%** dos produtos.
- A **mediana do tempo total de produção na malharia** (tingimento + produção) é de **85 dias**.
- Em **90,2%** das OPs de Triangulação, o tempo real de malharia foi incorporado ao prazo teórico para comparação justa.
- Em **17 OPs de Triangulação** (9,8%), foi aplicado o fallback metodológico de 60 dias.

### 2.6 Recomendação de prazo cadastrado

A análise de recomendação analisou **85 pares fornecedor × produto × fluxo** com amostra suficiente:

| Direção recomendada | Quantidade de pares |
|---|---|
| ▲ Aumentar prazo cadastrado | 33 |
| → Manter prazo cadastrado | 17 |
| ▼ Reduzir prazo cadastrado | 7 |

**A maioria dos pares com amostra suficiente aponta para necessidade de aumento de prazo**, o que sugere que o prazo teórico cadastrado está, em geral, subestimado frente à realidade operacional.

### 2.7 Risco single-source

- **34 pares fornecedor × produto** com histórico de OPs operam em regime single-source (produto sem alternativa de fornecedor cadastrada).
- O nível de atenção é classificado com base na criticidade ABC do produto e no desvio de lead time.

### 2.8 Cobertura das etapas produtivas

- A cobertura das **6 etapas preenchidas simultaneamente** é baixa no agregado: apenas **8,6%** das OPs têm todos os stamps registrados.
- Para PA, a cobertura é de **0%** — nenhuma OP de PA tem as 6 etapas completas no período.
- Para Tri, a cobertura é de **43,9%** — substancialmente melhor, permitindo análise de decomposição para esse fluxo.

> **Limitação importante:** os gráficos de decomposição por etapa dependem da cobertura de stamps. Para PA, a análise de etapas não está disponível neste cohort.

---

## 3. Metodologia

### 3.1 Fontes de dados utilizadas

O notebook combina quatro fontes principais do Data Lake da Insider:

**Base de capacidade de fornecedores e produtos**
Contém o prazo teórico cadastrado por fornecedor × produto, o tipo de fluxo (Produto Acabado ou Triangulação), a classificação ABC do produto, o custo de fabricação, a capacidade mensal máxima e o número de fornecedores que produzem cada produto. É a referência de "prazo prometido" contra a qual o realizado é comparado.

**Histórico de snapshots diários de OPs**
Fonte principal de dados operacionais. Registra, para cada Ordem de Produção, um snapshot diário com seu status produtivo, datas planejadas, quantidades, fornecedor e produto. É a partir dessa tabela que se extraem os stamps de etapa (primeiro dia em que a OP aparece em cada estágio produtivo) e a data real de entrada no armazém.

**Histórico de mudanças de data planejada de entrega**
Reconstruído a partir dos snapshots diários. Cada mudança na data planejada de entrada no armazém é registrada como um evento. Deslocamentos positivos (adiamentos) são identificados como postergações intencionais e somados por OP.

**Base de tecidos e malharias**
Mapeia, por produto, o tecido principal (artigo de malha) e o tempo médio de produção e tingimento na malharia. Esse dado é exclusivamente relevante para o fluxo de Triangulação, onde o lead time teórico precisa ser ajustado para incluir o tempo de matéria-prima.

---

### 3.2 Espaço amostral de OPs considerado

Entram no cálculo apenas OPs que atendem a **todos** os critérios abaixo:

- **Tipo de produção:** somente OPs com tipo "comprometida" (`committed`). Pedidos experimentais, reservas ou simulações são excluídos.
- **Ciclo:** B2B e EPA são removidos — esses ciclos têm dinâmicas diferentes e distorceriam a leitura operacional padrão.
- **Data de entrada no armazém:** apenas OPs com data de entrada já ocorrida até a data de execução do notebook. OPs com data futura (ainda em produção) não entram no cohort.
- **Lead time válido:** OPs com lead time calculado como nulo ou negativo são removidas — geralmente resultado de inconsistência de stamp.
- **Fornecedores ativos:** fornecedores com status de relacionamento encerrado (`terminated`) ou descontinuado (`discontinued`) são excluídos automaticamente pela query.

O cohort é definido pela **data planejada de entrada no armazém**. Isso significa que OPs entram na análise com base em quando estavam programadas para chegar, não pela data de criação ou faturamento.

**Filtros adicionais disponíveis (configuráveis no notebook):**
- Período do cohort: atualmente configurado como jan/2026 → mai/2026.
- Fornecedor específico: para análise focada em um parceiro.
- Produto específico: para análise focada em uma linha.

Alterar esses filtros muda o universo amostral e pode mudar os resultados — incluindo recomendações de prazo.

---

### 3.3 Diferença entre Produto Acabado e Triangulação

Os dois fluxos têm metodologias de cálculo de lead time distintas, porque a natureza de cada um é diferente.

**Produto Acabado (PA)**

No fluxo de PA, o fornecedor recebe o produto já com a matéria-prima (tecido) disponível — seja porque a Insider fornece diretamente, seja porque o tecido já está reservado. Portanto, o lead time realizado começa a contar a partir do momento em que o tecido chega ou é reservado para a OP, o que corresponde ao estágio de **"aguardando chegada de tecido"** (`waiting_fabric_arrival`).

A etapa anterior — criação e agendamento inicial da OP — não entra no lead time de PA porque o tempo de obtenção da matéria-prima não é responsabilidade operacional do fornecedor de confecção nesse fluxo.

**Triangulação (Tri)**

No fluxo de Triangulação, o fornecedor é responsável por obter a matéria-prima junto à malharia antes de iniciar a confecção. Por isso, o lead time realizado começa na **data de criação da OP** — que é o ponto em que a demanda pelo produto é comunicada e o processo de triangulação é iniciado.

O fluxo de Tri inclui, portanto, etapas anteriores ao início da confecção: solicitação à malharia, produção e tingimento do tecido, chegada da matéria-prima ao fornecedor e validação antes do corte. Isso explica por que o lead time mediano de Tri (116 dias) é significativamente maior do que o de PA (94 dias).

---

### 3.4 Etapas incluídas nos lead times teóricos de PA e Triangulação

O lead time teórico cadastrado na base de capacidade representa o compromisso de prazo para cada combinação fornecedor × produto. Esse prazo é a referência com a qual o realizado é comparado.

Para **PA**, o prazo teórico cadastrado cobre as etapas a partir da chegada da matéria-prima até a entrega no armazém — costura, inspeção, faturamento.

Para **Triangulação**, o prazo teórico cadastrado cobre o processo de confecção, mas **originalmente não inclui o tempo de produção e tingimento da malha na malharia**. O notebook corrige essa deficiência somando o tempo de malharia ao prazo teórico antes de fazer a comparação — detalhado na seção 3.5.

O **prazo de referência executiva usado nos KPIs é 120 dias**, que aparece como linha de corte nos gráficos. A proposta central da governança é justamente questionar esse padrão único e substitui-lo por prazos específicos por fornecedor × produto × fluxo.

---

### 3.5 Incremento do lead time de Triangulação com matéria-prima e malharia

O ajuste metodológico de Triangulação funciona da seguinte forma:

1. Para cada produto do portfólio, o notebook identifica o tecido principal (artigo de malha) utilizado.
2. Busca na base de malharias o tempo médio de tingimento e produção para esse tecido.
3. Soma esse tempo ao prazo teórico cadastrado de Triangulação, criando um prazo teórico ajustado.
4. Quando não há mapeamento disponível para um produto (ausência de vínculo produto–tecido–malharia), aplica um **fallback de 60 dias** como estimativa conservadora.
5. Toda a comparação de desvio e recomendação de prazo usa o prazo teórico ajustado, não o original.

**Cobertura do mapeamento na última execução:**
- Base de tecidos: 186 produtos mapeados, com cobertura de **82,3%**.
- Tempo mediano de malharia: **85 dias**.
- Cobertura nas OPs de Tri do cohort: **90,2%** com tempo real; fallback aplicado em **17 OPs**.

Sem esse ajuste, o lead time realizado de Triangulação seria comparado contra um prazo teórico incompleto (sem malharia), gerando desvio positivo artificial e recomendações enviesadas de aumento de prazo.

---

### 3.6 Etapas produtivas consideradas dentro do lead time

O notebook decompõe o lead time em **sete etapas**, calculadas pela diferença em dias entre o primeiro registro de status em cada estágio produtivo:

| Etapa | O que representa |
|---|---|
| **Criação → Agendamento** | Tempo entre a criação da OP e o primeiro agendamento formal. Válida apenas para Tri. |
| **Agendamento → Validação MP** | Tempo entre o agendamento e o momento em que o tecido chega para validação. Engloba o processo de solicitação e entrega da malha. |
| **Validação → Corte** | Tempo entre a validação do tecido e o início do processo de corte. Inclui pré-corte e preparação. |
| **Corte Executado** | Duração do processo de corte propriamente dito. |
| **Costura** | Tempo de costura e montagem das peças. |
| **Inspeção** | Tempo de inspeção de qualidade até o momento de liberação para faturamento. |
| **Faturamento → Entrada em Estoque** | Tempo entre o faturamento e a chegada efetiva no armazém da Insider. |

Cada etapa é calculada como a diferença entre o **primeiro** snapshot diário em que a OP aparece no estágio seguinte e o primeiro snapshot do estágio atual. Isso captura o tempo de espera real em cada fase, não apenas o tempo de execução.

**Atenção:** para Produto Acabado, a etapa **Criação → Agendamento** é desconsiderada no cálculo de decomposição, pois o lead time de PA começa apenas a partir da disponibilidade de matéria-prima.

A cobertura de stamps é o principal limitador dessa análise: somente OPs que tiveram todos os estágios registrados no sistema podem ser decompostas. No cohort atual, a cobertura completa é de 8,6% geral — muito baixa para PA (0%) e razoável para Tri (43,9%).

---

### 3.7 Limpeza de postergações dentro do lead time

**Por que limpar postergações**

Quando o planejamento decide adiar a data de entrega de uma OP — por ajuste de coleção, calendário comercial, priorização ou volume — esse adiamento aumenta artificialmente o lead time realizado. Sem a limpeza, gargalos operacionais reais ficam mascarados pelo ruído do replanning.

**Como a limpeza funciona**

1. O notebook reconstrói, dia a dia, o histórico de mudanças da data planejada de entrada no armazém para cada OP.
2. Somente deslocamentos positivos são considerados postergações — antecipações não são removidas.
3. A soma de todos os dias de postergação intencional é calculada por OP.
4. Esse total é subtraído do lead time bruto realizado.
5. O resultado é clipado para zero — o lead time ajustado nunca fica negativo.
6. Desvio contra prazo teórico e status "dentro do prazo" são recalculados com o lead time ajustado.
7. **Todos os gráficos e análises do notebook usam o lead time ajustado**, não o bruto.

**Como a postergação é alocada em etapas**

Para não contaminar a análise de gargalos operacionais, o notebook identifica em qual etapa a postergação se materializou:

1. Calcula a mediana de duração por etapa para cada combinação fornecedor × produto × fluxo, usando apenas OPs sem postergação como baseline.
2. Para cada OP postergada, compara a duração real de cada etapa com a mediana esperada.
3. A etapa com maior desvio positivo em relação à mediana é identificada como a **etapa inflada**.
4. A postergação é associada a essa etapa nos gráficos de decomposição.

Isso permite distinguir: "essa OP demorou mais em costura porque a fábrica teve problema" vs. "essa OP demorou mais em costura porque o planejamento postergou a entrega e o fornecedor realocou a capacidade".

**Contexto do cohort atual:**
- 44,7% das OPs tiveram pelo menos uma postergação intencional.
- Mediana de 35 dias postergados por OP afetada.
- Lead time médio bruto: 117 dias · Ajustado: 95 dias.
- 131 OPs postergadas não tiveram nenhuma etapa com desvio positivo identificável (postergação sem impacto operacional rastreável).

---

### 3.8 Metodologia de recomendação de lead time cadastrado

**Granularidade:** fornecedor × produto × fluxo (PA ou Tri).

**Como a recomendação é calculada:**

1. O notebook usa uma **janela longa de 12 meses** como base principal — suficiente para suavizar sazonalidades.
2. Também calcula a mesma recomendação com uma **janela curta de 3 meses** — para capturar tendência recente.
3. O prazo recomendado é o **percentil 75 do lead time realizado ajustado** na janela longa.
4. O resultado é arredondado para o **múltiplo de 5 dias** mais próximo.
5. Apenas pares com **mínimo de 3 OPs** são considerados (limite mais permissivo, dada a granularidade por produto).
6. A recomendação classifica a direção como: **▲ Aumentar**, **→ Manter** ou **▼ Reduzir**, com base na diferença entre o prazo recomendado e o prazo teórico cadastrado. Diferenças dentro de ±10 dias são tratadas como "Manter".
7. A **confiança** da recomendação é classificada em Alta (≥30 OPs), Média (10–29 OPs) ou Baixa (<10 OPs).
8. A **tendência recente** é derivada da diferença entre a janela curta e a longa: se a janela curta recomenda prazo menor (≥5 dias), o indicador é "Melhorando"; se recomenda prazo maior, "Piorando".

**Racional de negócio para o P75:**
Usar a mediana (P50) seria muito agressivo — metade das OPs chegaria atrasada. Usar P90 seria muito conservador — implobilizaria capital e atrasaria desnecessariamente o planejamento. O P75 entrega um prazo que cobre 75% das OPs históricas, equilibrando confiabilidade e eficiência operacional.

---

### 3.9 Metodologia de oportunidades de realocação

O notebook identifica situações em que **dois ou mais fornecedores produzem o mesmo produto, no mesmo fluxo e na mesma faixa de volume**, e calcula o spread de lead time entre eles.

**Lógica do cálculo:**

1. Agrupa OPs por produto × faixa de volume × fluxo × fornecedor.
2. Exige um mínimo de 3 OPs por célula para considerar o dado confiável.
3. Remove grupos com apenas um fornecedor.
4. Identifica o fornecedor mais rápido (menor lead time mediano) e o mais lento.
5. Calcula o spread: lead time do mais lento − lead time do mais rápido.
6. Ordena os top 20 maiores spreads como oportunidades prioritárias.
7. Um heatmap complementar exibe o spread por produto × faixa de volume, separado por fluxo.

**Como interpretar:** um spread grande indica que, para o mesmo produto e volume, fornecedores diferentes estão entregando com prazo muito diferente. Isso não é automaticamente uma recomendação de migração — pode haver diferenças de qualidade, custo, capacidade, localização ou mix de produto. É uma **lista priorizada para investigação**.

---

### 3.10 Buckets de volume

O notebook segmenta todas as OPs em seis faixas de volume planejado (em peças):

| Faixa | Representação |
|---|---|
| < 200 peças | Pequenos pedidos, desenvolvimento ou capsula |
| 200 – 499 | Pedidos médios-baixos |
| 500 – 999 | Pedidos médios |
| 1.000 – 1.999 | Pedidos médios-altos |
| 2.000 – 4.999 | Pedidos altos |
| 5.000+ | Grandes pedidos |

**Por que segmentar por volume:**
Lead time varia com escala. Uma fábrica pode entregar 200 peças em 60 dias e precisar de 150 dias para 5.000 peças do mesmo produto, pelo tempo de setup, capacidade de máquinas, disponibilidade de matéria-prima em volume e priorização interna. Comparar lead times sem separar por volume pode gerar conclusões enganosas.

---

### 3.11 Risco single-source

O notebook identifica **produtos sem alternativa de fornecedor cadastrada** e cruza essa informação com a performance de entrega:

1. Filtra na base de capacidade os produtos com apenas um fornecedor ativo.
2. Enriquece com lead time realizado, desvio contra prazo teórico e percentual dentro do prazo, a partir do histórico de OPs.
3. Classifica o **nível de atenção**:
   - 🔴 **Alto:** produto A ou B com desvio mediano acima de 30 dias.
   - 🟠 **Moderado:** produto A ou B com desvio entre 10 e 30 dias.
   - 🟡 **Acompanhar:** produto A ou B sem desvio crítico.
   - 🟢 **Baixo:** produto C ou sem tag ABC definida.
4. Uma matriz de dispersão (scatter) posiciona cada par fornecedor × produto pelo número de OPs (eixo x) e pelo desvio mediano (eixo y), colorindo pelo percentual dentro do prazo.

**Interpretação de negócio:** produtos A/B com fornecedor único e alto desvio combinam dois riscos — relevância comercial crítica com zero redundância de fornecimento. São os casos que merecem prioridade para desenvolvimento de alternativas de sourcing.

Na última execução, foram identificados **34 pares fornecedor × produto** com histórico de OPs em regime single-source.

---

## 4. Principais insights de negócio

> Esta seção aplica o framework de storytelling de dados: do que mais importa para a liderança até onde agir primeiro.

---

### O que os dados dizem, no essencial

**Situação atual (What Is):** Mediana geral de 99 dias, 77% dentro do prazo de 120 dias. O número parece confortável, mas esconde duas realidades muito diferentes: PA em 94 dias (dentro do limite com folga) e Triangulação em 116 dias (praticamente no limite). E o lead time ajustado de 95 dias médio foi alcançado com 45% das OPs postergadas — ou seja, o desempenho operacional real, sem replanning, está mais próximo dos 117 dias brutos.

**Potencial (What Could Be):** Com a limpeza de postergações e a revisão dos prazos cadastrados, a Insider pode ter um sistema de planejamento mais honesto e um nível de serviço mais previsível — sem necessariamente produzir mais rápido, mas planejando com prazos corretos.

**Tensão central:** Quase metade das OPs é postergada, com mediana de 35 dias de adiamento. Isso não é só um sinal de que o planejamento é ativo — é um sinal de que os prazos cadastrados podem estar errados e de que o replanning frequente está mascarando problemas estruturais de capacidade e execução.

**O que fazer agora:** Revisar os 33 pares onde o prazo cadastrado precisa aumentar; investigar os gargalos de corte e costura; e iniciar conversas com Sourcing sobre os casos single-source de maior risco.

---

### 4.1 Execução operacional

As etapas de **corte executado** (64 OPs com inflação) e **costura** (61 OPs com inflação) são as maiores fontes de materialização de atrasos identificados. São etapas de capacidade física — dependem de máquinas, operadores e setup de fábrica.

A etapa **validação → corte** (54 OPs) também aparece com frequência. Esse estágio envolve a conferência do tecido recebido e a preparação para o processo de corte. Problemas nessa etapa geralmente indicam divergências de qualidade de matéria-prima, retrabalho ou espera por aprovação.

**Implicações:**
- Fornecedores com atraso crônico em costura podem estar operando no limite de capacidade — especialmente em meses de alta sazonalidade.
- Gargalos em validação → corte podem indicar problemas com o fornecedor de tecido, não com o confeccionista.
- O número expressivo de OPs com etapa "indeterminada" (131) pode refletir baixa qualidade de registro de status no sistema — o que por si só é um problema a resolver.

**Ação prioritária:** levantar, para os fornecedores com maior frequência de inflação em costura e corte, se o problema é capacidade, mix de produto ou sazonalidade. Essa análise deve ser feita olhando para a tabela detalhada por fornecedor × produto × fluxo.

---

### 4.2 Repriorização e replanning a partir de postergações

O cenário de postergações é o dado mais estratégico desta governança.

**O que os dados mostram:**
- 44,7% das OPs foram postergadas.
- Mediana de 35 dias adiados por OP afetada.
- O lead time bruto médio é 117 dias — o ajustado cai para 95 dias ao remover as postergações.

**O que isso significa:**
Há dois tipos de postergação na prática. O primeiro é uma decisão legítima: o planning decidiu atrasar uma entrega porque o produto ainda não tem demanda confirmada, ou porque há prioridade em outro lote. O segundo é um sintoma: postergações que ocorrem porque o prazo cadastrado estava curto e a OP nunca seria entregue no prazo original. A governança, sozinha, não separa os dois — mas a análise de etapa inflada ajuda a identificar onde a postergação virou atraso operacional real.

**Como usar:**
- Em rituais de replanning (semanal ou quinzenal), acompanhar OPs com postergação acumulada acima de X dias e investigar causa raiz.
- Separar postergações originadas em PCP/Planejamento das originadas em produção.
- OPs que foram postergadas e ainda estão com desvio alto são as de maior risco de chegar atrasadas mesmo após o adiamento.

---

### 4.3 Matéria-prima e malharias

Para o fluxo de Triangulação, o tempo de malharia é **estrutural** — a mediana de 85 dias representa quase todo o prazo de lead time do fluxo. Isso significa que, em Tri, a confecção precisa ser iniciada antes mesmo de o tecido ser produzido, ou o prazo total estará comprometido desde o início.

**Pontos de atenção:**
- Produtos sem mapeamento de tecido (17,7% da base de produtos) recebem o fallback de 60 dias, que pode estar superestimando ou subestimando o tempo real. Ampliar o mapeamento reduz esse ruído.
- Produtos com malharias únicas (análogo ao risco single-source, mas para matéria-prima) criam gargalos invisíveis: se a malharia atrasa, todas as OPs de Triangulação daquele tecido atrasam juntas.
- As etapas de "agendamento → validação de matéria-prima" infladas em 38 OPs podem indicar problemas com malharias específicas ou tecidos com lead time mais longo que o esperado.

**Oportunidades:**
- Identificar tecidos de uso transversal em Triangulação e negociar reserva de capacidade antecipada na malharia.
- Usar o tempo de 85 dias de malharia como insumo para o calendário de programação de pedidos — não como variável de ajuste, mas como restrição física a planejar em torno.

---

### 4.4 Fornecedores detratores

O notebook não armazenou um ranking nominal de fornecedores detratores nos outputs disponíveis nesta execução — os gráficos de decomposição e heatmap geram a classificação dinamicamente. A leitura abaixo é metodológica, indicando como identificar os detratores na execução do notebook.

**Como identificar:**
- Na **tabela detalhada de etapas** (fornecedor × produto × fluxo), ordenar por `lt_total` decrescente identifica os pares com maior lead time mediano.
- No **heatmap Fornecedor × Faixa de Volume**, células vermelhas (próximas de 210 dias) indicam combinações críticas de fornecedor e volume.
- Na **tabela de recomendação**, fornecedores com múltiplas linhas marcadas como "▲ Aumentar" e com confiança alta são detratores sistemáticos.
- Nos **top spreads de realocação**, o fornecedor mais lento de cada grupo é um candidato a investigação.

**Cuidados na leitura:**
- Um fornecedor com lead time alto pode estar sendo penalizado por um mix de produtos mais complexos.
- Volume pequeno de OPs reduz a confiança da conclusão.
- Fornecedores que aparecem como lentos em Triangulação podem, na realidade, estar sendo penalizados por problemas na malharia, não na confecção.

---

### 4.5 Revisão de prazos cadastrados

Este é o output de maior impacto imediato da governança.

**O que a tabela de recomendação entrega:**
Para cada par fornecedor × produto × fluxo com amostra suficiente, a tabela mostra:
- Lead time mediano realizado (P50) e recomendado (P75).
- Prazo teórico atualmente cadastrado.
- Diferença em dias (delta vs. cadastrado).
- Direção: aumentar, manter ou reduzir.
- Confiança: alta, média ou baixa.
- Tendência recente (janela curta vs. longa): melhorando, piorando ou estável.

**O que os dados desta execução mostram:**
- **33 pares precisam de aumento de prazo** — são os mais urgentes, pois um prazo subestimado contamina o planejamento de coleção, o calculo de cobertura e a expectativa da equipe de produto.
- **7 pares podem ter prazo reduzido** — onde o realizado está consistentemente abaixo do cadastrado. Reduzir esses prazos libera antecipação de recebimento e melhora o giro.
- **17 pares estão adequados** — manter sem ação.

**Priorização para revisão:**
1. Pares com confiança Alta (≥30 OPs) e direção "Aumentar" — maior certeza, maior impacto.
2. Pares com tendência "Piorando" (janela curta pior que a longa) — requerem ação imediata.
3. Pares com confiança Baixa — aguardar mais dados antes de revisar o cadastro.

---

### 4.6 Oportunidades de realocação

O notebook identificou grupos de produtos onde diferentes fornecedores operam no mesmo segmento de volume com spreads relevantes de lead time. A tabela e o gráfico de barras (top 20 por spread) são os outputs mais acionáveis para Sourcing e Planejamento.

**Como interpretar cada oportunidade:**

Para cada linha do top de spreads, a leitura é:
- **Produto e fluxo:** define o contexto produtivo.
- **Faixa de volume:** garante que a comparação é justa (mesmo escala).
- **Fornecedor mais rápido e mais lento:** identifica os extremos do spread.
- **Spread em dias:** o ganho potencial de lead time se toda a demanda fosse migrada para o fornecedor mais rápido.

**O que fazer com essa lista:**
Não migre fornecedores baseado apenas no spread. Antes de qualquer realocação, valide:
- Capacidade do fornecedor mais rápido para absorver o volume adicional.
- Qualidade consistente — o fornecedor mais rápido pode ter mais reprove de inspeção.
- Custo de fabricação — lead time menor pode vir com custo maior.
- Risco de concentração — migrar demanda para um fornecedor pode criar um novo single-source.
- Relacionamento comercial — ruptura com um fornecedor lento pode fechar uma parceria relevante.

Trate cada oportunidade como **hipótese priorizada**, não como decisão tomada.

---

## 5. Como usar esta governança na rotina

### Ritual mensal (recomendado)

| Atividade | Responsável | Frequência | Output esperado |
|---|---|---|---|
| Atualizar e executar o notebook | Analista Supply Chain / BI | Mensal | KPIs atualizados, alertas de variação |
| Revisar top 5 fornecedores com maior lead time e desvio | Gestão de Fornecedores | Mensal | Lista de ações de cobrança e suporte |
| Acompanhar postergações acima de 30 dias | PCP / Planejamento | Mensal | Causa raiz e plano de mitigação |
| Revisar produtos single-source de nível Alto e Moderado | Sourcing | Mensal | Plano de desenvolvimento de alternativas |

### Ritual trimestral (recomendado)

| Atividade | Responsável | Frequência | Output esperado |
|---|---|---|---|
| Revisar tabela de recomendação de prazo | Planejamento + Sourcing | Trimestral | Atualização do cadastro de prazos |
| Validar oportunidades de realocação com Sourcing e Qualidade | Sourcing + Qualidade | Trimestral | Priorização de realocações para avaliação |
| Comitê de exceções para single-source críticos | Liderança SC + Produto | Trimestral | Decisão sobre desenvolvimento de alternativas |

### Rituais pontuais (conforme necessidade)

- **Após cada ciclo ou coleção:** atualizar o cohort e comparar o desempenho do ciclo encerrado.
- **Antes de fechar um novo pedido com fornecedor novo ou inativo:** verificar histórico de lead time na base.
- **Antes de revisar o calendário de lançamento:** consultar os prazos recomendados para os produtos da coleção planejada.

### Como usar o notebook como ferramenta de investigação

O notebook permite filtrar por fornecedor e produto específicos. Para uma reunião com um fornecedor, por exemplo:
1. Configurar `supplier_filtro` com o nome do fornecedor.
2. Reexecutar a partir da Célula 1.5 (filtros do cohort).
3. Todos os gráficos passam a refletir apenas aquele fornecedor: evolução mensal, decomposição de etapas, recomendação de prazo, heatmap de volume.

---

## 6. Limitações e cuidados de interpretação

### Qualidade de stamps

A análise de decomposição por etapa depende integralmente da qualidade e regularidade do registro de status produtivo no sistema operacional. Se a equipe não atualiza o status com frequência, os stamps ficam defasados e a etapa calculada não representa a realidade. No cohort atual, apenas 8,6% das OPs têm todos os stamps preenchidos — o que limita severamente a análise de etapas para PA.

### Amostras pequenas

Combinações com poucas OPs (< 10) geram conclusões estatisticamente frágeis. A confiança "Baixa" na tabela de recomendação é um sinal explícito de que o prazo não deve ser revisado com base naquele dado. Fornecedores novos ou produtos de baixo volume podem ter poucos dados históricos.

### Lead time ajustado vs. bruto

O lead time ajustado remove postergações intencionais — isso é metodologicamente correto para medir eficiência operacional, mas significa que o número exibido nos KPIs **não é o lead time que o planejamento está usando no dia a dia**. Para planejamento de coleção com data real, o lead time bruto (ou a soma do ajustado com os dias de postergação típicos) pode ser mais relevante.

### Fallback de malharia em Triangulação

O fallback de 60 dias aplicado a produtos sem mapeamento de tecido é uma estimativa. Produtos com malharia mais complexa (tingimento especial, fios técnicos) podem ter tempo de produção muito superior. Produtos mais simples podem estar sendo penalizados pelo fallback. Ampliar o mapeamento é a forma de eliminar esse viés.

### Ruído operacional residual

Mesmo após a limpeza de postergações, o lead time ajustado ainda pode conter eventos não capturados: greves, problemas de importação de matéria-prima, interrupções por qualidade. Esses eventos não são identificáveis pelo método atual e podem inflar pontualmente os dados.

### Comparação entre produtos de complexidade diferente

Lead time alto não significa necessariamente fornecedor ruim. Um fornecedor que produz peças técnicas complexas (como produtos com grafeno ou lã merino) naturalmente demora mais do que um que produz camisetas básicas. Qualquer comparação de ranking de fornecedores deve controlar o mix de produto.

### Realocação não é automática

A análise de oportunidades de realocação é uma lista priorizada para investigação — não uma recomendação de migração. Antes de qualquer realocação, é necessário validar capacidade, qualidade, custo, risco de concentração e relacionamento comercial.

### Dependência dos filtros ativos

Alterar o período do cohort, o filtro de fornecedor ou o filtro de produto muda o universo amostral e, portanto, os resultados — incluindo medianas, recomendações e spreads. Resultados de execuções com filtros diferentes não são diretamente comparáveis.

---

*Documento gerado a partir do notebook `lead_time_dashboard.ipynb` — Cohort: jan/2026–mai/2026 · Insider Store · Governança de Supply Chain*
