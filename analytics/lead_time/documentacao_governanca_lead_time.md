# Governança de Lead Time Produtivo — Insider Store

### Documentação Executiva e Metodológica

Esta documentação descreve a metodologia vigente do dashboard `Governança de Lead-Time`. Ela não contém snapshots, números atuais ou conclusões de uma execução específica.

> **Sincronização:** 07/08/2026. Fonte de verdade: notebook Deepnote
> [`lead_time_dashboard`](https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/leadtimedashboard-0b56034555ea4a43a7d80e0ede768923?utm_content=62b1671d-dc82-4747-9bbb-b134a69a3491),
> ID `0b56034555ea4a43a7d80e0ede768923`, no projeto `Governanca-Lead-Time`.
> A última execução verificada foi bem-sucedida em 07/08/2026. A documentação
> descreve regras; valores padrão dos filtros podem mudar em execuções futuras.

## Sumário

1. Objetivo e público
2. Fontes, universo e filtros
3. Definições de datas e eixos temporais
4. Cálculos de lead time, SLA e prazo
5. Postergação intencional
6. Métricas e visualizações
7. Ciclo Produtivo
8. Qualidade, exportação e limitações

## 1. Objetivo e público

O dashboard apoia a governança do tempo de produção das Ordens de Produção (OPs), permitindo acompanhar execução, pontualidade, gargalos, capacidade e necessidade de revisão de prazos.

Destina-se principalmente às equipes de Eficiência Produtiva, Planejamento, Supply Chain, Sourcing e lideranças responsáveis por fornecedores e portfólio.

## 2. Fontes, universo e filtros

### 2.1 Fontes principais

- `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`: histórico diário das OPs, estágios, quantidades, datas planejadas/revisadas e entrada em estoque.
- Tabelas integradas de produtos, fornecedores, células produtivas, custos e matérias-primas: capacidade, lead time cadastrado, composição de produto e risco de fornecimento.

### 2.2 Grão analítico

O grão canônico é uma linha por `op_code`. Antes dos cálculos, snapshots e linhas de SKU são consolidados no nível da OP. As validações impedem duplicidade de `op_code` nas bases executivas.

### 2.3 Tipo de OP — filtro exclusivo

O input obrigatório `production_order_type_filtro` aceita apenas:

- `committed`: OPs definitivas, valor padrão;
- `incubation`: OPs de incubação.

Não existe opção para combinar os dois tipos. Consultas de OPs, postergação, dropdowns, série trimestral, diagnósticos e validações partem do histórico previamente filtrado pelo tipo selecionado. O notebook confirma que todas as linhas resultantes possuem exatamente esse tipo.

Os demais controles são `supplier_filtro`, `product_filtro`,
`threshold_filtro_v2`, `data_inicio_analise` e `data_fim_analise`. A faixa de
SLA aceita somente `SLA < 100 dias`, `SLA ≥ 100 dias` ou `Todos`; o filtro de
produto é invalidado e restaurado para `(Todos)` quando não pertence ao
fornecedor selecionado.

### 2.4 Cohort temporal

`data_inicio_analise` e `data_fim_analise` filtram `dt_largest_entry_warehouse`, a data efetiva de entrega/entrada da OP no estoque.

- início: primeiro dia selecionado, inclusivo;
- fim: mês inteiro selecionado, implementado como `< data_fim + 1 mês`;
- somente OPs com `dt_largest_entry_warehouse <= CURRENT_DATE()`;
- a data planejada original não define a entrada no cohort.

### 2.5 Exclusões estruturais

São removidas OPs canceladas e ciclos B2B/EPA. Fornecedores encerrados/descontinuados e SKUs desativados permanecem como histórico válido. Consumidores dependentes de criação também excluem `created_at` ausente ou cronologicamente posterior ao prazo original ou à entrega.

Os filtros de fornecedor, produto e faixa de SLA são aplicados após a escolha exclusiva do tipo de OP.

Ausência de lead time teórico cadastral não remove a OP das métricas realizadas;
ela limita somente análises que dependem de comparação entre realizado e
teórico, mantendo a falha disponível para auditoria.

## 3. Definições de datas e eixos temporais

Três conceitos temporais coexistem e não devem ser confundidos:

| Conceito | Campo/regra | Uso |
| --- | --- | --- |
| Mês de fechamento/entrega | mês de `dt_largest_entry_warehouse` | cohort, séries mensais realizadas e auditoria mensal |
| Trimestre de entrega | trimestre de `dt_largest_entry_warehouse` | evolução trimestral |
| Mês do Ciclo Base | mês/ano extraídos de `cycle_name` no padrão `CMMYYYY` | eixo X do Ciclo Produtivo Base |
| Mês do Ciclo Extra | mês de `dt_planned_original` | eixo X do Ciclo Produtivo Extra |

Exemplo: `C022025` representa fevereiro de 2025. O Dashboard Geral usa entrega para o cohort, mas Ciclo Produtivo possui uma base própria: de jan/2025 ao mês atual, Base pelo código e Extra por planejamento original.

## 4. Cálculos de lead time, SLA e prazo

### 4.1 Data de criação canônica

A criação vigente é exclusivamente:

```sql
TIMESTAMP(DATE(MIN(created_at))) AS stamp_created_production_order
```

Não há fallback por `ingestion_date`. A primeira aparição no histórico é usada apenas no notebook de auditoria para reconstruir a metodologia antiga.

### 4.2 Data planejada original

`dt_planned_original` é o primeiro `dt_planned_entry_warehouse` não nulo após deduplicação diária da OP. Ele é usado para SLA planejado, classificação da faixa e mês do Ciclo Extra.

### 4.3 Produto Acabado e Triangulação

- Produto Acabado (PA): o lead time realizado começa em `waiting_fabric_arrival`.
- Triangulação (Tri): o lead time realizado começa em `stamp_created_production_order`.
- Ambos terminam em `dt_largest_entry_warehouse`.

### 4.4 Fórmulas

```text
SLA planejado = dt_planned_original - stamp_created_production_order

Lead time bruto PA = dt_largest_entry_warehouse - stamp_waiting_fabric_arrival
Lead time bruto Tri = dt_largest_entry_warehouse - stamp_created_production_order

Lead time realizado = max(lead time bruto - dias de postergação intencional, 0)
```

OPs com SLA ou lead time não positivo ficam fora dos consumidores que exigem essas métricas, permanecendo auditáveis.

### 4.5 Prazo por faixa de SLA

- `SLA < 100 dias`: prazo operacional de 90 dias;
- `SLA >= 100 dias`: prazo operacional de 120 dias;
- `Todos`: cada OP mantém o prazo correspondente à sua própria faixa; não se usa uma média entre 90 e 120 dias como critério.

`dentro_do_prazo` indica `lead_time_realizado <= prazo_op_dias`.

Para Triangulação, o lead time teórico comparativo soma o prazo cadastral ao
tempo de matéria-prima/malharia por produto. Sem mapeamento de tecido, o
notebook aplica fallback auditável de 60 dias. Esse ajuste é usado nas análises
de desvio e recomendação cadastral, não na fórmula de prazo operacional de
90/120 dias.

## 5. Postergação intencional

### 5.1 Objetivo da regra

Postergação intencional é uma reprogramação interna do prazo planejado de
entrada no armazém. Ela não deve ser interpretada como tempo adicional de
execução do fornecedor. Por isso, o dashboard a separa do atraso operacional:
o tempo bruto é preservado para auditoria, mas o tempo ajustado é a base dos
KPIs de lead time e pontualidade.

### 5.2 Identificação no histórico

O notebook lê somente o `production_order_type_filtro` selecionado e mantém o
primeiro snapshot de cada `op_code` por dia, ordenado por `ingestion_date`.
Depois compara cada snapshot diário com o anterior da mesma OP.

Um evento é contado como postergação somente quando todos os critérios abaixo
são verdadeiros:

1. o snapshot atual e o anterior pertencem ao mesmo tipo de OP selecionado;
2. `prev_planned` e `prev_reviewed` existem;
3. `prev_planned = prev_reviewed`, ou seja, o prazo vigente anterior ainda era
   o prazo originalmente assumido pela Insider;
4. a nova `dt_planned_entry_warehouse` é diferente e posterior a
   `prev_planned`;
5. `DATE_DIFF(dt_planned_entry_warehouse, prev_planned, DAY) > 0`.

Em cada evento válido, são somados os dias positivos de deslocamento. Uma OP
pode ter mais de uma postergação; `qt_dias_postergacao_intencional` é a soma
dos eventos válidos e `flag_teve_postergacao` indica se esse total é maior que
zero.

Não são descontados: antecipações ou datas inalteradas; mudança apenas da data
revisada; mudança de planejado quando o planejado e o revisado anteriores já
divergiam; registros sem uma das datas anteriores; ou transições entre tipos de
OP. Esses casos não satisfazem a evidência necessária para classificá-los como
reprogramação interna.

### 5.3 Aplicação e rastreabilidade

```text
lead_time_ajustado = max(lead_time_realizado_bruto - qt_dias_postergacao_intencional, 0)
lead_time_realizado = lead_time_ajustado
```

O ajuste não altera `dt_planned_original`, portanto não muda o cálculo do SLA
planejado, a faixa de SLA ou a atribuição da OP ao mês de Ciclo Extra. Ele é
aplicado após o cálculo do lead time bruto e antes dos KPIs, gráficos,
pontualidade, recomendações e da seção de Ciclo Produtivo.

Na exportação, os campos `lead_time_realizado_bruto`,
`qt_dias_postergacao_intencional`, `lead_time_ajustado` e
`flag_teve_postergacao` permitem reconstituir a regra por OP. Para a
decomposição de etapas exportada, quando há postergação o desconto é alocado à
etapa com maior desvio positivo versus a mediana do mesmo fornecedor × produto
× fluxo; isso é apenas uma convenção de atribuição da decomposição e não muda o
lead time ajustado da OP.

O Dashboard Geral aplica o cohort de entrega depois dessa reconstrução, enquanto
o Ciclo Produtivo utiliza sua base ampla dedicada. Em ambos, a postergação é
calculada no mesmo histórico filtrado pelo tipo de OP.

## 6. Métricas e visualizações

### 6.1 Cards executivos gerais

- População: OPs entregues, elegíveis e filtradas.
- Medidas: mediana de lead time realizado, percentual dentro do prazo, volume de OPs e comparações temporais quando aplicáveis.
- Denominador de pontualidade: OPs entregues com lead time realizado válido.

### 6.2 Série mensal por fluxo

- Eixo X: mês de fechamento/entrega.
- Eixo Y primário: mediana do lead time realizado.
- Eixo Y secundário: percentual de OPs dentro do prazo.
- Séries: Produto Acabado e Triangulação.

### 6.3 Evolução trimestral

- População: OPs entregues e elegíveis no tipo selecionado, após os filtros
  globais de fornecedor, produto, faixa de SLA e período.
- Eixo X: trimestre de `dt_largest_entry_warehouse`.
- Eixo Y: percentual de OPs entregues dentro do prazo.
- Denominador: todas as OPs efetivamente entregues no trimestre e elegíveis na faixa exibida.
- `dt_planned_original` participa do cálculo de SLA, não da atribuição trimestral.

### 6.4 Decomposição por etapa

- População: OPs entregues com stamps suficientes.
- Eixo/categorias: etapas cronológicas do fluxo produtivo.
- Medida: duração mediana ou participação da etapa no lead time, conforme o gráfico.
- PA e Tri são apresentados separadamente quando a composição de etapas difere.

### 6.5 Volume, fornecedor e produto

- Buckets de volume usam `planned_quantity_op`.
- Gráficos de fornecedor/produto usam lead time realizado mediano ou percentual dentro do prazo.
- Heatmaps exibem cruzamentos de dimensão e faixa/mês; células sem amostra permanecem vazias.
- Tabelas detalhadas preservam contagem de OPs para interpretação de amostras pequenas.

### 6.6 Recomendação de prazo cadastrado

A recomendação usa OPs entregues com lead time teórico válido, no mínimo cinco
OPs e percentil 75 do realizado ajustado. O desvio versus o teórico é sinalizado
como atenção a partir de 10 dias e crítico a partir de 30 dias. Não altera
automaticamente o cadastro; serve como evidência para revisão.

### 6.7 Risco single-source e capacidade

Cruza disponibilidade de fornecedores, produtos, células e capacidade cadastrada. Esses visuais não redefinem o cohort de lead time e devem ser interpretados como contexto de decisão, não como causalidade.

## 7. Ciclo Produtivo

### 7.1 Classificação e eixo X

- Base: `cycle_name` segue `^C(0[1-9]|1[0-2])[0-9]{4}$`; o eixo X vem do próprio código.
- Extra: demais ciclos; o eixo X é o mês de `dt_planned_original`.
- Não existe corte visual fixo em agosto de 2025.
- Títulos, eixos e tooltips usam **Mês do ciclo**.
- A seção usa uma base dedicada no grão de `op_code`, de jan/2025 ao mês atual, e não herda o cohort por entrega do Dashboard Geral.
- `is_op_open` no snapshot mais recente determina o status operacional: `TRUE` é pendente; `FALSE` ou nulo é OP fechada.

### 7.2 Cards de quantidade

Uma OP com data de entrega pode ainda ter quantidade parcial. Por isso, entrega registrada e atendimento integral são métricas distintas.

Para OPs com quantidade planejada positiva e quantidade recebida não nula/não negativa:

```text
peças atendidas por OP = min(received_quantity_op, planned_quantity_op)
saldo não atendido por OP = max(planned_quantity_op - received_quantity_op, 0)
% atendimento em peças = soma(peças atendidas) / soma(peças planejadas)
```

Os cards exibem OPs alocadas, entregues, pendentes, peças alocadas, recebidas e percentual dentro do prazo. As identidades obrigatórias são:

```text
OPs entregues + OPs pendentes = OPs alocadas
peças atendidas + saldo não atendido = peças planejadas
```

Quantidades planejadas ausentes/não positivas e recebidas ausentes/negativas são auditadas separadamente e não entram no denominador de atendimento.

### 7.3 Gráficos do Ciclo Produtivo

- Planejado versus atendido: X = mês do ciclo; Y = peças planejadas/atendidas; linha = percentual de atendimento.
- Status de prazo: X = mês do ciclo; Y = OPs pendentes, dentro e acima do prazo.
- Cumprimento Base x Extra: X = mês do ciclo; Y = percentual de peças recebidas dentro do prazo; denominador = peças recebidas de OPs fechadas.
- Heatmaps por fornecedor/produto: colunas = mês do ciclo; linhas = dimensão; cor = percentual dentro do prazo, com a mesma regra individual de SLA.
- CSV `ciclo_produtivo_por_mes.csv`: deriva da base dedicada e preserva OP, estado operacional, quantidades, prazo e mês do ciclo.

## 8. Qualidade, exportação e limitações

### 8.1 Guard rails

O notebook valida tipos permitidos, ausência de mistura entre `committed` e `incubation`, unicidade de `op_code`, cronologia das datas, cobertura das faixas de SLA, consistência Base/Extra e reconciliação dos cards.

Recortes vazios devem terminar sem erro e apresentar uma mensagem de ausência de dados, sem produzir métricas enganosas.

### 8.2 Exportação para Google Sheets

A planilha produtiva é sobrescrita somente quando `production_order_type_filtro = 'committed'` e todas as validações de contrato passam. Execuções de `incubation` realizam dry-run e não escrevem na planilha.

A exportação possui contrato de 38 colunas e uma linha por `op_code`. Antes de
limpar a aba, o notebook valida cabeçalho, unicidade, volume não vazio e número
de colunas; depois da escrita, valida novamente cabeçalho, linhas e 38 colunas.
Ela preserva as duas faixas válidas de SLA, independentemente do filtro visual.

> **Ponto de atenção:** no contrato atual da planilha, o campo exportado
> `dentro_do_prazo` ainda é calculado contra 120 dias. Os visuais e o Ciclo
> Produtivo usam a regra individual de 90/120 dias. Não use esse campo da
> planilha para avaliar SLA < 100 dias sem aplicar a regra por OP.

### 8.3 Limitações

- Stamps dependem da qualidade e cobertura do histórico diário.
- `created_at` ausente impede métricas dependentes de criação.
- Amostras pequenas tornam medianas e percentuais instáveis.
- O lead time ajustado remove apenas postergações que atendem à regra formal.
- Sobre-entrega não compensa falta de outra OP no percentual de atendimento.
- A série trimestral aplica todos os filtros globais, portanto comparações entre
  execuções exigem registrar os filtros utilizados.
- O campo `dentro_do_prazo` da exportação Google Sheets possui, neste momento,
  regra legada de 120 dias; a regra operacional vigente nos visuais é individual
  por OP.

## Referências operacionais

- Dashboard Deepnote: `lead_time_dashboard` (`0b56034555ea4a43a7d80e0ede768923`).
- Auditoria mensal: `0fc8465ceffc4558a59cb81257cf3e3a`.
- Fonte metodológica anterior: `[EP] Leadtime Produtivo - Metolodogia.md`.
