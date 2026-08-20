# Auditoria de premissas — OKR de Troca & Devolução

**Cohort (Source of Truth proposta) vs. Fluxo mensal (definição legada)**

| | |
|---|---|
| **Autor** | Lucas Sampaio |
| **Data da auditoria** | 2026-08-05 |
| **Execução das queries** | 2026-08-05, `insider-data-lake`, região `southamerica-east1` |
| **Janela auditada** | `2026-01-01` → `2026-07-31` (últimos 7 meses fechados) |
| **Notebook de origem** | [tempo_para_reversa_pos_entrega.ipynb](tempo_para_reversa_pos_entrega.ipynb) — Parte C |
| **Artefatos auditáveis** | [sql/td_cohort_mensal.sql](sql/td_cohort_mensal.sql) · [sql/td_cohort_semanal.sql](sql/td_cohort_semanal.sql) |
| **Público** | Analista de BI sênior — revisão premissa a premissa |

---

## 1. Objetivo e escopo

**O que esta auditoria faz:** materializa o OKR de T&D por coorte de compra em **duas queries SQL autocontidas** (mensal e semanal) e confronta cada premissa contra a definição legada (visão mensal por fluxo). Antes deste trabalho, a métrica cohort só existia em SQL na granularidade **diária**; a agregação mensal, a agregação semanal, o corte de maturação (dia 15 de M+1) e a janela W-2 viviam apenas em pandas — ou seja, **não eram auditáveis no BigQuery por um terceiro**.

**O que esta auditoria NÃO faz:**
- Não altera a definição do OKR. Corrigir premissas (ex.: o risco de dedup da §6.1) é decisão do dono da métrica.
- Não projeta meses/semanas imaturos. A projeção segue na Parte D do notebook (curva de maturação).
- Não valida a qualidade da fonte `prepared__troquecommerce_order_details_br` além do que a Parte D já mede.

**Critério de sucesso desta auditoria:** o analista consegue (a) rodar as três queries sozinho, (b) reconciliar o denominador em zero, (c) atribuir 100% do delta de numerador a causas nomeadas.

---

## 2. As duas definições, em uma frase cada

| | Definição |
|---|---|
| **Legado — fluxo mensal** | Peças com solicitação de reversa **criada** no mês M ÷ peças **vendidas** no mês M. Numerador e denominador são populações **independentes**: não há vínculo de coorte, pedido ou SKU entre eles. |
| **Cohort — Source of Truth** | Peças revertidas **em qualquer momento** de pedidos **comprados** no mês M ÷ peças **vendidas** no mês M. Numerador e denominador descrevem **a mesma população** de peças, ligada por `order_name`. |

A diferença conceitual é de **atribuição temporal**: o legado responde *"quanto de reversa entrou este mês?"* (métrica de fluxo operacional). O cohort responde *"desta safra de vendas, quanto voltou?"* (métrica de qualidade da safra). São perguntas diferentes; a segunda é a que fecha com o denominador.

---

## 3. Tabela mestra de premissas

Referências `arquivo:CTE` para verificação direta. `LEG` = query legada (§9), `COH` = `td_cohort_mensal.sql`.

| # | Dimensão | Legado | Cohort (SoT) | Impacto no número | Onde verificar |
|---|---|---|---|---|---|
| **P1** | **Ancoragem de data** | `DATE_TRUNC(data_reversa, MONTH)` — mês em que a reversa foi **aberta** | `DATE_TRUNC(data_compra, MONTH)` — mês em que o pedido foi **comprado** | **A única diferença estrutural.** Responde por 100% do delta (§4) | `LEG:reversas_mensais` vs. `COH:reversas_mensais` |
| **P2** | **Janela do numerador** | `data_reversa` **dentro** da janela | **Lifetime**: `created_at` só validado `IS NOT NULL`; nunca filtrado por data | Cohort captura a cauda longa; sofre censura à direita nos períodos recentes | `LEG:reversas_deduplicadas` (tem `data_reversa`) vs. `COH:reversas_deduplicadas` (não tem) |
| **P3** | **Universo de pedido do numerador** | Nenhum — o legado **nunca valida** o pedido de origem da reversa | Nenhum, deliberadamente: `todos_pedidos` aceita **qualquer** pedido com `processed_at`, sem `paid`/loja/cupom. Serve só para descobrir a data de compra | **Simétrico.** Cobertura de âncora medida: **99,98%** (só 28 de 142.768 unidades sem pedido âncora) | `COH:todos_pedidos` |
| **P4** | **Casamento de SKU** | Não exige que o SKU revertido conste como item comprado | Idem — não exige | **Simétrico** | ambos: `reversas_deduplicadas` sem join a `insider_order_items` |
| **P5** | **Filtro do denominador** | `paid` + `is_cancelled=FALSE` + exclusão de cupons (`TF-`, `TFIN`, `IR`, `%Item errado%`) + 2 lojas + `sku IS NOT NULL` | **Literalmente idêntico** — CTE copiada caractere a caractere | **Zero.** Verificado: denominador bate exatamente nos 7 meses (§4.1) | `LEG:compras_filtradas` ≡ `COH:compras_filtradas` |
| **P6** | **Dedup de reversa** | `DISTINCT (order_name, id_reversa, sku, data_reversa, return_quantity)` | `DISTINCT (order_name, id_reversa, sku, return_quantity)` | **Simétrico, e frágil em ambos** — `return_quantity` está *dentro* do DISTINCT. Ver risco §6.1 | ambos: `reversas_deduplicadas` |
| **P7** | **Grão de agregação do numerador** | Soma direta no grão `(order_name, id_reversa, sku)` | Colapsa para grão de **pedido** (`GROUP BY order_name`) antes de ancorar a data | Neutro no total. O SKU não chega ao grão final em nenhum dos dois | `COH:reversas_por_pedido` |
| **P8** | **Timezone** | `America/Sao_Paulo` em `processed_at` e `created_at` | Idem; `todos_pedidos` usa `MIN(processed_at)` por `order_name` | **Simétrico** | ambos |
| **P9** | **Maturação** | **Inexistente.** O legado é maduro no dia 1 de M+1 por construção — o mês fecha e não muda mais | Mês só é `oficial (maduro)` no **dia 15 de M+1**; semana só é reportável em **W-2** (`semana_ini + 21d`) | Trade-off central: o cohort troca imediatismo por atribuição correta | `COH` colunas `maduro_em`/`status`; semanal: `reportavel_em`/`status`/`is_w2` |
| **P10** | **Convenção de semana** | n/a (só mensal) | `DATE_TRUNC(d, WEEK(MONDAY))` = **segunda a domingo** | Equivale exatamente a `pd.Period(d,"W-SUN").start_time` do notebook. ⚠️ `WEEK` sem argumento no BQ começa no **domingo** — erro fácil | `td_cohort_semanal.sql:params` |
| **P11** | **Janela / limite superior** | `start_date = 2026-01-01`; até o **último mês fechado** (`M-1`) | **Mensal alinhada ao legado**: mesmo `start_date`, mesmo `ultimo_mes_fechado` — elimina a janela como ruído na reconciliação (§7). **Semanal deliberadamente diferente** (revisitado 2026-08-05): `start_date = 2025-01-01`, calendário até a **semana atual** (mesmo parcial) — não há legado semanal para reconciliar, e o HM (gráfico de tendência) precisa de histórico longo. Ver §6.4 e o cabeçalho de `td_cohort_semanal.sql` (P11') | `params` em ambos |
| **P12** | **Períodos sem dado** | CTE `calendar` via `GENERATE_DATE_ARRAY` + `LEFT JOIN` + `COALESCE(...,0)` | **Idêntico** (mensal e semanal) | Nenhum mês/semana desaparece da série | `calendar` em ambos |

**Leitura da tabela:** de 12 dimensões, **9 são idênticas ou simétricas** entre os dois métodos. As diferenças reais são **P1 (ancoragem)**, **P2 (janela do numerador, que é consequência de P1)** e **P9 (maturação, que é consequência de P2)**. Isso é intencional — ver §4.

---

## 4. Ancoragem: a única diferença que sobra

Antes da revisão de 2026-08-04, o cohort **também** exigia pedido válido (`paid`/loja/cupom) e SKU casado no numerador. Isso criava uma **segunda** diferença — assimetria de universo — misturada com o efeito de ancoragem, e tornava impossível dizer quanto do delta vinha de cada coisa. A revisão alinhou P3/P4/P5/P6 ao legado justamente para isolar P1.

**Consequência testável:** se P3–P6 estão de fato alinhados, os dois métodos enxergam **exatamente o mesmo conjunto de reversas** e o mesmo conjunto de vendas. Todo o delta tem de vir de *para qual mês cada reversa foi contada*. É o que os números mostram.

### 4.1 Reconciliação do denominador — deve ser ZERO

Execução 2026-08-05. `qt_itens_vendidos`, legado vs. cohort:

| mês | Legado | Cohort | Δ |
|---|---|---|---|
| 2026-01 | 217.787 | 217.787 | **0** |
| 2026-02 | 228.529 | 228.529 | **0** |
| 2026-03 | 376.554 | 376.554 | **0** |
| 2026-04 | 244.750 | 244.750 | **0** |
| 2026-05 | 195.610 | 195.610 | **0** |
| 2026-06 | 185.498 | 185.498 | **0** |
| 2026-07 | 188.788 | 188.788 | **0** |

✅ **P5 validado.** Qualquer diferença aqui invalidaria toda a comparação de numerador.

> Nota técnica: `COH:compras_filtradas` **não** carrega `order_name` (só `order_id`), igual ao legado. Verificado que `order_id → order_name` é **1:1** no universo relevante (5.665.872 = 5.665.872 distintos), portanto a escolha não tem efeito numérico. A célula 31 do notebook carrega `order_name` sem necessidade.

### 4.2 Ponte de numerador — 100% atribuído

Universo de reversas com `status <> 'Cancelado'`, na janela 2026-01→2026-07:

```
LEGADO   (reversa aberta na janela)      A = 142.768
COHORT   (compra feita na janela)        B = 132.037
Interseção (compra E reversa na janela)  C = 130.503

A = C + D  →  D = 12.265  = só no legado: reversa aberta em 2026, compra FORA da janela
                            ├─ 12.237  compra em 2025 ou antes
                            └─      28  sem pedido âncora (0,02% — cobertura 99,98%)
B = C + E  →  E =  1.534  = só no cohort: compra em 2026, reversa aberta em ago/26 ou depois
```

✅ **Fecha em zero.** `142.768 = 130.503 + 12.265` e `132.037 = 130.503 + 1.534`. Nenhuma unidade fica sem explicação.

`E = 1.534` é apenas a cauda **já materializada** em 5 dias de agosto. A cauda verdadeira dos coortes recentes é maior e ainda não observada — é exatamente a censura à direita que P9 endereça.

### 4.3 O efeito prático: janeiro/2026

Aqui está o argumento central da mudança de definição. Decomposição do numerador **legado** por safra de compra:

| mês da reversa (legado) | numerador total | vindo de compras de **2025 ou antes** | % importado |
|---|---|---|---|
| **2026-01** | 23.048 | **10.151** | **44,0%** |
| 2026-02 | 18.126 | 916 | 5,1% |
| 2026-03 | 33.481 | 690 | 2,1% |
| 2026-04 | 22.897 | 285 | 1,2% |
| 2026-05 | 15.330 | 142 | 0,9% |
| 2026-06 | 15.275 | 31 | 0,2% |
| 2026-07 | 14.611 | 22 | 0,2% |

**44% do numerador legado de janeiro/26 não pertence a janeiro/26.** São devoluções de compras de novembro/dezembro de 2025 — a safra de Black Friday (630.243 itens vendidos em nov/25, 3,4× a média mensal) chegando à janela de troca em janeiro. O legado divide essas reversas pelas vendas de janeiro (217.787 itens), que nada têm a ver com elas.

Resultado nas duas leituras:

| mês | Legado | Cohort | Δ (pp) |
|---|---|---|---|
| 2026-01 | **10,58%** | **8,42%** | −2,16 |
| 2026-02 | 7,93% | 8,49% | +0,56 |
| 2026-03 | 8,89% | 9,33% | +0,44 |
| 2026-04 | 9,36% | 7,48% | −1,88 |
| 2026-05 | 7,84% | 7,57% | −0,27 |
| 2026-06 | 8,23% | 7,52% | −0,71 |
| 2026-07 | 7,74% | 6,41% *(imaturo)* | −1,33 |

O pico de 10,58% em janeiro no legado é **artefato de mistura de safras**, não deterioração de qualidade. O cohort mostra janeiro em 8,42% e joga aquelas 10.151 unidades de volta para nov/dez-25, onde foram geradas. Simetricamente, o legado **subestima** fevereiro e março (7,93% e 8,89% vs. 9,33% no cohort de março) porque exporta parte das reversas daquelas safras para meses seguintes.

**Implicação para decisão:** com o legado, uma ação de melhoria de qualidade em nov/25 apareceria (diluída e deslocada) em jan/26; e um mês de vendas alto **reduz** mecanicamente o T&D legado do próprio mês, porque infla o denominador antes de a reversa correspondente existir. O legado é, portanto, **sensível ao mix de sazonalidade de vendas** — o que é fatal para um KR.

---

## 5. Equivalência entre o SQL novo e o pandas atual da Parte C

Teste de fidelidade da tradução. Notebook executado em **2026-08-04**; queries em **2026-08-05**.

| mês | pandas (célula 33) | SQL `td_cohort_mensal.sql` | Δ |
|---|---|---|---|
| 2026-01 | 8,42% | 8,42% | 0,00 |
| 2026-02 | 8,49% | 8,49% | 0,00 |
| 2026-03 | 9,33% | 9,33% | 0,00 |
| 2026-04 | 7,48% | 7,48% | 0,00 |
| 2026-05 | 7,56% | 7,57% | +0,01 |
| 2026-06 | 7,51% | 7,52% | +0,01 |

| semana | pandas (célula 35) | SQL `td_cohort_semanal.sql` | Δ |
|---|---|---|---|
| 2026-06-15 | 8,23% | 8,24% | +0,01 |
| 2026-06-22 | 8,56% | 8,57% | +0,01 |
| 2026-06-29 | 8,33% | 8,32% | −0,01 |
| 2026-07-06 | 7,85% | 7,89% | +0,04 |
| **2026-07-13 (W-2)** | **7,55%** | **7,64%** | **+0,09** |

✅ Tradução fiel. Os deltas residuais são **drift de fonte**, não erro de lógica, e têm assinatura consistente com isso:
- Numerador cresce com o tempo (P2 lifetime) — o efeito é maior nas semanas mais recentes (+0,09pp em W-2, +0,00 nos meses antigos). Comportamento esperado.
- Denominador também se move para baixo (jun/26: 185.524 → 185.498, −26 itens) porque `insider_orders` é **mutável**: cancelamentos e estornos são aplicados retroativamente.

⚠️ **Consequência operacional:** o KR **não é reprodutível bit a bit em datas diferentes**, nem no cohort nem no legado. Para o MBR, o número precisa ser **congelado** (snapshot datado) no momento da leitura oficial. Recomendação em §7.

Confirmações de convenção:
- `is_w2 = TRUE` caiu em **2026-07-13**, idêntico ao `w2_mon` do pandas. `DATE_TRUNC(hoje, WEEK(MONDAY)) - 21d ≡ monday(hoje - 21d)`.
- `media_movel_4s` retorna `NULL` nas 3 primeiras semanas, replicando `rolling(4).mean()`.

---

## 6. Riscos e dívidas conhecidas

### 6.1 🔴 `return_quantity` dentro do `DISTINCT` — exposição a dupla contagem

Presente em **ambos** os métodos:

```sql
SELECT DISTINCT order_name, id_reversa, sku, return_quantity FROM ...
```

Se a fonte emitir duas linhas para a mesma chave `(order_name, id_reversa, sku)` com quantidades diferentes (ex.: correção de 1 → 2), **as duas sobrevivem ao DISTINCT e as duas são somadas**. O dedup declarado no markdown da Parte C (`order_name,id_reversa,sku`) **não é o dedup codificado**.

- **Severidade hoje:** baixa e **simétrica** — os dois métodos erram igual, então a comparação desta auditoria permanece válida.
- **Severidade estrutural:** alta. É uma bomba silenciosa: qualquer mudança de comportamento no pipeline `prepared__troquecommerce` infla o KR sem nenhum alarme.
- **Contraste:** as Partes A e B do notebook usam dedup **estrito** (`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_name, id_reversa ORDER BY updated_at DESC) = 1`). A Parte C não usa `updated_at`. Há duas convenções de dedup convivendo no mesmo notebook.
- **Teste de exposição:** já instrumentado na Parte D (`n_linhas_dedup − n_chaves`, rotulado "risco de dupla contagem").
- **Ação proposta (fora do escopo desta auditoria):** trocar por `QUALIFY ROW_NUMBER() ... ORDER BY updated_at DESC` nos **dois** métodos simultaneamente, medindo o impacto antes de publicar.

### 6.2 🟡 Censura à direita — o cohort subestima o presente por construção

O numerador lifetime só está completo quando toda a janela de troca da safra expirou. As colunas `status`/`maduro_em`/`is_w2` **impedem a leitura errada, não corrigem o viés**. Nunca compare um período `em maturacao` com um `oficial (maduro)`.

Magnitude visível na própria série semanal: a semana de 27/jul (imatura, 5 dias de idade) marca **1,44%** contra ~7,6% das semanas maduras. Não houve melhora de 6pp — falta reversa por chegar.

O instrumento de projeção existe: a curva de maturação da Parte D (`pct_realizado(dias)` → `td_pct_projetado`). Está fora destas duas queries por decisão de escopo — a query entrega o número observado; a projeção é uma camada analítica separada.

### 6.3 🟡 Markdown da Parte C desatualizado vs. o código
A célula 29 afirma dedup por `order_name,id_reversa,sku` (§6.1) e cobertura de âncora de "99,99%" (medida: **99,98%**). Corrigir antes da apresentação.

### 6.4 🟢 Divergências entre as queries novas e a célula 31 do notebook
**Atualizado em 2026-08-05** — a janela da semanal foi revisitada e voltou a divergir da mensal, de propósito (motivo abaixo). Não é mais uma pendência, é uma decisão registrada.

| | célula 31 (diária) | `td_cohort_mensal.sql` | `td_cohort_semanal.sql` |
|---|---|---|---|
| `start_date` | `2025-01-01` | `2026-01-01` (= legado) | `2025-01-01` (= célula 31) |
| limite superior | nenhum — inclui o mês/semana corrente parcial | último mês fechado (= legado) | **semana atual**, mesmo parcial (= célula 31) |
| `order_name` em `compras_filtradas` | presente (sem uso) | ausente (= legado; impacto zero, §4.1) | ausente (idem) |

**Por que a mensal fica curta e a semanal fica longa:** a mensal alimenta a reconciliação linha a linha contra o legado (§7) — precisa da mesma janela para a diferença de denominador ser zero por construção. A legada só existe em granularidade mensal, então não existe reconciliação semanal para proteger; a semanal serve o gráfico de tendência do HM (planning de curto prazo), que só é útil com histórico longo (a leitura de médio prazo, via média móvel de 4 semanas, não faz sentido em 7 meses de dados). A semana corrente parcial aparece de propósito, como um ponto real e baixo — o status `em maturacao` e o sombreado "imaturo" no gráfico da Parte C já sinalizam que não é leitura de resultado.

A base diária de 2025 continua disponível na célula 31 para análise de sazonalidade.

### 6.5 🟡 O denominador não é estável no tempo
`insider_orders` é mutável (§5). Vale para os dois métodos. Endereçar com snapshot congelado (§7).

### 6.6 🟢 Primeira semana da série é parcial (artefato de borda)
**Atualizado em 2026-08-05** (janela da semanal ampliada, §6.4): `DATE_TRUNC('2025-01-01', WEEK(MONDAY)) = 2024-12-30`, então a primeira linha da série semanal é a semana de **30/dez/2024**, mas conta apenas os dias `>= start_date` (01–05/jan, 5 dias, não 7). Numerador e denominador são cortados **do mesmo jeito**, portanto o **percentual permanece válido** — só os volumes absolutos são de uma semana parcial. Não usar essa linha para comparação de volume. A **última** linha da série (semana atual, também parcial por construção — §6.4) tem o mesmo cuidado, ao contrário: fica **subestimada por censura à direita**, não por corte de início — é exatamente a leitura que o status `em maturacao` existe para sinalizar.

---

## 7. Protocolo de reconciliação — passo a passo para o analista

1. **Rodar as três queries** no `insider-data-lake` (região `southamerica-east1`):
   - [sql/td_cohort_mensal.sql](sql/td_cohort_mensal.sql)
   - [sql/td_cohort_semanal.sql](sql/td_cohort_semanal.sql)
   - a legada da §9
   Custo observado: ~1,5 GB por query cohort, ~0,9 GB na legada.

2. **Validar P5 — denominador.** Merge por `mes_referencia`; `Δ qt_itens_vendidos` deve ser **0 em todos os meses**. Se não for, pare: a comparação de numerador não é válida.

3. **Fechar a ponte de numerador** com a query da §4.2 e conferir `A = C + D` e `B = C + E`. Nenhuma unidade órfã.

4. **Ler o número certo.** Headline do KR = último mês com `status = 'oficial (maduro)'`. Headline do HM = linha com `is_w2 = TRUE`. Meses/semanas `em maturacao` são leitura de tendência, não de resultado.

5. **Coerência semanal ↔ mensal.** A soma das semanas contidas em um mês **não fecha exatamente** com o mensal — semanas cruzam a virada de mês (ex.: 2026-06-29 → 2026-07-05 divide-se entre jun e jul). Isso é correto por construção; não "conserte".

6. **Congelar o número.** Datar e versionar o resultado da leitura oficial (`outputs/okr_td_kr_mensal_<YYYYMMDD>.csv`). O KR do MBR é o valor congelado no dia 15 de M+1, não o que a query devolve hoje.

---

## 8. Recomendação

**Adotar o cohort como Source of Truth do KR de T&D**, com a régua de maturação explícita (KR oficial no dia 15 de M+1; HM em W-2).

**Por quê:** o legado é sensível ao mix de sazonalidade de vendas — a evidência da §4.3 mostra 44% do numerador de janeiro pertencendo a outra safra, produzindo um pico de 10,58% que não descreve nenhuma decisão tomável. O cohort fecha numerador e denominador na mesma população e atribui o resultado a quem o gerou.

**O que se perde, explicitamente:** imediatismo. O legado entrega o número no dia 1 de M+1; o cohort no dia 15. Esse é o preço da atribuição correta e deve ser aceito conscientemente pelo dono do KR.

**Manter o legado em paralelo** como métrica de **fluxo operacional** — é a certa para dimensionar capacidade de CD reverso, que responde a "quantas peças chegam este mês", não a "de qual safra elas vieram". São dois indicadores com dois donos, não duas versões da verdade.

**Próximos passos (proposta, não decisão):**

| # | O quê | Dono | Quando | Dependência |
|---|---|---|---|---|
| 1 | Revisar esta auditoria premissa a premissa | Analista de BI sênior + Lucas | nesta sessão | — |
| 2 | Decidir sobre o dedup da §6.1 (medir impacto nos dois métodos antes de mudar) | Dono do KR | antes do próximo MBR | Parte D já mede |
| 3 | Corrigir markdown da célula 29 e sumário executivo da célula 38 | Lucas | antes da apresentação | — |
| 4 | Definir e implementar o congelamento datado do KR (§7.6) | Lucas | próximo ciclo | decisão do item 1 |

---

## 9. Anexo — query legada (referência)

Reproduzida integralmente para este documento ser autocontido.

```sql
-- Source of Truth de Troca & Devolução — visão mensal por fluxo
--
-- Numerador: peças com solicitação de troca/devolução criada no mês.
-- Denominador: peças vendidas no mesmo mês.
-- Não há vínculo de coorte, pedido ou SKU entre vendas e reversas.

WITH params AS (
  SELECT
    DATE '2026-01-01' AS start_date,
    DATE_SUB(
      DATE_TRUNC(CURRENT_DATE('America/Sao_Paulo'), MONTH),
      INTERVAL 1 MONTH
    ) AS ultimo_mes_fechado
),

compras_filtradas AS (
  SELECT DISTINCT
    order_id,
    DATE(
      TIMESTAMP(processed_at),
      'America/Sao_Paulo'
    ) AS data_compra
  FROM `insider-data-lake.business.insider_orders`
  WHERE order_status = 'paid'
    AND is_cancelled = FALSE
    AND (
      coupon_code IS NULL
      OR (
        NOT STARTS_WITH(coupon_code, 'TF-')
        AND NOT STARTS_WITH(coupon_code, 'TFIN')
        AND NOT STARTS_WITH(coupon_code, 'IR')
        AND NOT coupon_code LIKE '%Item errado%'
      )
    )
    AND order_name IS NOT NULL
    AND processed_at IS NOT NULL
    AND store IN (
      'shopify_insider-store-loja',
      'shopify_insider-world'
    )
),

vendas_mensais AS (
  SELECT
    DATE_TRUNC(c.data_compra, MONTH) AS mes_referencia,
    SUM(COALESCE(i.quantity, 0)) AS qt_itens_vendidos
  FROM compras_filtradas AS c
  INNER JOIN `insider-data-lake.business.insider_order_items` AS i
    ON c.order_id = i.order_id
  CROSS JOIN params AS p
  WHERE i.sku IS NOT NULL
    AND c.data_compra >= p.start_date
    AND c.data_compra < DATE_ADD(p.ultimo_mes_fechado, INTERVAL 1 MONTH)
  GROUP BY 1
),

reversas_deduplicadas AS (
  SELECT DISTINCT
    order_name,
    id_reversa,
    sku,
    DATE(created_at, 'America/Sao_Paulo') AS data_reversa,
    SAFE_CAST(return_quantity AS FLOAT64) AS return_quantity
  FROM `insider-lake-sensitive.prepared_br.prepared__troquecommerce_order_details_br`
  WHERE status <> 'Cancelado'
    AND order_name IS NOT NULL
    AND sku IS NOT NULL
    AND id_reversa IS NOT NULL
    AND created_at IS NOT NULL
),

reversas_mensais AS (
  SELECT
    DATE_TRUNC(data_reversa, MONTH) AS mes_referencia,
    SUM(COALESCE(return_quantity, 0)) AS qt_itens_revertidos
  FROM reversas_deduplicadas
  CROSS JOIN params AS p
  WHERE data_reversa >= p.start_date
    AND data_reversa < DATE_ADD(p.ultimo_mes_fechado, INTERVAL 1 MONTH)
  GROUP BY 1
),

calendar AS (
  SELECT mes_referencia
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_TRUNC(start_date, MONTH),
      ultimo_mes_fechado,
      INTERVAL 1 MONTH
    )
  ) AS mes_referencia
)

SELECT
  FORMAT_DATE('%Y-%m', c.mes_referencia) AS mes_referencia,
  COALESCE(v.qt_itens_vendidos, 0) AS qt_itens_vendidos,
  COALESCE(r.qt_itens_revertidos, 0) AS qt_itens_revertidos,
  ROUND(
    SAFE_DIVIDE(
      COALESCE(r.qt_itens_revertidos, 0),
      COALESCE(v.qt_itens_vendidos, 0)
    ) ,
    4
  ) AS pct_reversas_sobre_vendas
FROM calendar AS c
LEFT JOIN vendas_mensais AS v USING (mes_referencia)
LEFT JOIN reversas_mensais AS r USING (mes_referencia)
ORDER BY c.mes_referencia DESC;
```

---

## 10. Limitações desta auditoria

- **Janela curta:** 7 meses. O efeito de sazonalidade da §4.3 foi observado em **um** ciclo de Black Friday. Repetir a decomposição com nov/25 → jan/26 da série 2025 (disponível na célula 31 do notebook) fortaleceria a conclusão — não foi feito porque o legado só cobre a partir de 2026-01.
- **Fonte mutável:** todos os números têm data de execução (2026-08-05) e mudam em reexecuções. Ver §6.5.
- **Não valida a fonte upstream:** assume-se que `prepared__troquecommerce_order_details_br` reflete corretamente as solicitações de reversa. Fora de escopo.
- **Cauda não observada:** `E = 1.534` (§4.2) é piso, não estimativa da cauda total dos coortes recentes.
- **Sem teste de sensibilidade ao dedup:** o impacto de trocar o dedup da §6.1 não foi quantificado — é o item 2 da §8.
