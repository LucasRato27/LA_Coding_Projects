# Governança de Priorização de Melhorias de Produtos

**Versão:** 1.0 · **Data:** 11/06/2026
**Owner:** Analytics / Supply Chain & SOP
**Stakeholders:** Time de Produto Físico (PF) · Produto · Comercial

---

## Objetivo

Definir o **processo de decisão estruturado** para determinar quais produtos do portfólio devem receber investimento de melhoria física, quais devem ser avaliados para descontinuação ou reformulação, e quais não justificam esforço no momento.

A governança responde a três perguntas:
1. **Onde a dor do cliente é maior?** → identificada pelo pipeline de T&D
2. **Qual produto justifica o investimento?** → cruzado com tração comercial
3. **Qual a decisão correta?** → determinada por uma regra de dois estados

---

## Fonte de Dados e Cadência

| Dado | Fonte | Atualização |
|---|---|---|
| Reversas e motivos (T&D) | `prepared_br.prepared__troquecommerce_order_details_br` + `sop_silver.return_reason_tags` | Contínua (Troquecommerce) |
| Vendas e receita | `integrated.orders` + `integrated.order_items` | Diária |
| Cluster estratégico | `sop_silver.portfolio_skp_clustering` | Mensal (ciclo de portfólio) |
| Scorecard de portfólio | `sop_bronze.eval_produto_portfolio` | Mensal (ciclo de portfólio) |

**Cadência de execução do pipeline:** mensal — rodar no início de cada mês com janela dos últimos 12 meses. Resultados entregues ao time PF via workbook Excel (`td_priorizacao_YYYYMMDD.xlsx`).

---

## Os Dois Pilares da Priorização

A decisão de melhoria combina **duas dimensões independentes**:

### Pilar 1 — Dor do Cliente (`td_score`)

Mede o quanto o produto concentra problema físico em relação ao portfólio:

```
td_score = 0.5 × percentil_td_rate
         + 0.3 × percentil_volume_td
         + 0.2 × percentil_delta_vs_categoria
```

- **`percentil_td_rate`** — % de itens retornados sobre itens vendidos, em relação ao portfólio
- **`percentil_volume_td`** — volume absoluto de retornos, em relação ao portfólio
- **`percentil_delta_vs_categoria`** — quanto o produto supera a taxa da sua categoria

> Todos os percentis usam `CUME_DIST()` sobre produtos com volume mínimo (≥30 vendas e ≥5 retornos).

### Pilar 2 — Tração Comercial (`commercial_score`)

Mede o peso comercial do produto no portfólio:

```
commercial_score = 0.6 × percentil_receita + 0.4 × percentil_unidades
```

> `commercial_score` é relativo ao período analisado (CUME_DIST). Os scores absolutos de portfólio (0–100) de `eval_produto_portfolio` são exibidos como referência na aba `relatorio_pf`, mas **não alimentam o sinal de priorização**.

---

## Sistema de Sinais

### Nível 1 — Farol Operacional (`sinal_priorizacao`, 5 buckets)

Calculado em SQL para todos os 287 produtos:

| Sinal | Critério | Significado |
|---|---|---|
| 🔴 **Priorizar melhoria** | `td_score ≥ 0.70` e `commercial_score ≥ 0.60` | Alta dor + alta tração — ação imediata justificada |
| 🟡 **Alerta em produto relevante** | `commercial_score ≥ 0.70` e `0.50 ≤ td_score < 0.70` | Produto de peso com dor moderada em escalada |
| 🟠 **Monitorar** | `td_score ≥ 0.70` e `commercial_score < 0.60` | Alta dor, mas tração insuficiente para priorizar |
| ⚪ **Não priorizar agora** | `td_score < 0.50` ou `commercial_score < 0.40` | Sem urgência no momento |
| ⬜ **Sem evidência suficiente** | `< 30 vendas` ou `< 5 retornos` | Volume insuficiente para diagnóstico |

### Nível 2 — Decisão Estratégica (`sinal_kill_keep`, 2 estados)

Calculado em Python sobre `td_score` e `commercial_score` para produtos com evidência suficiente:

```
Alta T&D (td_score ≥ 0.70) + Alta Tração (commercial_score ≥ 0.60)
  → "Priorizar melhoria" — investir em melhoria estrutural

Alta T&D (td_score ≥ 0.70) + Baixa Tração (commercial_score < 0.60)
  → "Não Priorizar (Avaliar Descontinuação/Reformulação)"

Demais casos com evidência
  → "Não priorizar agora"
```

> O `sinal_kill_keep` responde diretamente à pergunta do time PF: **"Vale melhorar ou descontinuar?"**

---

## Matriz de Decisão

|  | **Alta Tração Comercial** (`commercial_score ≥ 0.60`) | **Baixa Tração Comercial** (`commercial_score < 0.60`) |
|---|---|---|
| **Alta T&D** (`td_score ≥ 0.70`) | 🔴 **Priorizar melhoria** — alto retorno esperado da melhoria | 🟠 **Avaliar descontinuação ou reformulação** — dor alta, mas produto sem tração suficiente |
| **T&D moderada** (`0.50 ≤ td_score < 0.70`) | 🟡 **Alerta** — monitorar com atenção; risco de escalar | ⚪ **Não priorizar** — baixo impacto comercial e dor contida |
| **Baixa T&D** (`td_score < 0.50`) | ⚪ **Não priorizar** — produto saudável do ponto de vista de reversas | ⚪ **Não priorizar** |

---

## Como Usar o Output

### Aba `relatorio_pf` (Excel — principal entrega ao time PF)

| Coluna | Descrição |
|---|---|
| `product_name` | Nome do produto |
| `cluster` | Cluster estratégico (Hero · Core · Long tail · KILL · etc.) |
| `sinal_kill_keep` | Decisão consolidada de 2 estados |
| `score_tracao_comercial` | Pilar de scorecard (0–100) — referência de posicionamento no portfólio |
| `score_unit_economics` | Pilar de scorecard (0–100) — viabilidade financeira |
| `score_satisfacao_marca` | Pilar de scorecard (0–100) — satisfação e marca |
| `td_categoria_pct` | Taxa de T&D da categoria (benchmark) |
| `td_produto_pct` | Taxa de T&D do produto |
| `top_3_motivos_td` | Top 3 tags de motivo com % (HTML, gerado automaticamente) |

### Aba `farol_executivo`
Visão completa de todos os 287 produtos com todos os scores — para deep dive analítico.

### Aba `priorizar_melhoria`
Ranking dos 50 produtos no bucket de maior urgência, ordenado por `priority_score`.

---

## Fluxo de Decisão por Produto

```
Produto entra no pipeline
        │
        ▼
  Volume mínimo?   ──── NÃO ──→  "Sem evidência suficiente" (aguardar)
        │
       SIM
        │
        ▼
  td_score ≥ 0.70?
        │
        ├── NÃO ──→  commercial_score ≥ 0.70 e td_score ≥ 0.50?
        │                    │
        │              SIM ──→  "Alerta em produto relevante"
        │              NÃO ──→  "Não priorizar agora"
        │
        └── SIM ──→  commercial_score ≥ 0.60?
                            │
                      SIM ──→  🔴 "Priorizar melhoria"
                               (sinal_kill_keep = Priorizar melhoria)
                      NÃO ──→  🟠 "Monitorar"
                               (sinal_kill_keep = Avaliar Descontinuação/Reformulação)
```

---

## Scorecard de Portfólio — Referência de Qualidade

Os três pilares de `eval_produto_portfolio` **não determinam** o sinal de priorização, mas contextualizam a decisão:

| Pilar | Campo | Pergunta que responde |
|---|---|---|
| **Tração Comercial** | `score_vendas_geral × 100` | O produto vende bem? Tem demanda de mercado? |
| **Unit Economics** | `score_viabilidade_financeira × 100` | O produto gera margem? Vale economicamente? |
| **Satisfação e Marca** | `score_satisf_cliente × 100` | O cliente que ficou com o produto está satisfeito? |

**Interpretações cruzadas relevantes:**

| Combinação | Interpretação |
|---|---|
| Tração alta + Satisfação baixa | Produto vendido por tração de marketing, mas estruturalmente problemático — alta urgência de melhoria física |
| Unit Economics baixa + T&D alta | Duplo impacto financeiro: margem apertada e custo de reversa — candidato prioritário à descontinuação |
| Tração alta + Satisfação alta + T&D alta | Problema concentrado em SKU/cor específica (ex: transparência em cores claras) — correção cirúrgica recomendada |
| Cluster KILL + td_score alto | Produto já marcado para saída do portfólio com dor de cliente confirmada — descontinuação acelerada |

> ⚠️ Cobertura parcial: ~126 de 287 produtos têm scorecard preenchido em `eval_produto_portfolio`. Produtos ausentes não são excluídos do farol — o `sinal_kill_keep` continua funcionando via `commercial_score`.

---

## Gatilhos de Ação por Sinal

### 🔴 Priorizar Melhoria
**Ação imediata — time PF.**
1. Abrir diagnóstico do produto: ler `top_3_motivos_td` + tweet analítico
2. Verificar concentração de cor/tamanho (se ≥50% das reversas em uma única variante, a melhoria é cirúrgica)
3. Verificar tendência: se `tendencia_reversas = 'Aumentou'`, urgência dobra
4. Proposta de ação deve conter: (a) ajuste de curto prazo — comunicação/PDP, (b) ajuste de longo prazo — modelagem/gramatura

### 🟡 Alerta em Produto Relevante
**Monitoramento ativo — analytics + PF.**
1. Rastrear evolução do `td_score` mês a mês
2. Se `td_score` cruzar 0.70 no próximo ciclo → promoção automática para "Priorizar melhoria"
3. Não requer ação estrutural imediata, mas merece item de agenda em reunião mensal

### 🟠 Avaliar Descontinuação/Reformulação
**Decisão estratégica — Produto + Comercial + PF.**
1. Verificar `cluster` do produto: se KILL, descontinuação já prevista — T&D confirma
2. Verificar `score_unit_economics`: se < 30, produto sem margem e com dor de cliente — caso forte para saída
3. Reformulação só se justificada por hipótese clara (ex: único problema é gramatura em cor específica, não caimento sistêmico)

### ⚪ Não Priorizar Agora / Sem Evidência
**Sem ação — reprocessar no próximo ciclo mensal.**

---

## Limitações do Modelo

1. **`commercial_score` é relativo ao período** — um produto pode ter `commercial_score` baixo por estar em ramp-up (lançamento recente), não por ter baixa tração real. Cruzar com `cluster = 'Lancamento'` antes de recomendar descontinuação.

2. **Tags não cobrem 100% das reversas** — `pct_top_3_total` é percentual sobre reversas com ao menos uma tag. Produtos com baixa cobertura de tags têm diagnóstico qualitativo incompleto. O `td_rate` e `sinal_kill_keep` permanecem válidos mesmo sem tags.

3. **Janela de 12 meses** — reversas de pedidos anteriores à janela são excluídas. Produtos sazonais podem ter T&D subestimada fora do pico.

4. **Scorecard com cobertura parcial** — 161 produtos sem scorecard preenchido. Nesses casos, `score_tracao_comercial`, `score_unit_economics` e `score_satisfacao_marca` ficam em branco na aba `relatorio_pf`. O sinal de priorização não é afetado.

5. **Tweet analítico é heurístico** — `build_tweet_heuristic()` usa colunas pré-computadas pelo SQL. Não detecta padrões emergentes em comentários ainda não tagueados pelo time de LLM.

6. **Zona cinza do farol** — produtos com `td_score ∈ [0.50, 0.70)` e `commercial_score ∈ [0.40, 0.70)` caem em `Monitorar` via `ELSE`. São dois perfis distintos misturados nesse bucket; usar os scores brutos para distinguir.

---

## Glossário Rápido

| Termo | Definição |
|---|---|
| **T&D** | Trocas e Devoluções — evento de reversa gerado pelo cliente via Troquecommerce |
| **`td_rate`** | `itens_retornados / itens_vendidos` no período — taxa bruta de T&D do produto |
| **`delta_vs_categoria`** | `td_rate_produto − td_rate_categoria` — quanto o produto supera o benchmark da categoria |
| **`td_score`** | Score composto de dor do cliente (0–1, CUME_DIST) |
| **`commercial_score`** | Score composto de tração comercial (0–1, CUME_DIST) |
| **`priority_score`** | `0.6 × td_score + 0.4 × commercial_score` — ranking global |
| **`sinal_priorizacao`** | Farol operacional de 5 buckets — gerado em SQL |
| **`sinal_kill_keep`** | Decisão estratégica de 2 estados — gerado em Python |
| **`pct_top_3_total`** | % das reversas tagueadas que os 3 principais motivos representam — alavanca potencial |
| **Cluster** | Posicionamento estratégico do produto: Hero · Core · Long tail · KILL · Lancamento · Breakthrough |
