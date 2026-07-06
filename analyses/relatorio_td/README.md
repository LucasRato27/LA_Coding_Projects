# Priorização de Melhoria Física por T&D

* **O que é esse doc?** Diagnóstico de portfólio para identificar os produtos com maior necessidade de melhoria física, cruzando taxa de T&D, tags qualitativas de motivo e tração comercial. Serve como farol decisório para o time de Produto Físico (PF).
* **Por que?** Reversas representam custo operacional e sinal de dor do cliente. Identificar quais produtos têm alta dor **e** alta tração comercial permite priorizar esforço de melhoria com máximo retorno; produtos com alta dor e baixa tração são candidatos à descontinuação ou reformulação.
* **Responsável:** Analytics / Supply Chain & SOP
* **Quando foi feita a análise:** 11/06/2026
* **Período:** Últimos 12 meses | **BigQuery:** `insider-data-lake` · `insider-lake-sensitive`
* **Notebook:** `TD_Priorizacao_Melhorias_v20260611.ipynb`

---

## TL;DR

Análise de **287 produtos** com 12 meses de dados de reversas (Troquecommerce). **44 produtos** foram sinalizados como `Priorizar melhoria`, concentrando **36,8% do volume de T&D** com apenas **27,6% da receita** — desequilíbrio que indica dor estrutural desproporcional à contribuição comercial. O principal ofensor global é `caimento_ruim` (128 produtos afetados, 43 na zona de prioridade), seguido de `modelagem_ruim` e problemas de tamanho. **90,8% das reversas são de origem física** — não logística — o que aponta para oportunidade direta no produto. Pipeline equipado com farol de dois estados (`Priorizar melhoria` / `Não Priorizar — Avaliar Descontinuação`) e tweet analítico por produto para consumo direto do time PF.

---

## Resumo das Descobertas

### Status do Portfólio

* **44 produtos** merecem ação imediata (**Priorizar melhoria**): alta T&D e alta tração → melhoria estrutural gera retorno.
* **43 produtos** em **Alerta em produto relevante**: produtos de alto volume com dor moderada — monitoramento ativo justificado.
* **46 produtos** em **Monitorar** com T&D elevada e baixa tração → candidatos a `Não Priorizar (Avaliar Descontinuação/Reformulação)` via sinal consolidado QA.
* **49 produtos** com tendência de aumento de reversas nos últimos 3 meses — escalada a observar.

### Principais Motivos Globais de Insatisfação

* **`caimento_ruim`** — tag mais prevalente: 128 produtos afetados, 43 no bucket de prioridade. Sinal de desalinhamento sistêmico entre modelagem e expectativa do cliente.
* **`modelagem_ruim`** — 95 produtos, 31 no bucket de prioridade. Complementar ao caimento: o corte não favorece diferentes biotipos.
* **`tamanho_pequeno`** e **`tamanho_grande`** — cada um em ~68 produtos; as categorias Calça, Saia e Cropped femininas são as mais afetadas por fora de grade.
* **`tecido_transparente`** — tag de nicho (8 produtos), mas com share médio de 7,6% por produto quando presente — alta concentração por cor clara.
* **Origem física domina:** 90,8% das 242.383 reversas são problemas físicos (não logísticos), o que direciona o foco de melhoria ao produto em si.

---

## Metodologia

### Fontes de Dados

| Tabela | Uso |
|---|---|
| `business.insider_orders` | Pedidos válidos: `order_status = 'paid'`, `is_cancelled = FALSE`, exclusão cupons internos, lojas `shopify_insider-world` + `shopify_insider-store-loja` |
| `business.insider_order_items` | Itens de pedido: `sku`, `quantity`, `product_title`, `variant_color`, `variant_size` |
| `fpa.analytical_dre` | Receita e volume de vendas: `revenue_after_discounts`, `non_refunded_quantity` (agregado por `product_name`) |
| `integrated.skus` | Dimensão SKU (fallback): `product_name`, `category`, `gender`, `color`, `size`, `sku_state` |
| `prepared_br.prepared__troquecommerce_order_details_br` | Reversas ativas (status ≠ Cancelado), deduplicadas por `(order_name, id_reversa, sku)` |
| `sop_silver.return_reason_tags` | Tags qualitativas de motivo, deduplicadas por `(order_name, sku)` + UNNEST |
| `sop_silver.portfolio_skp_clustering` | Cluster estratégico/comercial do produto (grão `product_name`) |

**Parâmetros:** `min_items_vendidos = 30` · `min_items_returned = 5` · Timezone: `America/Sao_Paulo`

### Camadas de Output

| Camada | Granularidade | Uso |
|---|---|---|
| **Camada 2 — Executiva** | `product_name × category × gender` | Farol decisório, relatório, dashboard |
| **Camada 1 — Analítica** | `order_id × order_name × sku × problema_tag` | Auditoria, drill-down, investigação por produto |

### Score e Sinal de Priorização

```
commercial_score = 0.6 × percentil_receita + 0.4 × percentil_unidades

td_score = 0.5 × percentil_td_rate
         + 0.3 × percentil_volume_td
         + 0.2 × percentil_delta_vs_categoria

priority_score = 0.5 × td_score + 0.5 × commercial_score
```

Todos os percentis são calculados com `CUME_DIST()` sobre o universo de produtos com volume suficiente.

> **Tração Comercial — Pilares do scorecard:** Os três pilares são extraídos de uma query direta a `sop_bronze.eval_produto_portfolio` na seção 9 do notebook. Mapeamento: `score_vendas_geral × 100` → Score Tração Comercial · `score_viabilidade_financeira × 100` → Score Unit Economics · `score_satisf_cliente × 100` → Score Satisfação e Marca. Produtos sem entrada nessa tabela aparecem com `—` apenas na seção 9 — o `sinal_kill_keep` não é afetado, pois usa `commercial_score` (CUME_DIST).

### Sistema de Sinais (dois níveis)

**Nível 1 — `sinal_priorizacao` (5 buckets, gerado em SQL):**

| Sinal | Critério | Interpretação |
|---|---|---|
| **Priorizar melhoria** | td_score ≥ 0.70 e commercial_score ≥ 0.60 | Alta dor + alta tração → ação imediata |
| **Alerta em produto relevante** | commercial_score ≥ 0.70 e 0.50 ≤ td_score < 0.70 | Produto importante com dor moderada crescendo |
| **Monitorar** | td_score ≥ 0.70 e commercial_score < 0.60 (ou zona cinza médio-médio) | Dor alta mas impacto comercial limitado |
| **Não priorizar agora** | td_score < 0.50 ou commercial_score < 0.40 | Dor baixa ou produto sem tração relevante |
| **Sem evidência suficiente** | < 30 itens vendidos ou < 5 itens retornados | Volume insuficiente para diagnóstico |

**Nível 2 — `sinal_kill_keep` (regra QA spec, gerado em Python via `np.select`):**

| Sinal | Critério | Decisão |
|---|---|---|
| **Priorizar melhoria** | td_score ≥ 0.70 e commercial_score ≥ 0.60 | Alta T&D + Alta Tração → Investir na melhoria |
| **Não Priorizar (Avaliar Descontinuação/Reformulação)** | td_score ≥ 0.70 e commercial_score < 0.60 | Alta T&D + Baixa Tração → Avaliar saída ou reformulação |
| **Não priorizar agora** | demais casos com evidência suficiente | Dor ou tração insuficientes para ação imediata |
| **Sem evidência suficiente** | < 30 vendas ou < 5 retornos | Volume insuficiente |

### Classificação de Problemas (`tipo_problema`)

| Tipo | Motivos incluídos |
|---|---|
| **Físico** | Tamanho (grande/pequeno), Defeito, Desbotamento, Insatisfação com o produto, Cor diferente do esperado, Peça íntima |
| **Logístico** | Produto errado, Recebi novo pedido de reposição, Recebi pedido duplicado, Falha/Demora na entrega |
| **Desistência** | Arrependimento, Desistência |
| **Outros** | Motivos não mapeados |

---

## Resultados

> Gerado em 11/06/2026 via `TD_Priorizacao_Melhorias_v20260611.ipynb` a partir dos dados BigQuery (últimos 12 meses).

### 1. Resumo do Portfólio por Farol

| Farol | Produtos | Unid. Vendidas | Receita Líquida | Itens Retornados | TD Rate Médio | Share Receita | Share T&D |
|---|---|---|---|---|---|---|---|
| Priorizar melhoria | 44 | 681.663 | R$ 127.497.816 | 89.078 | 14,1% | 27,6% | 36,8% |
| Alerta em produto relevante | 43 | 2.523.665 | R$ 295.942.834 | 133.797 | 6,3% | 64,2% | 55,2% |
| Monitorar | 46 | 90.375 | R$ 13.335.755 | 9.554 | 13,0% | 2,9% | 3,9% |
| Sem evidência suficiente | 88 | 4.069 | R$ 490.459 | 70 | 3,9% | 0,1% | 0,0% |
| Não priorizar agora | 66 | 656.736 | R$ 23.975.731 | 9.884 | 4,2% | 5,2% | 4,1% |
| **Total** | **287** | **3.956.508** | **R$ 461.242.596** | **242.383** | — | 100% | 100% |

### 2. Top Produtos — Priorizar Melhoria

| Produto | Categoria | Gênero | Cluster | TD Rate | Delta vs Cat. | Itens Retorn. | Físico | Logístico | Receita Líq. | Score | Top Problema | Tendência |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Calça Pantalona InLounge Feminino | Calça | female | KILL | 23,8% | 7,6% | 10.390 | 9.652 | 0 | R$ 9.556.780 | 0,963 | tamanho_grande | Estável |
| Calça FutureForm Feminino | Calça | female | Hero | 16,9% | 0,7% | 4.712 | 4.198 | 16 | R$ 9.607.364 | 0,904 | caimento_ruim | Estável |
| Saia Mini Kyoto Feminino | Saia | female | Core | 20,4% | 8,7% | 2.417 | 2.157 | 3 | R$ 2.743.746 | 0,897 | caimento_ruim | Aumentou |
| Skin Cropped Long Sleeve Feminino | Cropped | female | Long tail | 15,7% | 5,7% | 3.427 | 3.209 | 9 | R$ 2.526.286 | 0,892 | tamanho_pequeno | Aumentou |
| Tube Dress Feminino | Vestido | female | Core | 16,9% | 0,4% | 3.796 | 3.475 | 5 | R$ 6.245.757 | 0,890 | tamanho_pequeno | Caiu |
| Camiseta Oversized Comfy InLounge | T-shirt | unisex | KILL | 19,1% | 13,5% | 2.191 | 1.943 | 0 | R$ 1.891.724 | 0,874 | tamanho_grande | Estável |
| Camisa FutureForm Masculino | Camisa Social | male | Core | 14,2% | 0,5% | 2.918 | 2.713 | 10 | R$ 7.289.586 | 0,873 | tamanho_grande | Aumentou |
| Camisa FutureForm Feminino | Camisa Social | female | Core | 13,5% | -0,1% | 3.406 | 3.133 | 8 | R$ 9.148.312 | 0,866 | tamanho_grande | Aumentou |
| Bermuda Kyoto Feminino | Bermuda | female | Core | 16,8% | 4,4% | 1.746 | 1.566 | 5 | R$ 2.728.231 | 0,856 | caimento_ruim | Estável |
| Shorts Kyoto Feminino | Shorts | female | Core | 18,2% | -0,6% | 2.998 | 2.774 | 10 | R$ 3.575.254 | 0,856 | caimento_ruim | Estável |
| Easy Legging Feminino | Calça Legging | female | KILL | 13,0% | 1,5% | 2.514 | 2.289 | 8 | R$ 3.667.800 | 0,853 | tecido_transparente | Aumentou |
| Saia Midi Kyoto Feminino | Saia | female | Core | 13,4% | 1,7% | 1.860 | 1.741 | 11 | R$ 5.436.947 | 0,848 | tamanho_pequeno | Estável |
| Tech T-shirt Heavy Slim Masculino | T-shirt | male | Core | 10,8% | 5,3% | 2.382 | 1.989 | 11 | R$ 3.259.390 | 0,841 | tamanho_pequeno | Caiu |
| Core T-shirt Masculino | T-shirt | male | — | 7,9% | 2,4% | 9.683 | 8.185 | 65 | R$ 15.809.413 | 0,840 | tamanho_pequeno | Estável |
| Shorts Esportivo Serotonin Feminino | Shorts Esportivo | female | Long tail | 17,6% | 7,7% | 1.259 | 1.154 | 2 | R$ 1.327.429 | 0,838 | tamanho_pequeno | Aumentou |

### 3. Alerta em Produto Relevante

| Produto | Categoria | Cluster | TD Rate | Delta vs Cat. | Receita Líq. | Share Receita | Itens Retorn. | Comm. Score | TD Score | Top Problema |
|---|---|---|---|---|---|---|---|---|---|---|
| The Perfect Top Feminino | Regata | Long tail | 7,1% | -0,2% | R$ 40.611.814 | 8,8% | 27.799 | 0,997 | 0,682 | tamanho_pequeno |
| Maxi Saia NYIN Feminino | Saia | Hero | 9,0% | -2,7% | R$ 13.794.385 | 3,0% | 3.946 | 0,967 | 0,669 | caimento_ruim |
| Tech T-shirt Heavy Masculino | T-shirt | Core | 7,1% | 1,5% | R$ 5.029.943 | 1,1% | 2.490 | 0,920 | 0,693 | tamanho_pequeno |
| Tech T-shirt Gola U Masculino | T-shirt | Hero | 5,1% | -0,5% | R$ 73.687.351 | 16,0% | 35.649 | 1,000 | 0,633 | tamanho_pequeno |
| Camiseta Polo Core Masculino | T-shirt | Hero | 5,7% | 0,1% | R$ 9.729.864 | 2,1% | 2.880 | 0,957 | 0,658 | tamanho_grande |
| Future Shorts 200 Masculino | Bermuda | Hero | 10,6% | -1,8% | R$ 4.288.386 | 0,9% | 1.931 | 0,882 | 0,699 | modelagem_ruim |
| Daily T-shirt Masculino | T-shirt | Hero | 5,1% | -0,4% | R$ 11.047.667 | 2,4% | 5.785 | 0,978 | 0,632 | feedback_positivo_geral |
| Camiseta Henley Core Masculino | T-shirt | Core | 6,4% | 0,9% | R$ 5.706.775 | 1,2% | 2.003 | 0,924 | 0,661 | tamanho_grande |
| Wingsuit Feminino | Casaco | Hero | 5,4% | -0,8% | R$ 19.277.967 | 4,2% | 2.931 | 0,974 | 0,615 | caimento_ruim |
| Calça FutureForm Masculino | Calça | Hero | 9,4% | -6,8% | R$ 12.396.398 | 2,7% | 3.422 | 0,959 | 0,621 | tamanho_pequeno |

### 4. Benchmark por Categoria

| Categoria | Produtos | Unid. Vendidas | Receita Líq. | Itens Retornados | TD Rate Categoria | Produtos "Priorizar" |
|---|---|---|---|---|---|---|
| T-shirt | 117 | 1.595.574 | R$ 188.673.399 | 88.768 | 5,6% | 8 |
| Vestido | 9 | 47.936 | R$ 14.127.739 | 7.921 | 16,5% | 5 |
| Calça | 14 | 150.648 | R$ 43.968.855 | 24.421 | 16,2% | 4 |
| Cropped | 10 | 126.978 | R$ 11.158.102 | 12.738 | 10,0% | 4 |
| Saia | 6 | 85.478 | R$ 25.320.176 | 9.974 | 11,7% | 3 |
| Tops | 7 | 47.359 | R$ 4.613.780 | 4.981 | 10,5% | 3 |
| Regata | 6 | 425.292 | R$ 46.856.089 | 31.158 | 7,3% | 2 |
| Camisa Social | 3 | 48.377 | R$ 17.111.480 | 6.605 | 13,7% | 2 |
| Calça Legging | 6 | 48.187 | R$ 9.933.063 | 5.532 | 11,5% | 2 |
| Shorts Esportivo | 7 | 51.840 | R$ 7.909.130 | 5.147 | 9,9% | 2 |
| Bermuda | 4 | 37.047 | R$ 8.596.859 | 4.604 | 12,4% | 2 |
| Top Esportivo | 5 | 37.758 | R$ 3.831.971 | 3.541 | 9,4% | 2 |
| Underwear | 8 | 135.859 | R$ 7.676.997 | 6.025 | 4,4% | 1 |
| Shorts | 2 | 18.425 | R$ 3.847.467 | 3.457 | 18,8% | 1 |
| Beachwear | 8 | 14.336 | R$ 1.574.904 | 1.830 | 12,8% | 1 |
| Vestido Esportivo | 1 | 8.566 | R$ 1.457.082 | 1.760 | 20,5% | 1 |
| Body | 4 | 12.576 | R$ 1.856.462 | 1.153 | 9,2% | 1 |
| Cueca | 6 | 440.499 | R$ 24.851.909 | 9.936 | 2,3% | 0 |
| Casaco | 15 | 59.655 | R$ 20.695.184 | 3.647 | 6,1% | 0 |
| Meia | 6 | 341.788 | R$ 7.140.924 | 3.313 | 1,0% | 0 |
| Regata Esportivo | 3 | 40.103 | R$ 3.820.116 | 2.059 | 5,1% | 0 |
| Acessório | 17 | 58.908 | R$ 2.105.453 | 1.285 | 2,2% | 0 |
| Calcinha | 3 | 27.595 | R$ 647.999 | 1.221 | 4,4% | 0 |
| Embalagem de Presente | 1 | 68.937 | R$ 496.601 | 825 | 1,2% | 0 |
| Undershirt | 5 | 16.727 | R$ 2.194.815 | 233 | 1,4% | 0 |

### 5. Top Problemas por Tag

| Tag do Problema | Produtos Afetados | Produtos "Priorizar" | % Médio no Produto |
|---|---|---|---|
| caimento_ruim | 128 | 43 | 8,6% |
| modelagem_ruim | 95 | 31 | 8,3% |
| tamanho_pequeno | 68 | 22 | 10,1% |
| tamanho_grande | 67 | 18 | 8,5% |
| feedback_positivo_geral | 45 | 8 | 7,0% |
| tecido_transparente | 8 | 5 | 7,6% |
| tecido_fino | 2 | 2 | 7,3% |
| conforto_negativo | 12 | 1 | 10,6% |
| cor_diferente_site | 11 | 1 | 4,4% |
| tecido_grosso | 3 | 1 | 6,3% |
| tecido_qualidade_ruim | 24 | 0 | 9,3% |
| logistica_item_errado | 14 | 0 | 25,3% |
| defeito_costura | 13 | 0 | 5,2% |
| defeito_furo_rasgo | 8 | 0 | 10,6% |
| atendimento_ineficiente | 5 | 0 | 10,7% |

### 6. Split por Tipo de Problema — Físico vs Logístico

| Tipo de Problema | Reversas | % do Total |
|---|---|---|
| Físico | 204.779 | 90,8% |
| Outros / Desistência | 19.163 | 8,5% |
| Logístico | 1.481 | 0,7% |

### 7. Tendência de Reversas (últimos 3 meses vs 3 meses anteriores)

| Tendência | Produtos | Reversas Últ. 3m | Reversas 3m Ant. | Itens Retornados |
|---|---|---|---|---|
| Aumentou nos últimos 3 meses | 49 | 19.580 | 9.975 | 51.348 |
| Apareceu nos últimos 3 meses | 12 | 585 | 0 | 593 |
| Estável | 42 | 28.544 | 28.801 | 112.306 |
| Caiu nos últimos 3 meses | 70 | 11.700 | 20.111 | 77.503 |
| Sem volume para tendência | 57 | 12 | 26 | 633 |

### 8. Concentração de Cor e Tamanho nos Top Produtos

Filtra produtos em `Priorizar melhoria` ou `Alerta em produto relevante` com concentração ≥ 50% de reversas em uma única cor ou tamanho. Identifica se o problema tem origem específica (ex: `tecido_transparente` exclusivo em Off White) ou se é difuso na grade inteira. Ver notebook seção 8 para lista completa.

### 9. Visão por Produto — Sinal Consolidado (`relatorio_pf`)

Base da aba **`relatorio_pf`** exportada no workbook, construída via `build_scorecard_product_view()`. A coluna `sinal_kill_keep` aplica a regra QA de dois estados. A coluna `Top 3 Motivos de T&D` é gerada em HTML (`<br>` entre itens) para consumo direto no Excel do time PF.

> *Fonte dos scores:* Os três pilares vêm de uma query direta a `sop_bronze.eval_produto_portfolio` executada na seção 9 do notebook. Mapeamento: `score_vendas_geral × 100` → Score Tração Comercial · `score_viabilidade_financeira × 100` → Score Unit Economics · `score_satisf_cliente × 100` → Score Satisfação e Marca. Produtos sem entrada nessa tabela aparecem com `—` nas colunas de score — são 11 produtos no portfólio atual (ex: Core T-shirt Masculino, Easy Legging Feminino) que não possuem avaliação de scorecard. O `sinal_kill_keep` usa `td_score` e `commercial_score` (CUME_DIST) — **independente** dos pilares de scorecard — e não é afetado por scores em branco.

| Produto | Cluster | Sinal QA (`sinal_kill_keep`) | Score Tração Comercial | Score Unit Economics | Score Satisfação e Marca | T&D Categoria | T&D Produto | Top 3 Motivos de T&D |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| **Calça Pantalona InLounge Feminino** | KILL | Priorizar melhoria | 84,8 | 53,3 | 8,1 | 16,2% | 23,8% | 1. tamanho_grande (17%)<br>2. caimento_ruim (7%)<br>3. feedback_positivo_geral (6%) |
| **Calça FutureForm Feminino** | Hero | Priorizar melhoria | 78,6 | 19,5 | 11,1 | 16,2% | 16,9% | 1. caimento_ruim (14%)<br>2. modelagem_ruim (12%)<br>3. feedback_positivo_geral (5%) |
| **Saia Mini Kyoto Feminino** | Core | Priorizar melhoria | 56,4 | 31,1 | 10,2 | 11,7% | 20,4% | 1. caimento_ruim (13%)<br>2. modelagem_ruim (8%)<br>3. tamanho_grande (6%) |
| **Skin Cropped Long Sleeve Feminino** | Long tail | Priorizar melhoria | 53,4 | 69,5 | 9,5 | 10,0% | 15,7% | 1. tamanho_pequeno (13%)<br>2. caimento_ruim (4%)<br>3. modelagem_ruim (4%) |
| **Tube Dress Feminino** | Core | Priorizar melhoria | 79,5 | 11,6 | 21,4 | 16,5% | 16,9% | 1. tamanho_pequeno (16%)<br>2. caimento_ruim (12%)<br>3. feedback_positivo_geral (7%) |
| **Camiseta Oversized Comfy InLounge** | KILL | Priorizar melhoria | 58,9 | 80,7 | 10,5 | 5,6% | 19,1% | 1. tamanho_grande (14%)<br>2. feedback_positivo_geral (6%)<br>3. caimento_ruim (6%) |
| **Camisa FutureForm Masculino** | Core | Priorizar melhoria | 67,1 | 13,9 | 45,8 | 13,7% | 14,2% | 1. tamanho_grande (11%)<br>2. feedback_positivo_geral (4%)<br>3. caimento_ruim (3%) |
| **Camisa FutureForm Feminino** | Core | Priorizar melhoria | 72,2 | 23,4 | 44,4 | 13,7% | 13,5% | 1. tamanho_grande (10%)<br>2. feedback_positivo_geral (6%)<br>3. cor_diferente_site (5%) |
| **Bermuda Kyoto Feminino** | Core | Priorizar melhoria | 60,0 | 63,0 | 35,2 | 12,4% | 16,8% | 1. caimento_ruim (10%)<br>2. tamanho_grande (10%)<br>3. modelagem_ruim (7%) |
| **Shorts Kyoto Feminino** | Core | Priorizar melhoria | 67,3 | 57,2 | 27,1 | 18,8% | 18,2% | 1. caimento_ruim (11%)<br>2. modelagem_ruim (8%)<br>3. tamanho_grande (7%) |
| **Easy Legging Feminino** | KILL | Priorizar melhoria | — | — | — | 11,5% | 13,0% | 1. tecido_transparente (7%)<br>2. caimento_ruim (7%)<br>3. tamanho_grande (5%) |
| **Saia Midi Kyoto Feminino** | Core | Priorizar melhoria | 56,8 | 41,9 | 40,2 | 11,7% | 13,4% | 1. tamanho_pequeno (9%)<br>2. feedback_positivo_geral (6%)<br>3. caimento_ruim (5%) |
| **Tech T-shirt Heavy Slim Masculino** | Core | Priorizar melhoria | 74,9 | 92,9 | 33,4 | 5,6% | 10,8% | 1. tamanho_pequeno (6%)<br>2. caimento_ruim (3%)<br>3. modelagem_ruim (3%) |
| **Core T-shirt Masculino** | — | Priorizar melhoria | — | — | — | 5,6% | 7,9% | 1. tamanho_pequeno (6%)<br>2. caimento_ruim (3%)<br>3. tecido_grosso (3%) |
| **Shorts Esportivo Serotonin Feminino** | Long tail | Priorizar melhoria | 45,5 | 35,1 | 19,7 | 9,9% | 17,6% | 1. tamanho_pequeno (9%)<br>2. caimento_ruim (8%)<br>3. modelagem_ruim (8%) |
| **Calça Wide Leg Feminino** | Core | Priorizar melhoria | 58,7 | 49,6 | 17,6 | 16,2% | — | 1. caimento_ruim (8%)<br>2. modelagem_ruim (5%)<br>3. tamanho_grande (4%) |
| **Structure Tank Feminino** | Long tail | Priorizar melhoria | 63,4 | 89,0 | 41,4 | 7,3% | — | 1. caimento_ruim (9%)<br>2. modelagem_ruim (7%)<br>3. tamanho_grande (7%) |
| **Vestido Esportivo Rush Feminino** | Long tail | Priorizar melhoria | 35,1 | 41,9 | 13,0 | 20,5% | — | 1. tamanho_pequeno (11%)<br>2. caimento_ruim (9%)<br>3. modelagem_ruim (8%) |
| **Structure Cropped Feminino** | Long tail | Priorizar melhoria | 60,6 | 85,9 | 20,8 | 10,0% | — | 1. tamanho_pequeno (7%)<br>2. caimento_ruim (5%)<br>3. modelagem_ruim (3%) |
| **Vestido Tube Dress Curto Feminino** | Lancamento | Priorizar melhoria | 39,3 | 22,4 | 16,8 | 16,5% | — | 1. tamanho_pequeno (17%)<br>2. caimento_ruim (13%)<br>3. modelagem_ruim (7%) |
| **Daily T-shirt Feminino** | Long tail | Priorizar melhoria | 79,9 | 58,2 | 73,8 | 5,6% | — | 1. tamanho_pequeno (7%)<br>2. caimento_ruim (6%)<br>3. modelagem_ruim (5%) |
| **Motion Shorts Feminino** | — | Priorizar melhoria | — | — | — | 9,9% | — | 1. tecido_transparente (7%)<br>2. tamanho_grande (6%)<br>3. caimento_ruim (5%) |
| **Oversized T-shirt** | — | Priorizar melhoria | — | — | — | 5,6% | — | 1. tamanho_grande (3%)<br>2. modelagem_ruim (3%)<br>3. caimento_ruim (2%) |
| **Regata Comfy InLounge Feminino** | KILL | Priorizar melhoria | 73,1 | 89,1 | 54,4 | 7,3% | — | 1. caimento_ruim (12%)<br>2. modelagem_ruim (10%)<br>3. tamanho_pequeno (5%) |
| **Saia Envelope Breeze Feminino** | Core | Priorizar melhoria | 62,3 | 70,9 | 23,1 | 11,7% | — | 1. caimento_ruim (11%)<br>2. modelagem_ruim (7%)<br>3. tecido_fino (7%) |
| **Boxy Cropped Feminino** | Long tail | Priorizar melhoria | 39,9 | 61,6 | 53,0 | 10,0% | — | 1. tamanho_pequeno (8%)<br>2. caimento_ruim (7%)<br>3. modelagem_ruim (4%) |
| **Calça Reta Ajustável FutureForm Feminino** | KILL | Priorizar melhoria | 16,2 | 5,5 | 39,9 | 16,2% | — | 1. caimento_ruim (15%)<br>2. tamanho_grande (15%)<br>3. modelagem_ruim (11%) |
| **Skin Cropped Feminino** | Long tail | Priorizar melhoria | 68,4 | 19,3 | 55,3 | 10,0% | — | 1. tamanho_pequeno (8%)<br>2. tecido_transparente (4%)<br>3. caimento_ruim (3%) |
| **Vestido Midi de Alça FutureForm Feminino** | Long tail | Priorizar melhoria | 29,1 | 86,7 | 11,1 | 16,5% | — | 1. caimento_ruim (22%)<br>2. tamanho_grande (14%)<br>3. modelagem_ruim (13%) |
| **Body InSculpt Feminino** | KILL | Priorizar melhoria | 24,8 | 10,3 | 8,7 | 9,2% | — | 1. caimento_ruim (22%)<br>2. modelagem_ruim (21%)<br>3. conforto_negativo (13%) |
| **Timeless Deep Cut Swimsuit Feminino** | — | Priorizar melhoria | — | — | — | 12,8% | — | 1. modelagem_ruim (10%)<br>2. caimento_ruim (9%)<br>3. tamanho_pequeno (8%) |
| **Chemise FutureForm Feminino** | Core | Priorizar melhoria | 43,5 | 31,7 | 41,5 | 13,7% | — | 1. tamanho_grande (16%)<br>2. caimento_ruim (9%)<br>3. feedback_positivo_geral (9%) |
| **The Perfect Top T-shirt Feminino** | — | Priorizar melhoria | — | — | — | 5,6% | — | 1. tamanho_pequeno (13%)<br>2. caimento_ruim (9%)<br>3. modelagem_ruim (6%) |
| **Everyday Shorts InLounge Masculino** | KILL | Priorizar melhoria | 50,1 | 71,9 | 22,6 | 18,8% | — | 1. tecido_fino (7%)<br>2. caimento_ruim (7%)<br>3. modelagem_ruim (7%) |
| **Vestido Curto Gola Canoa FutureForm Feminino** | Long tail | Priorizar melhoria | 29,2 | 79,4 | 13,2 | 16,5% | — | 1. caimento_ruim (18%)<br>2. tamanho_grande (12%)<br>3. modelagem_ruim (11%) |
| **Sutiã Comfy Feminino** | Long tail | Priorizar melhoria | 35,6 | 51,7 | 25,8 | 4,4% | — | 1. caimento_ruim (12%)<br>2. modelagem_ruim (9%)<br>3. tamanho_grande (7%) |
| **Action Top Feminino** | — | Priorizar melhoria | — | — | — | 9,4% | — | 1. tamanho_pequeno (8%)<br>2. caimento_ruim (7%)<br>3. modelagem_ruim (6%) |
| **The Perfect Top Asymmetric Feminino** | Long tail | Priorizar melhoria | 45,3 | 25,0 | 6,1 | 7,3% | — | 1. caimento_ruim (22%)<br>2. modelagem_ruim (14%)<br>3. tamanho_grande (7%) |
| **Blusa Manga Longa Comfy InLounge** | KILL | Não Priorizar (Avaliar Descontinuação/Reformulação) | 15,5 | 40,5 | 10,0 | 5,6% | — | 1. tamanho_grande (18%)<br>2. feedback_positivo_geral (5%)<br>3. caimento_ruim (4%) |
| **Calça Studio InLounge Masculino** | KILL | Não Priorizar (Avaliar Descontinuação/Reformulação) | 24,4 | 20,5 | 0,0 | 16,2% | — | 1. tamanho_grande (6%)<br>2. modelagem_ruim (3%)<br>3. caimento_ruim (3%) |
| **Legging Fitness IN-ACTION Seamless Feminino** | — | Não Priorizar (Avaliar Descontinuação/Reformulação) | — | — | — | 11,5% | — | 1. tamanho_pequeno (17%)<br>2. conforto_negativo (11%)<br>3. caimento_ruim (8%) |
| **Shorts Boxy InLounge Feminino** | — | Não Priorizar (Avaliar Descontinuação/Reformulação) | — | — | — | 18,8% | — | 1. tamanho_grande (11%)<br>2. caimento_ruim (11%)<br>3. modelagem_ruim (10%) |
| **Vestido Sharp InLounge Feminino** | — | Não Priorizar (Avaliar Descontinuação/Reformulação) | — | — | — | 16,5% | — | 1. caimento_ruim (25%)<br>2. modelagem_ruim (25%)<br>3. tamanho_grande (6%) |
| **The Perfect Top Feminino** | Long tail | Não priorizar agora | 95,0 | 20,6 | 60,7 | 7,3% | 7,1% | 1. tamanho_pequeno (6%)<br>2. caimento_ruim (6%)<br>3. modelagem_ruim (5%) |
| **Maxi Saia NYIN Feminino** | Hero | Não priorizar agora | 85,2 | 29,7 | 61,9 | 11,7% | 9,0% | 1. caimento_ruim (13%)<br>2. modelagem_ruim (8%)<br>3. tamanho_grande (5%) |
| **Tech T-shirt Heavy Masculino** | Core | Não priorizar agora | 82,8 | 94,1 | 26,1 | 5,6% | 7,1% | 1. tamanho_pequeno (8%)<br>2. caimento_ruim (4%)<br>3. modelagem_ruim (3%) |
| **Tech T-shirt Gola U Masculino** | Hero | Não priorizar agora | 100,0 | 16,4 | 24,8 | 5,6% | 5,1% | 1. tamanho_pequeno (6%)<br>2. feedback_positivo_geral (3%)<br>3. caimento_ruim (2%) |
| **Camiseta Polo Core Masculino** | Hero | Não priorizar agora | 89,0 | 69,2 | 61,0 | 5,6% | 5,7% | 1. tamanho_grande (4%)<br>2. feedback_positivo_geral (3%)<br>3. tecido_grosso (3%) |
| **Future Shorts 200 Masculino** | Hero | Não priorizar agora | 71,6 | 20,5 | 46,6 | 12,4% | 10,6% | 1. modelagem_ruim (8%)<br>2. caimento_ruim (6%)<br>3. tamanho_grande (6%) |
| **Daily T-shirt Masculino** | Hero | Não priorizar agora | 90,5 | 50,8 | 61,3 | 5,6% | 5,1% | 1. feedback_positivo_geral (4%)<br>2. tamanho_pequeno (3%)<br>3. tamanho_grande (3%) |
| **Camiseta Henley Core Masculino** | Core | Não priorizar agora | 79,2 | 78,7 | 67,4 | 5,6% | 6,4% | 1. tamanho_grande (4%)<br>2. caimento_ruim (4%)<br>3. modelagem_ruim (4%) |
| **Wingsuit Feminino** | Hero | Não priorizar agora | 84,2 | 39,7 | 79,6 | 6,1% | 5,4% | 1. caimento_ruim (7%)<br>2. modelagem_ruim (6%)<br>3. cor_diferente_site (4%) |
| **Calça FutureForm Masculino** | Hero | Não priorizar agora | 86,2 | 56,1 | 50,8 | 16,2% | 9,4% | 1. tamanho_pequeno (5%)<br>2. caimento_ruim (5%)<br>3. tecido_qualidade_ruim (4%) |
| **NEXTECH T-shirt Masculino** | Core | Não priorizar agora | 70,8 | 59,4 | 58,1 | 5,6% | 9,5% | 1. tamanho_grande (8%)<br>2. tecido_transparente (7%)<br>3. caimento_ruim (5%) |
| **Tech T-shirt Gola U Feminino** | Hero | Não priorizar agora | 89,7 | 68,7 | 79,9 | 5,6% | — | 1. tamanho_pequeno (5%)<br>2. caimento_ruim (3%)<br>3. cor_diferente_site (2%) |
| **Stirrup Legging Feminino** | Long tail | Não priorizar agora | 49,9 | 36,9 | 22,4 | 11,5% | — | 1. tamanho_grande (10%)<br>2. caimento_ruim (9%)<br>3. comprimento_longo (7%) |
| **Performance T-shirt 2.0 Masculino** | Long tail | Não priorizar agora | 54,1 | 9,2 | 36,7 | 5,6% | — | 1. tamanho_pequeno (7%)<br>2. modelagem_ruim (6%)<br>3. caimento_ruim (4%) |
| **Energy Top Feminino** | — | Não priorizar agora | — | — | — | 9,4% | — | 1. tamanho_pequeno (6%)<br>2. caimento_ruim (5%)<br>3. modelagem_ruim (4%) |
| **Tech T-shirt Gola V Masculino** | Hero | Não priorizar agora | 89,5 | 9,8 | 66,4 | 5,6% | — | 1. feedback_positivo_geral (3%)<br>2. tamanho_pequeno (3%)<br>3. tamanho_grande (2%) |
| **Cueca Boxer Performance Anti Suor Masculino** | Long tail | Não priorizar agora | 89,7 | 29,8 | 89,2 | 2,3% | — | 1. defeito_costura (9%)<br>2. feedback_positivo_geral (7%)<br>3. defeito_furo_rasgo (5%) |

### 10. Tweet Analista — Top Prioridades

Resumo analítico por produto gerado via `build_tweet_heuristic()` (`td_analysis_functions.py`). Combina Top 3 motivos (com % de alavanca), T&D do produto vs categoria, e tendência recente. Concentração de cor ou tamanho é incluída quando ≥ 50% das reversas. **Fonte: heurística baseada em regras — não usa LLM.**

---

> **Calça Pantalona InLounge Feminino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** tamanho_grande, caimento_ruim e feedback_positivo_geral
> * **Tweet:** Ofensores: tamanho_grande (17%), caimento_ruim (7%), feedback_positivo_geral (6%). T&D: 23,8% produto vs 16,2% categoria (7,6pp acima). Produto KILL com o maior volume de retornos da categoria Calça (10.390 itens). Tendência: Estável.

---

> **Calça FutureForm Feminino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** caimento_ruim, modelagem_ruim e feedback_positivo_geral
> * **Tweet:** Ofensores: caimento_ruim (14%), modelagem_ruim (12%), feedback_positivo_geral (5%). T&D: 16,9% produto vs 16,2% categoria (0,7pp acima). Produto Hero com dor estrutural em caimento e modelagem — 4.712 retornos no período. Tendência: Estável.

---

> **Saia Mini Kyoto Feminino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** caimento_ruim, modelagem_ruim e tamanho_grande
> * **Tweet:** Ofensores: caimento_ruim (13%), modelagem_ruim (8%), tamanho_grande (6%). T&D: 20,4% produto vs 11,7% categoria (8,7pp acima). Tendência recente: Aumentou nos últimos 3 meses — escalada em caimento e modelagem requer ação prioritária.

---

> **Core T-shirt Masculino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** tamanho_pequeno, caimento_ruim e tecido_grosso
> * **Tweet:** Ofensores: tamanho_pequeno (6%), caimento_ruim (3%), tecido_grosso (3%). T&D: 7,9% produto vs 5,6% categoria (2,4pp acima). Maior volume absoluto de retornos em T-shirt masculina (9.683 itens); alavanca do Top 3 distribuída. Tendência: Estável.

---

> **Camisa FutureForm Masculino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** tamanho_grande, feedback_positivo_geral e caimento_ruim
> * **Tweet:** Ofensores: tamanho_grande (11%), feedback_positivo_geral (4%), caimento_ruim (3%). T&D: 14,2% produto vs 13,7% categoria (0,5pp acima). Tendência recente: Aumentou nos últimos 3 meses — viés de tamanho grande na modelagem masculina em escalada.

---

> **Easy Legging Feminino** — Sinal: Priorizar melhoria
> * **Top 3 problemas:** tecido_transparente, caimento_ruim e tamanho_grande
> * **Tweet:** Ofensores: tecido_transparente (7%), caimento_ruim (7%), tamanho_grande (5%). T&D: 13,0% produto vs 11,5% categoria (1,5pp acima). Produto KILL com problema de opacidade em escalada. Tendência recente: Aumentou nos últimos 3 meses.

---

## Sugestão de Atuação para Melhoria de Produtos

Com base nos ofensores identificados, os maiores drivers de T&D são caimento e modelagem inadequados (sistêmicos em Calça, Saia e Cropped femininos), seguidos de fora de grade em tamanho e transparência de tecido em produtos específicos. Propõem-se duas frentes:

### Ações de Curto Prazo (Ajuste de Comunicação e Expectativa)

| Ação | Detalhamento |
| :--- | :--- |
| **Avisos de tamanho nas PDPs** | Para produtos com `tamanho_grande` ou `tamanho_pequeno` como top 1, adicionar alerta na seleção de tamanho indicando o viés da modelagem (ex: "Modelagem tende a ser maior que o padrão — considere um tamanho menor"). |
| **Recalibrar provador virtual** | Ajustar sugestão de tamanho na IA do site para os produtos com viés de grade confirmado por volume de reversas. |
| **Revisar fotos e descrição de tecido** | Para produtos com `tecido_transparente` ou `tecido_fino` como ofensor top, revisar imagens de divulgação e remover linguagem de "tecido encorpado" que cria expectativa incorreta (padrão identificado no diagnóstico Nextech). |

### Ações de Longo Prazo (Melhoria Estrutural e de Produto)

| Ação | Produtos prioritários |
| :--- | :--- |
| **Revisão de modelagem feminina** | Calça Pantalona InLounge, Tube Dress, Saia Mini e Midi Kyoto, Skin Cropped — caimento e tamanho como top ofensores, categorias acima de 8pp vs categoria. |
| **Revisão de grade masculina** | Camisa FutureForm Masculino, Core T-shirt — viés de tamanho grande em escalada nos últimos 3 meses. |
| **Revisão de gramatura em cores claras** | Easy Legging, Motion Shorts, Skin Cropped — produtos com `tecido_transparente` concentrado em Off White / Branco com tendência de aumento. |
| **Avaliação de descontinuação** | Produtos com `sinal_kill_keep = 'Não Priorizar (Avaliar Descontinuação/Reformulação)'` e cluster KILL: Blusa Manga Longa Comfy InLounge, Calça Studio InLounge Masculino, Vestido Sharp InLounge Feminino. |

---

## Limitações

1. **Janela de compra vs. janela de reversa:** reversas de pedidos com data de compra anterior a 12 meses são excluídas pelo `INNER JOIN` com `sales_item_base`. Dependendo do prazo médio de reversa, um subconjunto de T&D pode ser subcontado.

2. **Zona cinza no farol:** produtos com `td_score ∈ [0.50, 0.70)` e `commercial_score ∈ [0.40, 0.70)` recebem `Monitorar` via `ELSE`. O `sinal_kill_keep` resolve parcialmente — aplica a regra de dois estados, mas produtos no ELSE caem em `Não priorizar agora` independentemente do `td_score` real.

3. **Cobertura de tags:** nem toda reversa possui tag em `return_reason_tags`. `pct_top_3_total` é relativo ao total de reversas com ao menos uma tag, não ao total de reversas do produto. Produtos com baixa cobertura de tags têm diagnóstico qualitativo incompleto.

4. **Cluster de portfólio:** `portfolio_skp_clustering` pode não cobrir todos os SKUs. Produtos sem cluster aparecem com `portfolio_cluster = NULL` e `score_tracao_comercial = —` na seção 9.

5. **Scores de scorecard vs. `commercial_score`:** Os pilares (Score Tração Comercial, Unit Economics, Satisfação e Marca) vêm de `sop_bronze.eval_produto_portfolio` e refletem a avaliação de portfólio em escala 0–100. 11 produtos do portfólio atual não possuem entrada nessa tabela e ficam com `—` nas colunas de score da seção 9 — isso não impede o pipeline de rodar. O `commercial_score` (CUME_DIST 0–1) é calculado pelo SQL sobre os dados de vendas do período e usado exclusivamente para `sinal_priorizacao` e `sinal_kill_keep`; os dois não são diretamente comparáveis.

6. **Tweet analítico é heurístico:** `build_tweet_heuristic()` é baseado em regras sobre colunas pré-computadas pelo SQL (`resumo_pre_llm`, `tendencia_reversas`, `principal_cor_afetada`). Não detecta padrões emergentes em comentários ainda não tagueados e não usa LLM.

---

## Lista de Arquivos

| Arquivo | Descrição |
|---|---|
| `pipeline_reversas_priorizacao_produtos.sql` | Query BigQuery — Camada 2 (executiva) + Camada 1 (analítica) |
| `td_analysis_functions.py` | Funções Python: preparação, QA, scoring, `build_tweet_heuristic`, `build_scorecard_product_view`, export |
| `TD_Priorizacao_Melhorias_v20260611.ipynb` | Notebook de diagnóstico: 10 seções + export para `.xlsx` e `.csv` |
| `README.md` | Este documento |

**Abas do workbook exportado (`td_priorizacao_YYYYMMDD.xlsx`):**

| Aba | Conteúdo |
|---|---|
| `farol_executivo` | Tabela executiva completa com todos os scores e métricas (43 colunas) |
| `resumo_farol` | Resumo por bucket de priorização (seção 1) |
| `benchmark_categoria` | T&D por categoria (seção 4) |
| `resumo_tags` | Top tags globais (seção 5) |
| `priorizar_melhoria` | Top 50 produtos no bucket "Priorizar melhoria" |
| `relatorio_pf` | Visão consolidada para o time PF: `sinal_kill_keep`, `top_3_motivos_td` (HTML), scores e taxas (seção 9) |
