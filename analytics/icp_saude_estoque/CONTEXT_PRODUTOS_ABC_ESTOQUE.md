# Contexto: Produtos, Curva ABC e Saúde de Estoque

> **Escopo deste documento:** hierarquia de produtos, estado de SKUs, metodologia da curva ABC e campos de saúde de estoque. O contexto de ICP (cálculo, thresholds, análise de fornecedores) já está disponível separadamente.

---

## 1. Hierarquia de Produtos

A estrutura de produtos na Insider segue a hierarquia:

```
sku  →  product_name  →  family  →  category
```

| Campo | Descrição | Fonte |
|-------|-----------|-------|
| `sku` | Identificador único do SKU (combinação de modelo + cor + tamanho) | `integrated.skus` |
| `product_name` | Nome do produto — **chave de join entre curva ABC, ICP e estoque** | `integrated.skus` |
| `family` | Família do produto (ex: "Calça", "Moletom") | `integrated.skus` |
| `category` | Categoria (ex: "Feminino", "Masculino") | `integrated.skus` |
| `produto_pai` | Campo equivalente a `product_name` dentro de `sop_gold.stock_health` | `sop_gold.stock_health` |

> **Join canônico:** sempre usar `sk.product_name` de `integrated.skus` como chave de ligação entre as tabelas — não usar `produto_pai` de `stock_health` diretamente, pois pode ter variação de grafia.

---

## 2. Estados de SKU (`sku_state`)

O campo `sku_state` (de `integrated.skus`) indica a situação cadastral e comercial do SKU:

| Estado | Interpretação | Incluir em análises? |
|--------|--------------|----------------------|
| `ativo_perene` | Produto permanente no portfólio | ✅ Sim |
| `ativo_lancamento` | Produto novo em fase de introdução | ✅ Sim |
| `ativo_sazonal` | Produto com janela sazonal ativa | ✅ Sim |
| `desativado` | Produto descontinuado | ❌ Excluir |

**Filtro padrão obrigatório:** `sku_state != 'desativado'` em todas as análises operacionais.

---

## 3. Curva ABC

### O que é

Classificação de produtos por relevância de receita, usada para priorizar ações de planejamento e gestão de risco. Um produto **A** com ICP baixo tem impacto direto de receita; um produto **C** com ICP baixo tem impacto residual.

### Fonte e janela

```
Tabela  : insider-data-lake.sop_silver.demand_prediction_input
Campo   : treated_generated_revenue (receita por product_name por referência)
Janela  : últimos 3 meses fechados anteriores ao mês corrente
Filtro  : DATE(reference_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 3 MONTH)
          AND DATE(reference_date) < DATE_TRUNC(CURRENT_DATE(), MONTH)
```

### Classificação

| Classe | Critério (share acumulado de receita) | Interpretação |
|--------|--------------------------------------|---------------|
| **A** | ≤ 80% | Produtos de maior receita — criticidade máxima |
| **B** | > 80% e ≤ 95% | Produtos intermediários — atenção elevada |
| **C** | > 95% | Cauda longa — menor criticidade |

### Grão e limitações

- A curva ABC é calculada no nível **`product_name`** — não está disponível no nível SKU.
- O join com estoque é feito via `product_name` de `integrated.skus`.
- Produtos sem receita nos últimos 3 meses recebem `tag_abc = NULL`.

---

## 4. Saúde de Estoque (`sop_gold.stock_health`)

### Grão

`sku × dia` — snapshot diário por SKU. Aproximadamente 2.185 SKUs × N dias.

### Campos por categoria

#### Identificação e tempo

| Campo | Descrição |
|-------|-----------|
| `sku` | SKU — chave de join com `integrated.skus` |
| `nome_sku` | Nome descritivo do SKU |
| `produto_pai` | Produto pai (equivalente a `product_name`) |
| `categoria` | Categoria do produto |
| `variante_cor` | Variante de cor |
| `dia` | Data do snapshot |
| `isoweek` | Semana ISO |
| `mes` | Mês de referência |

#### Estoque

| Campo | Descrição |
|-------|-----------|
| `estoque_passado` | Estoque realizado (histórico) |
| `estoque_projetado` | Estoque projetado (futuro) |
| `estoque_passado_ou_projetado` | Combina realizado (passado) e projetado (futuro) |

#### Cobertura e saúde

| Campo | Descrição |
|-------|-----------|
| `estoque_passado_ou_projetado_d` | **Cobertura em dias** (estoque ÷ demanda prevista diarizada) — principal métrica de saúde |
| `estoque_atual_d_vendas_l7d` | Cobertura em dias usando vendas médias dos últimos 7 dias |
| `estoque_seguranca_d` | Dias de estoque de segurança (target operacional) |
| `estoque_excesso_d` | Excesso em dias acima do estoque de segurança |
| `stock_classification` | Classificação de saúde (ex: `ruptura`, `excesso`, `saudável`) — baseada em demanda prevista |
| `stock_classification_l7d_sales` | Mesma classificação usando média L7D de vendas reais |

#### Demanda e recebimentos

| Campo | Descrição |
|-------|-----------|
| `qtd_a_receber` | Quantidade a receber de OPs em aberto |
| `qtd_venda_prevista_diarizada` | Demanda prevista diária (do modelo de previsão) |
| `qtd_venda_media_l7d` | Média diária de vendas reais dos últimos 7 dias |

#### Financeiro

| Campo | Descrição |
|-------|-----------|
| `sku_sales_average_price` | Preço médio de venda do SKU |
| `sku_production_cost` | Custo de produção do SKU |
| `receita_prevista_diarizada` | Receita prevista diária |
| `custo_de_estoque` | Valor do estoque ao custo de produção |

---

## 5. Lógica de Ameaça de Estoque via ICP

### Premissa

Um ICP baixo de um fornecedor/produto significa que menos peças serão entregues do que o planejado. Se esse produto é relevante na curva ABC, a falta de entrega **ameaça diretamente a cobertura de estoque** futura — especialmente porque `qtd_a_receber` já incorpora as OPs em aberto como reposição esperada.

### Regras de priorização

| Nível | Condição | Ação recomendada |
|-------|----------|----|
| **CRITICO** | ICP < 70% **e** produto **A** | Escalar imediatamente — alto impacto em receita |
| **ALTO** | ICP < 70% **e** produto **B** | Acionar fornecedor e avaliar alternativas de reposição |
| **MEDIO** | ICP < 70% **e** produto **C** | Monitorar — baixa criticidade de receita |
| **MONITORAMENTO** | ICP ≥ 70% **e** produto **A** ou **B** | Manter atenção — pode degradar nos próximos ciclos |
| **SEM_OP_ATIVA** | ICP = NaN (sem OP ativa) **e** produto **A** ou **B** | Verificar se há plano de reposição — pode ser ruptura sem pipeline |
| **OK** | ICP ≥ 70% + produto **C**, ou sem tag ABC | Sem ação necessária |

### Threshold configurável

O limiar padrão é **70%** (`ICP_THRESHOLD = 0.70` no notebook). Pode ser ajustado conforme o contexto do ciclo. Abaixo de 60% = situação crítica na maioria dos contextos operacionais.

### Importante: ICP atual vs tendência

- **ICP no SQL principal** (`stock_health_abc_icp.sql`): calculado da **carteira ativa atual** de `supply_chain_efficiency_model_input`. Reflete o estado corrente das OPs.
- **Tendência histórica de ICP**: carregada separadamente de `scale_ra` (grão: `supplier_name × reference_date`). Permite identificar fornecedores com ICP em queda mesmo antes de atingir o threshold.

---

## 6. Arquivo SQL de Referência

`analytics/icp_saude_estoque/sql/stock_health_abc_icp.sql`

**Parâmetro obrigatório:** `{dia_ref}` — data do snapshot de stock_health (ex: `'2026-06-05'`)

**Grão do resultado:** `sku × dia`

**Campos-chave no output:**

| Campo | Origem | Uso |
|-------|--------|-----|
| `tag_abc` | `CTE_ABC_CURVE` | Criticidade do produto |
| `avg_icp` | `CTE_ICP_POR_PRODUTO` | Confiabilidade de entrega |
| `estoque_passado_ou_projetado_d` | `stock_health` | Cobertura atual em dias |
| `stock_classification` | `stock_health` | Estado de saúde do estoque |
| `flag_ameaca_estoque` | Calculado | `TRUE` se ICP < 70% e tag A ou B |
| `nivel_risco` | Calculado | Priorização: CRITICO → ALTO → MEDIO → MONITORAMENTO → OK |
