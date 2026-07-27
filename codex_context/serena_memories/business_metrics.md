# Métricas e Conceitos de Negócio — Insider Store

## Cadeia de Suprimentos (Supply Chain)

### OP (Ordem de Produção)
- `op_code`: identificador único da OP
- `production_order_type`: tipo da OP
- `current_production_stage`: estágio atual (pending, canceled, cutting, sewing, finishing, delivered, etc.)
- `is_finished_product_order`: TRUE = produto acabado (compra pronta), FALSE = triangulação (a Insider manda tecido)
- **Triangulação**: Insider envia tecido ao fornecedor; **Produto Acabado**: fornecedor compra tudo

### Ciclos de Produção
- `cycle_name`: nome do ciclo de alocação (ex: `C0120XX` = ciclo regular, `EPAxxxx`/`BOLD` = ciclo extra)
- `cycle_type`: "Alocação de ciclos" (regex `C\d{2}20\d{2}`) ou "Extra" (tudo que não é ciclo regular)
- Ciclos regulares = planejamento periódico; Extras = demandas pontuais/urgentes

### Alocação
- `allocation_type`: "Alocação Automática" (author_id específico) ou "Alocação Manual"
- Autor `cm4ii37rn000008l4bx9t2so2` = sistema automático

### Células Produtivas
- `cell_number` + `cell_label` = identifica a célula no fornecedor
- Cada célula tem capacidade semanal/mensal cadastrada
- Um fornecedor pode ter múltiplas células, cada uma com mix de produtos diferente

### Recebimentos
- `source`: "REAL" (recebido no warehouse via SFTP) ou "FORECAST" (previsão do modelo)
- `qty_received`: quantidade efetivamente recebida
- `dt_entry_warehouse`: data de entrada no armazém
- Fluxo: Planejado (`dt_planned_entry_warehouse`) → Revisado (`dt_reviewed_entry_warehouse`) → Real (`dt_entry_warehouse`)

### Quantidades da OP
- `planned_quantity`: quantidade planejada por SKU
- `cutted_quantity`: quantidade cortada
- `received_quantity`: quantidade recebida
- `perc_received`: % recebido vs planejado (`received / planned`)

## Métricas Financeiras

### Custo (CMV)
- `cmv_planned`: custo de manufatura planejado por unidade
- `cmv_real`: custo de manufatura real/faturado por unidade
- `w_average_planned_cost`: custo mediano planejado por SKU (janela móvel)
- `w_average_invoiced_cost`: custo mediano faturado por SKU (janela móvel)

### Markup
- `mark_up` = `full_price / cmv_real` (preço cheio ÷ custo)
- Calculado ponderado por qtd produzida: `SUM(full_price × qty) / SUM(cmv × qty)`

### Preço
- `full_price`: preço cheio (tabela) do produto
- `pmv_last_7_days`, `pmv_last_28_days`, `pmv_last_year`: preço médio de venda real (ponderado por qtd)

### Matéria-Prima (MP)
- `mp_cost_planned` / `mp_cost_real`: custo de matéria-prima planejado/real
- `mp_estimated_planned_cost` / `mp_estimated_real_cost`: custo estimado via composição de tecido
- `min_expected_fabric_cost` / `max_expected_fabric_cost`: faixa de custo esperado de tecido

### Receita (DRE)
- `revenue_after_discounts`: receita bruta − descontos
- `revenue_after_refunds`: receita líquida de devoluções
- `gross_profit`: lucro bruto
- `net_profit_after_marketing_costs`: lucro líquido pós-marketing

## Performance de Fornecedores

### SCALE (Sistema de Avaliação)
- Framework com 4 dimensões: **atraso** (`grade_delay_days`), **custo** (`grade_cost`), **ICP** (`grade_icp`), **qualidade** (`grade_quality`)
- `scale_grade_supplier` = soma das 4 notas
- Médias devem ser ponderadas: atraso por `total_received_quantity`, ICP por `total_planned_quantity_icp`, qualidade por `first_quantity_sent_to_audition`

### ICP (Índice de Conformidade de Produção)
- `icp` = `received_quantity / planned_quantity` (% do planejado que foi entregue)
- `avg_icp` = média ponderada por `total_planned_quantity_icp`

### Qualidade
- `first_inspection_defective_rate`: taxa de defeito na 1ª auditoria
- `last_inspection_defective_rate`: taxa de defeito na última auditoria
- `final_audit_evaluation` / `first_audit_result`: resultado da auditoria por OP

### Atraso
- `delay_days`: dias de atraso vs data planejada
- `avg_delay`: média ponderada de atraso por qtd entregue

## SKUs e Produtos

### Estados do SKU
- `sku_state`: "ativo_perene" (item permanente), "ativo_sazonal", "descontinuado", etc.
- Filtro comum: `sku_state == 'ativo_perene'` para análises de base instalada

### Hierarquia
- `sku` → `product_name` → `category` → `family` → `model`
- `color`, `size`, `gender` são atributos do SKU

### Classificação ABC
- `tag_abc`: classificação por curva de demanda (A = alto giro, B = médio, C = baixo)

## Estoque

### Tipos de Estoque
- `physical_stock`: estoque físico
- `virtual_stock`: estoque virtual (reservado)
- `wms_stock`: estoque no WMS
- `damaged_stock`: estoque avariado

### Valoração
- `stock_planned_cost` = physical_stock × custo planejado
- `stock_invoiced_cost` = physical_stock × custo faturado
- `stock_full_price_revenue` = physical_stock × preço cheio
- `stock_pmv_*_revenue` = physical_stock × PMV (7d, 28d, 12m)


## Métricas — Simulação de Atendimento (Seção 8 do NRP_Projecao_Simulador)

### Capacidade e Alocação
- `available_capacity` (por célula × mês): capacidade líquida para alocação no mês.
  - No notebook: ajustada por backlog de planejamento acumulado anterior.
  - Se célula/mês ausente no `cap_use`, usa capacidade máxima cadastrada da célula (`cell_max_monthly_capacity`).
- `productive_credit` = `cell_max_monthly_capacity / monthly_capacity`.
  - Converte unidades de produto em "créditos" consumidos da célula na alocação.
- `allocated_qty`: quantidade alocada (factível) do produto.
- `unallocated_qty`: necessidade não atendida.
- `fill_rate` (%) = `allocated_qty / total_need * 100`.
- `% não atendido` = `(total_need - allocated_qty) / total_need * 100`.

### Produção Factível e Estoque
- `factible_production`: produção factível distribuída para SKU.
- `factible_receipts`: parcela efetivamente recebida da produção factível (aplica `receipt_rate`).
- `sku_share`: participação do SKU na necessidade do `product_name` no mês.
  - Regra atual: `sku_share = new_need_sku / new_need_product` (não usar demanda mensal para esse share).
- `closing_stock_brl` = `closing_stock * avg_cmv`.

### Cobertura e Ruptura
- `coverage_days` (SKU) = `closing_stock / daily_demand`.
- `coverage_days` agregado: média ponderada por `closing_stock_brl` (não média simples).
- `stockout_weeks`: número de semanas com ruptura no mês (simulação semanal em 4 semanas).
- `% SKUs com ruptura`: SKUs com `stockout_weeks > 0` / total de SKUs no mês.

### Regras de cenário/override no módulo
- `df_monthly_alloc` é a base da simulação de atendimento.
- Override específico de simulação: em out/nov-2026, `new_need` por SKU é nivelado pela média do período (apenas no módulo de alocação).
