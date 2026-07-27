Arquivo ativo: 20260422_Projeção nec de prod/2_Códigos/NRP_Projecao_Simulador.ipynb

Seção 8 adicionada no notebook (Simulação de Atendimento):
- 8.0 parâmetros do módulo e seleção de cenário (ALLOC_SCENARIO)
- 8.1 preparação de insumos de alocação (need_by_product, cap_map, cell_elegibility)
- 8.2 função allocate_monthly (alocação greedy por mês)
- 8.4 simulate_post_allocation (simulação semanal pós-alocação)
- 8.6 consolidação com cobertura ponderada por BRL
- 8.7 visualizações do módulo

Regras importantes implementadas:
- cap_map completo para células ativas: célula ausente no cap_use em um mês recebe available_capacity = cell_max_monthly_capacity.
- fallback adicional na alocação: mês ausente em remaining_cap inicializa com capacidade máxima da célula.
- df_monthly_alloc é a fonte única de verdade para a simulação de alocação.
- Override out/nov 2026 aplicado em df_monthly_alloc (new_need por SKU nivelado pela média do período).
- need_by_product passa a ser agregado a partir de df_monthly_alloc.
- sku_share em simulate_post_allocation é calculado por participação no new_need (não na demanda).
- coverage_days consolidado: cálculo no nível SKU e agregação ponderada por closing_stock_brl.

Visualizações novas em 8.7:
- Top gaps de alocação out/nov 2026 por % não atendido (ranking por pct_unmet; hover com gap em unidades e necessidade total).
- Dias de estoque final em nov/2026 para top produtos em estoque (top por closing_stock_brl).

Datas de foco utilizadas nas análises do módulo:
- Out/2026: 2026-10-01
- Nov/2026: 2026-11-01
