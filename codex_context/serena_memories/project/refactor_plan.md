# Lead Time Dashboard Refactor — CONCLUÍDO

O refactor do `lead_time_dashboard.ipynb` foi concluído. As mudanças principais implementadas:

1. **SQL_OPS (Célula 2):** Migrado de 3 fontes (`supply_production_orders` + `muninn_production_orders_raw` + `supply_chain_efficiency_model_input`) para **1 única fonte**: `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`. Um único CTE (`CTE_OPS`) agrupa por `op_code`, calcula `MIN(ingestion_date)` como `stamp_created_production_order`, e usa `MIN(CASE WHEN current_production_stage = '<stage>') THEN CAST(ingestion_date AS TIMESTAMP) END)` para cada etapa.

2. **Estágios mapeados (7 stamps):**
   - `order_request_validation`
   - `waiting_fabric_arrival`
   - `fabric_validation_and_pre_cutting`
   - `cut_fabric_and_sewing_process`
   - `quality_inspection`
   - `items_delivery_and_invoicing`
   - `finished`

3. **Filtro de cohort de chegada (Célula 3):** Após converter `dt_largest_entry_warehouse` para datetime UTC, filtra `df_ops = df_ops[df_ops["dt_largest_entry_warehouse"] <= hoje]` para excluir OPs com data futura (planejada, não realizada). Garante que a série temporal só mostre OPs efetivamente entregues.

4. **Lead time:** `dt_largest_entry_warehouse - stamp_created_production_order` em dias.

5. **Prazo negociado:** `dt_reviewed_entry_warehouse` (deadline negociado com fornecedor), com fallback para `dt_planned_entry_warehouse + 120 dias`.

6. **Filtro padrão no SQL:** `supplier_relationship_status NOT IN ('terminated', 'discontinued')`.

## Estado atual
Refactor concluído. Nenhuma tarefa pendente de implementação.
