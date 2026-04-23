# Base de Dados: insider-data-lake

**Projeto BigQuery:** `insider-data-lake`
**Região:** `southamerica-east1`
**Contexto:** Data Lake da Insider Store, com dados de Supply Chain / SOP (Sales & Operations Planning).

---

## Tabelas Utilizadas no Projeto

### 1. `sop_silver.supply_chain_efficiency_model_input_history`

**Tipo:** Tabela de histórico (snapshots diários)
**Particionamento:** `ingestion_date`
**Volume:** ~12,3 milhões de linhas | ~242 partições | ~4,8 GB
**Grão:** Uma linha por `(op_code, product_sku, ingestion_date)` — cada combinação OP-SKU capturada diariamente.

**Descrição:** Tabela central do projeto. Armazena um snapshot diário do estado de cada OP-SKU no plano de produção. Permite rastrear mudanças ao longo do tempo comparando snapshots consecutivos — base para todo o cálculo de KR1.

#### Colunas Principais

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `op_code` | STRING | Código da Ordem de Produção |
| `product_sku` | STRING | SKU do produto (identificador único do item) |
| `cycle_name` | STRING | Nome do ciclo (ex: `C062026`). Base = `^C\d{2}20\d{2}$`, Extra = demais |
| `ingestion_date` | DATE | Data do snapshot (partição). Chave temporal da tabela |
| `supplier_id` | INTEGER | ID do fornecedor |
| `supplier_name` | STRING | Nome do fornecedor |
| `product_name` | STRING | Nome do produto |
| `product_color` | STRING | Cor do produto |
| `product_size` | STRING | Tamanho do produto |
| `current_production_stage` | STRING | Estágio atual da produção (ex: `pending`, `canceled`, `in_production`, ...) |
| `production_order_type` | STRING | Tipo da OP. Filtro padrão: `NOT IN ('flexible', 'converted')` |
| `is_finished_product_order` | BOOLEAN | Indica se é uma OP de produto acabado |
| `dt_planned_entry_warehouse` | DATE | **Data planejada de entrada no armazém** — usada para medir `INT_DATE` |
| `dt_reviewed_entry_warehouse` | DATE | **Data revisada de entrada no armazém** — usada para medir `EXT_DATE_REV` |
| `dt_planned_production_start` | DATE | Data planejada de início de produção |
| `dt_planned_production_end` | DATE | Data planejada de fim de produção |
| `dt_planned_fabric_sent` | DATE | Data planejada de envio do tecido |
| `dt_reviewed_fabric_sent` | DATE | Data revisada de envio do tecido |
| `dt_min_entry_warehouse` | DATETIME | Menor data de entrada registrada |
| `dt_max_entry_warehouse` | DATETIME | Maior data de entrada registrada |
| `dt_largest_entry_warehouse` | DATE | Data de entrada mais tardia |
| `production_stage_order` | NUMERIC | Ordem numérica do estágio de produção |
| `production_stage_sla` | NUMERIC | SLA do estágio atual |
| `dt_current_production_stage` | TIMESTAMP | Timestamp de início do estágio atual |
| `dt_last_order_update` | TIMESTAMP | Timestamp da última atualização da OP |
| `planned_quantity` | INTEGER | **Quantidade planejada** — usada para medir `INT_GRADE` |
| `cutted_quantity` | INTEGER | Quantidade cortada |
| `received_quantity` | INTEGER | Quantidade recebida no armazém |
| `invoiced_quantity` | INTEGER | Quantidade faturada |
| `planned_quantity_op` | INTEGER | Quantidade planejada no nível da OP (agregado) |
| `cutted_quantity_op` | INTEGER | Quantidade cortada no nível da OP |
| `received_quantity_op` | INTEGER | Quantidade recebida no nível da OP |
| `invoiced_quantity_op` | INTEGER | Quantidade faturada no nível da OP |
| `supplier_relationship_status` | STRING | Status do relacionamento com o fornecedor |
| `status_sku` | STRING | Status do SKU |
| `is_op_open` | BOOLEAN | Indica se a OP está aberta |
| `order_observations` | STRING | Observações da OP |
| `nfe_numbers` | STRING | Números das NF-e do SKU |
| `nfe_numbers_op` | STRING | Números das NF-e da OP |
| `flg_grouped_invoice` | INTEGER | Flag de fatura agrupada |
| `flg_partial_deliveries` | INTEGER | Flag de entregas parciais |
| `cell_number` | STRING | Número da célula de produção |
| `cell_label` | STRING | Label da célula de produção |
| `apparel_manufacturer_id` | INTEGER | ID do fabricante de vestuário |
| `dt_follow_up` | DATE | Data de follow-up |
| `follow_up_user_name` | STRING | Responsável pelo follow-up |
| `production_order_updated_by` | STRING | Usuário que atualizou a OP |
| `canceled_at` | DATE | Data de cancelamento da OP |
| `created_at` | DATETIME | Data de criação da OP |
| `icp_invoiced_quantity` | INTEGER | Quantidade faturada ICP |

**Padrão de uso nas queries:**
```sql
WHERE production_order_type NOT IN ('flexible', 'converted')
  AND cycle_name IS NOT NULL
```

---

### 2. `sop_silver.supply_chain_efficiency_model_input`

**Tipo:** Tabela de estado atual (latest snapshot)
**Volume:** Igual à última partição da tabela `_history`
**Grão:** Uma linha por `(op_code, product_sku)` — estado mais recente de cada OP-SKU.

**Descrição:** Versão "flat" da tabela history, contendo apenas o snapshot mais recente. Mesmas colunas que `_history`. Usada apenas em queries de teste e validação pontual — **não é usada nas análises principais**.

**Colunas:** Idênticas à `supply_chain_efficiency_model_input_history` (sem a dimensão temporal de `ingestion_date` como histórico).

**Uso no projeto:** Exclusivamente em [Testes/teste.sql](1_Inputs/1_SQL/Testes/teste.sql) e [Testes/scemi.sql](1_Inputs/1_SQL/Testes/scemi.sql).

---

### 3. `integrated.muninn_production_orders`

**Tipo:** Tabela dimensão (dump do sistema Muninn)
**Volume:** ~12.400 linhas | ~4,5 MB
**Grão:** Uma linha por `order_code` (Ordem de Produção).

**Descrição:** Dump da tabela de ordens de produção do sistema Muninn (ERP/operacional). Contém metadados detalhados de cada OP, incluindo o **motivo de cancelamento** — informação crítica para separar cancelamentos internos (`INT_CANCEL`) de externos (`EXT_CANCEL`).

#### Colunas Principais

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id` | INTEGER | ID interno da OP no Muninn |
| `order_code` | STRING | **Chave de JOIN com `op_code`** da tabela history |
| `canceled_production_reason` | STRING | **Motivo de cancelamento** — distingue `INT_CANCEL` (`'Revisão de Demanda (In Season)'`) de `EXT_CANCEL` (demais motivos) |
| `status` | STRING | Status da OP no Muninn |
| `production_stage` | STRING | Estágio de produção |
| `type` | STRING | Tipo da ordem |
| `is_finished_product_order` | BOOLEAN | Indica produto acabado |
| `is_size_set_order` | BOOLEAN | Indica se é pedido de grade completa |
| `is_international` | BOOLEAN | Indica se é produção internacional |
| `is_custom_receive_at` | BOOLEAN | Indica data de recebimento customizada |
| `cycle_id` | INTEGER | ID do ciclo no Muninn |
| `product_id` | INTEGER | ID do produto |
| `product_color_id` | INTEGER | ID da cor do produto |
| `apparel_manufacturer_id` | INTEGER | ID do fabricante |
| `author_id` | STRING | Criador da OP |
| `update_author_id` | STRING | Usuário da última atualização |
| `update_source` | STRING | Fonte da atualização |
| `update_reason` | STRING | Motivo da atualização |
| `send_at` | DATE | Data de envio ao fornecedor |
| `receive_at` | DATE | Data de recebimento atual |
| `planned_send_at` | DATE | Data planejada de envio |
| `planned_receive_at` | DATE | Data planejada de recebimento |
| `planned_demand_date` | DATE | Data de demanda planejada |
| `planned_production_delivery_date` | DATE | Data planejada de entrega da produção |
| `expected_production_delivery_date` | DATE | Data esperada de entrega |
| `real_production_delivery_date` | DATE | Data real de entrega |
| `real_production_start_date` | DATE | Data real de início de produção |
| `planned_lead_time` | INTEGER | Lead time planejado (dias) |
| `planned_manufacturer_cost` | FLOAT | Custo planejado do fabricante |
| `actual_product_cost` | NUMERIC | Custo real do produto |
| `planned_product_grade_id` | INTEGER | ID do grade planejado |
| `canceled_at` | DATE | Data de cancelamento |
| `created_at` | DATETIME | Data de criação |
| `updated_at` | DATETIME | Data da última atualização |
| `ingestion_date` | DATE | Data de ingestão no data lake |

**Chave de JOIN:**
```sql
LEFT JOIN integrated.muninn_production_orders AS po
  ON po.order_code = h.op_code
```

---

## Relacionamentos entre Tabelas

```
supply_chain_efficiency_model_input_history
    │  Grão: OP-SKU × ingestion_date
    │
    ├── AUTO-JOIN (window functions)
    │   └── LAG(PARTITION BY op_code, product_sku ORDER BY ingestion_date)
    │       → rastreia mudanças dia-a-dia por OP-SKU
    │
    ├── SELF-JOIN (baseline vs estado atual)
    │   └── JOIN ON cycle_name, ingestion_date = baseline_date
    │       → compara estado no baseline com estado mais recente
    │
    └── LEFT JOIN integrated.muninn_production_orders
        └── ON po.order_code = h.op_code
            → adiciona motivo de cancelamento para classificar INT vs EXT
```

---

## Classificações de Negócio

### Tipos de Ciclo
| Classificação | Padrão | Exemplo |
|---------------|--------|---------|
| **Base** | `REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}$')` | `C062026` |
| **Extra** | Todos os demais | `C06EX2026`, `C06EXTRA` |

### Tipos de Revisão (Flags INT/EXT)
| Flag | Condição | Significado |
|------|----------|-------------|
| `INT_DATE` | `current_dt_planned ≠ baseline_dt_planned` | Mudança interna de data planejada |
| `INT_CANCEL` | Cancelada com reason `'Revisão de Demanda (In Season)'` | Cancelamento por revisão de demanda |
| `INT_GRADE` | `current_planned_qty ≠ baseline_planned_qty` | Mudança interna de quantidade |
| `INT_ANY` | Qualquer flag INT | Qualquer interferência interna |
| `EXT_CANCEL` | Cancelada com reason diferente de `'Revisão de Demanda (In Season)'` | Cancelamento por motivo externo (fornecedor) |
| `EXT_DATE_REV` | `current_dt_reviewed ≠ baseline_dt_reviewed` sem mudança de dt_planned | Revisão de data pelo fornecedor |
| `EXT_ANY` | Qualquer flag EXT | Qualquer interferência externa |

### Definição de Baseline
O **baseline** de um ciclo é a primeira `ingestion_date` onde nenhuma OP está no estágio `'pending'` — representa o "congelamento do plano".

```sql
-- Lógica do baseline
SELECT cycle_name, MIN(ingestion_date) AS baseline_date
FROM supply_chain_efficiency_model_input_history
GROUP BY cycle_name, ingestion_date
HAVING COUNTIF(current_production_stage = 'pending') = 0
```

---

## Métricas Calculadas no Projeto

### KR1 — Plan Freeze Rate
```
KR1 = 1 - (vol_int_any / vol_original)
```
- `vol_original`: número de OP-SKUs no baseline
- `vol_int_any`: número de OP-SKUs com pelo menos 1 flag INT ativa

**Interpretação:** % do plano original que foi mantido sem interferências internas. Meta: maior é melhor (100% = nenhuma mudança interna após congelamento).
