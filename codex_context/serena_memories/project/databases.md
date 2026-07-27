# Bases de Dados — insider-data-lake

**Projeto BigQuery:** `insider-data-lake` | **Região:** `southamerica-east1`

---

## Tabela Principal: `sop_silver.supply_chain_efficiency_model_input_history`

**Tipo:** Snapshots diários | **Partição:** `ingestion_date`
**Volume:** ~12,3M linhas | ~242 partições | ~4,8 GB
**Grão:** `(op_code, product_sku, ingestion_date)`

**Uso no projeto:** Fonte única do SQL_OPS no lead_time_dashboard. Agrupa por `op_code` para derivar timestamps de chegada por etapa.

### Colunas Chave

| Coluna | Tipo | Uso |
|--------|------|-----|
| `op_code` | STRING | ID da OP — chave de agrupamento |
| `product_sku` | STRING | SKU do produto |
| `ingestion_date` | DATE | Data do snapshot (partição) |
| `current_production_stage` | STRING | Estágio atual normalizado (ver lista abaixo) |
| `production_stage_order` | NUMERIC | Ordem numérica do estágio |
| `production_stage_sla` | NUMERIC | SLA do estágio |
| `cycle_name` | STRING | Ciclo (ex: `C062026`). Base = `^C\d{2}20\d{2}$` |
| `supplier_name` | STRING | Nome do fornecedor |
| `supplier_id` | INTEGER | ID do fornecedor |
| `supplier_relationship_status` | STRING | Status do relacionamento (filtrar `NOT IN ('terminated','discontinued')`) |
| `product_name` | STRING | Nome do produto |
| `product_color` | STRING | Cor do produto |
| `product_size` | STRING | Tamanho |
| `is_finished_product_order` | BOOLEAN | Se é OP de produto acabado |
| `production_order_type` | STRING | Tipo da OP. Filtro padrão: `NOT IN ('flexible','converted')` |
| `dt_planned_entry_warehouse` | DATE | Data planejada de entrada no armazém (INT_DATE baseline) |
| `dt_reviewed_entry_warehouse` | DATE | Data revisada de entrada (deadline negociado com fornecedor) |
| `dt_largest_entry_warehouse` | DATE | Data de entrada mais tardia (realizada ou planejada) |
| `dt_planned_production_start` | DATE | Início planejado de produção |
| `dt_planned_production_end` | DATE | Fim planejado de produção |
| `planned_quantity` | INTEGER | Quantidade planejada por SKU (INT_GRADE baseline) |
| `planned_quantity_op` | INTEGER | Quantidade planejada agregada por OP |
| `received_quantity_op` | INTEGER | Quantidade recebida agregada por OP |
| `status_sku` | STRING | Status do SKU |
| `is_op_open` | BOOLEAN | Se a OP está aberta |
| `canceled_at` | DATE | Data de cancelamento |
| `created_at` | DATETIME | Data de criação da OP |

### Estágios Normalizados (`current_production_stage`)

Ordem de produção típica (usar `production_stage_order` para confirmar):
1. `pending` — OP ainda não emitida
2. `order_request_validation`
3. `waiting_fabric_arrival`
4. `fabric_validation_and_pre_cutting`
5. `cut_fabric_and_sewing_process`
6. `quality_inspection`
7. `items_delivery_and_invoicing`
8. `finished`
9. `canceled`
10. `in_production` (genérico)

### Padrão de query para timestamps por etapa
```sql
GROUP BY h.op_code
-- Timestamp da OP: primeiro snapshot
MIN(CAST(h.ingestion_date AS TIMESTAMP)) AS stamp_created_production_order
-- Timestamp de cada etapa: primeiro dia no estágio
MIN(CASE WHEN h.current_production_stage = '<stage>' THEN CAST(h.ingestion_date AS TIMESTAMP) END) AS stamp_stage_<stage>
-- Data de chegada ao armazém
MAX(h.dt_largest_entry_warehouse) AS dt_largest_entry_warehouse
```

### Filtros padrão
```sql
WHERE production_order_type NOT IN ('flexible', 'converted')
  AND cycle_name IS NOT NULL
-- Filtrar fornecedores ativos:
AND (supplier_relationship_status IS NULL OR supplier_relationship_status NOT IN ('terminated', 'discontinued'))
```

---

## Tabela: `sop_silver.supply_chain_efficiency_model_input`

**Tipo:** Latest snapshot (estado atual) — mesmas colunas que `_history`, sem histórico temporal.
**Uso:** Exclusivamente em testes/validação pontual. **Não usada nas análises principais.**

---

## Tabela: `integrated.muninn_production_orders`

**Tipo:** Dump do sistema Muninn (ERP) | **Grão:** `order_code` (1 linha por OP)
**Volume:** ~12.400 linhas | ~4,5 MB

**Uso no projeto:** Apenas para `canceled_production_reason` no KR1 (classificar INT_CANCEL vs EXT_CANCEL). **Não usada no lead_time_dashboard.**

**Colunas chave:**
| Coluna | Uso |
|--------|-----|
| `order_code` | JOIN com `op_code` da tabela history |
| `canceled_production_reason` | `'Revisão de Demanda (In Season)'` = INT_CANCEL; demais = EXT_CANCEL |
| `status` | Status da OP no Muninn |

**JOIN padrão:**
```sql
LEFT JOIN `insider-data-lake.integrated.muninn_production_orders` AS po
  ON po.order_code = h.op_code
```

---

## Classificações de Negócio

### Tipos de Ciclo
| Tipo | Padrão | Exemplo |
|------|--------|---------|
| Base | `REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}$')` | `C062026` |
| Extra | Todos os demais | `C06EX2026` |

### Flags de Revisão (KR1)
| Flag | Condição |
|------|----------|
| `INT_DATE` | `current_dt_planned ≠ baseline_dt_planned` |
| `INT_CANCEL` | Cancelada com reason `'Revisão de Demanda (In Season)'` |
| `INT_GRADE` | `current_planned_qty ≠ baseline_planned_qty` |
| `EXT_CANCEL` | Cancelada com outro motivo |
| `EXT_DATE_REV` | `current_dt_reviewed ≠ baseline_dt_reviewed` sem mudança de dt_planned |

### Baseline do Ciclo (KR1)
Primeiro `ingestion_date` onde nenhuma OP do ciclo tem `current_production_stage = 'pending'` — momento de congelamento do plano.

---

## Relacionamentos

```
supply_chain_efficiency_model_input_history
    │ Grão: op_code × product_sku × ingestion_date
    │
    ├── AUTO-JOIN (window functions por OP-SKU)
    │   └── LAG(PARTITION BY op_code, product_sku ORDER BY ingestion_date) → mudanças dia-a-dia
    │
    ├── SELF-JOIN (KR1 baseline vs estado atual)
    │   └── JOIN ON cycle_name, ingestion_date = baseline_date
    │
    └── LEFT JOIN integrated.muninn_production_orders
        └── ON order_code = op_code → motivo de cancelamento
```
