# Lead Time Dashboard — Inventário de Fontes

Atualizado em 2026-08-06.

## Fontes verificadas

| Fonte | Papel | Confiança |
|---|---|---|
| Notebook Deepnote `lead_time_dashboard` (`0b56034555ea4a43a7d80e0ede768923`) | Implementação vigente | Alta |
| Run `69f7ded9-c91e-494f-ba33-5d31d12b9de6` | Validação completa controlada da versão vigente; 62 blocos, 0 falhas | Alta |
| Run `a178ed9e-ac77-4136-b0b4-6768bea72fc3` | Validação histórica da regra intermediária com fallback; supersedida | Histórica |
| Run `7a59976c-b09a-4da1-be1c-7f1adf976344` | Último run completo anterior à decisão final | Histórica |
| Notebook de auditoria `0626482742d24c1b87ebf20cf411bb4d` | Análise retroativa de criação e cobertura | Alta |
| Run `9156ebae-f761-4663-bf08-93530ff72d0c` | Corte antes/depois do piso de cobertura | Alta |
| `sop_silver.supply_chain_efficiency_model_input_history` | Histórico principal de OPs | Alta |
| Memória Serena `lead_time_dashboard/sla90_deepnote_premissas` | Definições detalhadas da seção SLA | Alta, sujeita ao notebook atual |
| Memória Serena `lead_time_dashboard/bases_e_premissas` | Bases e regras do Dashboard Geral | Alta, sujeita ao notebook atual |
| Memória Serena `lead_time_dashboard/lead_time_limpo` | Regra detalhada de postergação | Alta, sujeita ao notebook atual |
| Decisão desta conversa em 2026-08-04 | `created_at` canônico exclusivo; restante da metodologia preservado | Alta |

## Lacunas e limitações

- Todas as queries de OP devem filtrar o `production_order_type` selecionado
  antes da agregação. `ANY_VALUE(production_order_type)` pode permanecer apenas
  como campo agregado e guard rail.
- `stamp_created_production_order` usa exclusivamente `MIN(created_at)`
  normalizado para data; não existe fallback por `ingestion_date`.
- OPs sem `created_at` são rotuladas `missing_created_at` e não entram nos KPIs
  dependentes de criação.
- O histórico committed começa em `2025-11-10`; primeiro snapshot anterior a
  esse corte é observação censurada pela cobertura.
- 140 OPs com `created_at` posterior ao planejado e 91 posteriores à entrega
  foram identificadas no recorte operacional e são excluídas dos KPIs.
- O snapshot em `codex_context` não sincroniza automaticamente com Serena ou
  Deepnote.
- A capacidade usada na visão Single Source é a capacidade cadastral atual, não
  uma reconstrução histórica.
- Ciclo Produtivo usa base independente do cohort de entrega: jan/2025 ao mês
  atual, Base por `CMMYYYY`, Extra por `dt_planned_original` e pendência por
  `is_op_open` do snapshot mais recente.

## Regra de atualização

Após mudança metodológica:

1. validar o notebook;
2. atualizar a memória Serena;
3. atualizar `premissas_fundamentais_calculo.md`;
4. registrar o novo run e eventuais divergências;
5. atualizar este inventário se fontes ou nível de confiança mudarem.
