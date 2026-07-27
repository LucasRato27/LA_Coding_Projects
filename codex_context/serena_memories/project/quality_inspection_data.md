# Tabela: `sop_gold.quality_inspection_data`

**Projeto BigQuery:** `insider-data-lake`
**Grão:** Uma linha por `op_code` (após agregação) — usar sempre com GROUP BY.

## Colunas relevantes

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `op_code` | STRING | Código da OP — chave de JOIN |
| `audit_count` | INTEGER | Número de auditorias realizadas |
| `dt_first_audit_completed` | DATE | Data de conclusão da primeira auditoria |
| `first_audit_result_standardized` | STRING | Resultado: `'qualita_approved'` ou `'qualita_rejected'` |
| `first_audit_deliberation_standardized` | STRING | Deliberação da Insider: `'insider_approved'`, `'insider_rejected'`, `'insider_no_deliberation'` |
| `first_quantity_sent_to_audition` | INTEGER | Peças enviadas à auditoria (denominador das taxas) |

## Métricas corretas de qualidade

**Não usar** `first_audit_defective_rate` (campo pré-calculado upstream com lógica diferente).

```sql
-- taxa_reprovacao_qualita: % peças reprovadas pela qualita
SAFE_DIVIDE(
  SUM(IF(first_audit_result_standardized = 'qualita_rejected', first_quantity_sent_to_audition, 0)),
  SUM(first_quantity_sent_to_audition)
)

-- taxa_reprovacao_etapa: % peças formalmente rejeitadas pela Insider (bloqueadas)
SAFE_DIVIDE(
  SUM(IF(first_audit_deliberation_standardized = 'insider_rejected', first_quantity_sent_to_audition, 0)),
  SUM(first_quantity_sent_to_audition)
)
```

## Padrão de CTE para análises de ICP

```sql
quality AS (
  SELECT
    op_code,
    audit_count,
    dt_first_audit_completed,
    first_audit_result_standardized,
    first_audit_deliberation_standardized,
    SUM(first_quantity_sent_to_audition)                      AS total_quantity_audited,
    SAFE_DIVIDE(
      SUM(IF(first_audit_result_standardized = 'qualita_rejected',
             first_quantity_sent_to_audition, 0)),
      SUM(first_quantity_sent_to_audition)
    )                                                         AS taxa_reprovacao_qualita,
    SAFE_DIVIDE(
      SUM(IF(first_audit_deliberation_standardized = 'insider_rejected',
             first_quantity_sent_to_audition, 0)),
      SUM(first_quantity_sent_to_audition)
    )                                                         AS taxa_reprovacao_etapa
  FROM `insider-data-lake.sop_gold.quality_inspection_data`
  GROUP BY op_code, audit_count, dt_first_audit_completed,
           first_audit_result_standardized, first_audit_deliberation_standardized
)
```

## Interpretação das deliberações

| `first_audit_deliberation_standardized` | Significado |
|---|---|
| `insider_approved` | Reprovada mas liberada para entrega (risco aceito) |
| `insider_rejected` | Reprovada e bloqueada — deve corrigir e reauditar |
| `insider_no_deliberation` | Reprovada, deliberação ainda não registrada |
