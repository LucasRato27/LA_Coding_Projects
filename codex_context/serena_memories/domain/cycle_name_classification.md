# Classificação de `cycle_name` — Ciclo Base vs Ciclo Extra

Fonte: [base.md](base.md#L26) e [base.md](base.md#L179), tabela `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history` (e `_input`).

## Regra de classificação

- **Ciclo Base**: `cycle_name` casa com o regex `^C\d{2}20\d{2}$`
  - Exemplo: `C062026` (2 dígitos do mês + "20" + 2 dígitos do ano, ex.: mês 06, ano 2026)
  - SQL: `REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}$')`
- **Ciclo Extra**: qualquer `cycle_name` que **não** case com o regex acima (todos os demais valores/formatos)

## Uso típico em SQL

```sql
CASE
  WHEN REGEXP_CONTAINS(cycle_name, r'^C\d{2}20\d{2}$') THEN 'Base'
  ELSE 'Extra'
END AS tipo_ciclo
```

## Contexto de aplicação

- Usado em cálculos de KR1 (Plan Freeze Rate), cascateamento de fornecedores e qualquer análise que precise separar ciclos de produção "regulares" (base, alinhados ao calendário mensal) de ciclos "extras" (fora do calendário padrão, ex. reposições pontuais, extras de urgência, etc.).
- Filtro obrigatório em queries usando `supply_chain_efficiency_model_input_history`: `cycle_name IS NOT NULL` (ver [base.md](base.md#L78)).
- O `cycle_name` também é usado para identificar o **baseline** de um ciclo: primeiro `ingestion_date` por `cycle_name` onde `COUNTIF(stage = 'pending') = 0` (ver [base.md](base.md#L198-L200)).
