# Governança de Lead Time — reestruturação e memorial de cálculo

> Espelho do memorial Serena, sincronizado em 2026-08-04. Em caso de conflito,
> prevalecem o pedido atual do usuário, `AGENTS.md` e o notebook/dados atuais.

O antes/depois completo da data de criação está registrado em
`transicao_data_criacao_ingestion_created_at.md`.

## Atualização operacional — 2026-08-04

### Período ativo do cohort

- Os controles funcionais são `data_inicio_analise` e `data_fim_analise`, com
  defaults `2025-01-01` e `2026-07-01`.
- A validação os normaliza para as SQLs centrais de OPs e postergação, mantendo
  o limite exclusivo `data_fim + 1 mês` sobre `dt_planned_original`.
- Os antigos `data_inicio` e `data_fim` estão no final do notebook, depois dos
  consumidores, e não são filtros funcionais.
- CSVs e Google Sheets usam o cohort principal; a faixa SLA continua visual.

### Evolução trimestral de pontualidade

- A base `df_ops_historico_trimestral_raw` ignora apenas o período do cohort e
  considera entregas desde `2025-01-01`, mantendo filtros de fornecedor,
  produto e faixa SLA.
- O trimestre é o da entrega efetiva. A fonte é `op_code × SKU × snapshot`,
  mas o indicador consolida uma linha por `op_code`.
- `dt_planned_original` é a primeira data planejada não nula pós-deduplicação
  diária; a entrega é `MAX(dt_largest_entry_warehouse)` por OP.
- A OP é excluída se esteve `canceled` em qualquer snapshot. Fornecedor
  descontinuado e SKU desativada não excluem entregas históricas.
- Lead time é ajustado pela postergação interna válida. Entram apenas OPs
  entregues com SLA e lead time ajustado positivos.
- Roxa: SLA planejado `<100` e lead time ajustado `≤90d`. Azul: SLA planejado
  `≥100` e lead time ajustado `≤120d`. Cada série usa somente as OPs entregues
  no trimestre de sua coorte.
- Não há mínimo de amostra trimestral; o hover expõe a contagem de OPs.

### Validação

- Run `f0449351-b0cb-4342-b041-740a2dfbc09d`: sucesso, zero falhas e sem
  escrita em Google Sheets (desabilitada somente no snapshot do run).

### Data de criação transacional — 2026-08-04

- As bases principal e trimestral usam `MIN(created_at)`, normalizado para data,
  como `stamp_created_production_order`.
- Na ausência de `created_at`, não há imputação: registram
  `creation_date_source = 'missing_created_at'` e a OP fica fora dos KPIs
  dependentes de criação.
- O primeiro `ingestion_date` é somente diagnóstico de latência/cobertura e não
  pode ser usado como criação.
- Run `a178ed9e-ac77-4136-b0b4-6768bea72fc3`: sucesso, sem escrita no Sheets e
  com a flag de exportação restaurada ao final.
- A paridade principal × histórica foi validada em 2.418 OPs, sem divergências.
- O filtro `sla_planejado_dias > 0` deixou 370 OPs em T1/2025, 999 em T2/2025
  e 1.205 em T3/2025. As duas séries trimestrais agora começam em T1/2025.
- Guard rails excluem dos KPIs os casos com `created_at` posterior ao planejado
  ou à entrega, sem correção automática; os casos permanecem na auditoria.
- No recorte operacional validado, foram identificadas 140 OPs posteriores ao
  planejado e 91 posteriores à entrega.
- A cobertura do histórico começa em 10/11/2025. A comparação metodológica
  justa entre snapshot e `created_at` deve usar `created_at >= 2025-11-10` ou
  separar explicitamente o legado pré-cobertura.
