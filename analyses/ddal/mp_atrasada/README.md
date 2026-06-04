# DDAL - Atraso de Materia-Prima (Tecido)

Analise de OPs do fornecedor DDAL detratoras do indicador de acuracia da producao: OPs com `dt_planned_entry_warehouse` nos ultimos 45 dias ate `2026-04-27` e ainda nao recebidas no CD.

Fonte principal:

```sql
`insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
```

Fonte de qualidade:

```sql
`insider-data-lake.sop_gold.quality_inspection_data`
```

Campos usados:

- `planned_production_delivery_date`: filtro de OPs com data planejada de entrega na janela.
- `dt_planned_entry_warehouse`: filtro de OPs com data planejada de entrega na janela. Esse campo bate com `planned_production_delivery_date` da Muninn para as OPs DDAL.
- `dt_min_entry_warehouse`: criterio de chegada no CD.
- `expected_fabric_receiving_date`: Data Esperada de Recebimento de Tecido.
- `real_fabric_receiving_date`: Data Real de Recebimento de Tecido.

Criterio de OP detratora ainda nao recebida:

```sql
dt_min_entry_warehouse IS NULL
OR dt_min_entry_warehouse > DATE '2026-04-27'
```

Tambem sao excluidas OPs nos estagios:

```sql
pending
canceled
```

Regra de atraso:

```sql
real_fabric_receiving_date > expected_fabric_receiving_date
```

Resultado principal desta execucao, ja filtrado para detratoras ainda nao recebidas:

- Total de OPs DDAL detratoras ainda nao recebidas: 18
- OPs com `dt_min_entry_warehouse` nulo: 18
- OPs com `dt_min_entry_warehouse` futuro: 0
- OPs com recebimento de MP/tecido atrasado: 5, ou 27.78%
- OPs com as duas datas de tecido preenchidas: 7
- Percentual de atraso de MP entre OPs com as duas datas de tecido preenchidas: 71.43%

Resultado estendido com qualidade e lead time:

- OPs com qualidade cruzada na tabela oficial: 14 de 18
- OPs com `first_audit_result_standardized = 'qualita_rejected'`: 6 de 18, ou 33.33%
- Reprovacao com entrega autorizada (`insider_approved`): 5 de 18, ou 27.78%
- Reprovacao com corrigir/reauditar (`insider_rejected`): 1 de 18, ou 5.56%
- OPs com lead time menor que 45 dias entre `real_fabric_receiving_date` e `expected_production_delivery_date`: 3 de 18, ou 16.67%

Tabela de causa raiz por volume:

| Causa Raiz | Qtd OPs | Volume (Pecas) | % do Volume Total |
|---|---:|---:|---:|
| Lead Time Produtivo (45 dias) | 11 | 9.203 | 58,79% |
| Atraso de Mp | 5 | 7.346 | 46,93% |
| Reprovacao de Qualidade (Entrega Autorizada) | 5 | 4.932 | 31,51% |
| Mp entregue dentro de um Lead Time menor que 45 dias | 3 | 3.365 | 21,50% |
| Reprovacao de Qualidade (Corrigir e Reauditar) | 1 | 754 | 4,82% |

Observacao: as causas podem se sobrepor na mesma OP, por isso os percentuais nao somam 100%.
Nao foi encontrada uma coluna de tipo de defeito em `sop_gold.quality_inspection_data`, `quality_summary` ou `quality_reaudit_data` para identificar "Problemas com Ribana (Cor/Tom)" de forma rastreavel.

Observacao sobre a reconciliacao com 16 OPs:

- Excluindo apenas `pending` e `canceled`, o universo fica em 18 OPs.
- As 2 OPs adicionais estao em `quality_inspection`: `OPF74N243` e `OPF74N260`.
- Se `quality_inspection` tambem for considerada fora do universo de "nao entregues", o total cai para 16 OPs.

Observacao de qualidade de dados da analise anterior:

- A OP `OPF74N237` aparece com `real_fabric_receiving_date = 2026-12-09`, data futura em relacao a `2026-04-27`.
- Essa OP nao esta no recorte atual de detratoras ainda nao recebidas.

Arquivos:

- `summary.sql`: query consolidada.
- `details.sql`: lista de OPs e classificacao.
- `extended_summary.sql`: query consolidada com qualidade e lead time de MP.
- `extended_details.sql`: lista de OPs com flags de qualidade e lead time de MP.
- `root_cause_summary.sql`: query da tabela final de causa raiz por volume.
- `summary.csv`: resultado consolidado.
- `details.csv`: resultado detalhado por OP.
- `extended_summary.csv`: resultado consolidado com qualidade e lead time de MP.
- `extended_details.csv`: resultado detalhado enriquecido.
- `root_cause_summary.csv`: tabela final de causa raiz por volume.
- `root_cause_summary.md`: tabela final em Markdown.
