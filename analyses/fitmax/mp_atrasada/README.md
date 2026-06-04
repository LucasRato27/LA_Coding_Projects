# FITMAX - Atraso de OPs no ICP (coorte de entrada semana 2026-06-01)

Analise das OPs do fornecedor Fitmax detratoras do indicador ICP.
A Fitmax entrou no coorte desta semana com ICP de 0%: 1,6k pecas planejadas, nenhuma recebida no CD.

Fonte principal:

```sql
`insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
```

Fonte de qualidade:

```sql
`insider-data-lake.sop_gold.quality_inspection_data`
```

## OPs apontadas

| OP | Produto | Pecas | Prazo | Atraso | Estagio Atual |
|---|---|---:|---|---:|---|
| OPF45N513 | Shorts Esportivo Serotonin Feminino | 702 | 2026-05-15 | 16 dias | Corte e Costura |
| OPF54N81 | Performance T-shirt 2.0 Masculino | 928 | 2026-05-25 | 7 dias | Corte e Costura |

## Criterios de analise

Campos usados:

- `dt_planned_entry_warehouse`: data planejada de entrada no CD — define a janela de analise.
- `dt_min_entry_warehouse`: criterio de chegada no CD (nulo = ainda nao chegou).
- `expected_fabric_receiving_date`: data esperada de recebimento de MP/tecido.
- `real_fabric_receiving_date`: data real de recebimento de MP/tecido.
- `real_production_start_date`: data real de inicio de producao (proxy de lead time produtivo).

Janela de analise: 90 dias anteriores a `2026-06-01` (de `2026-03-03` ate `2026-06-01`).
Janela ampliada em relacao ao padrao de 45 dias para garantir captura das OPs Fitmax com prazo em maio.

Criterio de OP detratora ainda nao recebida:

```sql
dt_min_entry_warehouse IS NULL
OR dt_min_entry_warehouse > DATE '2026-06-01'
```

Excluidas OPs nos estagios:

```sql
pending
canceled
```

Regra de atraso de MP:

```sql
real_fabric_receiving_date > expected_fabric_receiving_date
```

## Causa raiz — hipoteses a verificar

1. **Atraso de MP** — `real_fabric_receiving_date > expected_fabric_receiving_date`
2. **Lead time MP a fim < 45 dias** — MP chegou dentro do prazo mas sem tempo suficiente para producao
3. **Lead time produtivo < 45 dias** — inicio de producao planejado muito perto do prazo de entrega
4. **Reprovacao de qualidade** — `first_audit_result_standardized = 'qualita_rejected'`
5. **Capacidade** — sem campo direto nas fontes disponíveis; proxy via `current_production_stages` e `production_stage` da Muninn

## Resultado por OP (preencher apos execucao do BigQuery)

| OP | MP Esperada | MP Real | Atraso MP | LT MP→Fim (dias) | LT Produtivo (dias) | Qualidade |
|---|---|---|---:|---:|---:|---|
| OPF45N513 | — | — | — | — | — | — |
| OPF54N81  | — | — | — | — | — | — |

## Causa raiz por volume (preencher apos execucao)

| Causa Raiz | Qtd OPs | Volume (Pecas) | % do Volume Total |
|---|---:|---:|---:|
| — | — | — | — |

## Observacoes de qualidade de dados

- O campo `current_production_stages` em `supply_chain_efficiency_model_input` reflete o estagio mais recente registrado; pode haver defasagem de 24h em relacao ao sistema operacional.
- Causa raiz "Capacidade" nao tem campo diretamente observavel nas fontes atuais. Proxy possivel: comparar `production_stage` da Muninn com o numero de OPs ativas do fornecedor na mesma janela.
- Confirmar codigo exato da OP `OPF54N81` — padrao observado para Fitmax e `OPF54N2xx`; pode estar truncado.

## Arquivos

- `target_ops_details.sql`: query de detalhe das OPs OPF45N513 e OPF54N81.
- `details.sql`: todas as OPs Fitmax detratoras na janela de 90 dias.
- `root_cause_summary.sql`: tabela de causa raiz agregada por volume.
- `target_ops_details.csv`: resultado detalhado das OPs apontadas.
- `details.csv`: resultado detalhado de todas as OPs Fitmax detratoras.
- `root_cause_summary.csv`: tabela de causa raiz em CSV.
- `root_cause_summary.md`: tabela de causa raiz em Markdown.
