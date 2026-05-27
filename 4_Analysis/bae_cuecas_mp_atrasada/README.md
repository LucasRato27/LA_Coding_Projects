# BAE BRASIL - Cuecas Anti Suor com Atraso de MP

Analise das OPs `OPF37N1913` e `OPF37N1940`, seguindo a mesma proposta da analise `4_Analysis/ddal_mp_atrasada`.

Fonte principal:

```sql
`insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
```

Fonte de qualidade:

```sql
`insider-data-lake.sop_gold.quality_inspection_data`
```

Consulta executada em `2026-05-18`.

## Resultado por OP

| OP | Produto | Planejado OP | Recebido OP | MP Esperada | MP Real | Atraso MP | Lead Time MP ate entrega esperada | Qualidade |
|---|---|---:|---:|---|---|---:|---:|---|
| OPF37N1913 | Cueca Boxer Comfort Anti Suor Masculino | 2.099 | - | 2026-02-19 | 2026-02-27 | 8 dias | 83 dias | Aprovada em 2026-05-15 |
| OPF37N1940 | Cueca Boxer Performance Anti Suor Masculino | 2.126 | - | 2026-03-05 | 2026-03-17 | 12 dias | 76 dias | Sem registro de qualidade na fonte ate 2026-05-18 |

## Causa raiz

| Causa Raiz | Qtd OPs | Volume (Pecas) | % do Volume Total |
|---|---:|---:|---:|
| Atraso de Mp | 2 | 4.225 | 100,00% |
| Reprovacao de Qualidade / Reauditoria | 0 | 0 | 0,00% |
| Mp entregue dentro de um Lead Time menor que 45 dias | 0 | 0 | 0,00% |
| Lead Time Produtivo menor que 45 dias | 0 | 0 | 0,00% |

## Resposta objetiva

Nao. Para a `Cueca Boxer Comfort Anti Suor` afetada pela OP `OPF37N1913`, a causa raiz rastreavel e atraso de MP, mas nao ha evidencia de revisao/reprovacao de qualidade como causa raiz.

A OP `OPF37N1913` teve MP recebida 8 dias depois da data esperada (`2026-02-19` -> `2026-02-27`), mas a primeira auditoria de qualidade aparece como `qualita_approved` em `2026-05-15`, com deliberacao `insider_no_deliberation`.

Para comparacao, a OP `OPF37N1940` tambem teve atraso de MP, de 12 dias (`2026-03-05` -> `2026-03-17`), mas nao tinha registro de qualidade na tabela oficial ate a execucao de `2026-05-18`.

Conclusao: a causa comum entre as OPs e atraso de MP. A leitura "atraso de MP + revisao/reprovacao de qualidade" nao se confirma para a Comfort pela fonte oficial de qualidade usada nesta analise.

## Arquivos

- `target_ops_details.sql`: query de detalhe das duas OPs.
- `target_ops_details.csv`: resultado detalhado por OP.
- `root_cause_summary.csv`: resumo de causa raiz em CSV.
- `root_cause_summary.md`: resumo de causa raiz em Markdown.
