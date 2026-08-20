# Lead Time Dashboard — validação retroativa de `created_at` vs. primeiro snapshot

Atualizado em 2026-08-04. Esta memória registra a evidência que fundamentou a
remoção definitiva do fallback por `ingestion_date`.

## Escopo e fontes

- Notebook vigente: `lead_time_dashboard`
  (`ba6032bb1f0843f2ba934db8b66d0728`).
- Notebook de auditoria: `Auditoria retroativa — criação de OP (2026-08-04)`
  (`0626482742d24c1b87ebf20cf411bb4d`).
- Fonte: `insider-data-lake.sop_silver.supply_chain_efficiency_model_input_history`.
- Universo: OPs `committed`, cohort planejado de 2025-01-01 a 2026-07-31,
  filtros estruturais vigentes e cronologia válida de `created_at`.
- Run mensal: `e3aab7be-7029-4445-8b67-a13d4e9d3596`, `success`.
- Run de corte antes/depois: `9156ebae-f761-4663-bf08-93530ff72d0c`,
  `success`, execução detached com storage readonly.

## Piso de cobertura

O menor `DATE(ingestion_date)` committed disponível é `2025-11-10`. Logo, para
OPs criadas antes desse dia, o primeiro snapshot é censurado à esquerda: mede a
primeira carga preservada, não a criação.

| Data de referência | Período | OPs | Gap mediano | P90 | Gap até 7d | Snapshot no piso |
|---|---|---:|---:|---:|---:|---:|
| `created_at` | antes de 10/11/2025 | 1.659 | 200d | 277d | 0,0% | 98,5% |
| `created_at` | em/após 10/11/2025 | 592 | 2d | 5d | 100,0% | 0,0% |
| planejada | antes de 10/11/2025 | 1.201 | 265d | 277d | 0,0% | 99,3% |
| planejada | em/após 10/11/2025 | 1.050 | 5d | 131d | 56,4% | 42,0% |
| entrega | antes de 10/11/2025 | 1.039 | 265d | 277d | 0,0% | 99,8% |
| entrega | em/após 10/11/2025 | 978 | 11d | 138d | 46,2% | 52,7% |

Para datas planejadas ou entregas anteriores ao piso, 100% dos primeiros
snapshots ocorrem depois da data de referência. Depois do piso, esse percentual
cai para 0%. A cauda ainda alta em cohorts planejados/entregues após o piso vem
de OPs legadas criadas antes da cobertura.

## Impacto nos indicadores

Por OP:

```text
SLA_created_at - SLA_snapshot = first_snapshot - created_at
```

Para Triangulação, o mesmo delta afeta diretamente o lead time realizado porque
a criação é o início da contagem. Para Produto Acabado, o início permanece em
`waiting_fabric_arrival`, portanto a data de criação não altera diretamente o
lead time realizado.

No recorte recente sem censura (`created_at >= 2025-11-10`), a diferença entre
os métodos é pequena: mediana de 2 dias e P90 de 5 dias. A grande divergência
retroativa é concentrada nas OPs anteriores à cobertura.

## Qualidade de cronologia

No recorte operacional auditado:

- 140 OPs tinham `created_at` posterior ao planejado;
- 91 OPs tinham `created_at` posterior à entrega.

Esses registros ficam disponíveis nas auditorias de cronologia e são excluídos
antes de qualquer KPI. Não há correção ou imputação silenciosa.

## Decisão vigente

1. `created_at` é a data canônica e exclusiva de criação.
2. OP sem `created_at` recebe `creation_date_source = 'missing_created_at'` e
   fica fora dos KPIs dependentes de criação.
3. O primeiro snapshot é somente diagnóstico de latência/cobertura.
4. Comparações entre métodos devem separar o legado pré-10/11/2025 da coorte
   recente sem censura.
5. Filtros, cohort, postergação, regras PA/Tri, SLA e demais partes da
   metodologia permanecem inalterados.

## Limitação

O padrão temporal é consistente com a entrada de OPs recentes pelo Muninn, mas
o sistema de origem não foi validado diretamente nesta consulta. Para afirmar
causalidade, cruzar uma coluna ou dimensão explícita de origem/migração.
