# BAE Brasil — FUP ICP Semana 31/05/2026

Análise de causa raiz das OPs detratoras do ICP de BAE Brasil identificadas no acompanhamento semanal de 31/05.
Metodologia idêntica à usada em `4_Analysis/bae_cuecas_mp_atrasada`.

Fontes:

```sql
`insider-data-lake.sop_silver.supply_chain_efficiency_model_input`
`insider-data-lake.integrated.muninn_production_orders`
`insider-data-lake.sop_gold.quality_inspection_data`
```

Consulta executada em: **2026-06-01**

---

## Resultado por OP

| OP | Produto | Planejado OP | Recebido OP | Prazo Original | Prazo Revisado | MP Esperada | MP Real | Atraso MP | LT MP → Entrega | LT Produtivo | Qualidade | Causa Raiz |
|---|---|---:|---:|---|---|---|---|---:|---:|---:|---|---|
| OPF37N1991 | Cueca Boxer Performance Simples | 4.479 | — | 25/05 | 06/06 | 26/03/2026 | 06/04/2026 | **+11 dias** | 61 dias | 33 dias | Reprovada Qualita — deliberação Insider pendente (7,6% defeito) | **ATRASO_MP_E_QUALIDADE** |
| OPF37N1733 | Cueca Boxer Performance Simples | 1.000 | — | 15/05 | 03/06 | 08/01/2026 | 09/02/2026 | **+32 dias** | 114 dias | 98 dias | Reprovada Qualita + Rejeitada Insider — em reauditoria (7,1% defeito) | **ATRASO_MP_E_QUALIDADE** |
| OPF37N1940 | Cueca Boxer Performance Anti Suor | 2.126 | — | 04/05 | 29/05 | 05/03/2026 | 17/03/2026 | **+12 dias** | 73 dias | 44 dias | Aprovada em 22/05 (0% defeito) | **ATRASO_MP** |
| OPF37N1700 | Tech T-shirt Heavy Masculino | 822 | — | 15/05 | 08/06 | 25/12/2025 | 13/02/2026 | **+50 dias** | 115 dias | 101 dias | Reprovada Qualita + Rejeitada Insider — em reauditoria (11,9% defeito, 625 peças) | **ATRASO_MP_E_QUALIDADE** |
| OPF37N1875 | Spectrum Socks Mid 2.0 | 1.459 | 401 | 15/05 | 10/05 | 04/02/2026 | 05/02/2026 | +1 dia | 94 dias | 83 dias | Sem dado de qualidade | **ATRASO_MP** (entrega parcial) |

---

## Causa raiz agregada

| Causa Raiz | Qtd OPs | Volume em Aberto (Peças) | % do Volume |
|---|---:|---:|---:|
| ATRASO_MP_E_QUALIDADE | 3 | 6.301 | 59,4% |
| ATRASO_MP | 2 | 3.184 (1.126 faltantes socks) | 40,6% |

---

## Diagnóstico por OP

### OPF37N1991 — Cueca Boxer Performance Simples (4.479 peças)
**Causa raiz: Atraso de MP + Reprovação de Qualidade**

- MP chegou **11 dias atrasada** (26/03 → 06/04/2026), comprimindo o lead time produtivo para **33 dias** (abaixo do limiar de 45d).
- Auditoria realizada em **28/05/2026**: **reprovada pelo Qualita** com taxa de defeito de **7,6%**.
- Deliberação da Insider **ainda pendente** (`insider_no_deliberation`) — a OP está em `quality_inspection`, aguardando decisão: liberar para entrega ou reauditoria.
- Prazo revisado: **06/06/2026**.
- **Pergunta aberta:** qual o resultado esperado e prazo de deliberação da Insider?

---

### OPF37N1733 — Cueca Boxer Performance Simples (1.000 peças)
**Causa raiz: Atraso de MP + Reprovação de Qualidade**

- MP chegou **32 dias atrasada** (08/01 → 09/02/2026).
- **2 auditorias realizadas** (última em 04/05/2026): reprovada pelo Qualita e **rejeitada pela Insider** → OP está em **reauditoria**.
- Taxa de defeito: **7,1%**. Número de peças rejeitadas registrado pela Insider: 1.645 (dado acumulado entre as duas auditorias).
- Status atual: `items_delivery_and_invoicing` — faturamento aguardando liberação pós-reauditoria.
- Prazo revisado: **03/06/2026**.
- **Pergunta aberta:** qual o impedimento atual? A reauditoria já foi agendada/realizada?

---

### OPF37N1940 — Cueca Boxer Performance Anti Suor (2.126 peças)
**Causa raiz: Atraso de MP**

- MP chegou **12 dias atrasada** (05/03 → 17/03/2026) — mesma causa raiz da análise anterior de 18/05.
- **Auditoria aprovada em 22/05/2026** com 0% de defeito — qualidade não é mais bloqueio.
- Lead time produtivo: **44 dias** (borderline; ficou 1 dia abaixo do limiar de 45d).
- Status: `items_delivery_and_invoicing` — prazo revisado era 29/05 (já vencido).
- **Pergunta aberta:** com qualidade liberada, qual o impedimento para o faturamento? É um bloqueio sistêmico, fiscal ou de scheduling do CD?

---

### OPF37N1700 — Tech T-shirt Heavy Masculino (822 peças)
**Causa raiz: Atraso de MP + Reprovação de Qualidade (mais grave do lote)**

- MP chegou **50 dias atrasada** (25/12/2025 → 13/02/2026) — maior atraso absoluto de MP do grupo.
- **3 auditorias realizadas** (última em 11/05/2026): reprovada pelo Qualita e **rejeitada pela Insider** → em reauditoria.
- Taxa de defeito: **11,9%** com **625 peças rejeitadas** na primeira auditoria.
- Status: **`cut_fabric_and_sewing_process`** — a OP ainda não saiu de corte e costura, mesmo com 3 rodadas de auditoria.
- Prazo revisado: **08/06/2026**.
- **Pergunta aberta:** se a OP está em reauditoria, por que o estágio ainda aparece como corte e costura? Há produção de reposição das peças reprovadas em andamento? Qual a previsão de liberação?

---

### OPF37N1875 — Spectrum Socks Mid 2.0 (1.459 planejadas, 401 recebidas)
**Causa raiz: Entrega parcial — 1.058 peças não chegaram ao CD**

- Produção sinalizada como `finished`. MP chegou apenas **1 dia atrasada** (praticamente no prazo).
- Sem registro de qualidade na fonte — não há bloqueio de auditoria.
- **401 de 1.459 peças chegaram ao CD em 11/05**; as **~1.058 restantes não têm registro de entrada**.
- A classificação técnica é `ATRASO_MP` (pelo critério de 1 dia), mas a causa operacional real é **entrega fracionada sem segunda remessa**.
- **Pergunta aberta:** onde estão as ~1.058 peças restantes? Há NF emitida para o volume total? O transporte/faturamento do saldo foi agendado?

---

## Resumo executivo

Todas as 5 OPs têm **atraso de MP como causa raiz comum**. Em 3 delas (OPF37N1991, OPF37N1733, OPF37N1700), o atraso de MP comprimiu o lead time e combinado com falhas de qualidade gera o duplo bloqueio. Para OPF37N1940 e OPF37N1875, a MP foi a origem, mas o gargalo atual é operacional (faturamento e entrega parcial, respectivamente).

| Padrão | OPs |
|---|---|
| MP atrasada + Qualidade reprovada (bloqueio ativo) | OPF37N1991, OPF37N1733, OPF37N1700 |
| MP atrasada + Qualidade aprovada (bloqueio em faturamento) | OPF37N1940 |
| MP atrasada + Entrega parcial (saldo sem remessa) | OPF37N1875 |

---

## Arquivos

- `target_ops_details.sql`: query de detalhe das 5 OPs com classificação de causa raiz.
- `target_ops_details.csv`: resultado detalhado por OP exportado do BigQuery em 2026-06-01.
