# Qualidade DDAL — Reprovações Maio/2026 + OPs Detratoras ICP

**Data de análise:** 2026-05-25  
**Contexto:** DDAL com ICP em 65,4% (−3,3 p.p. WoW), tendência negativa há 4+ semanas. Dois pontos em aberto desde 18/05: status de 3 OPs com NF pendente e comportamento das reprovações de qualidade em maio.

---

## 1. Status das OPs com NF pendente no Muninn

As três OPs estão em **`waiting_fabric_arrival`** — a matéria-prima ainda não chegou. Nenhuma tem NF-e registrada no sistema (`nfe_numbers_op` vazio). As datas originais de previsão foram revisadas para julho/2026.

| OP | NF informada | Previsão original | Etapa atual | Planejado atual | Revisado atual | NF-e no sistema |
|---|---|---|---|---|---|---|
| OPF74N262 | 299.468 | 03/03/2026 | `waiting_fabric_arrival` | 2026-07-03 | 2026-07-03 | — |
| OPF74N310 | 302.596 | 12/04/2026 | `waiting_fabric_arrival` | 2026-07-06 | 2026-07-06 | — |
| OPF74N309 | — | 10/05/2026 | `waiting_fabric_arrival` | 2026-07-13 | 2026-07-16 | — |

**Leitura:** As três OPs tiveram suas datas de entrega revisadas massivamente para julho/26 (de 3 a 4 meses além das previsões originais). A NF de faturamento prevista para 18/19 de maio da OPF74N309 não está registrada como `nfe_numbers_op` no sistema — é provável que esteja em processo de aprovação no Muninn, ainda não vinculada à OP no pipeline. Enquanto estiverem em `waiting_fabric_arrival`, não há entrega possível.

> As três OPs não aparecem na análise de detratores ICP da semana de 2026-05-18 porque o `dt_planned_entry_warehouse` atual (julho/26) está fora da janela de análise. O atraso acumulado fica para as semanas seguintes.

---

## 2. Reprovações de Qualidade em Maio/2026

**9 OPs reprovadas**, 11.280 peças (61,4% das 18.370 auditadas no mês) — segunda maior taxa dos últimos 12 meses.

| OP | Ciclo | Auditada em | Peças Auditadas | taxa_reprovacao_qualita | taxa_reprovacao_etapa | Deliberação |
|---|---|---|---:|---:|---:|---|
| OPF74N316 | C062026 | 2026-05-11 | 228 | 100% | 0% | `insider_approved` — liberada |
| OPF74N270 | C052026 | 2026-05-11 | 793 | 100% | 0% | `insider_approved` — liberada |
| OPF74N271 | C052026 | 2026-05-11 | 711 | 100% | 0% | `insider_approved` — liberada |
| OPF74N284 | C052026 | 2026-05-13 | 364 | 100% | 0% | `insider_approved` — liberada |
| **OPF74N274** | **C052026** | **2026-05-13** | **2.486** | **100%** | **100%** | **`insider_rejected` — BLOQUEADA** |
| OPF74N293 | INS01 | 2026-05-13 | 1.972 | 100% | 0% | `insider_approved` — liberada |
| OPF74N285 | C052026 | 2026-05-13 | 229 | 100% | 0% | `insider_approved` — liberada |
| OPF74N232 | C032026 | 2026-05-19 | 2.024 | 100% | 0% | `insider_no_deliberation` — pendente |
| OPF74N275 | C052026 | 2026-05-22 | 2.473 | 100% | 0% | `insider_no_deliberation` — pendente |

> **`taxa_reprovacao_qualita`** = peças reprovadas na auditoria / total peças auditadas.  
> **`taxa_reprovacao_etapa`** = peças com deliberação `insider_rejected` / total (bloqueio efetivo).

**Destaque crítico:** OPF74N274 (2.505 peças planejadas, C052026) é a única OP com `insider_rejected` em maio — bloqueada, não pode ser entregue. Prazo revisado: 2026-06-15. Duas OPs (OPF74N232, OPF74N275) ainda aguardam deliberação.

---

## 3. Histórico de Reprovações (últimos 12 meses)

**Resposta direta: comportamento estrutural, não pontual.** 12 meses consecutivos com reprovações, sem nenhum mês zerado. Taxa oscila entre 24,4% e 94,7% — nenhuma tendência consistente de melhora.

| Mês | OPs Auditadas | OPs Reprovadas | Peças Auditadas | Peças Reprovadas | Taxa Reprovação |
|---|---:|---:|---:|---:|---:|
| Mai/2025 | 7 | 6 | 2.722 | 2.577 | **94,7%** |
| Jun/2025 | 24 | 10 | 8.716 | 4.163 | 47,8% |
| Jul/2025 | 22 | 18 | 7.721 | 6.234 | **80,7%** |
| Ago/2025 | 18 | 9 | 7.575 | 3.897 | 51,4% |
| Set/2025 | 13 | 8 | 8.524 | 3.809 | 44,7% |
| Out/2025 | 8 | 3 | 11.823 | 5.064 | 42,8% |
| Nov/2025 | 8 | 3 | 12.937 | 3.823 | 29,6% |
| Dez/2025 | 12 | 10 | 14.562 | 12.182 | **83,7%** |
| Jan/2026 | 6 | 2 | 10.330 | 4.132 | 40,0% |
| Fev/2026 | 15 | 10 | 13.300 | 9.874 | 74,2% |
| Mar/2026 | 18 | 12 | 18.967 | 13.314 | 70,2% |
| Abr/2026 | 16 | 4 | 15.313 | 3.739 | **24,4%** ← menor histórico |
| **Mai/2026** | **21** | **9** | **18.370** | **11.280** | **61,4%** ↑ piora |

**Leitura:** Não há padrão de melhora sustentada — a taxa oscila bruscamente mês a mês (dez/25 foi 83,7%, jan/26 caiu para 40,0%, fev/26 voltou a 74,2%). Abril/26 foi o único mês bom nos últimos 12 meses. Maio rompeu essa melhora pontual com +37 p.p. em relação a abril.

---

## 4. OPs Detratoras do ICP (semana de 2026-05-18)

**5 OPs detratoras, 6.695 peças.** Janela: 2026-04-03 a 2026-05-18.

### Resumo por Causa Raiz

| Causa Raiz | Qtd OPs | Volume (Peças) | % do Volume Total |
|---|---:|---:|---:|
| Atraso de MP | 3 | 4.343 | 64,9% |
| Reprovação de Qualidade (Entrega Autorizada) | 1 | 855 | 12,8% |
| Lead Time Produtivo (45 dias) | 1 | 754 | 11,3% |

### Detalhamento por OP

| OP | Ciclo | Atraso (dias) | Volume Plan. | Etapa Atual | Causa Raiz | Status MP | taxa_reprovacao_qualita | NF-e | Prazo Revisado |
|---|---|---:|---:|---|---|---|---:|---|---|
| OPF74N286 | EPA2201 | 31 | 1.599 | finished | SEM_CAUSA_IDENTIFICADA | ADIANTADA (−23d) | 0% | 15226 | 2026-04-17 |
| OPF74N271 | C052026 | 0 | 855 | finished | ATRASO_MP_E_QUALIDADE | ATRASADA (+10d) | 100% | 15441 | 2026-05-25 |
| OPF74N273 | C052026 | 0 | 983 | finished | ATRASO_MP | ATRASADA (+10d) | 0% | 15450 | 2026-05-25 |
| OPF74N275 | C052026 | 0 | 2.505 | cut_fabric_and_sewing_process | ATRASO_MP_E_QUALIDADE | ATRASADA (+10d) | 100% | — | 2026-05-29 |
| OPF74N281 | C052026 | 0 | 754 | items_delivery_and_invoicing | SEM_CAUSA_IDENTIFICADA | SEM_DATA_ESPERADA | 0% | — | 2026-05-26 |

### Observações

- **OPF74N286** (EPA2201, 31 dias de atraso) está em `finished` mas sem entrada registrada no CD — possível problema de faturamento/NF, similar ao caso das 3 OPs abertas. MP chegou 23 dias adiantada, sem reprovação de qualidade.
- **OPF74N271, OPF74N273 e OPF74N275** compartilham o mesmo lote de MP (mesma `real_fabric_receiving_date`: 2026-03-30, +10 dias de atraso). O atraso de MP propagou para todas as três OPs do ciclo C052026.
- **OPF74N275** (2.505 peças) é o maior risco do ICP da semana atual: reprovada na auditoria (22/05), sem deliberação ainda (`insider_no_deliberation`), em `cut_fabric_and_sewing_process` com prazo 2026-05-29.
- **OPF74N281** (INS01, `items_delivery_and_invoicing`) não tem data esperada de MP — pode ser OP de produto acabado ou importada. Sem NF-e registrada.
