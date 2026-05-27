# Qualidade LUTESTIL — Reprovações Maio/2026 + OPs Detratoras ICP

**Data de análise:** 2026-05-25  
**Contexto:** LUTESTIL fechou a semana de 2026-05-18 com ICP de 63,67%, saltando para 95,45% (+31,79 p.p. WoW). A pergunta em aberto é se as reprovações de qualidade são pontuais ou comportamento recorrente.

---

## 1. Reprovações de Qualidade em Maio/2026

**2 OPs reprovadas** na primeira auditoria, totalizando **2.900 peças** (38,3% das 7.573 peças auditadas no mês).

| OP | Ciclo | Auditada em | Volume Auditado | taxa_reprovacao_qualita | taxa_reprovacao_etapa | Deliberação |
|---|---|---|---:|---:|---:|---|
| OPF92N33 | C112025 | 2026-05-14 | 1.000 | 100% | 0% | `insider_approved` — liberada para entrega |
| OPF92N69 | C112025 | 2026-05-19 | 1.900 | 100% | 0% | `insider_no_deliberation` — sem deliberação ainda |

> **`taxa_reprovacao_qualita`** = peças reprovadas na auditoria / total auditadas.  
> **`taxa_reprovacao_etapa`** = peças com deliberação `insider_rejected` / total auditadas (bloqueio efetivo).

Ambas as OPs têm prazo revisado para **2026-05-29** e são do ciclo C112025.

---

## 2. Histórico de Reprovações (últimos 9 meses)

**Resposta direta: comportamento recorrente.** A LUTESTIL apresentou reprovações em todos os 9 meses analisados, sem nenhum mês zerado. Houve tendência de melhora de set/25 (100%) até abr/26 (15,7%), mas maio reverteu essa trajetória.

| Mês | OPs Auditadas | OPs Reprovadas | Peças Auditadas | Peças Reprovadas | Taxa Reprovação |
|---|---:|---:|---:|---:|---:|
| Set/2025 | 2 | 2 | 2.125 | 2.125 | **100,0%** |
| Out/2025 | 15 | 9 | 11.300 | 6.517 | **57,7%** |
| Nov/2025 | 8 | 5 | 6.790 | 3.576 | **52,7%** |
| Dez/2025 | 12 | 3 | 14.870 | 3.290 | 22,1% |
| Jan/2026 | 14 | 5 | 17.352 | 6.337 | 36,5% |
| Fev/2026 | 4 | 1 | 4.923 | 1.249 | 25,4% |
| Mar/2026 | 12 | 3 | 12.628 | 2.813 | 22,3% |
| Abr/2026 | 16 | 2 | 17.075 | 2.688 | **15,7%** ← menor histórico |
| **Mai/2026** | **6** | **2** | **7.573** | **2.900** | **38,3%** ↑ piora |

**Leitura:** A taxa caiu de 100% em set/25 para 15,7% em abr/26 — uma melhora consistente de 7 meses. Maio rompeu essa tendência com alta de +22,6 p.p. em relação a abril. O volume auditado em maio ainda é baixo (6 OPs), o que amplifica qualquer reprovação.

---

## 3. OPs Detratoras do ICP (semana de 2026-05-18)

**4 OPs detratoras, 5.427 peças.** Definição: planejadas para chegar ao CD entre 2026-04-03 e 2026-05-18 que não chegaram até 2026-05-18.

### Resumo por Causa Raiz

| Causa Raiz | Qtd OPs | Volume (Peças) | % do Volume Total |
|---|---:|---:|---:|
| Atraso de MP | 3 | 3.527 | 65,0% |
| Reprovação de Qualidade (Entrega Autorizada) | 1 | 1.000 | 18,4% |

### Detalhamento por OP

| OP | Ciclo | Atraso (dias) | Volume Plan. | Etapa Atual | Causa Raiz | Status MP | taxa_reprovacao_qualita | Prazo Revisado |
|---|---|---:|---:|---|---|---|---:|---|
| OPF92N167 | UW FEMININA - NOVO DROP - Revisada | 18 | 627 | finished | ATRASO_MP | ATRASADA (+4d) | 0% | 2026-04-30 |
| OPF92N33 | C112025 | 7 | 1.000 | quality_inspection | ATRASO_MP_E_QUALIDADE | ATRASADA (+1d) | 100% | 2026-05-29 |
| OPF92N66 | C112025 | 3 | 1.900 | cut_fabric_and_sewing_process | ATRASO_MP | ATRASADA (+1d) | — (sem auditoria) | 2026-05-29 |
| OPF92N69 | C112025 | 3 | 1.900 | quality_inspection | REPROVACAO_QUALIDADE | NO_PRAZO | 100% | 2026-05-29 |

### Observações

- **OPF92N167** já está em `finished` mas não entrou no CD — possível problema de faturamento/NF.
- **OPF92N33 e OPF92N66** são par do ciclo C112025 com o mesmo lote de tecido (mesma `expected_fabric_receiving_date`: 2025-10-07, recebida em 2025-10-08, +1 dia). O atraso de MP foi pequeno, mas o ciclo produtivo se arrastou.
- **OPF92N69** (C112025, 1.900 peças) é a OP de maior risco para o ICP desta semana: 100% de reprovação na auditoria de 2026-05-19 e sem deliberação da Insider até agora. Enquanto não houver `insider_approved` ou `insider_rejected`, a OP não avança.
- As 3 OPs do C112025 com prazo revisado para **2026-05-29** são o principal risco de continuidade para a semana de 2026-05-25.
