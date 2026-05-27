# Causas Raízes ICP — LUTESTIL

**Data de análise:** 2026-05-18  
**Janela:** 2026-04-03 a 2026-05-18 (45 dias)  
**Contexto:** LUTESTIL fechou a semana de 2026-05-18 com ICP de 63,67%. Na semana seguinte (2026-05-25) saltou para 95,45% (+31,79 p.p. WoW).

---

## Resumo por Causa Raiz

| Causa Raiz | Qtd OPs | Volume (Peças) | % do Volume Total |
|---|---:|---:|---:|
| Atraso de MP | 3 | 3.527 | 65,00% |
| Reprovação de Qualidade (Entrega Autorizada) | 1 | 1.000 | 18,43% |
| Lead Time Produtivo (45 dias) | 0 | 0 | 0,00% |
| MP entregue dentro de LT < 45 dias | 0 | 0 | 0,00% |

**Total detrator:** 4 OPs | 5.427 peças

> OPs podem se enquadrar em mais de uma causa simultaneamente (ex: ATRASO_MP_E_QUALIDADE). Os percentuais podem somar mais de 100%.

---

## Detalhamento por OP

| OP | Ciclo | Atraso (dias) | Volume Plan. | Etapa Atual | Causa Raiz | Status MP | Atraso MP (dias) | Status Qualidade | taxa_reprovacao_qualita | taxa_reprovacao_etapa |
|---|---|---:|---:|---|---|---|---:|---|---:|---:|
| OPF92N167 | UW FEMININA - NOVO DROP - Revisada | 18 | 627 | finished | ATRASO_MP | ATRASADA | +4 | APROVADA | 0% | 0% |
| OPF92N33 | C112025 | 7 | 1.000 | quality_inspection | ATRASO_MP_E_QUALIDADE | ATRASADA | +1 | REPROVADA_LIBERADA | 100% | 0% |
| OPF92N66 | C112025 | 3 | 1.900 | cut_fabric_and_sewing_process | ATRASO_MP | ATRASADA | +1 | SEM_DADO_QUALIDADE | — | — |
| OPF92N69 | C112025 | 3 | 1.900 | quality_inspection | REPROVACAO_QUALIDADE | NO_PRAZO | 0 | REPROVADA (sem deliberação) | 100% | 0% |

> **`taxa_reprovacao_qualita`** = peças reprovadas na auditoria / total peças auditadas.  
> **`taxa_reprovacao_etapa`** = peças com deliberação `insider_rejected` / total peças auditadas.

---

## Observações

- **Causa dominante: Atraso de MP.** 3 das 4 OPs detratoras tiveram matéria-prima entregue depois da data esperada. Os atrasos foram pequenos (1 a 4 dias), mas suficientes para comprometer o prazo final de entrega ao CD.

- **OPF92N33** acumula duas causas: MP chegou 1 dia atrasada e **100% das 1.000 peças foram reprovadas na auditoria** — mas liberadas para entrega pela Insider (`insider_approved`, `taxa_reprovacao_etapa = 0%`). Está em `quality_inspection` com prazo revisado para 2026-05-29.

- **OPF92N69** teve **100% das 1.900 peças reprovadas na auditoria** (`taxa_reprovacao_qualita = 100%`). A deliberação ainda não foi registrada (`insider_no_deliberation`) — `taxa_reprovacao_etapa = 0%` porque nenhuma peça foi formalmente rejeitada pela Insider ainda. É a OP de maior risco para o ICP da semana atual.

- **OPF92N66** ainda não passou por auditoria (em `cut_fabric_and_sewing_process`). Prazo revisado para 2026-05-29.

- As duas OPs do C112025 com prazo revisado para 2026-05-29 explicam o salto de ICP desta semana: passaram da janela de análise de 2026-05-18 para a próxima, reduzindo o universo de detratores.
