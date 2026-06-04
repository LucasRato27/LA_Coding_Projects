# FITMAX — Relatório de Causa Raiz de Atraso

**Data de análise:** `2026-06-01`
**Fornecedor:** FITMAX LTDA
**Contexto:** A Fitmax entrou no coorte de ICP na semana de 2026-06-01 com indicador de **0%**.
As 1.630 peças das OPs apontadas estão planejadas e nenhuma foi recebida no CD.

---

## Universo de OPs detratoras (janela 90 dias — de 2026-03-03 a 2026-06-01)

| Métrica | Valor |
|---|---|
| Total de OPs detratoras Fitmax | 3 |
| Volume total (peças) | 2.665 |
| OPs com MP atrasada (real > esperada) | 0 |
| OPs sem data real de MP | 2 |
| OPs sem data esperada de MP | 1 |

> ⚠️ **Além das 2 OPs apontadas, há uma terceira OP detratora (`OPF54N204`) com 61 dias de atraso
> e 1.035 peças, não mencionada na análise original.**

### Detalhamento

| OP | Ciclo | Peças | Prazo Orig. | Prazo Revisado | Dias Atraso | Status MP | LT Produtivo |
|---|---|---:|---|---|---:|---|---:|
| `OPF54N204` | C092025 | 1.035 | `2026-04-01` | `2026-06-19` | 61 | `SEM_DATA_REAL` | 66 |
| `OPF45N513` | 24-16-s3 | 702 | `2026-05-15` | `2026-06-19` | 17 | `SEM_DATA_REAL` | 102 |
| `OPF54N81` | 24-09-s3 | 928 | `2026-05-25` | `2026-06-30` | 7 | `SEM_DATA_ESPERADA` | 77 |

---

## Detalhe por OP apontada

### `OPF45N513` — Shorts Esportivo Serotonin Feminino

| Campo | Valor |
|---|---|
| **Peças planejadas** | 702 |
| **Prazo original (SC)** | `2026-05-15` |
| **Prazo revisado** | `2026-06-19` |
| **Atraso s/ prazo original** | 17 dias |
| **Ciclo** | `24-16-s3` |
| **Status Muninn** | `cut_fabric_and_sewing_process` |
| **Estágio SC** | `cut_fabric_and_sewing_process` |
| **Início produção planejado** | `2025-04-21` |

#### Matéria-Prima

| Campo | Valor |
|---|---|
| Status de recebimento de MP | `SEM_DATA_REAL` |
| MP esperada | `2025-08-07` |
| MP real | `—` |
| Atraso de MP | — |
| LT MP → entrega esperada | — |
| LT MP → fim < 45 dias? | — |

#### Lead Time Produtivo

| Campo | Valor |
|---|---|
| LT produtivo (início → entrega esperada) | 102 dias |
| LT produtivo < 45 dias? | ❌ Não |

#### Qualidade

| Campo | Valor |
|---|---|
| Resultado 1ª auditoria | Sem registro |
| Data 1ª auditoria | `—` |
| Taxa de defeitos | — |
| Peças rejeitadas (Insider) | — |

---

### `OPF54N81` — Performance T-shirt 2.0 Masculino

| Campo | Valor |
|---|---|
| **Peças planejadas** | 928 |
| **Prazo original (SC)** | `2026-05-25` |
| **Prazo revisado** | `2026-06-30` |
| **Atraso s/ prazo original** | 7 dias |
| **Ciclo** | `24-09-s3` |
| **Status Muninn** | `cut_fabric_and_sewing_process` |
| **Estágio SC** | `cut_fabric_and_sewing_process` |
| **Início produção planejado** | `2024-10-07` |

#### Matéria-Prima

| Campo | Valor |
|---|---|
| Status de recebimento de MP | `SEM_DATA_ESPERADA` |
| MP esperada | `—` |
| MP real | `2024-10-07` |
| Atraso de MP | — |
| LT MP → entrega esperada | 631 dias |
| LT MP → fim < 45 dias? | ❌ Não |

#### Lead Time Produtivo

| Campo | Valor |
|---|---|
| LT produtivo (início → entrega esperada) | 77 dias |
| LT produtivo < 45 dias? | ❌ Não |

#### Qualidade

| Campo | Valor |
|---|---|
| Resultado 1ª auditoria | Sem registro |
| Data 1ª auditoria | `—` |
| Taxa de defeitos | — |
| Peças rejeitadas (Insider) | — |

---

## Tabela de Causa Raiz por Volume (modelo padrão)

| Causa Raiz | Qtd OPs | Volume (Peças) | % do Volume Total |
|---|---:|---:|---:|
| — | — | — | — |
> ℹ️ Nenhuma causa raiz ativou no modelo padrão (ver análise abaixo).


---

## Análise de Causa Raiz — Achados

O modelo padrão (atraso MP / LT curto / reprovação de qualidade) **não ativou nenhuma causa**
para as OPs Fitmax. Isso ocorre porque os campos de data de MP estão incompletos.
Os achados reais são:

### 1. MP sem registro de recebimento real (OPF45N513 e OPF54N204)

As OPs `OPF45N513` e `OPF54N204` têm `expected_fabric_receiving_date` preenchida
(respectivamente `2025-08-07` e `2025-08-28`) mas **`real_fabric_receiving_date` é nulo**
— ou seja, a MP **nunca foi registrada como recebida** no sistema.

Hipóteses:
- A MP realmente não chegou ao fornecedor (bloqueio de cadeia de suprimentos).
- A MP chegou mas o recebimento não foi lançado na ferramenta.

**Ação recomendada:** confirmar com a Fitmax e/ou time de MP se o tecido foi recebido
e, em caso positivo, solicitar lançamento retroativo da data real.

### 2. OPF54N81 — OP de ciclo longo sem data esperada de MP

A `OPF54N81` (ciclo `24-09-s3`, setembro/2024) tem `real_fabric_receiving_date = 2024-10-07`
— **MP recebida há mais de 7 meses** — mas a OP ainda está em `cut_fabric_and_sewing_process`.
O `expected_fabric_receiving_date` está nulo.

Com LT produtivo de **77 dias** (início → entrega esperada de 30/06/2026), a OP está dentro
do prazo *revisado*, mas com **7 dias de atraso sobre o prazo original** (2026-05-25).

A causa de atraso aqui não é MP nem qualidade: é **lead time de produção** — a OP
está em corte/costura há meses com o prazo original já ultrapassado.

### 3. Prazos revisados indicam reprogramação ativa

Todos os prazos revisados (`dt_reviewed_entry_warehouse`) estão no futuro:
- `OPF45N513`: revisado para `2026-06-19`
- `OPF54N81`: revisado para `2026-06-30`
- `OPF54N204`: revisado para `2026-06-19`

Isso confirma que as OPs foram reprogramadas. O ICP mede sobre o prazo original,
daí o indicador de 0% mesmo com as OPs ainda "dentro do prazo revisado" do fornecedor.

### 4. Qualidade — sem registro

Nenhuma das 3 OPs tem registro na `quality_inspection_data`. Pode indicar que as
peças ainda não chegaram à etapa de auditoria (estão em corte/costura) ou que a
auditoria ainda não foi registrada.

---

## Conclusão

| Causa Raiz | OPs afetadas | Volume (peças) | Observação |
|---|---|---:|---|
| MP sem data real (não registrada/não chegou) | OPF45N513, OPF54N204 | 1.737 | Principal hipótese; requer confirmação |
| Lead time de produção longo (ciclo set/24) | OPF54N81 | 928 | MP OK, atraso no processo produtivo |
| Reprovação de qualidade | Nenhuma | 0 | Sem dado disponível ainda |

**Causa dominante (por volume): MP sem data real de recebimento — 1.737 peças (65,1% do volume).**

---

## Observações de qualidade de dados

- `real_fabric_receiving_date` nulo ≠ confirmação de que a MP não chegou. Verificar
  com time operacional antes de classificar como "atraso de MP".
- `OPF54N81` tem `real_fabric_receiving_date = 2024-10-07` sem `expected_fabric_receiving_date`.
  Possível inconsistência de cadastro ou OP com histórico atípico.
- O campo `current_production_stages` reflete o estágio mais recente com defasagem de até 24h.

---

## Arquivos

- `target_ops_details.sql` / `.csv`: detalhe das OPs OPF45N513 e OPF54N81.
- `details.sql` / `.csv`: todas as OPs Fitmax detratoras na janela de 90 dias.
- `root_cause_summary.sql` / `.csv`: tabela de causa raiz por volume.
- `fitmax_relatorio.ipynb`: notebook de execução e geração deste relatório.
