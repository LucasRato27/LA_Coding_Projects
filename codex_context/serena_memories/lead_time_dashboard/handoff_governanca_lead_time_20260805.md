# Handoff — Governança de Lead Time (2026-08-05)

Estado consolidado para continuidade em outra conversa. Validar sempre esta
memória contra o notebook atual antes de novas alterações.

> **Supersedido para referência do notebook:** consulte primeiro
> `handoff_governanca_lead_time_20260806.md`. Este documento permanece como
> registro histórico das decisões e runs de 2026-08-05.

## Artefatos principais

- Projeto Deepnote: `Governança Lead Time`
  (`62b1671d-dc82-4747-9bbb-b134a69a3491`).
- Notebook principal desta pasta: `lead_time_dashboard`
  (`ba6032bb1f0843f2ba934db8b66d0728`). Este é o destino padrão para pedidos
  relacionados a Lead Time, salvo indicação explícita em contrário.
- Para pedidos que mencionem **auditoria**, inspecionar o escopo aplicável em
  um ou ambos os notebooks abaixo antes de concluir ou alterar algo:
  - `Auditoria mensal — criação da OP: snapshot vs created_at`
    (`0fc8465ceffc4558a59cb81257cf3e3a`);
  - `Auditoria retroativa — criação de OP`
    (`0626482742d24c1b87ebf20cf411bb4d`).
- Links canônicos:
  - Dashboard: <https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/leadtimedashboard-ba6032bb1f0843f2ba934db8b66d0728>;
  - Auditoria mensal: <https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/Auditoria-mensal-criacao-da-OP-snapshot-vs-createdat-2026-08-04-0fc8465ceffc4558a59cb81257cf3e3a>;
  - Auditoria retroativa: <https://deepnote.com/workspace/INSIDER%20Store-c81c5c71-4837-4d5d-92f1-27ce0aff203f/project/Governanca-Lead-Time-62b1671d-dc82-4747-9bbb-b134a69a3491/notebook/Auditoria-retroativa-criacao-de-OP-2026-08-04-0626482742d24c1b87ebf20cf411bb4d>.
- Documentação local vigente:
  `analytics/lead_time/documentacao_governanca_lead_time.md`.

## Regras vigentes do dashboard

1. `production_order_type_filtro` é obrigatório e exclusivo:
   `committed` ou `incubation`, nunca os dois. O default é `committed`.
2. Todas as CTEs e bases derivadas devem respeitar o tipo selecionado. Há
   assertions em Python contra mistura de tipos.
3. O cohort temporal usa `dt_largest_entry_warehouse`: início inclusivo, mês
   final inteiro (`data_fim + 1 mês`) e apenas entregas até a data da execução.
4. Inputs de data vigentes: `data_inicio_analise` e `data_fim_analise`, com
   defaults jan/2025 e jul/2026. Os inputs legados duplicados foram removidos.
5. Séries realizadas e Evolução Trimestral usam mês/trimestre de entrega.
6. O eixo X do Ciclo Produtivo não usa entrega:
   - Base: `cycle_name` no padrão `CMMYYYY`; `C022025 = fev/2025`.
   - Extra: mês de `dt_planned_original`.
   - O nome interno `mes_alvo` foi preservado por compatibilidade; a interface
     usa “Mês do ciclo”.
7. A planilha Google Sheets produtiva só pode ser sobrescrita em runs
   `committed`. Runs `incubation` mantêm a exportação bloqueada.

## KPIs reconciliados do Ciclo Produtivo

- O cohort contém OPs com entrega registrada, mas isso não implica entrega
  integral em quantidade.
- Quantidade válida: `planned_quantity_op > 0`, recebida preenchida e não
  negativa.
- `pecas_atendidas = min(received_quantity_op, planned_quantity_op)` por OP.
- `saldo_nao_atendido = max(planned_quantity_op - received_quantity_op, 0)`.
- `% atendimento = sum(pecas_atendidas) / sum(pecas_planejadas_validas)`.
- Cards visíveis: OPs entregues, percentual de atendimento em peças, peças
  planejadas, peças atendidas e percentual dentro do prazo. Os indicadores de
  completas, parciais, quantidade inválida e saldo continuam calculados e
  validados para suportar as reconciliações, mas não são exibidos como cards.
- Invariantes obrigatórias:
  - completas + parciais = OPs com quantidade válida;
  - atendidas + saldo = planejadas.

No run produtivo final de `committed`, a validação observou 2.044 OPs no Ciclo
Produtivo: 873 completas, 1.171 parciais e zero inválidas. Peças planejadas =
3.363.295, atendidas = 2.827.558, saldo = 535.737 e atendimento = 84,1%.
Esses números são evidência da execução, não constantes metodológicas.

## Auditoria mensal da mudança de criação

- Método antigo: `MIN(ingestion_date)` dentro do histórico já `committed`.
- Método novo: `TIMESTAMP(DATE(MIN(created_at)))`, sem fallback.
- Cohort pareado: somente OPs entregues e elegíveis nos dois métodos, com os
  mesmos filtros estruturais, planejamento original e postergação.
- PA inicia nos dois métodos em `waiting_fabric_arrival`; Tri usa a data de
  criação correspondente a cada método.
- `mes_fechamento = DATE_TRUNC(dt_largest_entry_warehouse, MONTH)`.
- Janela: jan/2025 a jul/2026, com uma linha para cada um dos 19 meses.
- Deltas são medianas da diferença por OP (`novo - velho`).
- Resultado validado: 793 OPs pareadas, zero duplicidade mensal e 10 meses sem
  população elegível. A cobertura `committed` começa em nov/2025, explicando os
  meses anteriores zerados.

## Runs de aceite

- Auditoria mensal: `3db64093-ba2a-49ff-8a5d-d8379f4f8765` — sucesso, 19
  meses únicos e 793 OPs pareadas.
- Dashboard `committed`, sem exportação: `ffda9615-94b6-4e18-991c-7d46abde66ab`
  — 63/63 blocos aprovados.
- Dashboard `incubation`, sem exportação:
  `4fab971c-4b72-4bf8-8088-b5e4d2df1c37` — sucesso sem falhas.
- Run final sem overrides, default `committed` e exportação reativada:
  `44fab58c-4897-455e-91ba-24c2a059d843` — 63/63 blocos aprovados; 1.539 OPs
  × 38 colunas exportadas para a aba produtiva configurada.

## Cuidados para alterações futuras

- Não restaurar o mês de entrega como eixo do Ciclo Produtivo; entrega define
  apenas a entrada no cohort.
- Não adicionar opção `Todos` ao filtro de tipo.
- Não permitir exportação produtiva em `incubation`.
- Não calcular delta mensal como simples diferença entre medianas.
- Executar dry-runs com exportação desabilitada antes de um run produtivo.
- Preservar as validações para recortes vazios, unicidade de `op_code`, tipo de
  OP, datas, eixo híbrido e reconciliação de quantidades.
