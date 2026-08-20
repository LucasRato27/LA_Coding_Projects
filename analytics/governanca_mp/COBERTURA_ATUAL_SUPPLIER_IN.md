# Cobertura Atual para Supplier [IN]

Atualizado em 2026-08-10. Este material descreve o que pode ser sustentado pelo
BigQuery e pelo notebook Deepnote `fabric_governance` no estado atual. Ele separa
fato observado de capacidade futura; não trata campos ausentes como se já
estivessem disponíveis.

## Mensagem executiva

Já existe uma base confiável para centralizar o cadastro comercial e operacional
de tecido, malharia e custo, além de medir o lead time real de compras de tecido.
Ela é suficiente para uma primeira tecidoteca operacional no Sheets e para
priorizar riscos de fornecimento. Ainda não suporta um buscador técnico completo
ou um portal de fornecedores: faltam atributos técnicos, documentos e o fluxo de
intake/onboarding.

## Matriz de cobertura

| Capacidade do Supplier [IN] | Cobertura atual | Evidência / limite |
|---|---|---|
| Catálogo de malharias | Disponível | Nome, razão social, cidade, UF, país, tipo, frete e condição comercial estão no cadastro Muninn. |
| Artigo, tecido e cotação | Disponível | A dimensão preserva artigo, tecido/cor, `fabric_sku`, status, preço, MOQ, múltiplos e volume por rolo. |
| Consumo de MP por produto | Disponível | `fact_sku_bom` está no grão SKU × tecido e calcula custo de MP por consumo. |
| CMV e markup | Disponível, com ressalva | Há CMV por SKU e markup vigente; custo de MP e CMV têm escopos diferentes e não devem ser comparados como equivalentes. |
| Lead time de malharia | Disponível, parcialmente cadastrado | LT cadastrado vem de artigo × malharia; LT realizado e atraso usam pedidos de tecido e compromisso original. |
| Dual sourcing | Parcial | A base identifica `n_malharias`, nomes e faixa de custo por tecido, mas não comprova equivalência técnica entre alternativas. |
| Simulador com PV/meta de margem | Parcial | A base fornece custo, CMV e markup histórico; ainda não recebe PV alvo, meta de CMV ou volume do briefing. |
| Busca por composição, gramatura, largura, pilling e desempenho | Indisponível | Não há colunas estruturadas de composição, gramatura, largura, peeling/pilling ou requisitos funcionais nas fontes de MP atuais. |
| Capacidade produtiva da malharia | Indisponível | Há campos de capacidade de confecção em outras fontes, mas não uma capacidade mensal governada para malharia no escopo de MP. |
| Claims, certificações e laudos | Indisponível | Não há repositório nem metadados estruturados de claims, ISO/ABNT, laudos ou anexos. |
| Cotação em moeda estrangeira | Indisponível | Preço de cotação existe, mas moeda e regras de conversão não fazem parte do contrato atual. |
| Histórico de preço/status | Indisponível | Cadastros Muninn são snapshots; uma execução posterior não permite inferir variação temporal. |
| Intake, Kanban e SLA de sourcing | Indisponível | Não há entidade de briefing, status de atendimento, responsável ou datas de SLA. |
| Portal/self-onboarding | Indisponível | A base é interna e de leitura; não existe fluxo autenticado de fornecedor para atualização ou upload. |

## O que a primeira camada entrega

As sete tabelas publicadas no Sheets tornam a base consumível sem replicar regras
de negócio: cadastro de fornecimento, BOM por SKU, pedidos e LT realizado,
economics por SKU, mart comercial, dicionário de dados e checks de governança.

A primeira publicação foi concluída em 10/08/2026, após dry-run bem-sucedido. O
Sheets é camada de consumo; Deepnote e BigQuery continuam sendo a fonte de verdade.

O atraso de MP é medido contra `estimated_invoicing_date`, a data de compromisso
original. A data repactuada é diagnóstica; usá-la como referência mascara atraso.
`NULL` é ausência de cadastro, especialmente para gramatura, largura e LT não
cadastrado — não deve ser convertido em zero.

## Próxima fase recomendada

**Caminho viável.** Criar um template CSV de ficha técnica de MP, com validação
de composição, gramatura, largura, pilling, capacidade, moeda, claims e anexos;
fazer ingestão controlada no BigQuery antes de abrir acesso externo.

**Provocação 10x.** Persistir snapshots versionados do cadastro e implantar um
intake com briefing, matching técnico, workflow de homologação e portal de
fornecedor. Com isso, dual sourcing deixa de ser uma comparação de preço e passa
a considerar especificação, risco, capacidade, prazo e certificação.

## Fontes e confiança

- Notebook Deepnote `fabric_governance` e seu contrato de schema: alta confiança
  para as sete saídas publicadas.
- Consulta de metadados BigQuery em `insider-data-lake.integrated`: alta confiança
  para existência de campos de cadastro, custo, volume, LT e localização.
- Ausência de atributos técnicos: confirmada por varredura de metadados em
  `integrated` e `sop_silver`; ainda assim deve ser revalidada quando novas fontes
  ou integrações forem adicionadas.
