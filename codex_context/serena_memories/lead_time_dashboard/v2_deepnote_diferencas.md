# Lead Time Dashboard v2 (Deepnote) — Diferenças vs. notebook original

Arquivo: `analytics/lead_time/lead_time_dashboard-2.ipynb` (versão publicada/rodando no Deepnote)
Notebook original: `analytics/lead_time/lead_time_dashboard.ipynb` (ver memória `lead_time_dashboard/bases_e_premissas`)

A v2 é essencialmente o MESMO pipeline e as MESMAS premissas de negócio (TARGET_LEAD_TIME=120d, thresholds 10/30d, percentil 0.75, buckets de volume, metodologia PA vs Tri, lógica de Lead Time Limpo/postergação intencional, ajuste Tri com fallback 60d) — mas roda nativamente no Deepnote (`_dntk.execute_sql` com templates Jinja `{{ var }}` / `{% if %}` no lugar de `client.query(...).to_dataframe()` do notebook local). As diferenças materiais estão abaixo.

## 1. Mudança de metodologia — SQL_POSTPONEMENT (impacta "Lead Time Limpo")

**Original**: baseline (`dt_planned_original`) = primeira `dt_planned_entry_warehouse` observada por op_code (primeiro `ingestion_date`). Postergação = soma de qualquer delta positivo dia-a-dia em `dt_planned_entry_warehouse`.

**v2 — mudanças**:
- Filtro adicional `production_order_type = 'committed'` aplicado tanto em `ops_in_scope` quanto em `committed_history`, e também exige que o registro anterior (`prev_production_order_type`) também seja `'committed'` antes de contar a postergação. Isso evita contar mudanças de data que ocorreram enquanto a OP ainda não era `committed`.
- **`dt_planned_original` agora é `MIN(dt_planned_entry_warehouse) WHERE dt_reviewed_entry_warehouse IS NOT NULL`** (antes era simplesmente o primeiro valor histórico, sem exigir `dt_reviewed_entry_warehouse` preenchido). Isso muda a data-baseline usada como referência "pré-postergação".
- **Nova condição-chave em `postponement_totals`: só conta como postergação intencional quando `prev_planned = prev_reviewed`** (ou seja, no snapshot anterior, a data planejada e a data revisada pelo fornecedor eram iguais — não havia uma revisão pendente/divergente). Isso é uma tentativa de isolar melhor postergação puramente interna (Insider) de casos em que o fornecedor já havia sinalizado uma revisão de prazo (`dt_reviewed_entry_warehouse` diferente do planejado), que não deveria ser tratada como "postergação intencional Insider".
- **Impacto prático**: `qt_dias_postergacao_intencional` e `flag_teve_postergacao` por OP podem divergir entre as duas versões — a v2 é mais conservadora/precisa na atribuição de causa (menos falsos positivos de "postergação interna" quando na verdade havia uma revisão de fornecedor em curso).

## 2. Desacoplamento de `df_capacity` (capacidade) do lead time teórico cadastrado

**Original**: `df_capacity` era saneado (linhas com LT inválido removidas) e usado diretamente como fonte de lookup de `lead_time_teorico` no merge com `df_ops` (via `alias`/`product_name`/`is_finished_product`).

**v2 — mudança estrutural**:
- `df_capacity` é preservado INTACTO (não sofre mais remoção de linhas por LT inválido) — passa a servir só para métricas de capacidade produtiva e para trazer `tag_abc` (curva ABC) via merge separado.
- Nova tabela dedicada **`df_lead_time_cadastro`**: construída a partir de uma query separada e mais simples de `muninn_apparel_manufacturers_products` × `muninn_apparel_manufacturers` × `muninn_suppliers` × `muninn_products` (sem depender de `apparel_manufacturer_production_units_products`/capacidade), com dedup por `(supplier_name, product_name_key, is_finished_product_order)`, priorizando status `available > approved > incubation` e o menor `lead_time_teorico_base` válido em caso de empate.
- O join de `df_ops` para obter `lead_time_teorico_base` agora usa `df_lead_time_cadastro` (chave normalizada `product_name_key = lower(strip(product_name))`), não mais `df_capacity`.
- **Motivo provável**: evitar que o filtro de capacidade (`weekly_maximum_productive_capacity > 0`, status da célula produtiva) removesse cadastros de LT válidos que não tinham célula produtiva ativa no momento — separar "tenho LT cadastrado" de "tenho capacidade produtiva alocada".
- Auditoria de join (`df_lead_time_join_audit`) agora inclui um campo **`motivo_provavel`** categorizado: "sem cadastro fornecedor x produto x fluxo", "lead time cadastrado nulo, zero ou invalido", "lead time cadastrado sem valor positivo", "produto com nome divergente ou fluxo divergente" — diagnóstico mais granular do que a v1.
- Auditoria de fallback Tri (tecido não mapeado) agora também é registrada em `df_lead_time_join_audit` com `lead_time_status='ok_com_fallback_tri'` e motivo "tecido nao encontrado; fallback 60d aplicado" (na v1 isso só aparecia em prints de cobertura, não na tabela de auditoria).

## 3. Nova tabela `df_mpp` (distinta de `df_fabric_tempo`)

- Query SQL adicional e independente, quase idêntica à base de `df_fabric_tempo` (mesmo `fabric_costs`/`article_sku` etc.), mas SEM o join com `prepared_muninn_articles_knitting_factories` (não traz tempo de tingimento/produção) — apenas identifica o tecido principal por produto (`tecido_principal`) e adiciona `qtd_tecidos_total` (contagem de tecidos distintos por produto).
- Usada exclusivamente no bloco "Decomposição do Lead Time por Matéria Prima" para agrupar OPs por tecido principal — não substitui `df_fabric_tempo`, que continua sendo a fonte do ajuste de LT teórico Tri (+tempo_total_dias).
- **Atenção**: são duas queries paralelas de tecido principal, com lógicas ligeiramente diferentes (uma inclui tempo de malharia, a outra não) — se divergirem no futuro, podem gerar tecido_principal diferente para o mesmo produto entre os dois blocos.

## 4. Filtros de UI (Deepnote inputs) — equivalentes, com pequenas variações

- `data_inicio`/`data_fim` agora são objetos `date` (via `dateutil.parser`) vindos de inputs de data do Deepnote, com defaults `2026-01-01` e `2026-06-30` (não mais strings `YYYY-MM` como na v1, que tinha default `data_fim = hoje`).
- Listas de `suppliers`/`products` para os dropdowns agora vêm de queries SQL dedicadas direto na tabela `supply_chain_efficiency_model_input_history` (com filtro cascata: produtos dependem do fornecedor selecionado via Jinja), em vez de derivadas de `df_ops_raw` após carregado (como na v1).
- Célula 1.5 (validação de filtros) na v2 tem lógica adicional: se o `product_filtro` selecionado não pertence mais à lista de produtos válidos para o `supplier_filtro` atual, reseta automaticamente para `'(Todos)'` — proteção contra combinação de filtro inconsistente (não existia na v1).

## 5. SQL_OPS — pequena diferença de filtro condicional

- Na v2, o filtro de `supplier_relationship_status NOT IN ('terminated','discontinued')` só é aplicado quando `supplier_filtro != '(Todos)'` (dentro de um bloco `{% if %}` compartilhado com a condição de status ativo) — na v1 esse filtro era incondicional. Vale confirmar se isso é intencional ou um efeito colateral do template Jinja (pode estar deixando fornecedores terminated/discontinued entrarem quando nenhum filtro de fornecedor é aplicado).

## O que NÃO mudou (confirmado por leitura linha a linha)

- `TARGET_LEAD_TIME=120`, todos os valores de `CONFIG` (percentil 0.75, thresholds 10/30d, buckets de volume, min_ops)
- Metodologia dual PA (início em `waiting_fabric_arrival`) vs Tri (início em `stamp_created_production_order`)
- Filtro `production_order_type == 'committed'` e exclusão de ciclos B2B/EPA em `df_ops` (Célula 3)
- Fórmula de `lead_time_ajustado = (bruto - qt_dias_postergacao_intencional).clip(lower=0)` e lógica de `etapa_inflada` (baseline por OPs sem postergação, desvio positivo vs. mediana)
- `FALLBACK_TRI_DIAS = 60`
- ETAPAS_COLS (7 etapas) — porém `ETAPAS_LABELS` mudou de texto: v1 usava "Criação→agd, Agd→valid MP, Valid→corte, Corte exec, Costura, Inspeção, Fat→estoque"; v2 usa "Criação, Agd da OP, Aguardo MP, Aguardo Corte, Costura, Inspeção, Faturamento" (apenas rótulos de exibição, mesma ordem/semântica)
