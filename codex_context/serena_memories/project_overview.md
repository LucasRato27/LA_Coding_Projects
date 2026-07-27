# Project Overview: rp_coding_projects

## Purpose
Repositório de projetos de dados da Insider Store (e-commerce de moda). Cada projeto fica em uma pasta datada (ex: `20260415_Lookback cadeia/`).
O projeto atual, **Lookback Cadeia**, é uma análise da cadeia de suprimentos (supply chain) — recebimentos, operações produtivas, ciclos de produção, etc.

## Tech Stack
- **Linguagem**: Python 3 (Jupyter Notebooks via VS Code)
- **Data**: pandas, numpy
- **Visualização**: plotly (principal), seaborn, matplotlib
- **Database**: Google BigQuery (`insider-data-lake` project)
- **Integrações**: Google Sheets (via gspread + google-auth)
- **Exportação**: xlsxwriter (Excel)
- **Ambiente**: macOS, conda/miniconda

## Data Sources
- BigQuery: tabelas em `insider-data-lake.sop_silver` (supply chain efficiency model), `insider-lake-sensitive.integrated_br` (invoices), `insider-data-lake.silver` (Shopify orders), `insider-data-lake.integrated` (suppliers)
- SQL files armazenados em `1_Inputs/1_SQL/`
- Google Sheets para outputs colaborativos

## Projetos Ativos

| Pasta | Projeto | Descrição |
|-------|---------|-----------|
| `20260415_Lookback cadeia/` | Lookback Cadeia | Análise da cadeia de suprimentos: recebimentos, OPs, ciclos, fornecedores |
| `20260420_OKR Estabilidade Planejamento/` | OKR Estabilidade Planejamento | KR1 = Plan Freeze Rate — % do plano original de cada ciclo que sobreviveu sem alteração interna |

## Key Datasets (Lookback Cadeia)
- `receb.sql`: Recebimentos no warehouse — combina OPs, invoices, SKUs, suppliers, ciclos
- `ops.sql`: Detalhes de ordens de produção (supply chain efficiency model)
- `sales.sql`, `stock.sql`, `supplier_info.sql`: Vendas, estoque, informações de fornecedores
