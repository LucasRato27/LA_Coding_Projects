# Project Structure

```
rp_coding_projects/
├── requirements.txt              # Dependências do ambiente conda (pip freeze)
├── Templates/                    # Templates reutilizáveis entre projetos
│   ├── 1_Inputs/1_SQL/          # SQLs template (vazio por enquanto)
│   ├── 2_Códigos/
│   │   ├── conexao_bq.ipynb     # Template de conexão com BigQuery
│   │   └── credenciais_sheets.ipynb  # Template de conexão com Google Sheets
│   ├── 3_Outputs/
│   └── 4_Analysis/
├── 20260415_Lookback cadeia/     # Projeto ativo: Lookback da cadeia de suprimentos
│   ├── 1_Inputs/
│   │   └── 1_SQL/               # Queries SQL (receb.sql, ops.sql, sales.sql, stock.sql, supplier_info.sql)
│   │       └── 1_SQLs para Claudio/  # SQLs auxiliares para análises complementares
│   ├── 2_Códigos/
│   │   └── Análises Lookback_v20260414.ipynb  # Notebook principal de análise
│   ├── 3_Outputs/               # Outputs gerados (Excel, CSVs, etc.)
│   └── 4_Analysis/              # Análises finais / apresentações
```

## Padrão de Organização de Projetos
Cada projeto segue a estrutura: `1_Inputs/` → `2_Códigos/` → `3_Outputs/` → `4_Analysis/`
- Novos projetos devem ser criados como pastas datadas `YYYYMMDD_Nome do Projeto/`

---

## LA_Coding_Projects (workspace ativo — 2026-06-12)

```
LA_Coding_Projects/
├── base.md                        # Referência completa de tabelas BigQuery
├── bigquery_starter.ipynb         # Template de conexão BQ
├── CLAUDE.md                      # Persona e metodologia do assistente
├── .venv-1/                       # Virtualenv Python 3.13 (ativar: source .venv-1/bin/activate)
│
├── analyses/                      # Análises ad-hoc por tema
│   ├── relatorio_td/              # ← NOVO (2026-06-11) — Pipeline T&D Reversas
│   │   ├── pipeline_reversas_priorizacao_produtos.sql
│   │   ├── td_analysis_functions.py
│   │   ├── TD_Priorizacao_Melhorias_v20260611.ipynb
│   │   └── README.md              # Resultados reais populados via BigQuery
│   ├── cascateamento_fornecedores/
│   ├── ddal/
│   ├── fitmax/
│   ├── icp_causas_raiz/
│   ├── lutestil/
│   └── mp_skp_project/
│
├── analytics/                     # Dashboards e análises contínuas
│   ├── alerta_risco_cadeia/
│   │   └── Alerta_Risco_Cadeia_v20260429.ipynb
│   ├── icp_saude_estoque/
│   │   └── ICP_Saude_Estoque_v20260605.ipynb
│   ├── lead_time/
│   │   └── lead_time_dashboard.ipynb
│   └── plan_freeze_rate/
│       └── KR1_Plan_Freeze_Rate_v20260420.ipynb
│
├── outputs/                       # CSVs exportados pelos notebooks
│   ├── relatorio_td/              # ← NOVO
│   ├── alerta_risco_cadeia/
│   ├── icp_saude_estoque/
│   └── plan_freeze_rate/
│
└── skills/                        # Superpowers skills (SKILL.md por diretório)
```

## Convenções Nomeação
- Notebooks em `analytics/`: `{Tema}_v{YYYYMMDD}.ipynb`
- CSVs em `outputs/`: `{topico}_{YYYYMMDD}.csv`
- Arquivos de análise em `analyses/`: módulos `.py` de funções utilitárias + SQL + README
- Funções utilitárias de análise: arquivos `.py` separados (ex: `td_analysis_functions.py`)
  — novo padrão adotado em 2026-06 (antes eram inline nos notebooks)