# Code Style & Conventions

## Idioma
- **Código**: variáveis e funções em inglês (snake_case)
- **Comentários e markdown cells**: misto português/inglês (predomina português para anotações)
- **Docstrings**: em inglês, quando presentes

## Python Style
- snake_case para variáveis e funções
- Funções utilitárias definidas inline nos notebooks (não há módulos .py separados)
- Sem type hints nos notebooks
- Docstrings (Google style) nas funções utilitárias principais (ex: `query_to_dataframe`, `read_sql_file`)

## Notebook Conventions
- Célula 1: imports
- Célula 2: parâmetros (datas, configurações)
- Célula 3: data de referência (`today`)
- Célula 4: funções utilitárias (BigQuery client, helpers SQL)
- Markdown cells como separadores de seção
- Análises exploratórias seguidas de gráficos Plotly

## SQL Style
- CTEs nomeadas com prefixo `CTE_` (ex: `CTE_OPS_DETAILS`, `CTE_INVOICES_DETAILS`)
- Keywords SQL em UPPERCASE
- Tabelas referenciadas com caminho completo: `project.dataset.table`
- Parâmetros com `{day_ini}` e `{day_end}` para filtros de data via Python `.format()`

## Utility Modules (.py separados do notebook)

A partir de 2026-06, análises complexas usam um módulo `.py` separado com funções reutilizáveis:
- Localizado no mesmo diretório do notebook
- Carregado via `sys.path.insert(0, str(ANALYSIS_DIR))`
- Padrão de funções: pandas-only, side-effect free, testáveis com dados sintéticos
- Inclui dataclasses `@dataclass(frozen=True)` para thresholds/parâmetros
- Type hints usados nas assinaturas (`df: pd.DataFrame`, `n: int`, etc.)
- Docstrings em inglês com seção "Parameters" quando há múltiplos args

Exemplo de padrão de importação em notebooks:
```python
ANALYSIS_DIR = Path('/Users/insider/LA_Coding_Projects/analyses/relatorio_td')
sys.path.insert(0, str(ANALYSIS_DIR))
from td_analysis_functions import prepare_executive_df, classify_priority, ...
```

## README.md com Resultados Populados

Análises em `analyses/{tema}/` incluem `README.md` com tabelas de resultados reais.
As tabelas são preenchidas por scripts Python temporários (prefixo `_`) que:
1. Executam a query BigQuery
2. Processam com as funções do módulo `.py`
3. Injetam tabelas Markdown nas seções do README via string replace
4. São deletados após execução

O README segue estrutura: Objetivo → Metodologia → Resultados (seções numeradas) → Limitações → Lista de Arquivos.

## Visualization
- Plotly como lib principal (bar charts empilhados, orientação h e v)
- Cores fixas por categoria com `color_discrete_map`
- `'Outros'` sempre em cinza (#D3D3D3)
- Labels em português nos gráficos
- Texto % dentro das barras (`textposition='inside'`)
