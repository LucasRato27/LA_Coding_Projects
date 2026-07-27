# Suggested Commands

## Sistema (macOS / Darwin)
- `ls`, `cd`, `find`, `grep`, `cat`, `head`, `tail` — comandos padrão
- `open .` — abrir Finder no diretório atual
- `pbcopy`, `pbpaste` — clipboard do macOS

## Python / Ambiente
- `conda activate <env>` — ativar ambiente conda
- `pip install -r requirements.txt` — instalar dependências
- `pip freeze > requirements.txt` — atualizar requirements

## Git
- `git add .` — stage all
- `git commit -m "mensagem"` — commit
- `git push` — push para remote
- `git status`, `git log --oneline` — status e histórico

## Jupyter (VS Code)
- Executar células diretamente no VS Code (Ctrl+Enter / Shift+Enter)
- Não há necessidade de `jupyter notebook` ou `jupyter lab` — usar extensão do VS Code

## BigQuery
- Autenticação via `gcloud auth application-default login` (se necessário)
- Projeto padrão: `insider-data-lake`
- Client: `bigquery.Client(project="insider-data-lake")`
