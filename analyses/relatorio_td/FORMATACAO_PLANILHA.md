# Instruções de Formatação — Farol de Priorização TD

> Documento de referência para reprodução da planilha `TD_Priorizacao_Melhorias_Profissional.xlsx` em Python via `openpyxl`.  
> Toda instrução é suficientemente específica para uma LLM reproduzir sem ambiguidade.

---

## 1. Dependências e Setup

```python
import openpyxl
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
```

---

## 2. Paleta de Cores

Todas as cores são strings hexadecimais sem `#`, usadas em `PatternFill(fgColor=...)` e `Font(color=...)`.

### Cores da Marca
| Nome | Hex | Uso |
|---|---|---|
| `BRAND_BLACK` | `1A1A2E` | Fundo de headers principais, título da aba |
| `BRAND_WHITE` | `FFFFFF` | Texto sobre fundo escuro |
| `GRAY_LIGHT` | `F5F5F5` | Linhas alternadas (ímpares) nas tabelas |
| `GRAY_MID` | `E0E0E0` | Cor das bordas thin |
| `GRAY_HEADER` | `2D2D2D` | Texto em células de valor (bold dark) |

### Cores do Farol (sinal_priorizacao)
| Sinal | Hex fundo | Hex texto |
|---|---|---|
| Priorizar melhoria | `FF4444` | `FFFFFF` |
| Alerta em produto relevante | `FFC107` | `1A1A1A` (escuro) |
| Monitorar | `FF8C00` | `FFFFFF` |
| Não priorizar agora | `4CAF50` | `FFFFFF` |
| Sem evidência suficiente | `BDBDBD` | `FFFFFF` |

### Cores de Cluster (texto em negrito, fundo da linha inalterado)
| Cluster | Hex texto |
|---|---|
| HERO | `1565C0` (azul escuro) |
| CORE | `2E7D32` (verde escuro) |
| LONG TAIL | `6A1B9A` (roxo) |
| KILL | `B71C1C` (vermelho escuro) + `italic=True` |
| LANCAMENTO | `F57F17` (laranja) |

### Cores de Tendência (fundo da célula inteira)
| Tendência | Hex fundo | Texto |
|---|---|---|
| Aumentou | `FFEBEE` (rosa claro) | padrão |
| Estável | `F5F5F5` (cinza claro) | padrão |
| Caiu | `E8F5E9` (verde claro) | padrão |

### Cores de Header por Aba
| Aba | Hex fundo header | Hex tab (tabColor) |
|---|---|---|
| 📊 Resumo Executivo | `1A1A2E` | `1A1A2E` |
| 🔴 Priorizar Melhoria | `1A1A2E` (linha col) / `C62828` (banner) | `FF4444` |
| 📋 Relatório PF | `1A1A2E` | `1A1A2E` |
| 📂 Benchmark Categoria | `1565C0` | `1565C0` |
| 🏷️ Top Problemas | `6A1B9A` | `6A1B9A` |
| 🗂️ Farol Completo | `37474F` | `37474F` |
| ℹ️ Legenda | `455A64` | `455A64` |

---

## 3. Configuração Global de Todas as Abas

```python
ws.sheet_view.showGridLines = False   # sem linhas de grade
ws.sheet_properties.tabColor = "HEX" # cor da aba (ver tabela acima)
```

---

## 4. Padrão de Banner (Linha 1 de Cada Aba)

Toda aba começa com um banner na linha 1 que ocupa todas as colunas da tabela (merge total).

```python
ws.merge_cells("A1:XY1")           # XY = última coluna da tabela
ws["A1"] = "EMOJI  TÍTULO DA ABA"
ws["A1"].font  = Font(bold=True, color="FFFFFF", size=13)
ws["A1"].fill  = PatternFill("solid", fgColor="HEX_DA_ABA")
ws["A1"].alignment = Alignment(horizontal='left', vertical='center', indent=1)
ws.row_dimensions[1].height = 32
```

- **Exceção na aba 🔴 Priorizar Melhoria**: o fundo do banner é `C62828` (vermelho mais escuro que o farol), não `FF4444`.
- **Resumo Executivo**: banner é `size=16`, fundo `1A1A2E`, altura `40`.

---

## 5. Padrão de Header de Colunas (Linha 2 de Cada Aba)

Função auxiliar usada em todas as abas tabulares:

```python
def col_header(ws, row, col, value, bg="2D2D2D", fg="FFFFFF", wrap=True):
    cell = ws.cell(row=row, column=col, value=value)
    cell.font      = Font(bold=True, color=fg, size=10)
    cell.fill      = PatternFill("solid", fgColor=bg)
    cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=wrap)
```

- Altura da linha 2: `36` (exceto Resumo Executivo, que não tem linha 2 de colunas).
- O `bg` varia por aba (ver tabela de cores de header acima).

---

## 6. Padrão de Linhas de Dados

### Alternância de fundo
```python
bg_row = "FFFFFF" if i % 2 == 0 else "F5F5F5"  # branco / cinza claro
```
`i` é o índice 0-based da linha de dado (não a linha do Excel).

### Célula padrão de dado
```python
cell.fill      = PatternFill("solid", fgColor=bg_row)
cell.alignment = Alignment(horizontal='center', vertical='center')
```

### Coluna de nome do produto (sempre coluna A ou B nas abas)
```python
cell.font      = Font(bold=True, size=10)
cell.alignment = Alignment(horizontal='left', vertical='center', indent=1)
```

### Altura das linhas de dado
| Aba | Altura |
|---|---|
| 🔴 Priorizar Melhoria | 22 |
| 📋 Relatório PF | 44 (wrap_text ativo em diagnóstico e top_3) |
| 📂 Benchmark Categoria | 20 |
| 🏷️ Top Problemas | 20 |
| 🗂️ Farol Completo | 22 |

---

## 7. Bordas

Função auxiliar usada para aplicar borda thin em blocos retangulares:

```python
def set_thin_border(ws, min_row, max_row, min_col, max_col):
    thin   = Side(style='thin', color="E0E0E0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for r in range(min_row, max_row + 1):
        for c in range(min_col, max_col + 1):
            ws.cell(r, c).border = border
```

Chamada sempre com `(ws, 2, 2 + len(dados), 1, n_cols)` — inclui a linha de headers (linha 2) e todas as linhas de dado.

---

## 8. Freeze Panes

```python
ws.freeze_panes = "A3"   # congela linha 1 (banner) e linha 2 (headers)
```

Aplicado em todas as abas tabulares. **Não aplicado** em Resumo Executivo e Legenda.

---

## 9. Formatos Numéricos por Tipo de Dado

```python
cell.number_format = "0.000"      # scores (TD Score, Commercial Score, Priority Score)
cell.number_format = "0.0%"       # percentuais (TD Rate, Delta vs Categoria, Share)
cell.number_format = "#,##0"      # inteiros com separador de milhar (itens retornados, unidades)
cell.number_format = 'R$ #,##0'   # receita líquida em R$
cell.number_format = "0.0"        # scores de scorecard (0–100)
```

---

## 10. Larguras de Coluna por Aba

Definidas via `ws.column_dimensions[get_column_letter(i+1)].width = w`.

### 📊 Resumo Executivo
```
[4, 18, 4, 18, 4, 18, 4, 18, 4, 18]  # 10 colunas
```
(Colunas 1,3,5,7,9 são separadores estreitos de 4; colunas 2,4,6,8,10 são os cards de 18)

### 🔴 Priorizar Melhoria (17 colunas)
```
[5, 32, 16, 7, 10, 12, 11, 12, 10, 10, 11, 22, 22, 22, 20, 13, 16]
```
Ordem: Rank, Produto, Categoria, Gênero, Cluster, Priority Score, TD Score, Comm. Score, TD Rate, TD Categ., Δ vs Cat., Top 1, Top 2, Top 3, Tendência, Itens Retorn., Receita Líq.

### 📋 Relatório PF (18 colunas)
```
[32, 16, 7, 12, 28, 11, 13, 14, 13, 12, 12, 11, 20, 36, 52, 13, 13, 16]
```
Ordem: Produto, Categoria, Gênero, Cluster, Sinal Kill/Keep, TD Score, Score Tração, Score Unit Econ., Score Satisf., TD Categoria, TD Produto, Δ vs Cat., Tendência, Top 3 Motivos, Diagnóstico, Priority Score, Itens Retorn., Receita Líq.

### 📂 Benchmark Categoria (10 colunas)
```
[20, 13, 16, 18, 17, 17, 15, 16, 16, 12]
```

### 🏷️ Top Problemas (6 colunas)
```
[28, 18, 22, 22, 16, 22]
```

### 🗂️ Farol Completo (18 colunas)
```
[32, 16, 7, 12, 26, 13, 11, 12, 10, 10, 11, 24, 24, 24, 22, 13, 16, 11]
```

### ℹ️ Legenda (7 colunas)
```
A=15, B=15, C=D=E=F=G=20
```

---

## 11. Aba 📊 Resumo Executivo — Estrutura Específica

### Cards de Farol (linhas 4–11)
- 5 cards lado a lado, cada um ocupando 2 colunas (largura 18).
- Colunas de separação entre cards têm largura 4 e ficam vazias.
- Posições iniciais de coluna dos 5 cards: `[1, 3, 5, 7, 9]`.
- Linha 3 é vazia com altura 10 (espaçamento visual).
- Linha 4 é vazia com altura 15 (espaçamento visual).

**Linha 5 (título do card):**
```python
ws.merge_cells(start_row=5, start_column=col, end_row=5, end_column=col+1)
cell.font = Font(bold=True, color="FFFFFF" ou "1A1A1A", size=9)
cell.fill = PatternFill("solid", fgColor=HEX_DO_SINAL)
cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
ws.row_dimensions[5].height = 28
```
- Exceção: "Alerta em produto relevante" usa texto `1A1A1A` (escuro) porque o fundo `FFC107` é amarelo claro.

**Linhas 6–11 (métricas do card, 6 métricas):**
```python
# Coluna esquerda = label (texto cinza, alinhado à direita)
lc.font      = Font(color="757575", size=9)
lc.fill      = PatternFill("solid", fgColor="FAFAFA")
lc.alignment = Alignment(horizontal='right', vertical='center')

# Coluna direita = valor (bold, escuro, alinhado à esquerda)
vc.font      = Font(bold=True, color="2D2D2D", size=9)
vc.fill      = PatternFill("solid", fgColor="FAFAFA")
vc.alignment = Alignment(horizontal='left', vertical='center')
```
Altura de cada linha: 18.
Métricas exibidas: Produtos, Receita Líq. (R$ XM), Itens Retorn., TD Rate Médio, Share Receita, Share T&D.
Borda thin aplicada do bloco inteiro do card (linhas 5–11, colunas col–col+1).

### Seção Insights (linhas 13–18)
- Linha 12 vazia com altura 12 (espaçamento).
- Linha 13: header "PRINCIPAIS INSIGHTS" — merge A:J, fundo `1A1A2E`, texto branco bold size 11, altura 24.
- Linhas 14–18: 5 insights, altura 24 cada.

**Estrutura de cada linha de insight:**
```python
# Colunas 1–2: ícone emoji (merge)
ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=2)
c1.font      = Font(size=13)
c1.alignment = Alignment(horizontal='center', vertical='center')
c1.fill      = PatternFill("solid", fgColor="F5F5F5")

# Colunas 3–5: título em negrito (merge)
ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=5)
c2.font      = Font(bold=True, color="1A1A2E", size=10)
c2.fill      = PatternFill("solid", fgColor="F5F5F5")
c2.alignment = Alignment(vertical='center')

# Colunas 6–10: detalhe (merge, wrap_text)
ws.merge_cells(start_row=r, start_column=6, end_row=r, end_column=10)
c3.font      = Font(color="424242", size=9)
c3.fill      = PatternFill("solid", fgColor="F5F5F5")
c3.alignment = Alignment(vertical='center', wrap_text=True)
```
Borda thin aplicada em todo o bloco de insights.

---

## 12. Aba 📋 Relatório PF — Células Especiais

### Coluna Top 3 Motivos (coluna 14)
```python
cell.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
cell.font      = Font(size=9)
```
O texto vem com `<br>` do HTML — substituir por `\n` antes de inserir.

### Coluna Diagnóstico/Tweet (coluna 15)
```python
cell.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
cell.font      = Font(size=9, italic=True, color="424242")
```

### Coluna Sinal Kill/Keep (coluna 5)
Aplicar `sinal_fill` e `sinal_font` (mesmas funções do farol, descritas na seção 2).

---

## 13. Aba 🗂️ Farol Completo — Filtro de Dados

Exibe apenas produtos com `sinal_priorizacao != 'Sem evidência suficiente'`.
Ordenados por `priority_score` decrescente.

---

## 14. Aba ℹ️ Legenda — Estrutura Específica

Não tem linha 2 de headers de coluna. Começa na linha 3.

### Header de Seção
```python
ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=7)
cell.font      = Font(bold=True, color="FFFFFF", size=11)
cell.fill      = PatternFill("solid", fgColor="37474F")
cell.alignment = Alignment(horizontal='left', vertical='center', indent=1)
ws.row_dimensions[r].height = 26
```

### Item de Legenda
```python
# Colunas 1–2: rótulo colorido (merge, fundo da cor do item)
ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=2)
c1.font      = Font(bold=True, color=FG_DO_ITEM, size=10)
c1.fill      = PatternFill("solid", fgColor=BG_DO_ITEM)
c1.alignment = Alignment(horizontal='center', vertical='center')

# Colunas 3–7: descrição (merge, wrap_text, fundo FAFAFA)
ws.merge_cells(start_row=r, start_column=3, end_row=r, end_column=7)
c2.font      = Font(color="212121", size=10)
c2.fill      = PatternFill("solid", fgColor="FAFAFA")
c2.alignment = Alignment(horizontal='left', vertical='center', indent=1, wrap_text=True)
```
Borda thin em cada item. Altura: 22.
Linha de espaço (+1) após cada seção.

---

## 15. Ordenação dos Dados

| Aba | Ordenação |
|---|---|
| 🔴 Priorizar Melhoria | `priority_score` DESC |
| 📋 Relatório PF | `priority_score` DESC |
| 📂 Benchmark Categoria | `td_rate_categoria` DESC |
| 🏷️ Top Problemas | `produtos_priorizar` DESC (contagem de produtos no bucket "Priorizar") |
| 🗂️ Farol Completo | `priority_score` DESC |

---

## 16. Coluna Rank (somente na aba 🔴 Priorizar Melhoria)

```python
cell.font      = Font(bold=True, color="C62828", size=11)
cell.alignment = Alignment(horizontal='center', vertical='center')
```

---

## 17. Alertas Visuais de Threshold

### Aba 📂 Benchmark Categoria — Coluna "Produtos Priorizar" (coluna 6)
```python
if pp >= 2:  # 2 ou mais produtos em "Priorizar Melhoria" na categoria
    cell.font = Font(bold=True, color="C62828")
elif pp == 1:
    cell.font = Font(bold=True, color="E65100")
```

### Aba 🏷️ Top Problemas — Coluna "Produtos 'Priorizar'" (coluna 3)
```python
if priorizar >= 5:
    cell.font = Font(bold=True, color="C62828")
elif priorizar >= 2:
    cell.font = Font(bold=True, color="E65100")
```

---

## 18. Coluna Gênero

Valor exibido: `"F"` para `female`, `"M"` para `male`. Normalização feita antes de inserir:
```python
"F" if str(row[genero_col]).lower() == 'female' else "M"
```

---

## 19. Resumo das Funções Auxiliares

```python
def sinal_fill(sinal: str) -> PatternFill | None:
    s = str(sinal).lower()
    if s == 'priorizar melhoria':               return PatternFill("solid", fgColor="FF4444")
    if 'alerta' in s:                           return PatternFill("solid", fgColor="FFC107")
    if 'monitorar' in s:                        return PatternFill("solid", fgColor="FF8C00")
    if 'não priorizar agora' in s:              return PatternFill("solid", fgColor="4CAF50")
    if 'sem evidência' in s or 'sem evidencia': return PatternFill("solid", fgColor="BDBDBD")
    return None

def sinal_font(sinal: str) -> Font | None:
    if 'alerta' in str(sinal).lower(): return Font(bold=True, color="1A1A1A")
    return Font(bold=True, color="FFFFFF")

def tendencia_fill(tend: str) -> PatternFill | None:
    t = str(tend).lower()
    if 'aumentou' in t: return PatternFill("solid", fgColor="FFEBEE")
    if 'caiu' in t:     return PatternFill("solid", fgColor="E8F5E9")
    if 'estável' in t or 'estavel' in t: return PatternFill("solid", fgColor="F5F5F5")
    return None

def set_thin_border(ws, min_row, max_row, min_col, max_col):
    thin = Side(style='thin', color="E0E0E0")
    b    = Border(left=thin, right=thin, top=thin, bottom=thin)
    for r in range(min_row, max_row + 1):
        for c in range(min_col, max_col + 1):
            ws.cell(r, c).border = b

def col_header(ws, row, col, value, bg="2D2D2D", fg="FFFFFF", wrap=True):
    cell = ws.cell(row=row, column=col, value=value)
    cell.font      = Font(bold=True, color=fg, size=10)
    cell.fill      = PatternFill("solid", fgColor=bg)
    cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=wrap)
```
