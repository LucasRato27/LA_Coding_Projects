# Código complementar do bloco Deepnote; é concatenado após sheets_export.py.
# O runtime hospedado executa a cópia integral das duas fontes no notebook.

SAIDAS = {
    **TABELAS,
    "dicionario_dados": dicionario_dados,
    "governanca": df_governanca,
}


def exportar_csv(df: pd.DataFrame, nome: str) -> str:
    """Grava uma tabela governada em exports/. Ponto único de escrita em disco."""
    os.makedirs(DIR_EXPORT, exist_ok=True)
    caminho = os.path.join(DIR_EXPORT, f"governanca_mp_{nome}.csv")
    df.to_csv(caminho, index=False, encoding="utf-8")
    return caminho


if EXPORTAR_CSV:
    print(f"Exportando {len(SAIDAS)} tabelas para '{DIR_EXPORT}/'\n")
    for nome, df in SAIDAS.items():
        caminho = exportar_csv(df, nome)
        print(f"  {caminho:<50} {len(df):>7,} linhas x {df.shape[1]:>3} colunas")
else:
    print("EXPORTAR_CSV = False — nenhum arquivo escrito.")

if ESCREVER_SHEETS:
    SHEETS_EXPORT_RESULT = write_outputs_to_sheets(SAIDAS)
    for resultado in SHEETS_EXPORT_RESULT["tables"]:
        print(
            f"Sheets: {resultado['table']} → {resultado['worksheet']} | "
            f"{resultado['rows']:,} linhas × {resultado['columns']} colunas"
        )
else:
    SHEETS_EXPORT_RESULT = {"written": False, "tables": []}
    print("ESCREVER_SHEETS = False — dry-run: nenhuma aba foi alterada.")
