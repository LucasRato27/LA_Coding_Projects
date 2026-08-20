"""Camada de publicação governada para Google Sheets.

O notebook Deepnote mantém uma cópia desta implementação no bloco de exportação,
pois o runtime hospedado não monta este diretório local.
"""

from __future__ import annotations

import importlib.util
import json
import os
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import numpy as np
import pandas as pd

SHEETS_URL = "https://docs.google.com/spreadsheets/d/1XB9cZztzBziergCs7AzaeUMUzlM-Tg9vooakHSqaamk/edit"
SHEETS_BATCH_SIZE = 2_000
SHEETS_DESTINOS = (
    ("dim_mp_fornecimento", 0, "dim_mp_fornecimento"),
    ("fact_sku_bom", 1263944509, "fact_sku_bom"),
    ("fact_mp_lt_realizado", 784021430, "fact_mp_leadtime_realizado"),
    ("fact_sku_economics", 1858110063, "fact_sku_economics"),
    ("mart_produto_mp", 2006224731, "mart_produto_mp"),
    ("dicionario_dados", 1605547778, "dicionario_dados"),
    ("governanca", 1425809795, "df_governanca"),
)


def ensure_sheets_dependencies() -> None:
    """Install gspread only when the ephemeral Deepnote runtime needs it."""
    missing = [
        package
        for package, module in (("gspread", "gspread"), ("gspread-dataframe", "gspread_dataframe"))
        if importlib.util.find_spec(module) is None
    ]
    if missing:
        import subprocess
        import sys

        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])


def get_gspread_client():
    """Authenticate without logging credential contents, preferring Deepnote's service account."""
    ensure_sheets_dependencies()

    import google.auth
    import google.auth.transport.requests
    import google.oauth2.service_account
    import gspread

    scopes = (
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    )
    for variable in (
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "BIGQUERY_INTEGRATION_SERVICE_ACCOUNT",
        "BQ_SERVICE_ACCOUNT",
    ):
        raw = os.getenv(variable)
        if raw:
            credentials = google.oauth2.service_account.Credentials.from_service_account_info(
                json.loads(raw), scopes=scopes
            )
            print(f"GSheets auth: {variable}")
            return gspread.authorize(credentials)

    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(
            credentials_path, scopes=scopes
        )
        print("GSheets auth: GOOGLE_APPLICATION_CREDENTIALS")
        return gspread.authorize(credentials)

    try:
        credentials, _ = google.auth.default(scopes=scopes)
        if credentials and not credentials.valid:
            credentials.refresh(google.auth.transport.requests.Request())
        print("GSheets auth: Application Default Credentials")
        return gspread.authorize(credentials)
    except Exception as exc:
        raise RuntimeError(
            "Sem credenciais para Google Sheets. Use a service account compartilhada com a planilha."
        ) from exc


def value_for_sheets(value: Any) -> Any:
    """Keep scalars, write dates as ISO, and reject nested data before any clear."""
    if isinstance(value, (list, dict, set, tuple, np.ndarray)):
        raise TypeError(f"Valor aninhado não permitido na exportação: {type(value).__name__}")
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, pd.Timestamp):
        return "" if pd.isna(value) else value.date().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if pd.isna(value):
        return ""
    return value


def prepare_for_sheets(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """Validate one output and return a scalar-only Sheets payload."""
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"{name} não é DataFrame.")
    if df.empty:
        raise AssertionError(f"Preflight: {name} está vazio; nenhuma aba será limpa.")
    if df.columns.has_duplicates or any(
        not isinstance(column, str) or not column for column in df.columns
    ):
        raise AssertionError(f"Preflight: {name} tem cabeçalho vazio, inválido ou duplicado.")

    payload = df.copy()
    for column in payload.columns:
        payload[column] = payload[column].map(value_for_sheets)
    return payload


def preflight_outputs(outputs: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """Validate the complete publication set before mutating any destination tab."""
    expected = [name for name, _, _ in SHEETS_DESTINOS]
    missing = [name for name in expected if name not in outputs]
    extra = [name for name in outputs if name not in expected]
    if missing or extra:
        raise AssertionError(f"Preflight: SAIDAS divergente. Ausentes={missing}; extras={extra}.")
    return {name: prepare_for_sheets(outputs[name], name) for name in expected}


def resolve_destinations(client):
    """Resolve all worksheet IDs and titles before starting the first overwrite."""
    spreadsheet = client.open_by_url(SHEETS_URL)
    destinations = {}
    for name, gid, expected_title in SHEETS_DESTINOS:
        try:
            worksheet = spreadsheet.get_worksheet_by_id(gid)
        except Exception as exc:
            raise RuntimeError(f"Aba gid={gid} não encontrada para {name}.") from exc
        if worksheet.title != expected_title:
            raise RuntimeError(
                f"Aba gid={gid} mudou de nome: esperado={expected_title!r}; atual={worksheet.title!r}."
            )
        destinations[name] = worksheet
    return destinations


def write_worksheet(worksheet, payload: pd.DataFrame, name: str) -> dict[str, Any]:
    """Overwrite one tab in chunks and prove its structural post-condition."""
    from gspread_dataframe import set_with_dataframe
    from gspread.utils import rowcol_to_a1

    row_count, column_count = payload.shape
    worksheet.clear()
    worksheet.resize(rows=row_count + 1, cols=column_count)
    worksheet.update("A1", [payload.columns.tolist()], value_input_option="RAW")

    for start in range(0, row_count, SHEETS_BATCH_SIZE):
        set_with_dataframe(
            worksheet,
            payload.iloc[start : start + SHEETS_BATCH_SIZE],
            row=start + 2,
            col=1,
            include_index=False,
            include_column_header=False,
            resize=False,
        )

    if worksheet.row_values(1) != payload.columns.tolist():
        raise RuntimeError(f"{name}: cabeçalho pós-escrita divergente.")
    if worksheet.row_count != row_count + 1 or worksheet.col_count != column_count:
        raise RuntimeError(f"{name}: dimensão pós-escrita divergente.")

    last_row = worksheet.get(f"A{row_count + 1}:{rowcol_to_a1(1, column_count)}{row_count + 1}")
    if not last_row or not any(value != "" for value in last_row[0]):
        raise RuntimeError(f"{name}: última linha não foi encontrada após a escrita.")
    return {
        "table": name,
        "worksheet": worksheet.title,
        "gid": worksheet.id,
        "rows": row_count,
        "columns": column_count,
        "status": "success",
    }


def write_outputs_to_sheets(outputs: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Publish the seven outputs after global data and destination preflight."""
    payloads = preflight_outputs(outputs)
    client = get_gspread_client()
    destinations = resolve_destinations(client)
    results = [
        write_worksheet(destinations[name], payloads[name], name)
        for name, _, _ in SHEETS_DESTINOS
    ]
    return {"written": True, "spreadsheet_url": SHEETS_URL, "tables": results}
