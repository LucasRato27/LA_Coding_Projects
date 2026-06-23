"""
icp_root_cause_functions.py
===========================
Funções Python para classificação e agregação de causas-raiz de ICP.
Usadas pela skill icp-diagnostics após receber os dados do BigQuery.

Convenções:
  - Todas as funções aceitam DataFrame como entrada e retornam DataFrame (não modificam in-place).
  - Seguras para DataFrames vazios.
  - Nenhuma função usa iterrows() — operações são vetorizadas ou via apply() por grupo.
  - Contagem de OPs sempre via df['op_code'].nunique(), nunca len(df).
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# Constantes
# ─────────────────────────────────────────────────────────────────────────────

COHORT_WINDOW_DAYS: int = 45

ROOT_CAUSE_CATEGORIES: list[str] = [
    "REPROVACAO_QUALIDADE",
    "ATRASO_MP_CONFIRMADO",
    "ATRASO_FATURAMENTO_NF",
    "ATRASO_PRODUCAO",
    "ATRASO_PRODUCAO_DENTRO_PRAZO",
    "SEM_CAUSA_IDENTIFICADA",
]

_STAGES_MP:  frozenset[str] = frozenset({"raw_material_receiving", "raw_material_pending"})
_STAGES_NF:  frozenset[str] = frozenset({"finished", "items_delivery_and_invoicing"})
_STAGE_QA:   str = "quality_inspection"
_STAGE_SEW:  str = "cut_fabric_and_sewing_process"

_DETRACTOR_BASE_COLS: list[str] = [
    "op_code", "supplier_name", "product_name",
    "planned_quantity_op", "received_quantity_op", "gap_quantity",
    "dt_planned_entry_warehouse", "current_production_stage", "days_overdue",
]


# ─────────────────────────────────────────────────────────────────────────────
# Função 1 — Extrair OPs detratoras do cohort
# ─────────────────────────────────────────────────────────────────────────────

def get_detractor_ops(
    df: pd.DataFrame,
    reference_date: date,
    window_days: int = COHORT_WINDOW_DAYS,
) -> pd.DataFrame:
    """
    Filtra o DataFrame de supply_chain_efficiency_model_input e retorna
    uma linha por OP detratora dentro do cohort de ``window_days`` dias.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame completo da tabela supply_chain_efficiency_model_input,
        já com os filtros padrão de ICP aplicados pelo bigquery-icp-reader
        (production_order_type='committed', stages excluídos, etc.).
    reference_date : date
        Data de referência (D-1).
    window_days : int
        Tamanho da janela do cohort em dias (padrão: 45).

    Returns
    -------
    pd.DataFrame
        Uma linha por OP detratora com colunas definidas em _DETRACTOR_BASE_COLS.
        Retorna DataFrame vazio (com as mesmas colunas) se não houver detratoras.
    """
    if df.empty:
        return pd.DataFrame(columns=_DETRACTOR_BASE_COLS)

    ref = pd.Timestamp(reference_date)
    window_start = ref - pd.Timedelta(days=window_days)

    # 1. Filtrar cohort pelo dt_planned_entry_warehouse
    dt_planned_col = pd.to_datetime(df["dt_planned_entry_warehouse"], errors="coerce")
    in_cohort = (dt_planned_col > window_start) & (dt_planned_col <= ref)
    cohort = df.loc[in_cohort].copy()

    if cohort.empty:
        return pd.DataFrame(columns=_DETRACTOR_BASE_COLS)

    # 2. Deduplicar: grão OP×SKU → grão OP
    #    planned_quantity_op e received_quantity_op são repetidos por SKU; MAX evita soma duplicada.
    agg_spec: dict[str, str] = {
        "supplier_name":              "first",
        "product_name":               "first",
        "planned_quantity_op":        "max",
        "received_quantity_op":       "max",
        "dt_planned_entry_warehouse": "first",
        "dt_largest_entry_warehouse": "max",
        "current_production_stage":   "first",
    }
    # Manter apenas colunas presentes no DataFrame
    agg_spec = {k: v for k, v in agg_spec.items() if k in cohort.columns}
    dedup = cohort.groupby("op_code", as_index=False).agg(agg_spec)

    # 3. Filtrar OPs detratoras
    dt_recv = pd.to_datetime(dedup["dt_largest_entry_warehouse"], errors="coerce")
    is_detractor = dt_recv.isna() | (dt_recv > ref)
    detractors = dedup.loc[is_detractor].copy()

    if detractors.empty:
        return pd.DataFrame(columns=_DETRACTOR_BASE_COLS)

    # 4. Campos calculados
    dt_pl = pd.to_datetime(detractors["dt_planned_entry_warehouse"], errors="coerce")
    detractors["days_overdue"] = (ref - dt_pl).dt.days
    detractors["received_quantity_op"] = detractors["received_quantity_op"].fillna(0)
    detractors["gap_quantity"] = (
        detractors["planned_quantity_op"] - detractors["received_quantity_op"]
    )

    available = [c for c in _DETRACTOR_BASE_COLS if c in detractors.columns]
    return detractors[available].reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# Função 2 — Classificar causa-raiz de uma OP individual
# ─────────────────────────────────────────────────────────────────────────────

def classify_root_cause_single(
    op_row: dict | pd.Series,
    quality_row: Optional[dict | pd.Series] = None,
) -> str:
    """
    Classifica a causa-raiz de uma OP detratora individual.

    Aplica a taxonomia ICP em ordem de prioridade (primeira regra que bater vence):
      1. REPROVACAO_QUALIDADE
      2. ATRASO_MP_CONFIRMADO
      3. ATRASO_FATURAMENTO_NF
      4. ATRASO_PRODUCAO          (vencida: days_overdue > 0)
      5. ATRASO_PRODUCAO_DENTRO_PRAZO
      6. SEM_CAUSA_IDENTIFICADA

    Parameters
    ----------
    op_row : dict | pd.Series
        Campos da OP: current_production_stage, dt_planned_entry_warehouse,
        dt_largest_entry_warehouse, days_overdue.
    quality_row : dict | pd.Series | None
        Dados de qualidade da OP. Campos usados:
          - first_audit_result_standardized
          - first_audit_deliberation_standardized
        Pode ser None se a OP não tiver registro de auditoria.

    Returns
    -------
    str
        Uma das categorias em ROOT_CAUSE_CATEGORIES.
    """
    stage = op_row.get("current_production_stage") or ""

    # Avaliar dados de qualidade
    qa_blocked = False
    if quality_row is not None:
        result = quality_row.get("first_audit_result_standardized") or ""
        delib  = quality_row.get("first_audit_deliberation_standardized") or ""
        qa_blocked = (result == "qualita_rejected") and (delib != "insider_approved")

    # Lista ordenada de (condição, categoria) — primeira verdadeira vence
    rules: list[tuple[bool, str]] = [
        (
            stage == _STAGE_QA or qa_blocked,
            "REPROVACAO_QUALIDADE",
        ),
        (
            stage in _STAGES_MP,
            "ATRASO_MP_CONFIRMADO",
        ),
        (
            stage in _STAGES_NF and pd.isna(op_row.get("dt_largest_entry_warehouse")),
            "ATRASO_FATURAMENTO_NF",
        ),
        (
            stage == _STAGE_SEW and (op_row.get("days_overdue") or 0) > 0,
            "ATRASO_PRODUCAO",
        ),
        (
            stage == _STAGE_SEW,
            "ATRASO_PRODUCAO_DENTRO_PRAZO",
        ),
    ]

    for condition, category in rules:
        if condition:
            return category
    return "SEM_CAUSA_IDENTIFICADA"


# ─────────────────────────────────────────────────────────────────────────────
# Função 3 — Sinal cross-OP de atraso de MP por artigo/tecido
# ─────────────────────────────────────────────────────────────────────────────

def classify_mp_affinity_signal(
    df_detractors: pd.DataFrame,
    df_fabric: pd.DataFrame,
) -> pd.DataFrame:
    """
    Detecta o sinal ATRASO_MP_TECIDO: 2+ produtos detratores do mesmo fornecedor
    compartilham o mesmo article_id, sugerindo atraso de matéria-prima comum.

    Este sinal é cross-OP e não pode ser determinado linha a linha — é calculado
    após todas as OPs individuais terem sido classificadas.

    Parameters
    ----------
    df_detractors : pd.DataFrame
        OPs detratoras com colunas: op_code, supplier_name, product_name, gap_quantity.
    df_fabric : pd.DataFrame
        Mapeamento produto → artigo de fabric_product_query.sql com colunas:
        product_name, article_id, article_name.

    Returns
    -------
    pd.DataFrame
        Uma linha por supplier_name × article_id com colunas:
          supplier_name, article_id, article_name,
          affected_products (list[str]), n_ops_affected (int),
          total_gap_quantity (int), mp_affinity_signal (bool).
        mp_affinity_signal = True apenas quando 2+ produtos distintos
        do mesmo fornecedor compartilham o artigo.
    """
    _out_cols = [
        "supplier_name", "article_id", "article_name",
        "affected_products", "n_ops_affected", "total_gap_quantity", "mp_affinity_signal",
    ]

    if df_detractors.empty or df_fabric.empty:
        return pd.DataFrame(columns=_out_cols)

    # Join OPs detratoras com mapeamento de artigo (nível product_name)
    fabric_dedup = (
        df_fabric[["product_name", "article_id", "article_name"]]
        .drop_duplicates(subset=["product_name", "article_id"])
    )
    merged = df_detractors.merge(fabric_dedup, on="product_name", how="inner")

    if merged.empty:
        return pd.DataFrame(columns=_out_cols)

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        products = sorted(g["product_name"].unique().tolist())
        return pd.Series({
            "article_name":       g["article_name"].iloc[0],
            "affected_products":  products,
            "n_ops_affected":     g["op_code"].nunique(),
            "total_gap_quantity": int(g["gap_quantity"].fillna(0).sum()),
            "mp_affinity_signal": len(products) >= 2,
        })

    result = (
        merged.groupby(["supplier_name", "article_id"])
        .apply(_agg_group, include_groups=False)
        .reset_index()
    )

    return (
        result[_out_cols]
        .sort_values(["supplier_name", "total_gap_quantity"], ascending=[True, False])
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Função 4 — Sumarizar causas-raiz por fornecedor
# ─────────────────────────────────────────────────────────────────────────────

def summarize_root_causes(df_classified: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega OPs detratoras classificadas por supplier_name × root_cause_category.

    Parameters
    ----------
    df_classified : pd.DataFrame
        OPs detratoras com colunas: op_code, supplier_name, product_name,
        gap_quantity, root_cause_category.

    Returns
    -------
    pd.DataFrame
        Uma linha por supplier_name × root_cause_category com colunas:
          supplier_name, root_cause_category, n_ops (int),
          total_gap_quantity (int), pct_gap_of_supplier (float),
          primary_products (str — top 3 produtos por gap, separados por " | ").
        Ordenado por supplier_name ASC, total_gap_quantity DESC.
    """
    _out_cols = [
        "supplier_name", "root_cause_category", "n_ops",
        "total_gap_quantity", "pct_gap_of_supplier", "primary_products",
    ]

    if df_classified.empty:
        return pd.DataFrame(columns=_out_cols)

    # Total de gap por fornecedor (denominador para pct)
    supplier_total = (
        df_classified.groupby("supplier_name")["gap_quantity"]
        .sum()
        .rename("supplier_total_gap")
    )

    def _top3_products(g: pd.DataFrame) -> str:
        top = (
            g.groupby("product_name")["gap_quantity"]
            .sum()
            .nlargest(3)
            .index.tolist()
        )
        return " | ".join(top)

    def _agg_group(g: pd.DataFrame) -> pd.Series:
        return pd.Series({
            "n_ops":              g["op_code"].nunique(),
            "total_gap_quantity": int(g["gap_quantity"].fillna(0).sum()),
            "primary_products":   _top3_products(g),
        })

    agg = (
        df_classified.groupby(["supplier_name", "root_cause_category"])
        .apply(_agg_group, include_groups=False)
        .reset_index()
    )

    agg = agg.merge(supplier_total, on="supplier_name", how="left")
    agg["pct_gap_of_supplier"] = agg.apply(
        lambda r: round(r["total_gap_quantity"] / r["supplier_total_gap"], 4)
        if r["supplier_total_gap"] > 0
        else 0.0,
        axis=1,
    )

    return (
        agg[_out_cols]
        .sort_values(["supplier_name", "total_gap_quantity"], ascending=[True, False])
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────────────────────
# Função 5 — Wrapper para o icp-report-writer
# ─────────────────────────────────────────────────────────────────────────────

def get_root_cause_summary_for_report(
    df_classified: pd.DataFrame,
    df_fabric: pd.DataFrame,
) -> dict:
    """
    Wrapper que combina as funções de agregação e retorna um dict
    pronto para o icp-report-writer consumir.

    Parameters
    ----------
    df_classified : pd.DataFrame
        OPs detratoras com root_cause_category atribuída.
    df_fabric : pd.DataFrame
        Mapeamento produto → artigo de fabric_product_query.sql.

    Returns
    -------
    dict com chaves:
        "by_op"             → df_classified (nível OP, tal como recebido)
        "by_supplier_cause" → resultado de summarize_root_causes()
        "mp_affinity"       → resultado de classify_mp_affinity_signal()
        "top_detractors"    → top 5 OPs por gap_quantity com root_cause e produto
    """
    by_supplier_cause = summarize_root_causes(df_classified)
    mp_affinity = classify_mp_affinity_signal(df_classified, df_fabric)

    _top_cols = ["op_code", "supplier_name", "product_name", "gap_quantity", "root_cause_category"]

    if df_classified.empty:
        top_detractors = pd.DataFrame(columns=_top_cols)
    else:
        available = [c for c in _top_cols if c in df_classified.columns]
        top_detractors = (
            df_classified[available]
            .sort_values("gap_quantity", ascending=False)
            .head(5)
            .reset_index(drop=True)
        )

    return {
        "by_op":             df_classified,
        "by_supplier_cause": by_supplier_cause,
        "mp_affinity":       mp_affinity,
        "top_detractors":    top_detractors,
    }
