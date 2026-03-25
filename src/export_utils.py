"""Exportni utility."""

from __future__ import annotations

from io import BytesIO

import pandas as pd


def export_filtered_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """CSV bytes pro stazeni."""
    return df.to_csv(index=False).encode("utf-8-sig")


def export_filtered_to_excel_bytes(df: pd.DataFrame, summary_signature: pd.DataFrame) -> bytes:
    """XLSX bytes s listy podle klasifikace + souhrn."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet in ["AUTO_KANDIDAT", "RUCNI_POSOUZENI", "CHRANENO_VYJIMKOU", "PONECHAT"]:
            df[df["klasifikace"] == sheet].to_excel(writer, sheet_name=sheet[:31], index=False)
        summary_signature.to_excel(writer, sheet_name="SOUHRN_PO_SIGNATURACH", index=False)
    output.seek(0)
    return output.getvalue()
