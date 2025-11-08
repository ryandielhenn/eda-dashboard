"""
Drift detection using Population Stability Index (PSI)
"""

import numpy as np
import pandas as pd
from typing import List, Dict, Any


def psi_from_props(ref_p: pd.Series, cur_p: pd.Series) -> float:
    """Population Stability Index from two probability vectors"""
    eps = 1e-6
    ref_p = ref_p.clip(lower=eps)
    cur_p = cur_p.clip(lower=eps)
    return float(np.sum((ref_p - cur_p) * np.log(ref_p / cur_p)))


def psi_numeric(ref: pd.Series, cur: pd.Series, n_bins: int = 10) -> float:
    """PSI for numeric columns using quantile bins"""
    ref = pd.to_numeric(ref, errors="coerce").dropna()
    cur = pd.to_numeric(cur, errors="coerce").dropna()

    if ref.empty or cur.empty:
        return np.nan

    # Quantile edges from reference
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, n_bins + 1)))
    if len(edges) < 2:
        return 0.0  # constant column -> no shift

    ref_bins = pd.cut(ref, bins=edges, include_lowest=True)
    cur_bins = pd.cut(cur, bins=edges, include_lowest=True)

    ref_counts = ref_bins.value_counts().sort_index()
    cur_counts = cur_bins.value_counts().sort_index()

    ref_p = ref_counts / ref_counts.sum()
    cur_p = cur_counts / cur_counts.sum()

    return psi_from_props(ref_p, cur_p)


def psi_categorical(ref: pd.Series, cur: pd.Series) -> float:
    """PSI for categorical columns"""
    ref = ref.fillna("NA")
    cur = cur.fillna("NA")

    ref_counts = ref.astype(str).value_counts()
    cur_counts = cur.astype(str).value_counts()

    cats = ref_counts.index.union(cur_counts.index)
    ref_p = ref_counts.reindex(cats, fill_value=0) / ref_counts.sum()
    cur_p = cur_counts.reindex(cats, fill_value=0) / cur_counts.sum()

    return psi_from_props(ref_p, cur_p)


def compute_psi_table(
    ref_df: pd.DataFrame, cur_df: pd.DataFrame, columns: List[str], n_bins: int
) -> List[Dict[str, Any]]:
    """Compute PSI for multiple columns"""
    results = []
    for col in columns:
        if pd.api.types.is_numeric_dtype(ref_df[col]) and pd.api.types.is_numeric_dtype(
            cur_df[col]
        ):
            psi = psi_numeric(ref_df[col], cur_df[col], n_bins=n_bins)
            ctype = "numeric"
        else:
            psi = psi_categorical(ref_df[col], cur_df[col])
            ctype = "categorical"

        flag = "⚠️" if (pd.notna(psi) and psi > 0.2) else ""

        results.append(
            {
                "column": col,
                "type": ctype,
                "ref_n": int(ref_df[col].notna().sum()),
                "cur_n": int(cur_df[col].notna().sum()),
                "psi": float(psi) if pd.notna(psi) else None,
                "flag": flag,
            }
        )

    # Sort by PSI descending
    results.sort(key=lambda x: x["psi"] if x["psi"] is not None else -1, reverse=True)

    return results
