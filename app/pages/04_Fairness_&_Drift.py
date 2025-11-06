import numpy as np
import pandas as pd
import streamlit as st

# Utilities and helpers
from utils import inject_css, dataset_selector
from storage.duck import get_tables, load_table

# ───────────────────────────────────────────────
# PSI helpers
# ───────────────────────────────────────────────
def _psi_from_props(ref_p: pd.Series, cur_p: pd.Series) -> float:
    """Population Stability Index from two probability vectors (same index)."""
    eps = 1e-6
    ref_p = ref_p.clip(lower=eps)
    cur_p = cur_p.clip(lower=eps)
    return float(np.sum((ref_p - cur_p) * np.log(ref_p / cur_p)))

def psi_numeric(ref: pd.Series, cur: pd.Series, n_bins: int = 10) -> float:
    """PSI for numeric columns using quantile bins computed on reference only."""
    ref = pd.to_numeric(ref, errors="coerce").dropna()
    cur = pd.to_numeric(cur, errors="coerce").dropna()
    if ref.empty or cur.empty:
        return np.nan

    # Quantile edges from reference; unique to avoid degenerate bins on constants
    edges = np.unique(np.quantile(ref, np.linspace(0, 1, n_bins + 1)))
    if len(edges) < 2:
        return 0.0  # constant column -> no shift

    ref_bins = pd.cut(ref, bins=edges, include_lowest=True)
    cur_bins = pd.cut(cur, bins=edges, include_lowest=True)

    ref_counts = ref_bins.value_counts().sort_index()
    cur_counts = cur_bins.value_counts().sort_index()

    ref_p = ref_counts / ref_counts.sum()
    cur_p = cur_counts / cur_counts.sum()
    return _psi_from_props(ref_p, cur_p)

def psi_categorical(ref: pd.Series, cur: pd.Series, treat_nan_as_category: bool = True) -> float:
    """PSI for categorical columns by aligning category frequencies."""
    if treat_nan_as_category:
        ref = ref.fillna("NA")
        cur = cur.fillna("NA")
    else:
        ref = ref.dropna()
        cur = cur.dropna()

    ref_counts = ref.astype(str).value_counts()
    cur_counts = cur.astype(str).value_counts()

    cats = ref_counts.index.union(cur_counts.index)
    ref_p = ref_counts.reindex(cats, fill_value=0) / ref_counts.sum()
    cur_p = cur_counts.reindex(cats, fill_value=0) / cur_counts.sum()
    return _psi_from_props(ref_p, cur_p)

def compute_psi_table(ref_df: pd.DataFrame, cur_df: pd.DataFrame, columns=None, n_bins: int = 10) -> pd.DataFrame:
    """Compute PSI for shared columns; auto-detect numeric vs categorical."""
    if columns is None:
        columns = ref_df.columns.intersection(cur_df.columns)

    rows = []
    for col in columns:
        if pd.api.types.is_numeric_dtype(ref_df[col]) and pd.api.types.is_numeric_dtype(cur_df[col]):
            psi = psi_numeric(ref_df[col], cur_df[col], n_bins=n_bins)
            ctype = "numeric"
        else:
            psi = psi_categorical(ref_df[col], cur_df[col])
            ctype = "categorical"

        rows.append(
            {
                "column": col,
                "type": ctype,
                "ref_n": int(ref_df[col].notna().sum()),
                "cur_n": int(cur_df[col].notna().sum()),
                "psi": psi,
                "flag": "⚠️" if (pd.notna(psi) and psi > 0.2) else "",
            }
        )

    out = pd.DataFrame(rows).sort_values("psi", ascending=False, na_position="last").reset_index(drop=True)
    return out

# ───────────────────────────────────────────────
# UI
# ───────────────────────────────────────────────
inject_css()
st.title("04 · Fairness & Drift")

# Get active dataset (synced with Explore) — used as the Fairness source and as the default *reference* for Drift
dataset_choice = dataset_selector()

# Ensure DuckDB has data
tables = get_tables()
if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

# Self-heal if dataset not selected or stale
if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.caption(f"📂 Active dataset: `{dataset_choice}`")

# Load from DuckDB
df = load_table(dataset_choice)

# Tabs: Fairness | Drift
tab_fair, tab_drift = st.tabs(["Fairness", "Drift"])

# ──────────── Fairness ────────────
with tab_fair:
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in df.columns if c not in num_cols]

    st.markdown("**Create a binary target**")
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        tcol = st.selectbox("Numeric column", num_cols or ["<none>"])
    with c2:
        thresh = st.number_input("Threshold", value=float(df[tcol].median()) if num_cols else 0.0)
    with c3:
        positive_def = st.selectbox("Positive if", [f"{tcol} > {thresh}", f"{tcol} <= {thresh}"])

    st.markdown("**Sensitive attribute**")
    sattr = st.selectbox("Sensitive attribute", cat_cols or ["<none>"])

    if num_cols and cat_cols and tcol in df and sattr in df:
        y = (df[tcol] > thresh) if ">" in positive_def else (df[tcol] <= thresh)
        g = df[sattr].astype(str)

        grp = (
            pd.DataFrame({"group": g, "y": y.astype(int)})
            .groupby("group")
            .agg(selection_rate=("y", "mean"), n=("y", "size"))
            .reset_index()
        )
        dp = float(grp["selection_rate"].max() - grp["selection_rate"].min())

        st.success(f"Demographic parity difference: **{dp:.3f}**")
        st.dataframe(grp.sort_values("selection_rate", ascending=False), use_container_width=True, hide_index=True)
        st.caption(
            "This demo mimics Fairlearn's selection rate by group; in a full version, "
            "we would compute additional fairness metrics via Fairlearn/Evidently."
        )
    else:
        st.info("Pick a numeric column and a categorical sensitive attribute.")

# ──────────── Drift (actual PSI) ────────────
with tab_drift:
    st.markdown("Compare the **active dataset** (reference) vs a **current** dataset to compute PSI per column.")

    ref_table = dataset_choice  # use the globally selected dataset as the reference
    available = [t for t in tables if t != ref_table]

    if not available:
        st.info("No other datasets available to compare drift.")
    else:
        cur_table = st.selectbox("Current dataset", available, index=0, key="cur_ds")

        # Load both datasets from DuckDB
        ref_df = load_table(ref_table)
        cur_df = load_table(cur_table)

        # Choose columns (shared only)
        shared_cols = ref_df.columns.intersection(cur_df.columns).tolist()
        if not shared_cols:
            st.warning("No shared columns between the selected datasets.")
        else:
            cols = st.multiselect("Columns to evaluate (shared only)", shared_cols, default=shared_cols)

            # Bin control for numeric PSI
            n_bins = st.slider("Bins for numeric PSI", min_value=5, max_value=30, value=10)

            if cur_table == ref_table:
                st.info("Pick a dataset different from the reference to compute drift (PSI).")
            else:
                with st.spinner("Computing PSI…"):
                    psi_tbl = compute_psi_table(ref_df, cur_df, columns=cols, n_bins=n_bins)

                st.dataframe(psi_tbl, use_container_width=True, hide_index=True)
                st.caption("Rule of thumb: PSI > 0.2 indicates significant shift (⚠️).")

                # Download CSV (exclude the symbol 'flag' column from export)
                export_df = psi_tbl.drop(columns=["flag"], errors="ignore")
                csv = export_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download PSI table (CSV)",
                    data=csv,
                    file_name=f"psi_{ref_table}_vs_{cur_table}.csv",
                    mime="text/csv",
                )