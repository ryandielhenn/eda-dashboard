import os
import numpy as np
import pandas as pd
import streamlit as st
from utils import inject_css, load_parquet, kpi_grid
import plotly.express as px

DATA_PROC = "data/processed"
inject_css()
st.title("02 · Distributions")

# Load dataset (from saved parquet files)
files = sorted([f for f in os.listdir(DATA_PROC) if f.endswith(".parquet")]) if os.path.isdir(DATA_PROC) else []
if not files:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

choice = st.selectbox("Dataset", files)
df = load_parquet(os.path.join(DATA_PROC, choice))

# Split columns by dtype
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
cat_cols = [c for c in df.columns if c not in num_cols]

# ---------- Helpers (bias metrics) ----------
def format_pct(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "—"

def entropy_and_effective_k(p: np.ndarray) -> tuple[float, float]:
    p = p[p > 0]
    if p.size == 0:
        return 0.0, 0.0
    H = float(-(p * np.log(p)).sum())  # natural log entropy
    e_k = float(np.exp(H))
    return H, e_k

def severity_badge(level: str) -> str:
    return {"ok": "🟢 OK", "info": "🔵 Info", "mild": "🟠 Mild", "severe": "🔴 Severe"}.get(level, "🟢 OK")

def numeric_bias_metrics(s: pd.Series, bins: int) -> dict | None:
    s = s.astype("float64")
    total_rows = int(s.shape[0])
    nonnull = s.dropna()
    if total_rows == 0 or nonnull.empty:
        return None

    # Max-bin share via pd.cut using current bins slider
    try:
        bin_cuts = pd.cut(nonnull, bins=bins, include_lowest=True)
        bin_shares = bin_cuts.value_counts(normalize=True, sort=False)
        max_bin_share = float(bin_shares.max()) if not bin_shares.empty else 0.0
    except Exception:
        bin_shares = pd.Series(dtype="float64")
        max_bin_share = 0.0

    # Skewness & outliers (IQR rule)
    skew = float(nonnull.skew()) if not nonnull.empty else 0.0
    q1, q3 = (float(nonnull.quantile(0.25)), float(nonnull.quantile(0.75))) if not nonnull.empty else (0.0, 0.0)
    iqr = q3 - q1
    if iqr > 0:
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        outlier_frac = float(((nonnull < lower) | (nonnull > upper)).mean())
    else:
        outlier_frac = 0.0

    zero_share = float((s == 0).mean())
    missing_share = float(s.isna().mean())

    # Thresholds
    # max-bin @ ~30 bins: info ≥0.20, mild ≥0.25, severe ≥0.40
    if max_bin_share >= 0.40:
        bin_level = "severe"
    elif max_bin_share >= 0.25:
        bin_level = "mild"
    elif max_bin_share >= 0.20:
        bin_level = "info"
    else:
        bin_level = "ok"

    # outliers (IQR): info ≥5%, mild ≥10%, severe ≥20%
    if outlier_frac >= 0.20:
        out_level = "severe"
    elif outlier_frac >= 0.10:
        out_level = "mild"
    elif outlier_frac >= 0.05:
        out_level = "info"
    else:
        out_level = "ok"

    bins_table = (
        bin_shares.rename("share")
        .to_frame()
        .assign(bin=lambda d: d.index.astype(str))
        .loc[:, ["bin", "share"]]
        .sort_values("share", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    return {
        "max_bin_share": max_bin_share,
        "bin_level": bin_level,
        "skew": skew,
        "outlier_frac": outlier_frac,
        "out_level": out_level,
        "zero_share": zero_share,
        "missing_share": missing_share,
        "bins_table": bins_table,
    }

def categorical_bias_metrics(s: pd.Series) -> dict | None:
    total = int(s.shape[0])
    if total == 0:
        return None

    counts = s.astype("string").fillna("<NA>").value_counts(dropna=False)
    if counts.empty:
        return None

    shares = (counts / counts.sum()).astype(float)
    majority_label = str(shares.idxmax())
    majority_share = float(shares.max())
    nz = shares[shares > 0]
    minority_share = float(nz.min()) if not nz.empty else 0.0
    imbalance_ratio = float(majority_share / minority_share) if minority_share > 0 else float("inf")
    missing_share = float(s.isna().mean())

    H, e_k = entropy_and_effective_k(shares.to_numpy())

    # Thresholds
    # majority share: info ≥0.60, mild ≥0.70, severe ≥0.90
    if majority_share >= 0.90:
        maj_level = "severe"
    elif majority_share >= 0.70:
        maj_level = "mild"
    elif majority_share >= 0.60:
        maj_level = "info"
    else:
        maj_level = "ok"

    # imbalance ratio: info ≥3x, mild ≥5x, severe ≥10x
    if imbalance_ratio >= 10:
        irr_level = "severe"
    elif imbalance_ratio >= 5:
        irr_level = "mild"
    elif imbalance_ratio >= 3:
        irr_level = "info"
    else:
        irr_level = "ok"

    top_table = (
        counts.rename_axis("value").reset_index(name="count")
        .assign(share=lambda d: d["count"] / d["count"].sum())
        .head(20)
    )

    return {
        "majority_label": majority_label,
        "majority_share": majority_share,
        "minority_share": minority_share,
        "imbalance_ratio": imbalance_ratio,
        "entropy": H,
        "effective_k": e_k,
        "observed_k": int(counts.shape[0]),
        "missing_share": missing_share,
        "maj_level": maj_level,
        "irr_level": irr_level,
        "top_table": top_table,
        "total": total,
    }

# Tabs
tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

# ---------- Numeric tab ----------
with tab_num:
    if not num_cols:
        st.caption("No numeric columns detected.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 80, 30)

        fig = px.histogram(df, x=col, nbins=bins, opacity=0.9, marginal="box")
        fig.update_layout(height=380, bargap=0.05)
        st.plotly_chart(fig, use_container_width=True)

        # Bias check uses same selected column
        st.subheader("Bias check")
        st.caption(f"Using numeric column: **{col}**")
        nm = numeric_bias_metrics(df[col], bins=bins)
        if nm is None:
            st.info("No numeric bias metrics available for this selection.")
        else:
            kpi_grid({
                "Max-bin share": f"{format_pct(nm['max_bin_share'])} • {severity_badge(nm['bin_level'])}",
                "Skewness": f"{nm['skew']:.2f}",
                "Outlier fraction": f"{format_pct(nm['outlier_frac'])} • {severity_badge(nm['out_level'])}",
                "Zero share": format_pct(nm['zero_share']),
                "Missing": format_pct(nm['missing_share']),
            })
            with st.expander("Top bins by share", expanded=False):
                st.dataframe(nm["bins_table"], use_container_width=True)
            st.caption("Heuristics: max-bin ≥ 25% (mild), ≥ 40% (severe); outliers ≥ 10% (mild), ≥ 20% (severe).")

# ---------- Categorical tab ----------
with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns detected.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            colc = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)

        vc = df[colc].astype(str).fillna("<NA>").value_counts().reset_index().head(top_k)
        vc.columns = [colc, "count"]
        fig = px.bar(vc, x=colc, y="count")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

        # Bias check uses same selected column
        st.subheader("Bias check")
        st.caption(f"Using categorical column: **{colc}**")
        cm = categorical_bias_metrics(df[colc])
        if cm is None:
            st.info("No categorical bias metrics available for this selection.")
        else:
            kpi_grid({
                "Majority class": f"{cm['majority_label']} ({format_pct(cm['majority_share'])}) • {severity_badge(cm['maj_level'])}",
                "Imbalance ratio": f"{cm['imbalance_ratio']:.1f}× • {severity_badge(cm['irr_level'])}",
                "Effective #classes": f"{cm['effective_k']:.2f} / {cm['observed_k']}",
                "Missing": format_pct(cm['missing_share']),
                "Total rows": f"{cm['total']:,}",
            })
            with st.expander("Top categories", expanded=False):
                st.dataframe(cm["top_table"], use_container_width=True)
            st.caption("Heuristics: majority share ≥ 70% (mild), ≥ 90% (severe); imbalance ratio ≥ 5× (mild), ≥ 10× (severe).")
