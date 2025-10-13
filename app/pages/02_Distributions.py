import os
import pandas as pd
import streamlit as st
import duckdb
import plotly.express as px
from utils import inject_css, kpi_grid
from bias_metrics import (
    numeric_bias_metrics_duckdb,
    categorical_bias_metrics_duckdb,
    format_pct,
    severity_badge
)

DUCKDB_PATH = "data/duckdb/eda.duckdb"
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

inject_css()
st.title("02 · Distributions")

# ───────────────────────────────
# Get the active dataset
# ───────────────────────────────
dataset_choice = st.session_state.get("dataset_choice")

with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
tables = [t for t in tables if t != "datasets"]

if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.markdown(f"### 📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────
# Schema + columns
# ───────────────────────────────
@st.cache_data(ttl=600)
def get_schema(tbl):
    with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
        return con.execute(f"DESCRIBE SELECT * FROM {tbl} LIMIT 0").df()

schema_df = get_schema(dataset_choice)
num_cols = schema_df[schema_df["column_type"]
    .str.contains("INT|DOUBLE|FLOAT|DECIMAL|NUMERIC", case=False, regex=True)
]["column_name"].tolist()
cat_cols = [c for c in schema_df["column_name"].tolist() if c not in num_cols]

tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

# ───────────────────────────────
# Numeric columns tab
# ───────────────────────────────
with tab_num:
    if not num_cols:
        st.caption("No numeric columns found.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 100, 30)

        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            hist = con.execute(f"""
                SELECT "{col}" AS val, COUNT(*) AS count
                FROM {dataset_choice}
                WHERE "{col}" IS NOT NULL
                GROUP BY val
                ORDER BY val
            """).df()

        # Plot histogram
        fig = px.bar(hist, x="val", y="count", labels={"val": col, "count": "Frequency"})
        st.plotly_chart(fig, use_container_width=True)

        # ───────────────────────────────
        # Bias metrics (numeric)
        # ───────────────────────────────
        st.divider()
        st.subheader("Bias Check (Numeric)")

        nm = numeric_bias_metrics_duckdb(DUCKDB_PATH, dataset_choice, col, bins)
        if nm is None:
            st.info("No numeric bias metrics available.")
        else:
            kpi_grid({
                "Max-bin share": f"{format_pct(nm['max_bin_share'])} • {severity_badge(nm['bin_level'])}",
                "Skewness": f"{nm['skew']:.2f}",
                "Outlier fraction": f"{format_pct(nm['outlier_frac'])} • {severity_badge(nm['out_level'])}",
                "Zero share": format_pct(nm['zero_share']),
                "Missing": format_pct(nm['missing_share']),
            })

            with st.expander("📊 Top bins by share", expanded=False):
                st.dataframe(nm["bins_table"], use_container_width=True, hide_index=True)

            st.caption("Heuristics: max-bin ≥25% (mild), ≥40% (severe); outliers ≥10% (mild), ≥20% (severe)")

# ───────────────────────────────
# Categorical columns tab
# ───────────────────────────────
with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns found.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            col = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)

        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            vc = con.execute(f"""
                SELECT "{col}" AS val, COUNT(*) AS count
                FROM {dataset_choice}
                GROUP BY val
                ORDER BY count DESC
                LIMIT {top_k}
            """).df()

        # Plot bar
        fig = px.bar(vc, x="val", y="count", labels={"val": col, "count": "Frequency"})
        st.plotly_chart(fig, use_container_width=True)

        # ───────────────────────────────
        # Bias metrics (categorical)
        # ───────────────────────────────
        st.divider()
        st.subheader("Bias Check (Categorical)")

        cm = categorical_bias_metrics_duckdb(DUCKDB_PATH, dataset_choice, col)
        if cm is None:
            st.info("No categorical bias metrics available.")
        else:
            kpi_grid({
                "Majority class": f"{cm['majority_label']} ({format_pct(cm['majority_share'])}) • {severity_badge(cm['maj_level'])}",
                "Imbalance ratio": f"{cm['imbalance_ratio']:.1f}× • {severity_badge(cm['irr_level'])}",
                "Effective #classes": f"{cm['effective_k']:.2f} / {cm['observed_k']}",
                "Missing": format_pct(cm['missing_share']),
                "Total rows": f"{cm['total']:,}",
            })

            with st.expander("Top categories", expanded=False):
                st.dataframe(cm["top_table"], use_container_width=True, hide_index=True)

            st.caption("💡 Heuristics: majority share ≥70% (mild), ≥90% (severe); imbalance ratio ≥5× (mild), ≥10× (severe)")
