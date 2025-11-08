import os
import sys
import streamlit as st
import plotly.express as px

from utils import (
    inject_css,
    kpi_grid,
    dataset_selector,
    format_pct,
    severity_badge,
)
from storage.duck import (
    get_tables,
    get_schema,
    get_value_counts,
    get_numeric_histogram,
    get_categorical_bias_metrics,
    get_numeric_bias_metrics,
)

# Add root storage folder to sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STORAGE_DIR = os.path.join(ROOT_DIR, "storage")
if STORAGE_DIR not in sys.path:
    sys.path.insert(0, STORAGE_DIR)

DATA_PROC = "data/processed"
DUCKDB_PATH = "data/duckdb/eda.duckdb"

inject_css()
st.title("02 · Distributions")

dataset_choice = dataset_selector()


@st.cache_data(ttl=3600)
def cached_schema(table_name):
    return get_schema(table_name)


@st.cache_data(ttl=3600)
def cached_histogram(table_name, col, bins, sample_size=100000):
    return get_numeric_histogram(table_name, col, bins, sample_size)


@st.cache_data(ttl=3600)
def cached_value_counts(table_name, col, top_k):
    return get_value_counts(table_name, col, top_k)


# ───────────────────────────────
# Active dataset (from Explore)
# ───────────────────────────────
tables = get_tables()
if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

# Use current selection from Explore (no dropdown)
dataset_choice = st.session_state.get("dataset_choice")
if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.caption(f"📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────
# Schema + column typing
# ───────────────────────────────
schema_df = cached_schema(dataset_choice)
num_cols = schema_df[
    schema_df["column_type"].str.contains(
        "INT|DOUBLE|FLOAT|DECIMAL|NUMERIC", case=False, regex=True
    )
]["column_name"].tolist()
cat_cols = [c for c in schema_df["column_name"].tolist() if c not in num_cols]

tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

# ───────────────────────────────
# Numeric tab
# ───────────────────────────────
with tab_num:
    if not num_cols:
        st.caption("No numeric columns detected.")
    else:
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 80, 30)
        with c3:
            sample_size = st.number_input(
                "Sample size", 10000, 500000, 100000, step=10000
            )

        hist_data, sample_data = cached_histogram(
            dataset_choice, col, bins, sample_size
        )

        if hist_data is None:
            st.warning("No data available for this column.")
        else:
            st.caption(f"Box plot based on {len(sample_data):,} sampled rows")
            fig_box = px.box(sample_data, x=col)
            fig_box.update_layout(height=200)
            st.plotly_chart(
                fig_box, config={"responsive": True, "displayModeBar": False}
            )

            fig = px.bar(
                hist_data,
                x="bin_start",
                y="count",
                labels={"bin_start": col, "count": "Frequency"},
            )
            fig.update_traces(marker_line_width=0)
            fig.update_layout(height=380, bargap=0.05, showlegend=False)
            st.plotly_chart(fig, config={"responsive": True, "displayModeBar": False})

            st.divider()
            st.subheader("Bias Check")

            nm = get_numeric_bias_metrics(dataset_choice, col, bins)
            if nm is None:
                st.info("No numeric bias metrics available.")
            else:
                kpi_grid(
                    {
                        "Max-bin share": f"{format_pct(nm['max_bin_share'])} • {severity_badge(nm['bin_level'])}",
                        "Skewness": f"{nm['skew']:.2f}",
                        "Outlier fraction": f"{format_pct(nm['outlier_frac'])} • {severity_badge(nm['out_level'])}",
                        "Zero share": format_pct(nm["zero_share"]),
                        "Missing": format_pct(nm["missing_share"]),
                    }
                )
                with st.expander("📊 Top bins by share", expanded=False):
                    st.dataframe(nm["bins_table"], width="stretch", hide_index=True)
                st.caption(
                    "Heuristics: max-bin ≥25% (mild), ≥40% (severe); outliers ≥10% (mild), ≥20% (severe)"
                )

# ───────────────────────────────
# Categorical tab
# ───────────────────────────────
with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns detected.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            colc = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)

        vc = cached_value_counts(dataset_choice, colc, top_k)

        fig = px.bar(vc, x=colc, y="count")
        fig.update_layout(height=360)
        st.plotly_chart(fig, config={"responsive": True, "displayModeBar": False})

        st.divider()
        st.subheader("Bias Check")

        cm = get_categorical_bias_metrics(dataset_choice, colc)
        if cm is None:
            st.info("No categorical bias metrics available.")
        else:
            kpi_grid(
                {
                    "Majority class": f"{cm['majority_label']} ({format_pct(cm['majority_share'])}) • {severity_badge(cm['maj_level'])}",
                    "Imbalance ratio": f"{cm['imbalance_ratio']:.1f}× • {severity_badge(cm['irr_level'])}",
                    "Effective #classes": f"{cm['effective_k']:.2f} / {cm['observed_k']}",
                    "Missing": format_pct(cm["missing_share"]),
                    "Total rows": f"{cm['total']:,}",
                }
            )
            with st.expander("Top categories", expanded=False):
                st.dataframe(cm["top_table"], width="stretch", hide_index=True)
            st.caption(
                "💡 Heuristics: majority share ≥70% (mild), ≥90% (severe); "
                "imbalance ratio ≥5× (mild), ≥10× (severe)"
            )
