import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# Utilities and helpers
from utils import inject_css, dataset_selector
from storage.duck import connect, get_tables


DUCKDB_PATH = "data/duckdb/eda.duckdb"

inject_css()
st.title("03 · Correlation")

# ───────────────────────────────────────────────
# Get active dataset (synced with Explore)
# ───────────────────────────────────────────────
dataset_choice = st.session_state.get("dataset_choice")
dataset_choice = dataset_selector()
# Check DuckDB tables
con = connect()
tables = get_tables()

if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

# Self-heal if dataset not selected or stale
if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.markdown(f"### 📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────────────────────
# Get numeric columns from DuckDB schema
# ───────────────────────────────────────────────
columns_query = f"DESCRIBE SELECT * FROM '{dataset_choice}'"
columns_info = con.execute(columns_query).df()

# Filter to numeric types
numeric_types = [
    "BIGINT",
    "INTEGER",
    "DOUBLE",
    "FLOAT",
    "DECIMAL",
    "HUGEINT",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
    "REAL",
]

num_cols = columns_info[columns_info["column_type"].isin(numeric_types)][
    "column_name"
].tolist()

if len(num_cols) < 2:
    st.caption("Need at least two numeric columns for a correlation matrix.")
    st.stop()

# ───────────────────────────────────────────────
# Fetch ONLY numeric columns and compute correlation
# ───────────────────────────────────────────────
with st.spinner("Computing Pearson correlation…"):
    # Build query to compute all correlations in DuckDB
    corr_calcs = []
    for col in num_cols:
        corr_calcs.append(
            f'CORR("{col}", "{col}") as "{col}_{col}"'
        )  # Diagonal (always 1.0)
        for other_col in num_cols:
            if col < other_col:  # Avoid duplicates
                corr_calcs.append(
                    f'CORR("{col}", "{other_col}") as "{col}_{other_col}"'
                )

    query = f"""
    SELECT {', '.join(corr_calcs)}
    FROM '{dataset_choice}'
    """

    corr_results = con.execute(query).df()

    # Reconstruct symmetric correlation matrix
    corr = pd.DataFrame(np.eye(len(num_cols)), index=num_cols, columns=num_cols)

    for col in num_cols:
        corr.loc[col, col] = 1.0
        for other_col in num_cols:
            if col < other_col:
                val = corr_results[f"{col}_{other_col}"].iloc[0]
                corr.loc[col, other_col] = val
                corr.loc[other_col, col] = val

# ───────────────────────────────────────────────
# Heatmap
# ───────────────────────────────────────────────
st.subheader("Correlation heatmap (Pearson)")
fig = px.imshow(corr, text_auto=".2f", aspect="auto", color_continuous_scale="Blues")
fig.update_layout(height=520, margin=dict(l=0, r=0, t=24, b=0))
st.plotly_chart(fig, config={"responsive": True, "displayModeBar": False})

# ---- Stable pair extraction (works across pandas versions) ----
idx_i, idx_j = np.triu_indices_from(corr.values, k=1)
pairs = pd.DataFrame(
    {
        "col_i": corr.index.values[idx_i],
        "col_j": corr.columns.values[idx_j],
        "value": corr.values[idx_i, idx_j],
    }
)

topk = st.slider("Show top | lowest pairs (by absolute value)", 5, 20, 10)

# Top absolute correlations
top_pairs = pairs.reindex(pairs["value"].abs().sort_values(ascending=False).index).head(
    topk
)
low_pairs = pairs.reindex(pairs["value"].abs().sort_values(ascending=True).index).head(
    topk
)

c1, c2 = st.columns(2)
with c1:
    st.write("**Top pairs**")
    st.dataframe(top_pairs.reset_index(drop=True), width="stretch", hide_index=True)
with c2:
    st.write("**Lowest pairs**")
    st.dataframe(low_pairs.reset_index(drop=True), width="stretch", hide_index=True)

# ───────────────────────────────────────────────
# Download button for the full correlation matrix
# ───────────────────────────────────────────────
csv = corr.to_csv().encode()
st.download_button(
    "Download correlation CSV",
    data=csv,
    file_name=f"{dataset_choice}_correlation.csv",
    mime="text/csv",
)

st.caption("Tip: Investigate pairs in **02 · Distributions**.")
