import os
import sys
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

# Utilities and helpers
from utils import inject_css, dataset_selector

# Add root /storage to sys.path
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
STORAGE_DIR = os.path.join(ROOT_DIR, "storage")
if STORAGE_DIR not in sys.path:
    sys.path.insert(0, STORAGE_DIR)

from duck import connect, ingest_parquet, list_datasets, sql, table_name  # ✅ includes connect

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
tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
tables = [t for t in tables if t != "datasets"]

if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

# Self-heal if dataset not selected or stale
if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.markdown(f"### 📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────────────────────
# Load data directly from DuckDB
# ───────────────────────────────────────────────
df = con.execute(f"SELECT * FROM {dataset_choice}").df()

# Work with numeric columns only
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
if len(num_cols) < 2:
    st.caption("Need at least two numeric columns for a correlation matrix.")
    st.stop()

with st.spinner("Computing Pearson correlation…"):
    corr = df[num_cols].corr(numeric_only=True, method="pearson")

# ───────────────────────────────────────────────
# Heatmap
# ───────────────────────────────────────────────
st.subheader("Correlation heatmap (Pearson)")
fig = px.imshow(
    corr,
    text_auto=".2f",
    aspect="auto",
    color_continuous_scale="Blues"
)
fig.update_layout(height=520, margin=dict(l=0, r=0, t=24, b=0))
st.plotly_chart(fig, config={"responsive": True, "displayModeBar": False})

# ---- Stable pair extraction (works across pandas versions) ----
idx_i, idx_j = np.triu_indices_from(corr.values, k=1)
pairs = pd.DataFrame({
    "col_i": corr.index.values[idx_i],
    "col_j": corr.columns.values[idx_j],
    "value": corr.values[idx_i, idx_j],
})

topk = st.slider("Show top | lowest pairs (by absolute value)", 5, 20, 10)

# Top absolute correlations
top_pairs = pairs.reindex(pairs["value"].abs().sort_values(ascending=False).index).head(topk)
low_pairs = pairs.reindex(pairs["value"].abs().sort_values(ascending=True).index).head(topk)

c1, c2 = st.columns(2)
with c1:
    st.write("**Top pairs**")
    st.dataframe(top_pairs.reset_index(drop=True), use_container_width=True, hide_index=True)
with c2:
    st.write("**Lowest pairs**")
    st.dataframe(low_pairs.reset_index(drop=True), use_container_width=True, hide_index=True)

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
