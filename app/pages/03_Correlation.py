import os, io
import pandas as pd
import streamlit as st
import numpy as np
from utils import inject_css, load_parquet
import plotly.express as px

DATA_PROC = "data/processed"

inject_css()
st.title("03 · Correlation")

# List available parquet datasets
files = sorted([f for f in os.listdir(DATA_PROC) if f.endswith(".parquet")]) if os.path.isdir(DATA_PROC) else []
if not files:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

choice = st.selectbox("Dataset", files)
df = load_parquet(os.path.join(DATA_PROC, choice))

# Work with numeric columns only
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
if len(num_cols) < 2:
    st.caption("Need at least two numeric columns for a correlation matrix.")
    st.stop()

with st.spinner("Computing Pearson correlation…"):
    corr = df[num_cols].corr(numeric_only=True, method="pearson")

# Heatmap
st.subheader("Correlation heatmap (Pearson)")
fig = px.imshow(corr, text_auto=".2f", aspect="auto", color_continuous_scale="Blues")
fig.update_layout(height=520, margin=dict(l=0, r=0, t=24, b=0))
st.plotly_chart(fig)

# ---- Stable pair extraction (works across pandas versions) ----
# Take the upper triangle (k=1 skips the diagonal)
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
    st.dataframe(top_pairs.reset_index(drop=True))
with c2:
    st.write("**Lowest pairs**")
    st.dataframe(low_pairs.reset_index(drop=True))

# Download button for the full correlation matrix
csv = corr.to_csv().encode()
st.download_button(
    "Download correlation CSV",
    data=csv,
    file_name="correlation.csv",
    mime="text/csv",
)

st.caption("Tip: Investigate pairs in **02 · Distributions**.")
