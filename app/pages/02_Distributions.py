import os
import pandas as pd
import streamlit as st
from utils import inject_css, load_parquet

import plotly.express as px

DATA_PROC = "data/processed"
inject_css()
st.title("02 · Distributions")

files = sorted([f for f in os.listdir(DATA_PROC) if f.endswith(".parquet")]) if os.path.isdir(DATA_PROC) else []
if not files:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

choice = st.selectbox("Dataset", files)
df = load_parquet(os.path.join(DATA_PROC, choice))

num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
cat_cols = [c for c in df.columns if c not in num_cols]

tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

with tab_num:
    if not num_cols:
        st.caption("No numeric columns detected.")
    else:
        c1, c2 = st.columns([2,1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 80, 30)
        fig = px.histogram(df, x=col, nbins=bins, opacity=0.9, marginal="box")
        fig.update_layout(height=380, bargap=0.05)
        st.plotly_chart(fig, use_container_width=True)

with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns detected.")
    else:
        c1, c2 = st.columns([2,1])
        with c1:
            colc = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)
        vc = df[colc].astype(str).fillna("<NA>").value_counts().reset_index().head(top_k)
        vc.columns = [colc, "count"]
        fig = px.bar(vc, x=colc, y="count")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
