import os
import numpy as np
import pandas as pd
import streamlit as st
from utils import inject_css, load_parquet

inject_css()
DATA_PROC="data/processed"
st.title("04 · Fairness & Drift")

files = sorted([f for f in os.listdir(DATA_PROC) if f.endswith(".parquet")]) if os.path.isdir(DATA_PROC) else []
if not files:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

tab_fair, tab_drift = st.tabs(["Fairness (interactive demo)", "Drift (mock)"])

with tab_fair:
    ds = st.selectbox("Dataset", files, key="fair_ds")
    df = load_parquet(os.path.join(DATA_PROC, ds))

    num_cols=[c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols=[c for c in df.columns if c not in num_cols]

    st.markdown("**Create a binary target**")
    c1,c2,c3 = st.columns([2,1,1])
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

        tbl = pd.DataFrame({"group": g, "y": y.astype(int)}).groupby("group")["y"].mean().rename("selection_rate").reset_index()
        dp = float(tbl["selection_rate"].max() - tbl["selection_rate"].min())
        st.success(f"Demographic parity difference: **{dp:.3f}**")
        st.dataframe(tbl.sort_values("selection_rate", ascending=False), use_container_width=True)
        st.caption("This demo mimics Fairlearn's selection rate by group; in the full version we'll compute real fairness metrics via Fairlearn/Evidently.")
    else:
        st.info("Pick a numeric column and a categorical sensitive attribute.")

with tab_drift:
    st.caption("Mock drift to show planned UX.")
    ref = st.selectbox("Reference dataset", files, key="ref")
    cur = st.selectbox("Current dataset", files, key="cur")
    if ref and cur and ref != cur:
        psi_tbl = pd.DataFrame({"column": ["age","income","score"], "psi": [0.07, 0.21, 0.11]})
        psi_tbl["flag"] = np.where(psi_tbl["psi"]>0.2, "⚠️", "")
        st.dataframe(psi_tbl, use_container_width=True)
        st.caption("Rule of thumb: PSI > 0.2 indicates significant shift.")
