import streamlit as st
from utils import inject_css

st.set_page_config(page_title="EDA Dashboard", layout="wide")
inject_css()

st.title("EDA Dashboard")
st.write("Explore datasets, visualize distributions & correlations, and preview fairness/drift audits.")

col1, col2 = st.columns([2,1])
with col1:
    st.markdown("> **Start in**:  **01 · Explore** → upload a CSV. Then visit **02 · Distributions** and **03 · Correlation**.")
with col2:
    st.markdown("**Docs**")
    st.markdown("- Interactive viz plan in README\n- Roadmap & stretch goals")

st.caption("Tip: Keep files small for demo (≤ 5–10MB).")
