import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import requests

# Utilities and helpers
from utils import inject_css, dataset_selector

API_BASE = "http://api:8000"  # or "http://api:8000" in Docker

inject_css()
st.title("03 · Correlation")

# ───────────────────────────────────────────────
# Get active dataset (synced with Explore)
# ───────────────────────────────────────────────
dataset_choice = dataset_selector()

# Remove ds_ prefix for display
dataset_id = dataset_choice.replace("ds_", "")

st.markdown(f"### 📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────────────────────
# Fetch correlation matrix from API
# ───────────────────────────────────────────────
with st.spinner("Computing Pearson correlation…"):
    try:
        response = requests.get(f"{API_BASE}/datasets/{dataset_id}/correlation")

        if response.status_code != 200:
            error_detail = response.json().get("detail", "Unknown error")
            if "at least 2 numeric columns" in error_detail:
                st.caption(
                    "Need at least two numeric columns for a correlation matrix."
                )
            else:
                st.error(f"API Error: {error_detail}")
            st.stop()

        data = response.json()
        corr_dict = data["correlation"]
        num_cols = data["columns"]

        # Convert dict back to DataFrame
        corr = pd.DataFrame(corr_dict)

    except requests.exceptions.ConnectionError:
        st.error(
            "Cannot connect to API. Make sure it's running on http://localhost:8000"
        )
        st.stop()
    except Exception as e:
        st.error(f"Failed to fetch correlation: {e}")
        st.stop()

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
