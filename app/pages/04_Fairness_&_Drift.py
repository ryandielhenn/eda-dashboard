import pandas as pd
import streamlit as st

# Utilities and helpers
from analytics.drift import compute_psi_table
from utils import inject_css, dataset_selector
from storage.duck import get_tables, load_table

# ───────────────────────────────────────────────
# UI
# ───────────────────────────────────────────────
inject_css()
st.title("04 · Fairness & Drift")

# Get active dataset (synced with Explore) — used as the Fairness source and as the default *reference* for Drift
dataset_choice = dataset_selector()

# Ensure DuckDB has data
tables = get_tables()
if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

# Self-heal if dataset not selected or stale
if not dataset_choice or dataset_choice not in tables:
    dataset_choice = tables[-1]
    st.session_state["dataset_choice"] = dataset_choice

st.caption(f"📂 Active dataset: `{dataset_choice}`")

# Load from DuckDB
df = load_table(dataset_choice)

# Tabs: Fairness | Drift
tab_fair, tab_drift = st.tabs(["Fairness", "Drift"])

# ──────────── Fairness ────────────
with tab_fair:
    num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    cat_cols = [c for c in df.columns if c not in num_cols]

    st.markdown("**Create a binary target**")
    c1, c2, c3 = st.columns([2, 1, 1])
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

        grp = (
            pd.DataFrame({"group": g, "y": y.astype(int)})
            .groupby("group")
            .agg(selection_rate=("y", "mean"), n=("y", "size"))
            .reset_index()
        )
        dp = float(grp["selection_rate"].max() - grp["selection_rate"].min())

        st.success(f"Demographic parity difference: **{dp:.3f}**")
        st.dataframe(grp.sort_values("selection_rate", ascending=False), hide_index=True)
        st.caption(
            "This demo mimics Fairlearn's selection rate by group; in a full version, "
            "we would compute additional fairness metrics via Fairlearn/Evidently."
        )
    else:
        st.info("Pick a numeric column and a categorical sensitive attribute.")

# ──────────── Drift (actual PSI) ────────────
with tab_drift:
    st.markdown("Compare the **active dataset** (reference) vs a **current** dataset to compute PSI per column.")

    ref_table = dataset_choice  # use the globally selected dataset as the reference
    available = [t for t in tables if t != ref_table]

    if not available:
        st.info("No other datasets available to compare drift.")
    else:
        cur_table = st.selectbox("Current dataset", available, index=0, key="cur_ds")

        # Load both datasets from DuckDB
        ref_df = load_table(ref_table)
        cur_df = load_table(cur_table)

        # Choose columns (shared only)
        shared_cols = ref_df.columns.intersection(cur_df.columns).tolist()
        if not shared_cols:
            st.warning("No shared columns between the selected datasets.")
        else:
            cols = st.multiselect("Columns to evaluate (shared only)", shared_cols, default=shared_cols)

            # Bin control for numeric PSI
            n_bins = st.slider("Bins for numeric PSI", min_value=5, max_value=30, value=10)

            if cur_table == ref_table:
                st.info("Pick a dataset different from the reference to compute drift (PSI).")
            else:
                with st.spinner("Computing PSI…"):
                    psi_tbl = compute_psi_table(ref_df, cur_df, columns=cols, n_bins=n_bins)

                st.dataframe(psi_tbl, hide_index=True)
                st.caption("Rule of thumb: PSI > 0.2 indicates significant shift (⚠️).")

                psi_tbl_df = pd.DataFrame(psi_tbl)

                # Download CSV (exclude the symbol 'flag' column from export)
                export_df = psi_tbl_df.drop(columns=["flag"], errors="ignore")
                csv = export_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download PSI table (CSV)",
                    data=csv,
                    file_name=f"psi_{ref_table}_vs_{cur_table}.csv",
                    mime="text/csv",
                )
