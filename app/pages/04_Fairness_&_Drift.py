import pandas as pd
import streamlit as st

# Utilities and helpers
from analytics.drift import compute_psi_table
from utils import inject_css, dataset_selector
from storage.duck import get_tables, connect

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

# Get connection
con = connect()

# Get column types from DuckDB schema
columns_query = f"DESCRIBE SELECT * FROM '{dataset_choice}'"
columns_info = con.execute(columns_query).df()

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
cat_cols = columns_info[~columns_info["column_type"].isin(numeric_types)][
    "column_name"
].tolist()

# Tabs: Fairness | Drift
tab_fair, tab_drift = st.tabs(["Fairness", "Drift"])

# ──────────── Fairness ────────────
with tab_fair:
    st.markdown("**Create a binary target**")
    c1, c2, c3 = st.columns([2, 1, 1])

    with c1:
        tcol = st.selectbox("Numeric column", num_cols or ["<none>"])

    if num_cols and tcol:
        # Get median from DuckDB
        median_query = f"SELECT MEDIAN(\"{tcol}\") as med FROM '{dataset_choice}'"
        median_val = con.execute(median_query).fetchone()[0]

        with c2:
            thresh = st.number_input("Threshold", value=float(median_val))
        with c3:
            positive_def = st.selectbox(
                "Positive if", [f"{tcol} > {thresh}", f"{tcol} <= {thresh}"]
            )
    else:
        with c2:
            thresh = st.number_input("Threshold", value=0.0)
        with c3:
            positive_def = st.selectbox("Positive if", ["", ""])

    st.markdown("**Sensitive attribute**")
    sattr = st.selectbox("Sensitive attribute", cat_cols or ["<none>"])

    if num_cols and cat_cols and tcol and sattr:
        # Compute fairness metrics in DuckDB
        operator = ">" if ">" in positive_def else "<="

        fairness_query = f"""
        SELECT 
            "{sattr}" as group,
            AVG(CASE WHEN "{tcol}" {operator} {thresh} THEN 1 ELSE 0 END) as selection_rate,
            COUNT(*) as n
        FROM '{dataset_choice}'
        GROUP BY "{sattr}"
        ORDER BY selection_rate DESC
        """

        with st.spinner("Computing fairness metrics..."):
            grp = con.execute(fairness_query).df()

        if len(grp) > 0:
            dp = float(grp["selection_rate"].max() - grp["selection_rate"].min())
            st.success(f"Demographic parity difference: **{dp:.3f}**")
            st.dataframe(grp, hide_index=True)
        else:
            st.warning("No data to compute fairness metrics.")
    else:
        st.info("Pick a numeric column and a categorical sensitive attribute.")

# ──────────── Drift (PSI) ────────────
with tab_drift:
    st.markdown(
        "Compare the **active dataset** (reference) vs a **current** dataset to compute PSI per column."
    )

    ref_table = dataset_choice
    available = [t for t in tables if t != ref_table]

    if not available:
        st.info("No other datasets available to compare drift.")
    else:
        cur_table = st.selectbox("Current dataset", available, index=0, key="cur_ds")

        # Get shared columns from schema (no data fetch)
        ref_cols_query = f"DESCRIBE SELECT * FROM '{ref_table}'"
        cur_cols_query = f"DESCRIBE SELECT * FROM '{cur_table}'"

        ref_cols = set(con.execute(ref_cols_query).df()["column_name"].tolist())
        cur_cols = set(con.execute(cur_cols_query).df()["column_name"].tolist())
        shared_cols = list(ref_cols.intersection(cur_cols))

        if not shared_cols:
            st.warning("No shared columns between the selected datasets.")
        else:
            cols = st.multiselect(
                "Columns to evaluate (shared only)", shared_cols, default=shared_cols
            )

            # Bin control for numeric PSI
            n_bins = st.slider(
                "Bins for numeric PSI", min_value=5, max_value=30, value=10
            )

            if cur_table == ref_table:
                st.info(
                    "Pick a dataset different from the reference to compute drift (PSI)."
                )
            elif cols:
                with st.spinner("Computing PSI…"):
                    # Fetch ONLY the selected columns for PSI computation
                    cols_str = ", ".join([f'"{col}"' for col in cols])
                    ref_query = f"SELECT {cols_str} FROM '{ref_table}'"
                    cur_query = f"SELECT {cols_str} FROM '{cur_table}'"

                    ref_df = con.execute(ref_query).df()
                    cur_df = con.execute(cur_query).df()

                    psi_tbl = compute_psi_table(
                        ref_df, cur_df, columns=cols, n_bins=n_bins
                    )

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
