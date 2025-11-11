import pandas as pd
import streamlit as st
import requests
from config import API_BASE

from utils import inject_css, dataset_selector

inject_css()
st.title("04 · Fairness & Drift")

# Get active dataset (synced with Explore)
dataset_choice = dataset_selector()
dataset_id = dataset_choice.replace("ds_", "")

st.caption(f"📂 Active dataset: `{dataset_choice}`")

# Get column types from API
try:
    response = requests.get(f"{API_BASE}/datasets/{dataset_id}/schema")
    schema_data = response.json()["schema"]
    
    numeric_types = ["BIGINT", "INTEGER", "DOUBLE", "FLOAT", "DECIMAL", "HUGEINT", 
                     "SMALLINT", "TINYINT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "REAL"]
    
    num_cols = [
        col["column_name"] for col in schema_data
        if col["column_type"].upper() in numeric_types
    ]
    cat_cols = [
        col["column_name"] for col in schema_data
        if col["column_type"].upper() not in numeric_types
    ]
except Exception as e:
    st.error(f"Failed to load schema: {e}")
    st.stop()

# Tabs: Fairness | Drift
tab_fair, tab_drift = st.tabs(["Fairness", "Drift"])

# ──────────── Fairness ────────────
with tab_fair:
    st.markdown("**Create a binary target**")
    c1, c2, c3 = st.columns([2, 1, 1])

    with c1:
        tcol = st.selectbox("Numeric column", num_cols or ["<none>"])

    if num_cols and tcol:
        # Get median from API (could add dedicated endpoint, for now use preview)
        try:
            preview_response = requests.get(
                f"{API_BASE}/datasets/{dataset_id}/preview"
            )
            preview_data = preview_response.json()
            df_sample = pd.DataFrame(preview_data["data"], columns=preview_data["columns"])
            median_val = df_sample[tcol].median()
        except:
            median_val = 0.0

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
        operator = ">" if ">" in positive_def else "<="
        
        with st.spinner("Computing fairness metrics..."):
            try:
                response = requests.get(
                    f"{API_BASE}/datasets/{dataset_id}/fairness",
                    params={
                        "target_column": tcol,
                        "threshold": thresh,
                        "comparison_operator": operator,
                        "sensitive_attribute": sattr
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    dp = data["demographic_parity_difference"]
                    grp = pd.DataFrame(data["group_statistics"])
                    
                    st.success(f"Demographic parity difference: **{dp:.3f}**")
                    st.dataframe(grp, hide_index=True)
                else:
                    st.warning("No data to compute fairness metrics.")
                    
            except Exception as e:
                st.error(f"Failed to compute fairness: {e}")
    else:
        st.info("Pick a numeric column and a categorical sensitive attribute.")

# ──────────── Drift (PSI) ────────────
with tab_drift:
    st.markdown(
        "Compare the **active dataset** (reference) vs a **current** dataset to compute PSI per column."
    )

    # Get all datasets from API
    try:
        datasets_response = requests.get(f"{API_BASE}/datasets")
        all_datasets = datasets_response.json()["datasets"]
        all_tables = [f"ds_{d['dataset_id']}" for d in all_datasets]
    except:
        st.error("Failed to fetch datasets")
        st.stop()

    ref_table = dataset_choice
    available = [t for t in all_tables if t != ref_table]

    if not available:
        st.info("No other datasets available to compare drift.")
    else:
        cur_table = st.selectbox("Current dataset", available, index=0, key="cur_ds")
        cur_id = cur_table.replace("ds_", "")
        ref_id = ref_table.replace("ds_", "")

        # Get shared columns from schemas
        try:
            ref_schema_response = requests.get(f"{API_BASE}/datasets/{ref_id}/schema")
            cur_schema_response = requests.get(f"{API_BASE}/datasets/{cur_id}/schema")
            
            ref_cols = set([col["column_name"] for col in ref_schema_response.json()["schema"]])
            cur_cols = set([col["column_name"] for col in cur_schema_response.json()["schema"]])
            shared_cols = list(ref_cols.intersection(cur_cols))
        except Exception as e:
            st.error(f"Failed to get schemas: {e}")
            st.stop()

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
                    try:
                        response = requests.get(
                            f"{API_BASE}/datasets/{ref_id}/drift/{cur_id}",
                            params={
                                "columns": cols,
                                "n_bins": n_bins
                            }
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            psi_tbl = pd.DataFrame(data["psi_metrics"])
                            
                            st.dataframe(psi_tbl, hide_index=True)
                            st.caption("Rule of thumb: PSI > 0.2 indicates significant shift (⚠️).")
                            
                            # Download CSV (exclude the symbol 'flag' column from export)
                            export_df = psi_tbl.drop(columns=["flag"], errors="ignore")
                            csv = export_df.to_csv(index=False).encode("utf-8")
                            st.download_button(
                                "Download PSI table (CSV)",
                                data=csv,
                                file_name=f"psi_{ref_table}_vs_{cur_table}.csv",
                                mime="text/csv",
                            )
                        else:
                            st.error(f"Drift computation failed: {response.json().get('detail', 'Unknown error')}")
                            
                    except Exception as e:
                        st.error(f"Failed to compute drift: {e}")
