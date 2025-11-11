import os
import streamlit as st
import pandas as pd
import requests

from utils import inject_css, kpi_grid, spinner

API_BASE = "http://api:8000"
DATA_PROC = "data/processed"
os.makedirs(DATA_PROC, exist_ok=True)


def sanitize_id(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


inject_css()
st.title("01 · Explore")

# ───────────────────────────────
# Flash from previous run
# ───────────────────────────────
flash = st.session_state.pop("flash", None)
if flash:
    st.success(flash)

# ───────────────────────────────
# Upload or ingest datasets
# ───────────────────────────────
nonce = st.session_state.get("uploader_nonce", 0)
uploaded = st.file_uploader(
    "Upload CSV or ZIP (containing CSV)", type=["csv", "zip"], key=f"upload_{nonce}"
)


def _upload_to_api(file):
    """Upload file to API"""
    try:
        with spinner("Uploading and ingesting..."):
            files = {"file": (file.name, file.getvalue(), file.type)}
            response = requests.post(f"{API_BASE}/upload", files=files)

            if response.status_code != 200:
                st.error(
                    f"Upload failed: {response.json().get('detail', 'Unknown error')}"
                )
                return None

            return response.json()
    except Exception as e:
        st.error(f"Upload failed: {e}")
        return None


if uploaded is not None:
    if uploaded.name.lower().endswith(".csv"):
        result = _upload_to_api(uploaded)

        if result:
            dataset_id = result["dataset_id"]
            table_name = result["table_name"]
            n_rows = result["n_rows"]
            n_cols = result["n_cols"]

            # Update session state and rerun
            st.session_state["dataset_choice"] = table_name
            st.session_state["uploader_nonce"] = nonce + 1
            st.session_state["flash"] = (
                f"Ingested **{dataset_id}** as `{table_name}` ({n_rows}×{n_cols})."
            )
            st.rerun()

    elif uploaded.name.lower().endswith(".zip"):
        st.warning(
            "ZIP upload not yet implemented via API. Extract and upload CSV directly."
        )

# ───────────────────────────────
# Datasets known to API
# ───────────────────────────────
st.subheader("Datasets")

try:
    response = requests.get(f"{API_BASE}/datasets")
    datasets = response.json()["datasets"]
except Exception as e:
    st.error(f"Failed to fetch datasets: {e}")
    st.stop()

if not datasets:
    st.caption("No datasets yet. Upload a CSV above.")
    st.stop()

# Sort by last_ingested DESC
datasets_sorted = sorted(datasets, key=lambda d: d["last_ingested"], reverse=True)

# Friendly display names (show with ds_ prefix for consistency)
table_names = [f"ds_{d['dataset_id']}" for d in datasets_sorted]
display_names = [d["dataset_id"] for d in datasets_sorted]

# Initialize if needed
if "dataset_choice" not in st.session_state:
    st.session_state["dataset_choice"] = table_names[0]

# Current active
current_tbl = st.session_state["dataset_choice"]
current_display = current_tbl.replace("ds_", "", 1)
default_index = (
    display_names.index(current_display) if current_display in display_names else 0
)

# Stable dropdown
choice_display = st.selectbox(
    "Select dataset", display_names, index=default_index, key="dataset_dropdown"
)

# Convert back to table name
selected_tbl = f"ds_{choice_display}"

# Update only if changed by user
if selected_tbl != st.session_state["dataset_choice"]:
    st.session_state["dataset_choice"] = selected_tbl

# Get metadata for caption
selected_data = next(
    (d for d in datasets_sorted if d["dataset_id"] == choice_display), None
)
if selected_data:
    st.caption(
        f"{selected_data['n_rows']}×{selected_data['n_cols']} • "
        f"{selected_data['path']} • ingested {selected_data['last_ingested']}"
    )

# ───────────────────────────────
# KPIs
# ───────────────────────────────
dataset_id = choice_display

try:
    # Get schema for KPIs
    schema_response = requests.get(f"{API_BASE}/datasets/{dataset_id}/schema")
    schema_data = schema_response.json()["schema"]

    # Count numeric columns
    num_cols = sum(
        1
        for col in schema_data
        if any(
            t in col["column_type"].upper()
            for t in [
                "INT",
                "DECIMAL",
                "DOUBLE",
                "FLOAT",
                "REAL",
                "HUGEINT",
                "SMALLINT",
                "TINYINT",
            ]
        )
    )

    # Get preview for missing calculation (simplified - just show 0 for now)
    # TODO: Add proper missing calculation to API
    missing_pct = 0.0

    kpi_grid(
        {
            "Rows": selected_data["n_rows"],
            "Columns": selected_data["n_cols"],
            "Numeric cols": num_cols,
            "Missing % (any row)": missing_pct,
        }
    )
except Exception as e:
    st.error(f"KPI render failed: {e}")

# ---------- Preview ----------
st.markdown("##### Preview")
n = st.slider("Rows to preview", 10, 500, 25, key="preview_rows")

try:
    preview_response = requests.get(
        f"{API_BASE}/datasets/{dataset_id}/preview", params={"limit": n}
    )

    if preview_response.status_code == 200:
        preview_data = preview_response.json()
        df = pd.DataFrame(preview_data["data"], columns=preview_data["columns"])
        st.dataframe(df, width="stretch")
        st.caption(f"Showing first {len(df)} rows")
    else:
        st.error("Preview failed")

except Exception as e:
    st.error(f"Preview failed: {e}")

# ---------- Schema ----------
with st.expander("Schema", expanded=False):
    try:
        schema_response = requests.get(f"{API_BASE}/datasets/{dataset_id}/schema")
        if schema_response.status_code == 200:
            schema = schema_response.json()["schema"]
            schema_df = pd.DataFrame(schema)
            st.dataframe(schema_df, width="stretch")
        else:
            st.error("Schema fetch failed")
    except Exception as e:
        st.error(f"Schema failed: {e}")
