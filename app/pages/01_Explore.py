import os
from typing import List

import streamlit as st
import pandas as pd
import requests

from utils import inject_css, kpi_grid, spinner
from config import API_BASE

DATA_PROC = "data/processed"
os.makedirs(DATA_PROC, exist_ok=True)

ZIP_SUPPORTED_SUFFIXES = (".csv", ".csv.gz", ".parquet")


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
    "Upload CSV or ZIP (containing CSV)",
    type=["csv", "parquet", "zip"],
    key=f"upload_{nonce}",
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


def _upload_zip_to_api(file):
    """Initialize ZIP upload session via the API."""
    try:
        with spinner("Uploading ZIP and extracting..."):
            files = {
                "file": (
                    file.name,
                    file.getvalue(),
                    file.type or "application/zip",
                )
            }
            response = requests.post(f"{API_BASE}/upload_zip", files=files)

        if response.status_code != 200:
            detail = response.json().get("detail", "Unknown error")
            st.error(f"ZIP upload failed: {detail}")
            return None

        return response.json()
    except Exception as e:  # pragma: no cover - user feedback path
        st.error(f"ZIP upload failed: {e}")
        return None


def _ingest_zip_selection(zip_id: str, selected: List[str], dataset_name: str):
    payload = {
        "zip_id": zip_id,
        "selected_files": selected,
        "dataset_name": dataset_name,
    }

    try:
        with spinner("Ingesting selected files..."):
            response = requests.post(f"{API_BASE}/ingest_zip_files", json=payload)
    except Exception as e:  # pragma: no cover - user feedback path
        st.error(f"ZIP ingestion failed: {e}")
        return

    try:
        data = response.json()
    except Exception:
        st.error("Unexpected response from API during ZIP ingestion")
        return

    if response.status_code != 200:
        st.error(data.get("detail", "ZIP ingestion failed"))
        return

    table_name = data.get("table_name")
    dataset_id = data.get("dataset_id")
    if not table_name and dataset_id:
        table_name = f"ds_{dataset_id}"

    rows = data.get("rows_loaded", 0)
    target_label = table_name or (f"ds_{dataset_id}" if dataset_id else "dataset")
    st.session_state["zip_session"] = None
    st.session_state.pop("zip_dataset_name", None)
    st.session_state["uploader_nonce"] = st.session_state.get("uploader_nonce", 0) + 1

    if table_name:
        st.session_state["dataset_choice"] = table_name

    st.session_state["flash"] = f"Ingested ZIP selection as `{target_label}` with {rows} rows."
    st.rerun()


def _is_supported_zip_file(name: str) -> bool:
    lower = name.lower()
    return any(lower.endswith(ext) for ext in ZIP_SUPPORTED_SUFFIXES)


if uploaded is not None:
    lower_name = uploaded.name.lower()
    if lower_name.endswith(".csv") or lower_name.endswith(".parquet"):
        result = _upload_to_api(uploaded)

        if result:
            dataset_id = result["dataset_id"]
            table_name = result["table_name"]
            n_rows = result["n_rows"]
            n_cols = result["n_cols"]

            st.session_state["dataset_choice"] = table_name
            st.session_state["uploader_nonce"] = nonce + 1
            st.session_state["flash"] = (
                f"Ingested **{dataset_id}** as `{table_name}` ({n_rows}×{n_cols})."
            )
            st.rerun()

    elif lower_name.endswith(".zip"):
        response = _upload_zip_to_api(uploaded)
        if response:
            default_name = sanitize_id(os.path.splitext(uploaded.name)[0])
            if not default_name:
                default_name = f"zip_{response['zip_id'][:8]}"
            st.session_state["zip_session"] = {
                "zip_id": response["zip_id"],
                "files": response.get("files", []),
                "filename": uploaded.name,
            }
            st.session_state["zip_dataset_name"] = default_name
            st.session_state["uploader_nonce"] = nonce + 1
            st.rerun()


zip_session = st.session_state.get("zip_session") or None

if zip_session:
    st.subheader("ZIP ingestion")
    st.caption(
        "Select which files inside the archive should be combined and ingested into DuckDB."
    )

    files_in_zip = zip_session.get("files", [])
    supported_files = [f for f in files_in_zip if _is_supported_zip_file(f)]
    unsupported_files = sorted(set(files_in_zip) - set(supported_files))

    if unsupported_files:
        st.info(
            "Unsupported files will be skipped: " + ", ".join(unsupported_files)
        )

    selection_key = f"zip_files_{zip_session['zip_id']}"
    selected_files = st.multiselect(
        "Choose files to ingest",
        options=supported_files,
        default=supported_files,
        key=selection_key,
    )

    if not supported_files:
        st.error("No supported files available in this ZIP archive. Please upload another ZIP or discard this session.")

    dataset_default = st.session_state.get("zip_dataset_name", "")
    dataset_name = st.text_input(
        "Dataset name",
        value=dataset_default,
        key=f"zip_dataset_name_{zip_session['zip_id']}",
    )
    st.session_state["zip_dataset_name"] = dataset_name

    cols = st.columns([1, 1])
    with cols[0]:
        ingest_disabled = not selected_files
        if st.button(
            "Ingest selected files",
            disabled=ingest_disabled,
            key=f"ingest_zip_{zip_session['zip_id']}",
        ):
            if not selected_files:
                st.warning("Please pick at least one file to ingest.")
            else:
                _ingest_zip_selection(zip_session["zip_id"], selected_files, dataset_name)

    with cols[1]:
        if st.button("Discard ZIP upload", key=f"clear_zip_{zip_session['zip_id']}"):
            st.session_state["zip_session"] = None
            st.session_state.pop("zip_dataset_name", None)
            st.rerun()

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
