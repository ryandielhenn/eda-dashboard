import os
import pandas as pd
import streamlit as st
import duckdb
from utils import inject_css, spinner
from storage.duck import ingest_parquet, list_datasets, sql

DUCKDB_PATH = "data/duckdb/eda.duckdb"
DATA_PROC = "data/processed"
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
os.makedirs(DATA_PROC, exist_ok=True)

inject_css()
st.title("01 · Explore")

# ───────────────────────────────
# Upload or ingest datasets
# ───────────────────────────────
nonce = st.session_state.get("uploader_nonce", 0)
uploaded = st.file_uploader("Upload CSV or ZIP (containing CSV)", type=["csv", "zip"], key=f"upload_{nonce}")

def save_df_as_parquet(df, basename):
    path = os.path.join(DATA_PROC, f"{basename}.parquet")
    df.to_parquet(path, index=False)
    return path

def sanitize(name):
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)

def _ingest(df, basename):
    path = save_df_as_parquet(df, basename)
    with spinner("Ingesting into DuckDB…"):
        tbl, n_rows, n_cols = ingest_parquet(path, dataset_id=basename)
    st.session_state["dataset_choice"] = tbl  # true internal name (ds_)
    st.session_state["uploader_nonce"] = nonce + 1
    st.success(f"✅ Ingested **{basename}** → `{tbl}` ({n_rows}×{n_cols})")
    st.rerun()

if uploaded is not None:
    if uploaded.name.lower().endswith(".csv"):
        df = pd.read_csv(uploaded, low_memory=False)
        _ingest(df, sanitize(os.path.splitext(uploaded.name)[0]))
    elif uploaded.name.lower().endswith(".zip"):
        import zipfile
        with zipfile.ZipFile(uploaded, "r") as zf:
            csvs = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        if not csvs:
            st.error("No CSV found in ZIP.")
        else:
            pick = st.selectbox("Select CSV inside ZIP", csvs)
            if st.button("Ingest selected CSV"):
                with zipfile.ZipFile(uploaded, "r") as zf:
                    with zf.open(pick) as f:
                        df = pd.read_csv(f, low_memory=False)
                _ingest(df, sanitize(os.path.splitext(os.path.basename(pick))[0]))

# ───────────────────────────────
# Existing datasets
# ───────────────────────────────
# ───────────────────────────────
# Existing datasets
# ───────────────────────────────
st.subheader("Datasets")
rows = list_datasets()
if not rows:
    st.caption("No datasets yet. Upload a CSV above.")
    st.stop()

rows = sorted(rows, key=lambda r: r[4], reverse=True)

# Build friendly mappings
real_to_pretty = {r[0]: r[0].replace("ds_", "", 1) for r in rows}
pretty_to_real = {v: k for k, v in real_to_pretty.items()}
pretty_names = list(pretty_to_real.keys())

# 🔹 Keep dataset stable — only update if user selects new one
if "dataset_choice" not in st.session_state:
    # initialize with latest dataset
    st.session_state["dataset_choice"] = rows[0][0]  # the ds_ name

# Current active table
current_tbl = st.session_state["dataset_choice"]
current_pretty = real_to_pretty.get(current_tbl, current_tbl.replace("ds_", "", 1))
default_index = pretty_names.index(current_pretty) if current_pretty in pretty_names else 0

# Show dropdown for manual switching
choice_pretty = st.selectbox(
    "Select dataset", 
    pretty_names, 
    index=default_index, 
    key="dataset_dropdown"
)

# Map to real DuckDB table
tbl = pretty_to_real[choice_pretty]
if not tbl.startswith("ds_"):
    tbl = f"ds_{tbl}"

# Update only if changed
if tbl != st.session_state["dataset_choice"]:
    st.session_state["dataset_choice"] = tbl

st.caption(f"📂 Active table: `{tbl}`")

# ───────────────────────────────
# KPIs + preview
# ───────────────────────────────
try:
    _, cnt = sql(f"SELECT COUNT(*) AS n FROM {tbl}")
    total_rows = int(cnt[0][0])
    st.metric("Rows", total_rows)
except Exception as e:
    st.error(f"Error reading table `{tbl}`: {e}")
    st.stop()

try:
    cols, rows_ = sql(f"SELECT * FROM {tbl} LIMIT 25")
    st.dataframe(pd.DataFrame(rows_, columns=cols), use_container_width=True)
except Exception as e:
    st.error(f"Preview failed: {e}")
