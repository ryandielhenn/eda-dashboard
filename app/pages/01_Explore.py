import os
import pandas as pd
import streamlit as st

from utils import inject_css, kpi_grid, spinner
from storage.duck import ingest_csv, list_datasets, sql


DATA_PROC = "data/processed"
os.makedirs(DATA_PROC, exist_ok=True)

def sanitize_id(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)

def quote_indent(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'

def render_kpis_from_duckdb(tbl: str):
    # total rows
    _, cnt_rows = sql(f"SELECT COUNT(*) AS n FROM {tbl}")
    total_rows = int(cnt_rows[0][0]) if cnt_rows else 0

    # schema
    s_cols, s_rows = sql(f"DESCRIBE SELECT * FROM {tbl}")
    schema_df = pd.DataFrame(s_rows, columns=s_cols)
    colnames = schema_df["column_name"].tolist() if not schema_df.empty else []

    # numeric columns
    num_cols = int(
        schema_df["column_type"]
        .str.contains(r"(?:INT|DECIMAL|DOUBLE|FLOAT|REAL|HUGEINT|SMALLINT|TINYINT)", case=False, regex=True)
        .sum()
    ) if not schema_df.empty else 0

    # % rows with ANY missing value
    if total_rows > 0 and colnames:
        or_expr = " OR ".join(f"{quote_indent(c)} IS NULL" for c in colnames)
        _, miss_rows = sql(
            f"SELECT 100.0 * SUM(CASE WHEN {or_expr} THEN 1 ELSE 0 END)/COUNT(*) AS pct FROM {tbl}"
        )
        missing_any_pct = round(float(miss_rows[0][0] or 0.0), 2)
    else:
        missing_any_pct = 0.0

    kpi_grid({
        "Rows": total_rows,
        "Columns": len(colnames),
        "Numeric cols": num_cols,
        "Missing % (any row)": missing_any_pct,
    })


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

def _ingest_csv(csv_path: str, dataset_basename: str):
    """Ingest CSV from disk path"""
    with spinner("Ingesting into DuckDB…"):
        tbl, n_rows, n_cols = ingest_csv(csv_path, dataset_id=dataset_basename)
    # Update session state and rerun
    st.session_state["dataset_choice"] = tbl
    st.session_state["uploader_nonce"] = nonce + 1
    st.session_state["flash"] = f"Ingested **{dataset_basename}** as `{tbl}` ({n_rows}×{n_cols})."
    st.rerun()

if uploaded is not None:
    if uploaded.name.lower().endswith(".csv"):
        dataset_id = sanitize_id(os.path.splitext(uploaded.name)[0])
        
        # Save uploaded file to disk first
        csv_path = os.path.join(DATA_PROC, f"{dataset_id}.csv")
        with open(csv_path, 'wb') as f:
            f.write(uploaded.getbuffer())
        
        # Ingest from disk
        _ingest_csv(csv_path, dataset_id)
        
    elif uploaded.name.lower().endswith(".zip"):
        import zipfile
        
        with zipfile.ZipFile(uploaded, "r") as zf:
            csvs = [f for f in zf.namelist() if f.lower().endswith(".csv")]
        
        if not csvs:
            st.error("No CSV file found inside the ZIP.")
        else:
            pick = st.selectbox("Select a CSV inside ZIP", csvs)
            if st.button("Ingest selected CSV"):
                # Extract CSV from ZIP to disk
                with zipfile.ZipFile(uploaded, "r") as zf:
                    csv_content = zf.read(pick)
                
                dataset_id = sanitize_id(os.path.splitext(os.path.basename(pick))[0])
                csv_path = os.path.join(DATA_PROC, f"{dataset_id}.csv")
                
                with open(csv_path, 'wb') as f:
                    f.write(csv_content)
                _ingest_csv(csv_path, dataset_id)

# ───────────────────────────────
# Datasets known to DuckDB
# ───────────────────────────────
st.subheader("Datasets")
duck_rows = list_datasets()
if not duck_rows:
    st.caption("No datasets yet. Upload a CSV above.")
    st.stop()

# Sort by last_ingested DESC
duck_rows_sorted = sorted(duck_rows, key=lambda r: r[4], reverse=True)
ids = [r[0] for r in duck_rows_sorted]

# Friendly display names (hide ds_ prefix)
real_to_pretty = {r[0]: r[0].replace("ds_", "", 1) for r in duck_rows_sorted}
pretty_to_real = {v: k for k, v in real_to_pretty.items()}
pretty_names = list(pretty_to_real.keys())

# Initialize if needed
if "dataset_choice" not in st.session_state:
    st.session_state["dataset_choice"] = duck_rows_sorted[0][0]

# Current active
current_tbl = st.session_state["dataset_choice"]
current_pretty = real_to_pretty.get(current_tbl, current_tbl.replace("ds_", "", 1))
default_index = pretty_names.index(current_pretty) if current_pretty in pretty_names else 0

# Stable dropdown that doesn’t reset other pages
choice_pretty = st.selectbox(
    "Select dataset",
    pretty_names,
    index=default_index,
    key="dataset_dropdown"
)

tbl = pretty_to_real[choice_pretty]
if not tbl.startswith("ds_"):
    tbl = f"ds_{tbl}"

# Update only if changed by user
if tbl != st.session_state["dataset_choice"]:
    st.session_state["dataset_choice"] = tbl

# Caption
meta = next((r for r in duck_rows_sorted if r[0] == tbl), None)
if meta:
    st.caption(f"{meta[2]}×{meta[3]} • {meta[1]} • ingested {meta[4]}")

# ───────────────────────────────
# KPIs + Preview + Schema
# ───────────────────────────────
try:
    render_kpis_from_duckdb(tbl)
except Exception as e:
    st.error(f"KPI render failed for `{tbl}`: {e}")

# ---------- Preview ----------
st.markdown("##### Preview")
n = st.slider("Rows to preview", 10, 500, 25, key="preview_rows")
try:
    prev_cols, prev_rows = sql(f"SELECT * FROM {tbl} LIMIT {n}")
    st.dataframe(pd.DataFrame(prev_rows, columns=prev_cols), width='stretch')
    st.caption(f"Showing first {len(prev_rows)} rows")
except Exception as e:
    st.error(f"Preview failed: {e}")

# ---------- Schema ----------
with st.expander("Schema", expanded=False):
    try:
        prev_cols, prev_rows = sql(f"SELECT * FROM {tbl} LIMIT {n}")
        st.dataframe(pd.DataFrame(prev_rows, columns=prev_cols), width='stretch')
        st.caption(f"Showing first {len(prev_rows)} rows")
    except Exception as e:
        st.error(f"Preview failed for `{tbl}`: {e}")
