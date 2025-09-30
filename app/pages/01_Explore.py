# app/pages/01_Explore.py
import os
import pandas as pd
import streamlit as st
from utils import inject_css, kpi_grid, spinner
from storage.duck import ingest_parquet, list_datasets, sql, table_name

DATA_PROC = "data/processed"
os.makedirs(DATA_PROC, exist_ok=True)

def save_df_as_parquet(df: pd.DataFrame, basename: str) -> str:
    path = os.path.join(DATA_PROC, f"{basename}.parquet")
    df.to_parquet(path, index=False)
    return path

def sanitize_id(name: str) -> str:
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)

def quote_ident(col: str) -> str:
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
        .str.contains(r"(INT|DECIMAL|DOUBLE|FLOAT|REAL|HUGEINT|SMALLINT|TINYINT)", case=False, regex=True)
        .sum()
    ) if not schema_df.empty else 0

    # % rows with ANY missing value
    if total_rows > 0 and colnames:
        or_expr = " OR ".join(f"{quote_ident(c)} IS NULL" for c in colnames)
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

# Flash success message from previous run (if set)
flash = st.session_state.pop("flash", None)
if flash:
    st.success(flash)

# ──────────────────────────────────────────────────────────────────────────────
# Upload → save parquet → ingest into DuckDB
# Use a NONCE in the uploader key so it resets after each ingest
# ──────────────────────────────────────────────────────────────────────────────
nonce = st.session_state.get("uploader_nonce", 0)
uploaded = st.file_uploader("Upload CSV", type=["csv"], key=f"csv_upload_{nonce}")
if uploaded is not None:
    with spinner("Reading CSV…"):
        df = pd.read_csv(uploaded, low_memory=False)

    dataset_id = sanitize_id(os.path.splitext(uploaded.name)[0])
    path = save_df_as_parquet(df, dataset_id)

    with spinner("Ingesting into DuckDB…"):
        tbl, n_rows, n_cols = ingest_parquet(path, dataset_id=dataset_id)

    # Select the new dataset, bump nonce to reset uploader, flash, rerun
    st.session_state["dataset_choice"] = dataset_id
    st.session_state["uploader_nonce"] = nonce + 1
    st.session_state["flash"] = (
        f"Saved **{dataset_id}** → `{path}` and ingested as `{tbl}` ({n_rows}×{n_cols})."
    )
    st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# Datasets known to DuckDB (KPIs, Preview, Schema all from DuckDB)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("Datasets")
duck_rows = list_datasets()  # [(dataset_id, path, n_rows, n_cols, last_ingested), ...]

if not duck_rows:
    st.caption("No datasets yet. Upload a CSV above.")
else:
    # Sort by last_ingested DESC so newest appears first
    duck_rows_sorted = sorted(duck_rows, key=lambda r: r[4], reverse=True)
    ids = [r[0] for r in duck_rows_sorted]

    # Seed default only if not set or stale
    if "dataset_choice" not in st.session_state or st.session_state["dataset_choice"] not in ids:
        st.session_state["dataset_choice"] = ids[0]

    # Bind the selectbox to a persistent key so user choice sticks
    choice = st.selectbox("Select dataset", ids, key="dataset_choice")

    # Find meta for caption (from sorted rows)
    meta = next((r for r in duck_rows_sorted if r[0] == choice), None)
    if meta:
        st.caption(f"{meta[2]}×{meta[3]} • {meta[1]} • ingested {meta[4]}")

    tbl = table_name(choice)

    # KPIs (single source of truth)
    render_kpis_from_duckdb(tbl)

    # ---------- Preview ----------
    st.markdown("##### Preview")
    n = st.slider("Rows to preview", 10, 500, 25, key="preview_rows")

    try:
        prev_cols, prev_rows = sql(f"SELECT * FROM {tbl} LIMIT {n}")
        st.dataframe(pd.DataFrame(prev_rows, columns=prev_cols), use_container_width=True)
        st.caption(f"Showing first {len(prev_rows)} rows")
    except Exception as e:
        st.error(f"Preview failed for `{tbl}`: {e}")

    # ---------- Schema ----------
    with st.expander("Schema", expanded=False):
        try:
            s_cols, s_rows = sql(f"DESCRIBE SELECT * FROM {tbl}")
            st.dataframe(pd.DataFrame(s_rows, columns=s_cols), use_container_width=True)
        except Exception as e:
            st.error(f"Schema fetch failed: {e}")

