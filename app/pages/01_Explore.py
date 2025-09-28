import os, json
import pandas as pd
import streamlit as st
from utils import inject_css, kpi_grid, spinner

DATA_PROC = "data/processed"
os.makedirs(DATA_PROC, exist_ok=True)

def save_df_as_parquet(df: pd.DataFrame, basename: str) -> str:
    path = os.path.join(DATA_PROC, f"{basename}.parquet")
    df.to_parquet(path, index=False)
    return path

def list_parquet_datasets():
    items=[]
    for fname in sorted(os.listdir(DATA_PROC)):
        if fname.endswith(".parquet"):
            path=os.path.join(DATA_PROC,fname)
            try:
                df=pd.read_parquet(path)
                items.append({"name":fname[:-8], "path_parquet":path, "n_rows":df.shape[0], "n_cols":df.shape[1]})
            except: pass
    return items

inject_css()
st.title("01 · Explore")

uploaded = st.file_uploader("Upload CSV", type=["csv"])
if uploaded:
    with spinner("Reading CSV…"):
        df = pd.read_csv(uploaded)
    base = os.path.splitext(uploaded.name)[0]
    path = save_df_as_parquet(df, base)
    st.success(f"Saved **{base}** → `{path}`")

    kpi_grid({
        "Rows": df.shape[0],
        "Columns": df.shape[1],
        "Numeric cols": sum(pd.api.types.is_numeric_dtype(df[c]) for c in df.columns),
        "Missing % (any)": round(df.isna().any(axis=1).mean()*100, 2),
    })
    with st.expander("Preview & schema", expanded=True):
        st.dataframe(df.head(), use_container_width=True)
        st.json({c:str(t) for c,t in df.dtypes.items()})

st.subheader("Existing datasets")
rows = list_parquet_datasets()
if not rows:
    st.caption("No datasets yet. Upload a CSV above.")
else:
    for r in rows:
        st.write(f"- **{r['name']}**  ({r['n_rows']}×{r['n_cols']}) — `{r['path_parquet']}`")
