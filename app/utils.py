import streamlit as st
import pandas as pd
from contextlib import contextmanager

def inject_css():
    st.markdown(
        """
        <style>
          .kpi {display:grid; grid-template-columns: repeat(4, minmax(140px,1fr)); gap:12px; margin-top:8px;}
          .kpi .card {background:#fff; border:1px solid #eee; border-radius:16px; padding:14px 16px;
                      box-shadow:0 1px 3px rgba(0,0,0,0.04);}
          .kpi .label {font-size:12px; color:#6b7280; margin-bottom:4px;}
          .kpi .value {font-weight:700; font-size:22px;}
        </style>
        """,
        unsafe_allow_html=True,
    )

def kpi_grid(items: dict[str, str | int | float]):
    st.markdown('<div class="kpi">' + "".join(
        [f'<div class="card"><div class="label">{k}</div><div class="value">{v}</div></div>' for k, v in items.items()]
    ) + '</div>', unsafe_allow_html=True)

@st.cache_data(show_spinner=False)
def load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

@contextmanager
def spinner(msg: str):
    with st.spinner(msg):
        yield
