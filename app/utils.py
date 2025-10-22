# app/utils.py
import sys
import os

# Add project root to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import streamlit as st
import pandas as pd
from contextlib import contextmanager
from storage.duck import connect

def format_pct(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "—"

def severity_badge(level: str) -> str:
    return {"ok": "🟢 OK", "info": "🔵 Info", "mild": "🟠 Mild", "severe": "🔴 Severe"}.get(level, "🟢 OK")

def dataset_selector(label="Select dataset"):
    """Shared dataset dropdown across all pages"""
    con = connect()
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    tables = [t for t in tables if t != "datasets"]

    if not tables:
        st.warning("No datasets found. Go to **01 · Explore** to upload a CSV.")
        st.stop()

    # Current active dataset
    current = st.session_state.get("dataset_choice")
    if not current or current not in tables:
        current = tables[-1]
        st.session_state["dataset_choice"] = current

    # Dropdown
    choice = st.selectbox(label, tables, index=tables.index(current))
    if choice != st.session_state.get("dataset_choice"):
        st.session_state["dataset_choice"] = choice
        st.rerun()

    return st.session_state["dataset_choice"]

def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    """Convert #RRGGBB to rgba(r,g,b,a)."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"
def inject_css():
    import streamlit as st
    st.markdown(
        """
        <style>
          :root {
            --kpi-bg: #F0F2F6;
            --kpi-text: #31333F;
          }
          @media (prefers-color-scheme: dark) {
            :root {
              --kpi-bg: #262730;
              --kpi-text: #FAFAFA;
            }
          }

          .kpi {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
            margin: 8px 0 16px;
          }
          .kpi .card {
            background: var(--kpi-bg);
            color: var(--kpi-text);
            border: 1px solid var(--kpi-border);
            border-radius: 14px;
            padding: 12px 14px;
            box-shadow: 0 1px 3px rgba(0,0,0,.06);
          }
          .kpi .label {
            font-size: 0.85rem;
            margin-bottom: 4px;
            color: var(--kpi-text);
            opacity: .75;
          }
          .kpi .value {
            font-weight: 700;
            font-size: 1.35rem;
            line-height: 1.2;
            color: var(--kpi-text);
          }
          /* Hide Streamlit's automatic loading bar */
          div[data-testid="stStatusWidget"] {
            display: none;
          }
        
          /* Also hide the running man animation */
          .stApp > header [data-testid="stStatusWidget"] {
            display: none;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )
def kpi_grid(items: dict[str, str | int | float]):
    st.markdown(
        '<div class="kpi">' + "".join(
            f'<div class="card"><div class="label">{k}</div><div class="value">{v}</div></div>'
            for k, v in items.items()
        ) + '</div>',
        unsafe_allow_html=True,
    )

@st.cache_data(show_spinner=False)
def load_parquet(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

@contextmanager
def spinner(msg: str):
    with st.spinner(msg):
        yield

