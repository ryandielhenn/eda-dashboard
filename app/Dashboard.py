import streamlit as st
from streamlit_lottie import st_lottie
import requests
from utils import inject_css

st.set_page_config(page_title="EDA Dashboard", layout="wide", page_icon="✨")
inject_css()

# ─────────────────────────────
# Helper
# ─────────────────────────────
def load_lottieurl(url: str):
    try:
        r = requests.get(url)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

anim = load_lottieurl("https://assets10.lottiefiles.com/packages/lf20_3rwasyjy.json")

# ─────────────────────────────
# Style
# ─────────────────────────────
st.markdown("""
<style>
:root {
    --accent: #2563eb;
    --accent-hover: #1e40af;
}

.card {
    background: linear-gradient(145deg, #ffffff, #f3f4f6);
    border-radius: 14px;
    padding: 25px;
    text-align: center;
    border: 1px solid rgba(0,0,0,0.05);
    box-shadow: 0 3px 8px rgba(0,0,0,0.08);
    transition: all 0.3s ease;
}
[data-theme="dark"] .card {
    background: linear-gradient(145deg, #1f2937, #111827);
    color: #f9fafb;
}
.card:hover {
    transform: translateY(-4px);
    box-shadow: 0 6px 16px rgba(0,0,0,0.15);
}

.flow-arrow {
    font-size: 2rem;
    color: #9ca3af;
    text-align: center;
    animation: pulse 2s infinite;
}
@keyframes pulse {
    0%,100% {opacity:.3; transform:translateY(0);}
    50% {opacity:1; transform:translateY(-4px);}
}

.stPageLink-container {
    text-align: center;
    margin-top: 0.4rem;
}

hr {
    border: none;
    height: 1px;
    background: linear-gradient(to right, transparent, #9ca3af, transparent);
    margin: 2rem 0;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────
# Header
# ─────────────────────────────
col1, col2 = st.columns([1.5, 1])
with col1:
    st.title("✨ EDA Dashboard")
    st.markdown("""
Welcome to your **interactive environment** for data exploration, visualization, and fairness analysis.  
Follow the flow below to explore your data from raw to refined insights.
""")
with col2:
    if anim:
        st_lottie(anim, height=200)

st.markdown("<hr>", unsafe_allow_html=True)

# ─────────────────────────────
# Workflow Overview
# ─────────────────────────────
st.subheader("🧭 Workflow Overview")

col1, col2, col3 = st.columns(3, gap="large")

with col1:
    st.markdown('<div class="card"><h3>1️⃣ Explore</h3><p>Upload and preview your dataset. Filter columns, inspect datatypes, and view summaries.</p></div>', unsafe_allow_html=True)
    st.page_link("pages/01_Explore.py", label="Go to Explore ➜")

with col2:
    st.markdown('<div class="card"><h3>2️⃣ Distributions</h3><p>Visualize feature distributions, histograms, and outliers. Identify key trends quickly.</p></div>', unsafe_allow_html=True)
    st.page_link("pages/02_Distributions.py", label="Go to Distributions ➜")

with col3:
    st.markdown('<div class="card"><h3>3️⃣ Correlation</h3><p>Analyze relationships using correlation matrices and heatmaps to understand feature interplay.</p></div>', unsafe_allow_html=True)
    st.page_link("pages/03_Correlation.py", label="Go to Correlation ➜")

st.markdown("<hr>", unsafe_allow_html=True)

# ─────────────────────────────
# Upcoming Features
# ─────────────────────────────
st.subheader("🌟 Upcoming Features")

col1, col2 = st.columns(2)
with col1:
    st.write("""
- True fairness
""")
with col2:
    future_anim = load_lottieurl("https://assets6.lottiefiles.com/packages/lf20_t24tpvcu.json")
    if future_anim:
        st_lottie(future_anim, height=180)

st.caption("💡 Tip: Keep dataset files under 10 MB for smooth performance.")
