import streamlit as st

# Optional util
try:
    from utils import inject_css
except Exception:
    inject_css = lambda: None

st.set_page_config(page_title="EDA Dashboard", layout="wide", page_icon="✨")
inject_css()

# ──────────────────────────────────────────────────────────────────────────────
# CSS — no reveal/opacity tricks; correct light/dark tokens
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
:root{
  /* LIGHT THEME DEFAULTS */
  --accent:#2563eb;
  --accent-2:#8b5cf6;
  --accent-hover:#1e40af;
  --text:#0f172a;            /* slate-900 */
  --muted:#64748b;           /* slate-500 */
  --surface:#ffffff;         /* card start */
  --surface-2:#f8fafc;       /* card end */
  --border:rgba(2,6,23,.08);
  --ring: rgba(37,99,235,.35);
}
[data-theme="dark"]{
  /* DARK THEME OVERRIDES */
  --accent:#3b82f6;
  --accent-2:#a78bfa;
  --accent-hover:#1d4ed8;
  --text:#e5e7eb;            /* slate-200 */
  --muted:#94a3b8;           /* slate-400 */
  --surface:#0f172a;         /* slate-900-ish */
  --surface-2:#111827;       /* slate-800-ish */
  --border:rgba(255,255,255,.08);
  --ring: rgba(59,130,246,.45);
}

/* Layout + ambient background */
.block-container{ max-width:1280px; padding-top:1rem; position:relative; }
.block-container::before{
  content:""; position:fixed; inset:0; z-index:-2;
  background:
    radial-gradient(1200px 500px at -10% -20%, rgba(37,99,235,.12), transparent 60%),
    radial-gradient(900px 400px at 110% -30%, rgba(139,92,246,.12), transparent 70%),
    linear-gradient(180deg, var(--surface), var(--surface-2));
}
.block-container::after{
  content:""; position:fixed; inset:0; z-index:-1; pointer-events:none;
  background-image:
    linear-gradient(rgba(2,6,23,.05) 1px, transparent 1px),
    linear-gradient(90deg, rgba(2,6,23,.05) 1px, transparent 1px);
  background-size:22px 22px; opacity:.18;
}

/* Divider & headings */
hr{ border:none; height:1px; background:linear-gradient(to right,transparent,#94a3b8,transparent); margin:2rem 0; }
h2{ font-weight:900; letter-spacing:-.01em; }
st.markdown("<div style='height:3rem'></div>", unsafe_allow_html=True)

/* HERO (always visible, centered) */
.hero{
  text-align:center;
  background:linear-gradient(180deg, var(--surface), var(--surface-2));
  border:1px solid var(--border);
  border-radius:24px;
  padding:36px 28px;
  box-shadow:0 10px 30px rgba(0,0,0,.08);
  position:relative; overflow:hidden;
}
.hero::before{
  content:""; position:absolute; inset:-1px; border-radius:24px;
  background:conic-gradient(from 180deg, rgba(37,99,235,.22), rgba(139,92,246,.22), rgba(37,99,235,.22));
  filter:blur(28px); opacity:.2;
}
.hero h1{
  font-size:clamp(2rem,2.8vw,2.8rem);
  font-weight:900; margin:.1rem 0 .35rem 0;
  background:linear-gradient(90deg, var(--accent), var(--accent-2));
  -webkit-background-clip:text; background-clip:text; color:transparent;
}
.hero p{ color:var(--muted); font-size:1.05rem; max-width:760px; margin:auto; }

/* Toolbar */
.toolbar{ margin-top:1.2rem; display:flex; justify-content:center; }
.btn{
  display:inline-flex; align-items:center; gap:.6rem; font-weight:800; color:#fff;
  background:linear-gradient(90deg, var(--accent), var(--accent-2));
  padding:.75rem 1.25rem; border-radius:14px; text-decoration:none; border:none;
  box-shadow:0 12px 28px rgba(37,99,235,.3); transition:transform .12s ease, filter .12s ease;
}
.btn:hover{ transform:translateY(-2px); filter:brightness(1.08); }

/* Workflow grid */
.workflow{ margin-top:1rem; }
.workflow [data-testid="stHorizontalBlock"]{ display:flex; flex-wrap:wrap; gap:1.6rem; }
.workflow [data-testid="column"]{ flex:1 1 300px; min-width:280px; }

/* Cards */
.card{
  position:relative;
  background:linear-gradient(180deg, var(--surface), var(--surface-2));
  border-radius:18px;
  border:1px solid var(--border);
  box-shadow:0 6px 20px rgba(0,0,0,.08);
  padding:20px;
  isolation:isolate;
  transition: transform .2s ease, box-shadow .2s ease, border-color .2s ease;
}
.card:hover{ transform:translateY(-3px); box-shadow:0 20px 40px rgba(0,0,0,.15); border-color:rgba(2,6,23,.14); }
.card::after{
  content:""; position:absolute; inset:-1px; border-radius:18px;
  background:linear-gradient(120deg, transparent, rgba(37,99,235,.35) 40%, rgba(139,92,246,.35) 60%, transparent);
  filter:blur(18px); opacity:0; transition:.25s ease;
}
.card:hover::after{ opacity:1; }

.badge{
  display:inline-flex; align-items:center; justify-content:center;
  height:26px; min-width:26px; padding:0 .5rem;
  border-radius:10px; font-weight:900; color:var(--text);
  background:linear-gradient(180deg, rgba(148,163,184,.28), rgba(148,163,184,.16));
  border:1px solid var(--border);
}
.card-title{ font-weight:900; color:var(--text); font-size:1.1rem; }
.card-desc{ color:var(--muted); font-size:.95rem; line-height:1.5; }

/* CTA link */
.stPageLink a{
  display:block; width:100%; text-align:center;
  padding:.75rem 1rem; border-radius:12px; margin-top:.6rem;
  font-weight:800; background:linear-gradient(90deg,var(--accent),var(--accent-2));
  color:#fff !important; text-decoration:none; box-shadow:0 10px 26px rgba(37,99,235,.3);
  transition:transform .1s ease, filter .1s ease;
}
.stPageLink a:hover{ transform:translateY(-2px); filter:brightness(1.08); }

/* Footer */
.footer{ margin-top:2rem; padding:.8rem 0; color:var(--muted); text-align:center; border-top:1px dashed var(--border); }

/* Avoid awkward line breaks */
.card-title, .card-desc{ word-break: normal; overflow-wrap: break-word; hyphens: auto; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# HERO (centered, single button)
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="hero">
      <h1>✨ EDA Dashboard</h1>
      <p>Explore data, visualize distributions, analyze correlations, and run fairness &amp; drift checks.</p>
      
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<hr>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# WORKFLOW
# ──────────────────────────────────────────────────────────────────────────────
st.markdown('<div id="workflow"></div>', unsafe_allow_html=True)
st.subheader("🧭 Workflow Overview")

st.markdown('<div class="workflow">', unsafe_allow_html=True)
cols = st.columns(4, gap="large")

steps = [
    ("1", "Explore", "Upload and preview your dataset. Filter columns, inspect datatypes, and view summaries.", "pages/01_Explore.py"),
    ("2", "Distributions", "Visualize feature distributions, histograms, and outliers to spot trends quickly.", "pages/02_Distributions.py"),
    ("3", "Correlation", "Analyze relationships with correlation matrices/heatmaps to understand feature interplay.", "pages/03_Correlation.py"),
    ("4", "Fairness & Drift", "Run parity checks, group metrics, and dataset drift diagnostics.", "pages/04_Fairness_&_Drift.py"),
]

for (num, title, desc, link), col in zip(steps, cols):
    with col:
        st.markdown(
            f"""
            <div class="card" tabindex="0" data-link="{link}">
              <div class="badge">{num}</div>
              <div class="card-title">{title}</div>
              <div class="card-desc">{desc}</div>
            """,
            unsafe_allow_html=True,
        )
        st.page_link(link, label=f"Go to {title} ➜", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

st.markdown("</div>", unsafe_allow_html=True)
st.markdown("<hr>", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────────────────────
# UPCOMING (xxxx items)
# ──────────────────────────────────────────────────────────────────────────────
st.subheader("🌟 Upcoming Features")
st.write("- xxxx\n- xxxx\n- xxxx\n- xxxx\n- xxxx")


# ──────────────────────────────────────────────────────────────────────────────
# JS — Tilt hover + keyboard shortcuts (no reveal logic)
# ──────────────────────────────────────────────────────────────────────────────
st.markdown("""
<script>
(function(){
  const root = parent.document;
  const cards = Array.from(root.querySelectorAll('.card[data-link]'));

  // Tilt hover (subtle)
  cards.forEach(card=>{
    let raf;
    function tilt(e){
      const r = card.getBoundingClientRect();
      const x = (e.clientX - r.left)/r.width, y = (e.clientY - r.top)/r.height;
      const rotX = (0.5 - y)*6, rotY = (x - 0.5)*6;
      card.style.transform=`translateY(-3px) rotateX(${rotX}deg) rotateY(${rotY}deg)`;
    }
    function reset(){ card.style.transform=''; }
    card.addEventListener('mousemove', e=>{ cancelAnimationFrame(raf); raf=requestAnimationFrame(()=>tilt(e)); });
    card.addEventListener('mouseleave', ()=>{ cancelAnimationFrame(raf); reset(); });
  });

  // Keyboard shortcuts
  let idx=0;
  function focusCard(i){ if(!cards.length) return; idx=(i+cards.length)%cards.length; cards[idx].focus({preventScroll:false}); cards[idx].scrollIntoView({block:'nearest',behavior:'smooth'}); }
  function openCard(i){ const a=cards[i]?.parentElement?.querySelector('.stPageLink a'); if(a) a.click(); }
  focusCard(0);
  root.addEventListener('keydown', e=>{
    if(['INPUT','TEXTAREA'].includes((e.target||{}).tagName)) return;
    if(e.key==='ArrowRight'){e.preventDefault();focusCard(idx+1);}
    if(e.key==='ArrowLeft'){e.preventDefault();focusCard(idx-1);}
    if(e.key==='Enter'){e.preventDefault();openCard(idx);}
    if(['1','2','3','4'].includes(e.key)){e.preventDefault();const n=+e.key-1;if(cards[n])openCard(n);}
  },{passive:false});
})();
</script>
""", unsafe_allow_html=True)
