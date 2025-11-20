import streamlit as st
import streamlit.components.v1 as components

# Optional util
try:
    from utils import inject_css
except Exception:

    def inject_css():
        pass


st.set_page_config(page_title="EDA Dashboard", layout="wide", page_icon="📊")
inject_css()

# Custom styling for Streamlit container
st.markdown(
    """
<style>
/* Remove all extra backgrounds and ensure proper theme colors */
.stApp {
    background: transparent !important;
}
.main, .block-container {
    background: transparent !important;
    padding: 0 !important;
}
iframe {
    border: none !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# ──────────────────────────────────────────────────────────────────────────────
# Modern Dashboard HTML
# ──────────────────────────────────────────────────────────────────────────────
html_content = """
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

:root {
  color-scheme: light dark;
}

html, body {
  width: 100%;
  height: 100%;
  font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 
               'Helvetica Neue', Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
}

/* Light theme (default) */
body {
  --bg-primary: #ffffff;
  --bg-secondary: #f8fafc;
  --text-primary: #0f172a;
  --text-secondary: #64748b;
  --text-tertiary: #94a3b8;
  --border-color: #e2e8f0;
  --card-bg: #ffffff;
  --card-hover: #f8fafc;
  --accent: #3b82f6;
  --accent-hover: #2563eb;
  --shadow: rgba(0, 0, 0, 0.1);
  --shadow-hover: rgba(59, 130, 246, 0.15);
}

/* Dark theme */
@media (prefers-color-scheme: dark) {
  body {
    --bg-primary: #0e1117;
    --bg-secondary: #262730;
    --text-primary: #f1f5f9;
    --text-secondary: #cbd5e1;
    --text-tertiary: #94a3b8;
    --border-color: #3d3d4d;
    --card-bg: #262730;
    --card-hover: #2e2e3e;
    --accent: #60a5fa;
    --accent-hover: #3b82f6;
    --shadow: rgba(0, 0, 0, 0.3);
    --shadow-hover: rgba(96, 165, 250, 0.2);
  }
}

body {
  background: var(--bg-primary);
  color: var(--text-primary);
  line-height: 1.6;
}

.container {
  max-width: 1000px;
  margin: 0 auto;
  padding: 3rem 2rem;
}

/* Header Section */
.header {
  text-align: center;
  margin-bottom: 4rem;
  padding-bottom: 2rem;
  border-bottom: 1px solid var(--border-color);
}

.header h1 {
  font-size: 3rem;
  font-weight: 700;
  margin-bottom: 1rem;
  color: var(--text-primary);
  letter-spacing: -0.02em;
}

.header p {
  font-size: 1.25rem;
  color: var(--text-secondary);
  max-width: 700px;
  margin: 0 auto;
  line-height: 1.7;
}

/* Section Headers */
.section-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin: 3rem 0 1.5rem 0;
}

.section-header h2 {
  font-size: 1.75rem;
  font-weight: 600;
  color: var(--text-primary);
}

.section-header .badge {
  font-size: 0.75rem;
  padding: 0.25rem 0.75rem;
  background: var(--accent);
  color: white;
  border-radius: 1rem;
  font-weight: 600;
}

/* Workflow Cards */
.workflow-grid {
  display: grid;
  gap: 1rem;
  margin-bottom: 2rem;
}

.workflow-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 1rem;
  padding: 1.75rem;
  cursor: pointer;
  transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1);
  text-decoration: none;
  display: block;
  position: relative;
  overflow: hidden;
}

.workflow-card::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  width: 4px;
  height: 100%;
  background: var(--accent);
  transform: scaleY(0);
  transition: transform 0.2s cubic-bezier(0.4, 0, 0.2, 1);
}

.workflow-card:hover {
  transform: translateX(4px);
  background: var(--card-hover);
  border-color: var(--accent);
  box-shadow: 0 8px 16px var(--shadow-hover);
}

.workflow-card:hover::before {
  transform: scaleY(1);
}

.card-header {
  margin-bottom: 0.75rem;
}

.card-title {
  font-size: 1.375rem;
  font-weight: 600;
  color: var(--text-primary);
  letter-spacing: -0.01em;
}

.card-description {
  font-size: 1rem;
  color: var(--text-secondary);
  line-height: 1.7;
}

/* Feature Cards */
.feature-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 1.25rem;
  margin: 2rem 0;
}

.feature-card {
  background: var(--card-bg);
  border: 1px solid var(--border-color);
  border-radius: 1rem;
  padding: 1.5rem;
  transition: all 0.2s ease;
}

.feature-card:hover {
  border-color: var(--accent);
  box-shadow: 0 4px 12px var(--shadow-hover);
}

.feature-title {
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 0.5rem;
}

.feature-description {
  font-size: 0.9375rem;
  color: var(--text-secondary);
  line-height: 1.6;
}

/* Future Work Section */
.future-work {
  background: linear-gradient(135deg, var(--bg-secondary) 0%, var(--card-bg) 100%);
  border: 1px solid var(--border-color);
  border-radius: 1rem;
  padding: 2rem;
  margin: 3rem 0;
}

.future-work h3 {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--text-primary);
  margin-bottom: 1rem;
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.future-work ul {
  list-style: none;
  padding: 0;
}

.future-work li {
  color: var(--text-secondary);
  padding-left: 1.75rem;
  margin-bottom: 0.75rem;
  position: relative;
  line-height: 1.7;
}

.future-work li::before {
  content: '→';
  position: absolute;
  left: 0;
  color: var(--accent);
  font-weight: bold;
}

/* Animations */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.workflow-card {
  animation: fadeIn 0.4s ease forwards;
}

/* Responsive Design */
@media (max-width: 768px) {
  .container {
    padding: 2rem 1rem;
  }
  
  .header h1 {
    font-size: 2rem;
  }
  
  .header p {
    font-size: 1rem;
  }
}
</style>
</head>
<body>
<div class="container">
  <!-- Header -->
  <div class="header">
    <h1>EDA Dashboard</h1>
    <p>A comprehensive platform for exploring datasets, visualizing distributions, analyzing correlations, and running fairness & drift diagnostics.</p>
  </div>

  <!-- Workflow Section -->
  <div class="section-header">
    <h2>Analysis Tools</h2>
  </div>

  <div class="workflow-grid">
    <div class="workflow-card" onclick="navigateTo('Explore')">
      <div class="card-header">
        <div class="card-title">Explore</div>
      </div>
      <div class="card-description">
        Upload and preview your dataset. Filter columns, inspect datatypes, and view comprehensive statistical summaries.
      </div>
    </div>

    <div class="workflow-card" onclick="navigateTo('Distributions')">
      <div class="card-header">
        <div class="card-title">Distributions</div>
      </div>
      <div class="card-description">
        Visualize feature distributions with histograms and density plots. Identify outliers and spot trends at a glance.
      </div>
    </div>

    <div class="workflow-card" onclick="navigateTo('Correlation')">
      <div class="card-header">
        <div class="card-title">Correlation</div>
      </div>
      <div class="card-description">
        Analyze relationships between features using correlation matrices and interactive heatmaps to understand feature interplay.
      </div>
    </div>

    <div class="workflow-card" onclick="navigateTo('Fairness_&_Drift')">
      <div class="card-header">
        <div class="card-title">Fairness & Drift</div>
      </div>
      <div class="card-description">
        Run parity checks across demographic groups, compute fairness metrics, and perform dataset drift diagnostics.
      </div>
    </div>
  </div>

</div>

<script>
function navigateTo(page) {
  try {
    const parent = window.parent.document;
    const sidebarLinks = parent.querySelectorAll('[data-testid="stSidebarNav"] a');
    
    for (const link of sidebarLinks) {
      const href = link.getAttribute('href') || '';
      if (href.includes(page)) {
        link.click();
        break;
      }
    }
  } catch (e) {
    console.warn('Navigation error:', e);
  }
}
</script>
</body>
</html>
"""

# Render the custom HTML
components.html(html_content, height=1800, scrolling=True)
