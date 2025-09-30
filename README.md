# EDA Dashboard

A batch-first, stream-ready **Exploratory Data Analysis (EDA) dashboard** for exploring datasets, detecting bias, and generating reproducible insights.  

The dashboard is designed to:  
- Handle multiple data formats (CSV, Parquet, logs, sensors).  
- Provide automated profiling, visualization, and fairness/drift checks.  
- Support batch workflows for stability, with hooks for optional “live” updates in the future.  

---

## Interactive Visualization

Interactive visualization is a core feature of the dashboard. The system uses [Streamlit](https://streamlit.io/) to provide a responsive web-based interface and will integrate Plotly for richer, interactive charts.  

Planned capabilities include:  
- **Exploration**: Upload datasets and interactively view tables, feature distributions, and correlations with adjustable parameters and filters.  
- **Distributions**: Histograms, KDE plots, and bar charts with interactive zoom and pan.  
- **Correlation Heatmaps**: Hoverable heatmaps and pair plots for exploring feature relationships.  
- **Fairness & Drift**: Dynamic selection of reference vs. current datasets and sensitive attributes, with metrics and plots updated in real time.  
- **Extensibility**: Designed to support advanced charting features such as tooltips, drill-downs, and export options.  

---

## Repository Structure

```text
eda-dashboard/
├─ app/                      # Streamlit UI
│  ├─ pages/                 # Multi-page dashboard (organized by topic)
│  │  ├─ 01_Explore.py            # Upload datasets, preview raw data
│  │  ├─ 02_Distributions.py      # Feature distributions, histograms, KDE plots
│  │  ├─ 03_Correlation.py        # Correlation heatmaps, pairwise plots
│  │  └─ 04_Fairness_&_Drift.py   # Bias detection, drift analysis reports
│  └─ streamlit_app.py        # Main entry point for the dashboard
│
├─ api/                      # Thin FastAPI backend (optional, for long jobs/caching)
│  └─ main.py
│
├─ analytics/                # Profiling, drift, and fairness logic
│  ├─ profiling.py            # Summary stats, missingness, skew/kurtosis
│  ├─ drift.py                # Drift detection (PSI, KL divergence, etc.)
│  └─ fairness.py             # Fairness metrics (e.g., selection rate by group)
│
├─ storage/                  # Data and metrics persistence
│  ├─ duck.py                 # DuckDB interface for cached metrics
│  └─ files.py                # File helpers for Parquet/CSV IO
│
├─ jobs/                     # Optional background jobs
│  ├─ microbatch.py           # Periodic profiling (simulates live updates)
│  └─ watch.py                # File watcher to trigger profiling on new data
│
├─ data/                     # Local data storage
│  ├─ raw/                    # Uploaded files
│  ├─ processed/              # Standardized Parquet copies
│  └─ duckdb/                 # DuckDB database file
│
├─ tests/                    # Unit tests
│  ├─ test_profiling.py
│  └─ test_storage.py
│
├─ notebooks/                # Prototyping in Jupyter
│  └─ prototype.ipynb
│
├─ .env.example              # Example environment variables (e.g., DUCKDB_PATH)
├─ .gitignore                # Ignore rules (venv, data, cache, etc.)
├─ Dockerfile                # Docker image definition
├─ docker-compose.yml        # Compose config (Streamlit + optional FastAPI)
├─ Makefile                  # Shortcuts for setup, run, lint, test
├─ pyproject.toml            # Project dependencies (or requirements.txt)
└─ README.md                 # Project overview (this file)
````

---

## Architecture Diagram

```text
 ┌───────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌───────────────┐
 │   Raw Data    │ ──▶ │   Parquet   │ ──▶ │   DuckDB    │ ──▶ │ Analytics Layer  │ ──▶ │ Streamlit UI  │
 │ (CSV, logs,   │     │ (columnar   │     │ (fast SQL,  │     │ Profiling, Drift │     │ Dashboards &  │
 │  sensors)     │     │  storage)   │     │  metrics)   │     │ Fairness checks  │     │ Visuals       │
 └───────────────┘     └─────────────┘     └─────────────┘     └──────────────────┘     └───────────────┘
```

---

## Roadmap

### MVP (Batch-first)

* Upload dataset (CSV/Parquet)
* Automated profiling (summary stats, distributions, correlations)
* Interactive visualizations (via Streamlit + Plotly)
* Bias/fairness checks with [Evidently](https://github.com/evidentlyai/evidently) & [Fairlearn](https://github.com/fairlearn/fairlearn)
* Store metrics in DuckDB
* Streamlit dashboard with charts and reports

### Stretch Goals

* Background jobs for “live” refresh (microbatch or file-watcher)
* Thin FastAPI service for cached results and long-running tasks
* Export reproducible reports

### Running application and required packages

#### Requirements

- Python 3.10+  
- Recommended: a virtual environment (e.g., `venv` or `conda`)


Install dependencies:

```bash
pip install -r requirements.txt
```

Run streamlit application:
```bash
streamlit run app/Dashboard.py
```
