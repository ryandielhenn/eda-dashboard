# EDA Dashboard

A batch-first, stream-ready **Exploratory Data Analysis (EDA) dashboard** for exploring datasets, detecting bias, and generating reproducible insights.  

The dashboard is designed to:  
- Handle multiple data formats (CSV, CSV.GZ, Parquet, ZIP).  
- Provide automated profiling, visualization, and fairness/drift checks.  
- Support batch workflows for stability, with hooks for optional “live” updates in the future.  

### Running application and required packages

#### Requirements

- Python 3.10+  
- Recommended: a virtual environment (e.g., `venv` or `conda`)


Install dependencies:

```bash
pip install -r requirements.txt
```

**Option 1: Streamlit only (no api)**

Run streamlit application:
```bash
streamlit run app/Dashboard.py
```

**Option 2: With FastAPI Backend (New - frontend not integrated to communicate)**

If you want to test the new REST API backend:

1. **Start the FastAPI backend** (in one terminal):
```bash
   uvicorn api.main:app --reload --host 127.0.0.1 --port 8000
```
   - API will be available at http://127.0.0.1:8000
   - Interactive docs at http://127.0.0.1:8000/docs

2. **Start Streamlit** (in another terminal):
```bash
   streamlit run app/Dashboard.py
```
   - Frontend will be at http://localhost:8501
   - Currently still uses direct DuckDB (not integrated with API yet)

**Note:** The FastAPI backend is functional and can be tested via the `/docs` page, but the Streamlit frontend is not yet integrated to call the API. Both systems work independently and share the same DuckDB database file.

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

## Architecture Diagram
![Architecure](diagrams/diagram.png)

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


