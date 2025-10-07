import os
import pandas as pd
import streamlit as st
from utils import inject_css, kpi_grid
import plotly.express as px
import duckdb
from bias_metrics import (  # ADD THIS IMPORT
    numeric_bias_metrics_duckdb,
    categorical_bias_metrics_duckdb,
    format_pct,
    severity_badge
)

DATA_PROC = "data/processed"
DUCKDB_PATH = "data/duckdb/eda.duckdb"

inject_css()
st.title("02 · Distributions")

# Cache expensive operations
def get_tables():
    with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
        return con.execute("SHOW TABLES").df()['name'].tolist()

@st.cache_data(ttl=3600)
def get_schema(table_name):
    with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
        return con.execute(f"DESCRIBE SELECT * FROM {table_name} LIMIT 0").df()

@st.cache_data(ttl=3600)
def get_numeric_histogram(table_name, col, bins, sample_size=100000):
    """Get histogram data using DuckDB aggregation"""
    with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
        # Get min/max and total count
        stats = con.execute(f"""
            SELECT 
                MIN(\"{col}\") as min_val,
                MAX(\"{col}\") as max_val,
                COUNT(*) as total_count
            FROM {table_name}
            WHERE \"{col}\" IS NOT NULL
        """).fetchone()
        
        min_val, max_val, total_count = stats
        
        if min_val is None or max_val is None:
            return None, None
        
        # Calculate bin width
        bin_width = (max_val - min_val) / bins
        
        # Create histogram using FLOOR division
        hist_data = con.execute(f"""
            SELECT 
                FLOOR((\"{col}\" - {min_val}) / {bin_width}) as bin_num,
                {min_val} + FLOOR((\"{col}\" - {min_val}) / {bin_width}) * {bin_width} as bin_start,
                COUNT(*) as count
            FROM {table_name}
            WHERE \"{col}\" IS NOT NULL
            GROUP BY bin_num
            ORDER BY bin_num
        """).df()
        
        # Sample for box plot
        sample_data = con.execute(f"""
            SELECT \"{col}\"
            FROM {table_name}
            WHERE \"{col}\" IS NOT NULL
            USING SAMPLE {min(sample_size, total_count)} ROWS
        """).df()
        
        return hist_data, sample_data

@st.cache_data(ttl=3600)
def get_value_counts(table_name, col, top_k):
    """Get value counts - already aggregated"""
    with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
        query = f"""
            SELECT 
                COALESCE(CAST(\"{col}\" AS VARCHAR), '<NA>') as \"{col}\",
                COUNT(*) as count
            FROM {table_name}
            GROUP BY \"{col}\"
            ORDER BY count DESC
            LIMIT {top_k}
        """
        return con.execute(query).df()

# Main app
tables = get_tables()

if not tables:
    st.info("No datasets found. Go to **01 · Explore** and upload a CSV.")
    st.stop()

choice = st.selectbox("Dataset", sorted(tables))

# Get column info
schema_df = get_schema(choice)

# Identify numeric and categorical columns
num_cols = schema_df[schema_df['column_type'].str.contains('INT|DOUBLE|FLOAT|DECIMAL|NUMERIC', case=False, regex=True)]['column_name'].tolist()
cat_cols = [c for c in schema_df['column_name'].tolist() if c not in num_cols]

tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

with tab_num:
    if not num_cols:
        st.caption("No numeric columns detected.")
    else:
        c1, c2, c3 = st.columns([2,1,1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 80, 30)
        with c3:
            sample_size = st.number_input("Sample size", 10000, 500000, 100000, step=10000)
        
        hist_data, sample_data = get_numeric_histogram(choice, col, bins, sample_size)
        
        if hist_data is None:
            st.warning("No data available for this column")
        else:
            # Show box plot from sample
            st.caption(f"Box plot based on {len(sample_data):,} sampled rows")
            fig_box = px.box(sample_data, x=col)
            fig_box.update_layout(height=200)
            st.plotly_chart(fig_box, use_container_width=True)

            # Create histogram from aggregated data
            fig = px.bar(hist_data, x='bin_start', y='count', 
                        labels={'bin_start': col, 'count': 'Frequency'})
            fig.update_traces(marker_line_width=0)
            fig.update_layout(height=380, bargap=0.05, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

            st.divider()
            st.subheader("Bias Check")
            
            nm = numeric_bias_metrics_duckdb(DUCKDB_PATH, choice, col, bins)
            # In the numeric tab, replace the metrics section with:
            if nm is None:
                st.info("No numeric bias metrics available.")
            else:
                kpi_grid({
                    "Max-bin share": f"{format_pct(nm['max_bin_share'])} • {severity_badge(nm['bin_level'])}",
                    "Skewness": f"{nm['skew']:.2f}",
                    "Outlier fraction": f"{format_pct(nm['outlier_frac'])} • {severity_badge(nm['out_level'])}",
                    "Zero share": format_pct(nm['zero_share']),
                    "Missing": format_pct(nm['missing_share']),
                })
                
                with st.expander("📊 Top bins by share", expanded=False):
                    st.dataframe(nm["bins_table"], use_container_width=True, hide_index=True)
                
                st.caption("Heuristics: max-bin ≥25% (mild), ≥40% (severe); outliers ≥10% (mild), ≥20% (severe)")
                
            

with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns detected.")
    else:
        c1, c2 = st.columns([2,1])
        with c1:
            colc = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)
        
        vc = get_value_counts(choice, colc, top_k)
        
        fig = px.bar(vc, x=colc, y="count")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)

        st.divider()
        st.subheader("Bias Check")
        
        cm = categorical_bias_metrics_duckdb(DUCKDB_PATH, choice, colc)
        # In the categorical tab, replace with:
        if cm is None:
            st.info("No categorical bias metrics available.")
        else:
            kpi_grid({
                "Majority class": f"{cm['majority_label']} ({format_pct(cm['majority_share'])}) • {severity_badge(cm['maj_level'])}",
                "Imbalance ratio": f"{cm['imbalance_ratio']:.1f}× • {severity_badge(cm['irr_level'])}",
                "Effective #classes": f"{cm['effective_k']:.2f} / {cm['observed_k']}",
                "Missing": format_pct(cm['missing_share']),
                "Total rows": f"{cm['total']:,}",
            })
            
            with st.expander("Top categories", expanded=False):
                st.dataframe(cm["top_table"], use_container_width=True, hide_index=True)
            
            st.caption("💡 Heuristics: majority share ≥70% (mild), ≥90% (severe); imbalance ratio ≥5× (mild), ≥10× (severe)")
