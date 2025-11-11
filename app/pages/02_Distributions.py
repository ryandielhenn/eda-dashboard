import streamlit as st
import plotly.express as px
import requests
from config import API_BASE

from utils import (
    inject_css,
    kpi_grid,
    dataset_selector,
    format_pct,
    severity_badge,
)


inject_css()
st.title("02 · Distributions")

dataset_choice = dataset_selector()

# Remove ds_ prefix for API calls
dataset_id = dataset_choice.replace("ds_", "")

st.caption(f"📂 Active dataset: `{dataset_choice}`")

# ───────────────────────────────
# Get schema from API
# ───────────────────────────────
try:
    response = requests.get(f"{API_BASE}/datasets/{dataset_id}/schema")
    schema_data = response.json()["schema"]

    # Separate numeric and categorical columns
    num_cols = [
        col["column_name"]
        for col in schema_data
        if any(
            t in col["column_type"].upper()
            for t in ["INT", "DOUBLE", "FLOAT", "DECIMAL", "NUMERIC"]
        )
    ]
    cat_cols = [
        col["column_name"] for col in schema_data if col["column_name"] not in num_cols
    ]

except Exception as e:
    st.error(f"Failed to load schema: {e}")
    st.stop()

tab_num, tab_cat = st.tabs(["Numeric", "Categorical"])

# ───────────────────────────────
# Numeric tab
# ───────────────────────────────
with tab_num:
    if not num_cols:
        st.caption("No numeric columns detected.")
    else:
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            col = st.selectbox("Numeric column", num_cols)
        with c2:
            bins = st.slider("Bins", 5, 80, 30)
        with c3:
            sample_size = st.number_input(
                "Sample size", 10000, 500000, 100000, step=10000
            )

        # Fetch histogram from API
        try:
            response = requests.get(
                f"{API_BASE}/datasets/{dataset_id}/distributions/numeric",
                params={"column": col, "bins": bins, "sample_size": sample_size},
            )

            if response.status_code != 200:
                st.warning("No data available for this column.")
            else:
                data = response.json()
                hist_data = data["histogram"]
                sample_data = data["sample"]

                st.caption(f"Box plot based on {len(sample_data):,} sampled rows")

                # Box plot
                import pandas as pd

                sample_df = pd.DataFrame(sample_data)
                fig_box = px.box(sample_df, x=col)
                fig_box.update_layout(height=200)
                st.plotly_chart(
                    fig_box, config={"responsive": True, "displayModeBar": False}
                )

                # Histogram
                hist_df = pd.DataFrame(hist_data)
                fig = px.bar(
                    hist_df,
                    x="bin_start",
                    y="count",
                    labels={"bin_start": col, "count": "Frequency"},
                )
                fig.update_traces(marker_line_width=0)
                fig.update_layout(height=380, bargap=0.05, showlegend=False)
                st.plotly_chart(
                    fig, config={"responsive": True, "displayModeBar": False}
                )

                st.divider()
                st.subheader("Bias Check")

                # Fetch bias metrics from API
                bias_response = requests.get(
                    f"{API_BASE}/datasets/{dataset_id}/bias/numeric",
                    params={"column": col, "bins": bins},
                )

                if bias_response.status_code != 200:
                    st.info("No numeric bias metrics available.")
                else:
                    nm = bias_response.json()["metrics"]

                    kpi_grid(
                        {
                            "Max-bin share": f"{format_pct(nm['max_bin_share'])} • {severity_badge(nm['bin_level'])}",
                            "Skewness": f"{nm['skew']:.2f}",
                            "Outlier fraction": f"{format_pct(nm['outlier_frac'])} • {severity_badge(nm['out_level'])}",
                            "Zero share": format_pct(nm["zero_share"]),
                            "Missing": format_pct(nm["missing_share"]),
                        }
                    )
                    with st.expander("📊 Top bins by share", expanded=False):
                        bins_df = pd.DataFrame(nm["bins_table"])
                        st.dataframe(bins_df, width="stretch", hide_index=True)
                    st.caption(
                        "Heuristics: max-bin ≥25% (mild), ≥40% (severe); outliers ≥10% (mild), ≥20% (severe)"
                    )

        except Exception as e:
            st.error(f"Failed to fetch data: {e}")

# ───────────────────────────────
# Categorical tab
# ───────────────────────────────
with tab_cat:
    if not cat_cols:
        st.caption("No categorical columns detected.")
    else:
        c1, c2 = st.columns([2, 1])
        with c1:
            colc = st.selectbox("Categorical column", cat_cols)
        with c2:
            top_k = st.slider("Show top K categories", 5, 50, 20)

        # Fetch value counts from API
        try:
            response = requests.get(
                f"{API_BASE}/datasets/{dataset_id}/distributions/categorical",
                params={"column": colc, "top_k": top_k},
            )

            vc_data = response.json()["value_counts"]
            import pandas as pd

            vc = pd.DataFrame(vc_data)

            fig = px.bar(vc, x=colc, y="count")
            fig.update_layout(height=360)
            st.plotly_chart(fig, config={"responsive": True, "displayModeBar": False})

            st.divider()
            st.subheader("Bias Check")

            # Fetch categorical bias metrics
            bias_response = requests.get(
                f"{API_BASE}/datasets/{dataset_id}/bias/categorical",
                params={"column": colc},
            )

            if bias_response.status_code != 200:
                st.info("No categorical bias metrics available.")
            else:
                cm = bias_response.json()["metrics"]

                kpi_grid(
                    {
                        "Majority class": f"{cm['majority_label']} ({format_pct(cm['majority_share'])}) • {severity_badge(cm['maj_level'])}",
                        "Imbalance ratio": f"{cm['imbalance_ratio']:.1f}× • {severity_badge(cm['irr_level'])}",
                        "Effective #classes": f"{cm['effective_k']:.2f} / {cm['observed_k']}",
                        "Missing": format_pct(cm["missing_share"]),
                        "Total rows": f"{cm['total']:,}",
                    }
                )
                with st.expander("Top categories", expanded=False):
                    top_df = pd.DataFrame(cm["top_table"])
                    st.dataframe(top_df, width="stretch", hide_index=True)
                st.caption(
                    "💡 Heuristics: majority share ≥70% (mild), ≥90% (severe); "
                    "imbalance ratio ≥5× (mild), ≥10× (severe)"
                )

        except Exception as e:
            st.error(f"Failed to fetch data: {e}")
