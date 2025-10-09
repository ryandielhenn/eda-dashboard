import numpy as np
import pandas as pd
import duckdb

def format_pct(x: float) -> str:
    try:
        return f"{x*100:.1f}%"
    except Exception:
        return "—"

def severity_badge(level: str) -> str:
    return {"ok": "🟢 OK", "info": "🔵 Info", "mild": "🟠 Mild", "severe": "🔴 Severe"}.get(level, "🟢 OK")

def numeric_bias_metrics_duckdb(duckdb_path: str, table_name: str, col: str, bins: int) -> dict | None:
    """Compute numeric bias metrics using DuckDB"""
    with duckdb.connect(duckdb_path, read_only=True) as con:
        # Get all numeric stats in one query
        stats_query = f"""
            WITH stats AS (
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT("{col}") as non_null_count,
                    AVG("{col}") as mean_val,
                    STDDEV("{col}") as std_val,
                    SKEWNESS("{col}") as skew_val,
                    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}") as q1,
                    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}") as q3,
                    MIN("{col}") as min_val,
                    MAX("{col}") as max_val,
                    SUM(CASE WHEN "{col}" = 0 THEN 1 ELSE 0 END) as zero_count,
                    SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) as null_count
                FROM {table_name}
            )
            SELECT * FROM stats
        """
        
        stats = con.execute(stats_query).fetchone()
        
        if not stats or stats[0] == 0:
            return None
        
        (total_rows, non_null_count, mean_val, std_val, skew_val, 
         q1, q3, min_val, max_val, zero_count, null_count) = stats
        
        if min_val is None or max_val is None:
            return None
        
        # Calculate outliers using IQR
        iqr = q3 - q1
        if iqr > 0:
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outlier_query = f"""
                SELECT COUNT(*) as outlier_count
                FROM {table_name}
                WHERE "{col}" IS NOT NULL 
                  AND ("{col}" < {lower_bound} OR "{col}" > {upper_bound})
            """
            outlier_count = con.execute(outlier_query).fetchone()[0]
            outlier_frac = outlier_count / non_null_count if non_null_count > 0 else 0.0
        else:
            outlier_frac = 0.0
        
        # Get bin distribution using FLOOR (same as your existing histogram code)
        bin_width = (max_val - min_val) / bins
        
        bin_query = f"""
            WITH binned AS (
                SELECT 
                    FLOOR(("{col}" - {min_val}) / {bin_width}) as bin_num,
                    {min_val} + FLOOR(("{col}" - {min_val}) / {bin_width}) * {bin_width} as bin_start,
                    COUNT(*) as count
                FROM {table_name}
                WHERE "{col}" IS NOT NULL
                GROUP BY bin_num
            )
            SELECT 
                bin_start,
                count,
                CAST(count AS DOUBLE) / {non_null_count} as share
            FROM binned
            ORDER BY share DESC
            LIMIT 10
        """
        
        bins_df = con.execute(bin_query).df()
        max_bin_share = float(bins_df['share'].max()) if not bins_df.empty else 0.0
        
        # Format bins table
        bins_df['bin'] = bins_df['bin_start'].apply(lambda x: f"[{x:.2f}, {x + bin_width:.2f})")
        bins_table = bins_df[['bin', 'share']].copy()
        
        # Calculate shares
        zero_share = zero_count / total_rows
        missing_share = null_count / total_rows
        
        # Determine severity levels
        if max_bin_share >= 0.40:
            bin_level = "severe"
        elif max_bin_share >= 0.25:
            bin_level = "mild"
        elif max_bin_share >= 0.20:
            bin_level = "info"
        else:
            bin_level = "ok"
        
        if outlier_frac >= 0.20:
            out_level = "severe"
        elif outlier_frac >= 0.10:
            out_level = "mild"
        elif outlier_frac >= 0.05:
            out_level = "info"
        else:
            out_level = "ok"
        
        return {
            "max_bin_share": max_bin_share,
            "bin_level": bin_level,
            "skew": float(skew_val) if skew_val else 0.0,
            "outlier_frac": outlier_frac,
            "out_level": out_level,
            "zero_share": zero_share,
            "missing_share": missing_share,
            "bins_table": bins_table,
        }

def categorical_bias_metrics_duckdb(duckdb_path: str, table_name: str, col: str) -> dict | None:
    """Compute categorical bias metrics using DuckDB"""
    with duckdb.connect(duckdb_path, read_only=True) as con:
        # Get value counts with NULL handling
        query = f"""
            WITH value_counts AS (
                SELECT 
                    COALESCE(CAST("{col}" AS VARCHAR), '<NA>') as value,
                    COUNT(*) as count
                FROM {table_name}
                GROUP BY "{col}"
            ),
            totals AS (
                SELECT 
                    COUNT(*) as total_rows,
                    SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) as null_count
                FROM {table_name}
            )
            SELECT 
                v.value,
                v.count,
                CAST(v.count AS DOUBLE) / t.total_rows as share,
                t.total_rows,
                t.null_count
            FROM value_counts v
            CROSS JOIN totals t
            ORDER BY v.count DESC
            LIMIT 20
        """
        
        result_df = con.execute(query).df()
        
        if result_df.empty:
            return None
        
        total_rows = int(result_df['total_rows'].iloc[0])
        null_count = int(result_df['null_count'].iloc[0])
        
        # Extract shares
        shares = result_df['share'].values
        majority_label = str(result_df['value'].iloc[0])
        majority_share = float(shares[0])
        minority_share = float(shares[-1])
        
        imbalance_ratio = majority_share / minority_share if minority_share > 0 else float('inf')
        missing_share = null_count / total_rows if total_rows > 0 else 0.0
        
        # Calculate entropy (using natural log)
        entropy_query = f"""
            WITH value_shares AS (
                SELECT 
                    CAST(COUNT(*) AS DOUBLE) / (SELECT COUNT(*) FROM {table_name}) as p
                FROM {table_name}
                GROUP BY "{col}"
                HAVING p > 0
            )
            SELECT 
                SUM(-p * LN(p)) as entropy,
                COUNT(*) as observed_k
            FROM value_shares
        """
        
        entropy_result = con.execute(entropy_query).fetchone()
        entropy = float(entropy_result[0]) if entropy_result[0] else 0.0
        observed_k = int(entropy_result[1])
        effective_k = float(np.exp(entropy))
        
        # Determine severity levels
        if majority_share >= 0.90:
            maj_level = "severe"
        elif majority_share >= 0.70:
            maj_level = "mild"
        elif majority_share >= 0.60:
            maj_level = "info"
        else:
            maj_level = "ok"
        
        if imbalance_ratio >= 10:
            irr_level = "severe"
        elif imbalance_ratio >= 5:
            irr_level = "mild"
        elif imbalance_ratio >= 3:
            irr_level = "info"
        else:
            irr_level = "ok"
        
        # Format top table
        top_table = result_df[['value', 'count', 'share']].copy()
        
        return {
            "majority_label": majority_label,
            "majority_share": majority_share,
            "minority_share": minority_share,
            "imbalance_ratio": imbalance_ratio,
            "entropy": entropy,
            "effective_k": effective_k,
            "observed_k": observed_k,
            "missing_share": missing_share,
            "maj_level": maj_level,
            "irr_level": irr_level,
            "top_table": top_table,
            "total": total_rows,
        }
