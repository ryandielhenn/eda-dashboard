# storage/duck.py
import duckdb
import pathlib
import atexit
from threading import Lock

DB = pathlib.Path("data/duckdb/eda.duckdb")
DB.parent.mkdir(parents=True, exist_ok=True)

_conn = None
_lock = Lock()

def connect():
    """Return the shared DuckDB connection (thread-safe)."""
    global _conn
    with _lock:
        if _conn is None:
            _conn = duckdb.connect(str(DB))
    return _conn

@atexit.register
def close_connection():
    """Close DuckDB connection on exit."""
    global _conn
    with _lock:
        if _conn is not None:
            _conn.close()
            _conn = None

def init_db():
    con = connect()
    with _lock:
        con.execute("""
            CREATE TABLE IF NOT EXISTS datasets (
                dataset_id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                n_rows BIGINT,
                n_cols INTEGER,
                last_ingested TIMESTAMP DEFAULT now()
            );
        """)

def table_name(dataset_id: str) -> str:
    return "ds_" + "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in dataset_id)

def ingest_parquet(parquet_path: str, dataset_id: str):
    """Ingest parquet with thread safety."""
    init_db()
    con = connect()
    tbl = table_name(dataset_id)
    
    with _lock:
        con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{parquet_path}')")
        n_rows = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        n_cols = len(con.execute(f"SELECT * FROM {tbl} LIMIT 0").description or [])
        con.execute("""
            INSERT INTO datasets(dataset_id, path, n_rows, n_cols, last_ingested)
            VALUES (?, ?, ?, ?, now())
            ON CONFLICT(dataset_id) DO UPDATE SET
                path = excluded.path,
                n_rows = excluded.n_rows,
                n_cols = excluded.n_cols,
                last_ingested = now();
        """, [dataset_id, parquet_path, n_rows, n_cols])
    
    return tbl, n_rows, n_cols

def ingest_csv(csv_path: str, dataset_id: str):
    """Ingest CSV directly with thread safety."""
    init_db()
    con = connect()
    tbl = table_name(dataset_id)
    
    with _lock:
        con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_csv_auto('{csv_path}')")
        n_rows = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        n_cols = len(con.execute(f"SELECT * FROM {tbl} LIMIT 0").description or [])
        con.execute("""
            INSERT INTO datasets(dataset_id, path, n_rows, n_cols, last_ingested)
            VALUES (?, ?, ?, ?, now())
            ON CONFLICT(dataset_id) DO UPDATE SET
                path = excluded.path,
                n_rows = excluded.n_rows,
                n_cols = excluded.n_cols,
                last_ingested = now();
        """, [dataset_id, csv_path, n_rows, n_cols])
    
    return tbl, n_rows, n_cols

def list_datasets():
    """List datasets using shared connection."""
    init_db()
    con = connect()
    with _lock:
        rows = con.execute("""
            SELECT dataset_id, path, n_rows, n_cols, last_ingested
            FROM datasets
            ORDER BY dataset_id
        """).fetchall()
    return rows

def load_dataset(dataset_id: str):
    """Safely load a dataset by ID."""
    tbl = table_name(dataset_id)
    con = connect()
    
    try:
        with _lock:
            df = con.execute(f"SELECT * FROM {tbl}").df()
        return df
    except Exception as e:
        raise ValueError(f"Failed to load dataset '{dataset_id}': {e}")

def sql(q: str):
    """Execute SQL query with locking."""
    con = connect()
    with _lock:
        res = con.execute(q)
        cols = [d[0] for d in (res.description or [])]
        rows = res.fetchall()
    return cols, rows

# ───────────────────────────────
# Cached helpers distributions
# ───────────────────────────────
def load_table(table_name):
    """Load entire table as DataFrame."""
    con = connect()
    with _lock:
        return con.execute(f"SELECT * FROM {table_name}").df()

def get_tables():
    con = connect()
    return [t[0] for t in con.execute("SHOW TABLES").fetchall() if t[0] != "datasets"]

def get_schema(table_name):
    con = connect()
    return con.execute(f"DESCRIBE SELECT * FROM {table_name} LIMIT 0").df()

def get_numeric_histogram(table_name, col, bins, sample_size=100000):
    """Get histogram + sample data for numeric columns"""
    con = connect()
    stats = con.execute(f"""
        SELECT 
            MIN("{col}") AS min_val,
            MAX("{col}") AS max_val,
            COUNT(*) AS total_count
        FROM {table_name}
        WHERE "{col}" IS NOT NULL
    """).fetchone()
    
    min_val, max_val, total_count = stats
    if min_val is None or max_val is None or bins <= 0:
        return None, None
    
    bin_width = (max_val - min_val) / bins if bins else 0
    if bin_width == 0:
        return None, None
    
    hist_data = con.execute(f"""
        SELECT 
            FLOOR(("{col}" - {min_val}) / {bin_width}) AS bin_num,
            {min_val} + FLOOR(("{col}" - {min_val}) / {bin_width}) * {bin_width} AS bin_start,
            COUNT(*) AS count
        FROM {table_name}
        WHERE "{col}" IS NOT NULL
        GROUP BY bin_num
        ORDER BY bin_num
    """).df()
    
    sample_data = con.execute(f"""
        SELECT "{col}"
        FROM {table_name}
        WHERE "{col}" IS NOT NULL
        USING SAMPLE {min(sample_size, total_count)} ROWS
    """).df()
    
    return hist_data, sample_data

def get_value_counts(table_name, col, top_k):
    """Get categorical value counts"""
    con = connect()
    query = f"""
        SELECT 
            COALESCE(CAST("{col}" AS VARCHAR), '<NA>') AS "{col}",
            COUNT(*) AS count
        FROM {table_name}
        GROUP BY "{col}"
        ORDER BY count DESC
        LIMIT {top_k}
    """
    return con.execute(query).df()

def get_numeric_bias_metrics(table_name: str, col: str, bins: int) -> dict | None:
    """Compute numeric bias metrics - all queries locked together."""
    con = connect()
    
    with _lock:  # ← One lock for the entire operation
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
        
        # Get bin distribution
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
    
    # Processing outside lock (no DB access)
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

def get_categorical_bias_metrics(table_name: str, col: str) -> dict | None:
    """Compute categorical bias metrics - all queries locked together."""
    import numpy as np
    con = connect()
    
    with _lock:  # ← One lock for all queries
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
        
        # Calculate entropy
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
    
    # Processing outside lock (no DB access)
    shares = result_df['share'].values
    majority_label = str(result_df['value'].iloc[0])
    majority_share = float(shares[0])
    minority_share = float(shares[-1])
    
    imbalance_ratio = majority_share / minority_share if minority_share > 0 else float('inf')
    missing_share = null_count / total_rows if total_rows > 0 else 0.0
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
