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
