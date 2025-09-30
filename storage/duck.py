'''DuckDB interface for storing and querying metrics.'''
# storage/duck.py
import duckdb, pathlib

DB = pathlib.Path("data/duckdb/eda.duckdb")
DB.parent.mkdir(parents=True, exist_ok=True)

def connect(read_only=False):
    return duckdb.connect(str(DB), read_only=read_only)

def init_db():
    con = connect()
    con.execute("""
      CREATE TABLE IF NOT EXISTS datasets(
        dataset_id TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        n_rows BIGINT,
        n_cols INTEGER,
        last_ingested TIMESTAMP DEFAULT now()
      );
    """)
    con.close()

def table_name(dataset_id: str) -> str:
    return "ds_" + "".join(ch if ch.isalnum() or ch=="_" else "_" for ch in dataset_id)

def ingest_parquet(parquet_path: str, dataset_id: str):
    init_db()
    con = connect()
    tbl = table_name(dataset_id)
    con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_parquet('{parquet_path}');")
    row = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
    n_rows = row[0] if row else 0
    n_cols = len(con.execute(f"SELECT * FROM {tbl} LIMIT 0").description or [])
    con.execute("""
      INSERT INTO datasets(dataset_id, path, n_rows, n_cols, last_ingested)
      VALUES (?, ?, ?, ?, now())
      ON CONFLICT(dataset_id) DO UPDATE SET
        path=excluded.path, n_rows=excluded.n_rows, n_cols=excluded.n_cols, last_ingested=now();
    """, [dataset_id, parquet_path, n_rows, n_cols])
    con.close()
    return tbl, n_rows, n_cols

def list_datasets():
    init_db()
    con = connect(True)
    rows = con.execute("""
      SELECT dataset_id, path, n_rows, n_cols, last_ingested
      FROM datasets ORDER BY dataset_id
    """).fetchall()
    con.close()
    return rows

def sql(q: str):
    con = connect(True)
    try:
        res = con.execute(q)
        cols = [d[0] for d in (res.description or [])]
        rows = res.fetchall()
        return cols, rows
    finally:
        con.close()

