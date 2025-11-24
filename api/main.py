"""
FastAPI backend for EDA Dashboard
Handles file uploads, DuckDB operations, and data analysis
"""

from analytics.drift import compute_psi_table
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
import numpy as np
import os
from pathlib import Path

try:
    from .routers import zip as zip_router
except ImportError:  # pragma: no cover - fallback when running as script
    from routers import zip as zip_router
from storage.duck import (
    ingest_file,
    list_datasets,
    get_tables,
    get_schema,
    get_numeric_histogram,
    get_value_counts,
    get_numeric_bias_metrics,
    get_categorical_bias_metrics,
    sql,
    connect,
)

app = FastAPI(title="EDA Dashboard API", version="1.0.0")
app.include_router(zip_router.router)

# CORS - Allow Streamlit to talk to FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your Streamlit URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directory
DATA_PROC = Path("data/processed")
DATA_PROC.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────────────────────


def sanitize_id(name: str) -> str:
    """Sanitize dataset name for use as table identifier"""
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


def save_df_as_parquet(df: pd.DataFrame, basename: str) -> str:
    """Save DataFrame as Parquet file"""
    path = DATA_PROC / f"{basename}.parquet"
    df.to_parquet(path, index=False)
    return str(path)


# ────────────────────────────────────────────────────────────────────────────────
# Request/Response Models
# ────────────────────────────────────────────────────────────────────────────────


class QueryRequest(BaseModel):
    query: str


class DatasetInfo(BaseModel):
    dataset_id: str
    table_name: str
    path: str
    n_rows: int
    n_cols: int
    last_ingested: str


# ────────────────────────────────────────────────────────────────────────────────
# Health Check
# ────────────────────────────────────────────────────────────────────────────────


@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "EDA Dashboard API is running"}


@app.get("/health")
def health():
    """Detailed health check"""
    try:
        # Test DuckDB connection
        connect()
        tables = get_tables()
        return {
            "status": "healthy",
            "database": "connected",
            "tables_count": len(tables),
        }
    except Exception as e:
        return JSONResponse(
            status_code=503, content={"status": "unhealthy", "error": str(e)}
        )


# ────────────────────────────────────────────────────────────────────────────────
# Dataset Management
# ────────────────────────────────────────────────────────────────────────────────


@app.post("/upload")
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload CSV or ZIP file containing CSV
    Returns the ingested dataset information
    """
    try:
        # Validate file type
        if not file.filename.lower().endswith((".csv", ".parquet")):
            raise HTTPException(
                status_code=400, detail="Only CSV and ZIP files are supported"
            )

        # Generate dataset ID
        dataset_id = sanitize_id(os.path.splitext(file.filename)[0])
        filename = file.filename.lower()

        file_path = DATA_PROC / f"{dataset_id}{Path(filename).suffix}"

        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        # Ingest CSV directly into DuckDB
        table_name, n_rows, n_cols = ingest_file(str(file_path), dataset_id)

        return {
            "success": True,
            "dataset_id": dataset_id,
            "table_name": table_name,
            "path": str(file_path),
            "n_rows": n_rows,
            "n_cols": n_cols,
            "message": f"Successfully ingested {file.filename}",
        }

    except pd.errors.ParserError as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


@app.get("/datasets")
def get_datasets():
    """List all datasets in DuckDB"""
    try:
        datasets = list_datasets()
        return {
            "success": True,
            "datasets": [
                {
                    "dataset_id": row[0],
                    "path": row[1],
                    "n_rows": row[2],
                    "n_cols": row[3],
                    "last_ingested": str(row[4]),
                }
                for row in datasets
            ],
        }
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to list datasets: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/preview")
def preview_dataset(dataset_id: str, limit: int = Query(default=25, ge=1, le=500)):
    """Preview first N rows of a dataset"""
    try:
        table_name = f"ds_{dataset_id}"
        cols, rows = sql(f"SELECT * FROM {table_name} LIMIT {limit}")

        return {
            "success": True,
            "columns": cols,
            "data": rows,
            "rows_returned": len(rows),
        }
    except Exception as e:
        raise HTTPException(
            status_code=404, detail=f"Dataset not found or query failed: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/schema")
def get_dataset_schema(dataset_id: str):
    """Get schema information for a dataset"""
    try:
        table_name = f"ds_{dataset_id}"
        schema_df = get_schema(table_name)

        return {"success": True, "schema": schema_df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to get schema: {str(e)}")


# ────────────────────────────────────────────────────────────────────────────────
# Analysis Endpoints
# ────────────────────────────────────────────────────────────────────────────────


@app.get("/datasets/{dataset_id}/distributions/numeric")
def get_numeric_distribution(
    dataset_id: str,
    column: str,
    bins: int = Query(default=30, ge=5, le=80),
    sample_size: int = Query(default=100000, ge=1000, le=500000),
):
    """Get histogram and statistics for numeric column"""
    try:
        table_name = f"ds_{dataset_id}"
        hist_data, sample_data = get_numeric_histogram(
            table_name, column, bins, sample_size
        )

        if hist_data is None:
            raise HTTPException(
                status_code=404, detail="No data available for this column"
            )

        return {
            "success": True,
            "histogram": hist_data.to_dict(orient="records"),
            "sample": sample_data.to_dict(orient="records"),
            "sample_size": len(sample_data),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute distribution: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/distributions/categorical")
def get_categorical_distribution(
    dataset_id: str, column: str, top_k: int = Query(default=20, ge=5, le=50)
):
    """Get value counts for categorical column"""
    try:
        table_name = f"ds_{dataset_id}"
        value_counts = get_value_counts(table_name, column, top_k)

        return {"success": True, "value_counts": value_counts.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute value counts: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/bias/numeric")
def get_numeric_bias(
    dataset_id: str, column: str, bins: int = Query(default=30, ge=5, le=80)
):
    """Get bias metrics for numeric column"""
    try:
        table_name = f"ds_{dataset_id}"
        metrics = get_numeric_bias_metrics(table_name, column, bins)

        if metrics is None:
            raise HTTPException(
                status_code=404, detail="Could not compute bias metrics for this column"
            )

        # Convert DataFrame to dict for JSON serialization
        metrics["bins_table"] = metrics["bins_table"].to_dict(orient="records")

        return {"success": True, "metrics": metrics}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute bias metrics: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/bias/categorical")
def get_categorical_bias(dataset_id: str, column: str):
    """Get bias metrics for categorical column"""
    try:
        table_name = f"ds_{dataset_id}"
        metrics = get_categorical_bias_metrics(table_name, column)

        if metrics is None:
            raise HTTPException(
                status_code=404, detail="Could not compute bias metrics for this column"
            )

        # Convert DataFrame to dict for JSON serialization
        metrics["top_table"] = metrics["top_table"].to_dict(orient="records")

        return {"success": True, "metrics": metrics}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute bias metrics: {str(e)}"
        )


@app.get("/datasets/{dataset_id}/correlation")
def get_correlation_matrix(dataset_id: str):
    """Get correlation matrix for all numeric columns"""
    try:
        table_name = f"ds_{dataset_id}"
        con = connect()

        # Get numeric columns from schema (no data load)
        columns_query = f"DESCRIBE SELECT * FROM {table_name}"
        columns_info = con.execute(columns_query).df()

        numeric_types = [
            "BIGINT",
            "INTEGER",
            "DOUBLE",
            "FLOAT",
            "DECIMAL",
            "HUGEINT",
            "SMALLINT",
            "TINYINT",
            "UBIGINT",
            "UINTEGER",
            "USMALLINT",
            "UTINYINT",
            "REAL",
        ]

        num_cols = columns_info[columns_info["column_type"].isin(numeric_types)][
            "column_name"
        ].tolist()

        if len(num_cols) < 2:
            raise HTTPException(
                status_code=400,
                detail="Need at least 2 numeric columns for correlation",
            )

        # Compute all correlations in DuckDB (no table load!)
        corr_calcs = []
        for col in num_cols:
            corr_calcs.append(f'CORR("{col}", "{col}") as "{col}_{col}"')  # Diagonal
            for other_col in num_cols:
                if col < other_col:  # Avoid duplicates
                    corr_calcs.append(
                        f'CORR("{col}", "{other_col}") as "{col}_{other_col}"'
                    )

        query = f"SELECT {', '.join(corr_calcs)} FROM {table_name}"
        corr_results = con.execute(query).df()

        # Reconstruct symmetric correlation matrix
        corr = pd.DataFrame(np.eye(len(num_cols)), index=num_cols, columns=num_cols)

        for col in num_cols:
            corr.loc[col, col] = 1.0
            for other_col in num_cols:
                if col < other_col:
                    val = corr_results[f"{col}_{other_col}"].iloc[0]
                    corr.loc[col, other_col] = val
                    corr.loc[other_col, col] = val

        return {"success": True, "correlation": corr.to_dict(), "columns": num_cols}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute correlation: {str(e)}"
        )


@app.post("/query")
def execute_query(request: QueryRequest):
    """Execute arbitrary SQL query (use with caution)"""
    try:
        cols, rows = sql(request.query)

        return {
            "success": True,
            "columns": cols,
            "data": rows,
            "rows_returned": len(rows),
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Query execution failed: {str(e)}")


# ────────────────────────────────────────────────────────────────────────────────
# Fairness & Drift Endpoints
# ────────────────────────────────────────────────────────────────────────────────


@app.get("/datasets/{dataset_id}/fairness")
def compute_fairness_metrics(
    dataset_id: str,
    target_column: str,
    threshold: float,
    comparison_operator: str = Query(default=">", regex="^(>|<=)$"),
    sensitive_attribute: str = None,
):
    """
    Compute demographic parity for fairness analysis
    """
    try:
        table_name = f"ds_{dataset_id}"
        con = connect()

        # Validate columns exist using DESCRIBE (no data load)
        columns_query = f"DESCRIBE SELECT * FROM {table_name}"
        columns_info = con.execute(columns_query).df()
        available_cols = columns_info["column_name"].tolist()

        if target_column not in available_cols:
            raise HTTPException(
                status_code=400, detail=f"Column '{target_column}' not found"
            )
        if sensitive_attribute and sensitive_attribute not in available_cols:
            raise HTTPException(
                status_code=400, detail=f"Column '{sensitive_attribute}' not found"
            )

        # If no sensitive attribute, return overall selection rate
        if not sensitive_attribute:
            overall_query = f"""
                SELECT AVG(CASE WHEN "{target_column}" {comparison_operator} {threshold} THEN 1 ELSE 0 END) as selection_rate
                FROM {table_name}
            """
            result = con.execute(overall_query).fetchone()
            return {"success": True, "overall_selection_rate": float(result[0])}

        # Compute fairness metrics using SQL aggregation (no full table load)
        fairness_query = f"""
            SELECT 
                "{sensitive_attribute}" as group,
                AVG(CASE WHEN "{target_column}" {comparison_operator} {threshold} THEN 1 ELSE 0 END) as selection_rate,
                COUNT(*) as n
            FROM {table_name}
            GROUP BY "{sensitive_attribute}"
            ORDER BY selection_rate DESC
        """

        grp = con.execute(fairness_query).df()

        if len(grp) == 0:
            raise HTTPException(
                status_code=404, detail="No data to compute fairness metrics"
            )

        # Demographic parity difference
        dp = float(grp["selection_rate"].max() - grp["selection_rate"].min())

        return {
            "success": True,
            "demographic_parity_difference": dp,
            "group_statistics": grp.to_dict(orient="records"),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute fairness metrics: {str(e)}"
        )


@app.get("/datasets/{ref_id}/drift/{cur_id}")
def compute_drift_psi(
    ref_id: str,
    cur_id: str,
    columns: Optional[List[str]] = Query(default=None),
    n_bins: int = Query(default=10, ge=5, le=30),
):
    """
    Compute PSI (Population Stability Index) between reference and current datasets
    """
    try:
        ref_table = f"ds_{ref_id}"
        cur_table = f"ds_{cur_id}"
        con = connect()

        # Get shared columns from schema (no data fetch)
        ref_cols_query = f"DESCRIBE SELECT * FROM {ref_table}"
        cur_cols_query = f"DESCRIBE SELECT * FROM {cur_table}"

        ref_cols = set(con.execute(ref_cols_query).df()["column_name"].tolist())
        cur_cols = set(con.execute(cur_cols_query).df()["column_name"].tolist())

        # Get shared columns
        if columns is None:
            columns = list(ref_cols.intersection(cur_cols))
        else:
            # Validate requested columns exist in both datasets
            missing_ref = set(columns) - ref_cols
            missing_cur = set(columns) - cur_cols
            if missing_ref or missing_cur:
                raise HTTPException(
                    status_code=400,
                    detail=f"Columns missing - ref: {missing_ref}, cur: {missing_cur}",
                )

        if not columns:
            raise HTTPException(
                status_code=400, detail="No shared columns between datasets"
            )

        # Fetch ONLY the selected columns (not entire tables!)
        cols_str = ", ".join([f'"{col}"' for col in columns])
        ref_query = f"SELECT {cols_str} FROM {ref_table}"
        cur_query = f"SELECT {cols_str} FROM {cur_table}"

        ref_df = con.execute(ref_query).df()
        cur_df = con.execute(cur_query).df()

        # Compute PSI for each column
        psi_results = compute_psi_table(ref_df, cur_df, columns, n_bins)

        return {
            "success": True,
            "reference_dataset": ref_id,
            "current_dataset": cur_id,
            "n_bins": n_bins,
            "psi_metrics": psi_results,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to compute drift: {str(e)}"
        )


# ────────────────────────────────────────────────────────────────────────────────
# Run the server
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
