"""
FastAPI backend for EDA Dashboard
Handles file uploads, DuckDB operations, and data analysis
"""
from analytics.drift import compute_psi_table
from analytics.fairness import compute_demographic_parity
from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
import os
import tempfile
import zipfile
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from storage.duck import (
    ingest_parquet,
    list_datasets,
    load_table,
    get_tables,
    get_schema,
    get_numeric_histogram,
    get_value_counts,
    get_numeric_bias_metrics,
    get_categorical_bias_metrics,
    sql,
    connect
)

app = FastAPI(title="EDA Dashboard API", version="1.0.0")

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
        con = connect()
        tables = get_tables()
        return {
            "status": "healthy",
            "database": "connected",
            "tables_count": len(tables)
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "error": str(e)}
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
        if not file.filename.lower().endswith(('.csv', '.zip')):
            raise HTTPException(
                status_code=400,
                detail="Only CSV and ZIP files are supported"
            )
        
        # Handle CSV
        if file.filename.lower().endswith('.csv'):
            # Read CSV into DataFrame
            content = await file.read()
            df = pd.read_csv(pd.io.common.BytesIO(content), low_memory=False)
            
            # Generate dataset ID
            dataset_id = sanitize_id(os.path.splitext(file.filename)[0])
            
            # Save as Parquet
            parquet_path = save_df_as_parquet(df, dataset_id)
            
            # Ingest into DuckDB
            table_name, n_rows, n_cols = ingest_parquet(parquet_path, dataset_id)
            
            return {
                "success": True,
                "dataset_id": dataset_id,
                "table_name": table_name,
                "path": parquet_path,
                "n_rows": n_rows,
                "n_cols": n_cols,
                "message": f"Successfully ingested {file.filename}"
            }
        
        # Handle ZIP
        elif file.filename.lower().endswith('.zip'):
            # Save ZIP temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:
                content = await file.read()
                tmp.write(content)
                tmp_path = tmp.name
            
            try:
                # Extract CSVs from ZIP
                with zipfile.ZipFile(tmp_path, 'r') as zf:
                    csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
                
                if not csv_files:
                    raise HTTPException(
                        status_code=400,
                        detail="No CSV files found in ZIP"
                    )
                
                # Return list of available CSVs
                return {
                    "success": True,
                    "type": "zip",
                    "csv_files": csv_files,
                    "message": "ZIP contains multiple CSVs. Use /upload/zip endpoint to select one."
                }
            finally:
                os.unlink(tmp_path)
                
    except pd.errors.ParserError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to parse CSV: {str(e)}"
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )

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
                    "last_ingested": str(row[4])
                }
                for row in datasets
            ]
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list datasets: {str(e)}"
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
            "rows_returned": len(rows)
        }
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset not found or query failed: {str(e)}"
        )

@app.get("/datasets/{dataset_id}/schema")
def get_dataset_schema(dataset_id: str):
    """Get schema information for a dataset"""
    try:
        table_name = f"ds_{dataset_id}"
        schema_df = get_schema(table_name)
        
        return {
            "success": True,
            "schema": schema_df.to_dict(orient='records')
        }
    except Exception as e:
        raise HTTPException(
            status_code=404,
            detail=f"Failed to get schema: {str(e)}"
        )

# ────────────────────────────────────────────────────────────────────────────────
# Analysis Endpoints
# ────────────────────────────────────────────────────────────────────────────────

@app.get("/datasets/{dataset_id}/distributions/numeric")
def get_numeric_distribution(
    dataset_id: str,
    column: str,
    bins: int = Query(default=30, ge=5, le=80),
    sample_size: int = Query(default=100000, ge=1000, le=500000)
):
    """Get histogram and statistics for numeric column"""
    try:
        table_name = f"ds_{dataset_id}"
        hist_data, sample_data = get_numeric_histogram(table_name, column, bins, sample_size)
        
        if hist_data is None:
            raise HTTPException(
                status_code=404,
                detail="No data available for this column"
            )
        
        return {
            "success": True,
            "histogram": hist_data.to_dict(orient='records'),
            "sample": sample_data.to_dict(orient='records'),
            "sample_size": len(sample_data)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute distribution: {str(e)}"
        )

@app.get("/datasets/{dataset_id}/distributions/categorical")
def get_categorical_distribution(
    dataset_id: str,
    column: str,
    top_k: int = Query(default=20, ge=5, le=50)
):
    """Get value counts for categorical column"""
    try:
        table_name = f"ds_{dataset_id}"
        value_counts = get_value_counts(table_name, column, top_k)
        
        return {
            "success": True,
            "value_counts": value_counts.to_dict(orient='records')
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute value counts: {str(e)}"
        )

@app.get("/datasets/{dataset_id}/bias/numeric")
def get_numeric_bias(
    dataset_id: str,
    column: str,
    bins: int = Query(default=30, ge=5, le=80)
):
    """Get bias metrics for numeric column"""
    try:
        table_name = f"ds_{dataset_id}"
        metrics = get_numeric_bias_metrics(table_name, column, bins)
        
        if metrics is None:
            raise HTTPException(
                status_code=404,
                detail="Could not compute bias metrics for this column"
            )
        
        # Convert DataFrame to dict for JSON serialization
        metrics['bins_table'] = metrics['bins_table'].to_dict(orient='records')
        
        return {
            "success": True,
            "metrics": metrics
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute bias metrics: {str(e)}"
        )

@app.get("/datasets/{dataset_id}/bias/categorical")
def get_categorical_bias(dataset_id: str, column: str):
    """Get bias metrics for categorical column"""
    try:
        table_name = f"ds_{dataset_id}"
        metrics = get_categorical_bias_metrics(table_name, column)
        
        if metrics is None:
            raise HTTPException(
                status_code=404,
                detail="Could not compute bias metrics for this column"
            )
        
        # Convert DataFrame to dict for JSON serialization
        metrics['top_table'] = metrics['top_table'].to_dict(orient='records')
        
        return {
            "success": True,
            "metrics": metrics
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute bias metrics: {str(e)}"
        )

@app.get("/datasets/{dataset_id}/correlation")
def get_correlation_matrix(dataset_id: str):
    """Get correlation matrix for all numeric columns"""
    try:
        table_name = f"ds_{dataset_id}"
        df = load_table(table_name)
        
        # Get numeric columns
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        
        if len(num_cols) < 2:
            raise HTTPException(
                status_code=400,
                detail="Need at least 2 numeric columns for correlation"
            )
        
        # Compute correlation
        corr = df[num_cols].corr(numeric_only=True, method="pearson")
        
        return {
            "success": True,
            "correlation": corr.to_dict(),
            "columns": num_cols
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute correlation: {str(e)}"
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
            "rows_returned": len(rows)
        }
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Query execution failed: {str(e)}"
        )

# ────────────────────────────────────────────────────────────────────────────────
# Fairness & Drift Endpoints
# ────────────────────────────────────────────────────────────────────────────────

@app.get("/datasets/{dataset_id}/fairness")
def compute_fairness_metrics(
    dataset_id: str,
    target_column: str,
    threshold: float,
    comparison_operator: str = Query(default=">", regex="^(>|<=)$"),
    sensitive_attribute: str = None
):
    """
    Compute demographic parity for fairness analysis
    """
    try:
        table_name = f"ds_{dataset_id}"
        df = load_table(table_name)
        
        # Call the analytics function
        result = compute_demographic_parity(
            df=df,
            target_column=target_column,
            threshold=threshold,
            comparison_operator=comparison_operator,
            sensitive_attribute=sensitive_attribute
        )
        
        return {
            "success": True,
            **result  # Unpack the result dictionary
        }
        
    except ValueError as e:
        # Column validation errors
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute fairness metrics: {str(e)}"
        )

@app.get("/datasets/{ref_id}/drift/{cur_id}")
def compute_drift_psi(
    ref_id: str,
    cur_id: str,
    columns: Optional[List[str]] = Query(default=None),
    n_bins: int = Query(default=10, ge=5, le=30)
):
    """
    Compute PSI (Population Stability Index) between reference and current datasets
    """
    try:
        ref_table = f"ds_{ref_id}"
        cur_table = f"ds_{cur_id}"
        
        # Load both datasets
        ref_df = load_table(ref_table)
        cur_df = load_table(cur_table)
        
        # Get shared columns
        if columns is None:
            columns = ref_df.columns.intersection(cur_df.columns).tolist()
        else:
            # Validate requested columns exist in both datasets
            missing_ref = set(columns) - set(ref_df.columns)
            missing_cur = set(columns) - set(cur_df.columns)
            if missing_ref or missing_cur:
                raise HTTPException(
                    status_code=400,
                    detail=f"Columns missing - ref: {missing_ref}, cur: {missing_cur}"
                )
        
        if not columns:
            raise HTTPException(
                status_code=400,
                detail="No shared columns between datasets"
            )
        
        # Compute PSI for each column
        psi_results = compute_psi_table(ref_df, cur_df, columns, n_bins)
        
        return {
            "success": True,
            "reference_dataset": ref_id,
            "current_dataset": cur_id,
            "n_bins": n_bins,
            "psi_metrics": psi_results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to compute drift: {str(e)}"
        )

# ────────────────────────────────────────────────────────────────────────────────
# Run the server
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
