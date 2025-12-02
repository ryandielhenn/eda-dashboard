from __future__ import annotations
import os
import pandas as pd

from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from pathlib import Path
from storage.duck import get_schema, ingest_file, list_datasets, sql

router = APIRouter()

# Data directory
DATA_PROC = Path("data/processed")
DATA_PROC.mkdir(parents=True, exist_ok=True)

# ────────────────────────────────────────────────────────────────────────────────
# Utility Functions
# ────────────────────────────────────────────────────────────────────────────────


def sanitize_id(name: str) -> str:
    """Sanitize dataset name for use as table identifier"""
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name)


# ────────────────────────────────────────────────────────────────────────────────
# Dataset Management
# ────────────────────────────────────────────────────────────────────────────────
@router.post("/upload", tags=["Dataset Upload"])
async def upload_dataset(file: UploadFile = File(...)):
    """
    Upload CSV or ZIP file containing CSV
    Returns the ingested dataset information
    """
    try:
        # Validate file in request
        if not file.filename:
            raise HTTPException(status_code=400, detail="Filename missing or invalid")
        # Validate file type
        filename = file.filename.lower()
        if not filename.endswith((".csv", ".parquet")):
            raise HTTPException(
                status_code=400, detail="Only CSV and ZIP files are supported"
            )
        # Generate dataset ID
        dataset_id = sanitize_id(os.path.splitext(file.filename)[0])

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


@router.get("/datasets", tags=["Dataset Retrieval"])
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


@router.get("/datasets/{dataset_id}/preview", tags=["Dataset Retrieval"])
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


@router.get("/datasets/{dataset_id}/schema", tags=["Dataset Retrieval"])
def get_dataset_schema(dataset_id: str):
    """Get schema information for a dataset"""
    try:
        table_name = f"ds_{dataset_id}"
        schema_df = get_schema(table_name)

        return {"success": True, "schema": schema_df.to_dict(orient="records")}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Failed to get schema: {str(e)}")
