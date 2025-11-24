"""
FastAPI backend for EDA Dashboard
Handles file uploads, DuckDB operations, and data analysis
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .routers import zip as zip_router
from .routers import datasets as dataset_router
from .routers import distributions as distribution_router
from .routers import correlation as correlation_router
from .routers import fairness_drift as fairness_drift_router

from storage.duck import (
    get_tables,
    connect,
)

app = FastAPI(title="EDA Dashboard API", version="1.0.0")
app.include_router(dataset_router.router)
app.include_router(zip_router.router)
app.include_router(distribution_router.router)
app.include_router(correlation_router.router)
app.include_router(fairness_drift_router.router)

# CORS - Allow Streamlit to talk to FastAPI
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
@app.get("/", tags=["Health Check"])
def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "EDA Dashboard API is running"}


@app.get("/health", tags=["Health Check"])
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
# Run the server
# ────────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000, reload=True)
