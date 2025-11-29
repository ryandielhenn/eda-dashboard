from fastapi import APIRouter, HTTPException, Query

from storage.duck import (
    get_categorical_bias_metrics,
    get_numeric_bias_metrics,
    get_numeric_histogram,
    get_value_counts,
)

router = APIRouter()


# ────────────────────────────────────────────────────────────────────────────────
# Distribution and Bias Endpoints
# ────────────────────────────────────────────────────────────────────────────────
@router.get(
    "/datasets/{dataset_id}/distributions/numeric", tags=["Distribution Methods"]
)
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

        if hist_data is None or sample_data is None:
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


@router.get(
    "/datasets/{dataset_id}/distributions/categorical", tags=["Distribution Methods"]
)
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


@router.get("/datasets/{dataset_id}/bias/numeric", tags=["Bias Methods"])
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


@router.get("/datasets/{dataset_id}/bias/categorical", tags=["Bias Methods"])
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
