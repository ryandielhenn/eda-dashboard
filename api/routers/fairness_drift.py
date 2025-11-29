from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query

from storage.duck import connect
from analytics.drift import compute_psi_table

router = APIRouter()


# ────────────────────────────────────────────────────────────────────────────────
# Fairness & Drift Endpoints
# ────────────────────────────────────────────────────────────────────────────────
@router.get("/datasets/{dataset_id}/fairness", tags=["Fairness Calculation"])
def compute_fairness_metrics(
    dataset_id: str,
    target_column: str,
    threshold: float,
    comparison_operator: str = Query(default=">", regex="^(>|<=)$"),
    sensitive_attribute: Optional[str] = None,
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
            return {
                "success": True,
                "overall_selection_rate": float(result[0] if result else 0),
            }

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


@router.get("/datasets/{ref_id}/drift/{cur_id}", tags=["Drift Calculation"])
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
