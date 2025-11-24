from fastapi import APIRouter, HTTPException
import pandas as pd
import numpy as np

from storage.duck import connect

router = APIRouter()


@router.get("/datasets/{dataset_id}/correlation", tags=["Correlation Matrix"])
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
