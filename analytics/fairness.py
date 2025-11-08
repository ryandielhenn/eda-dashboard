"""
Fairness metrics and demographic parity analysis
"""

import pandas as pd
from typing import Dict, Any, Optional


def compute_demographic_parity(
    df: pd.DataFrame,
    target_column: str,
    threshold: float,
    comparison_operator: str = ">",
    sensitive_attribute: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Compute demographic parity metrics

    Args:
        df: Input DataFrame
        target_column: Column to binarize as target
        threshold: Threshold value for binarization
        comparison_operator: Either ">" or "<="
        sensitive_attribute: Optional column for group analysis

    Returns:
        Dictionary with demographic parity metrics
    """
    # Validate columns
    if target_column not in df.columns:
        raise ValueError(f"Column '{target_column}' not found")
    if sensitive_attribute and sensitive_attribute not in df.columns:
        raise ValueError(f"Column '{sensitive_attribute}' not found")

    # Create binary target
    if comparison_operator == ">":
        y = (df[target_column] > threshold).astype(int)
    else:
        y = (df[target_column] <= threshold).astype(int)

    # If no sensitive attribute, return overall rate
    if not sensitive_attribute:
        return {"overall_selection_rate": float(y.mean())}

    # Compute selection rates by group
    g = df[sensitive_attribute].astype(str)
    group_stats = (
        pd.DataFrame({"group": g, "y": y})
        .groupby("group")
        .agg(selection_rate=("y", "mean"), n=("y", "size"))
        .reset_index()
    )

    # Demographic parity difference
    dp = float(
        group_stats["selection_rate"].max() - group_stats["selection_rate"].min()
    )

    return {
        "demographic_parity_difference": dp,
        "group_statistics": group_stats.to_dict(orient="records"),
    }
