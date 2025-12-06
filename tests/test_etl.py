import pandas as pd
from src.etl_pipeline import transform


def test_transform_removes_duplicates_and_fixes_status():
    df = pd.DataFrame(
        {
            "order_id": [1, 1, 2],
            "customer_id": [10, 10, 20],
            "order_date": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-02"]),
            "amount": [100.0, 100.0, None],
            "status": ["completed", "completed", "unknown"],
        }
    )

    out, _ = transform(df)
    assert len(out) == 2
    assert set(out["status"]) <= {"completed", "pending", "cancelled"}
