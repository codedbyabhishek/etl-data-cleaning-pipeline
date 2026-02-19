import pandas as pd
from src.etl_pipeline import transform


def test_transform_removes_duplicates_and_fixes_status_and_payment():
    orders_csv = pd.DataFrame(
        {
            "order_id": [1, 1, 2],
            "customer_id": [10, 10, 20],
            "order_date": pd.to_datetime(["2025-01-01", "2025-01-01", None]),
            "amount": [100.0, 100.0, None],
            "status": ["completed", "completed", "unknown"],
            "payment_mode": ["card", "card", "invalid"],
            "source_system": ["web", "web", "crm"],
        }
    )
    customers = pd.DataFrame(
        {
            "customer_id": [10, 20],
            "customer_tier": ["silver", "gold"],
            "country": ["US", "IN"],
            "is_active": [1, 1],
        }
    )
    orders_db = pd.DataFrame(columns=orders_csv.columns)

    _, curated, _ = transform(orders_csv, customers, orders_db)
    assert len(curated) == 2
    assert set(curated["status"]) <= {"completed", "pending", "cancelled"}
    assert set(curated["payment_mode"]) <= {"card", "upi", "wallet", "bank_transfer"}
