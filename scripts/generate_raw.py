import os
import numpy as np
import pandas as pd

RNG = np.random.default_rng(2027)


def main(n: int = 120000) -> None:
    os.makedirs("data/raw", exist_ok=True)

    status = ["completed", "pending", "cancelled", "unknown"]
    payment_modes = ["card", "upi", "wallet", "bank_transfer"]

    orders = pd.DataFrame(
        {
            "order_id": np.arange(1, n + 1),
            "customer_id": RNG.integers(1000, 50000, n),
            "order_date": RNG.choice(pd.date_range("2025-01-01", "2026-02-15", freq="h"), n),
            "amount": np.round(RNG.gamma(2.8, 52, n), 2),
            "status": RNG.choice(status, n, p=[0.72, 0.17, 0.09, 0.02]),
            "payment_mode": RNG.choice(payment_modes, n, p=[0.46, 0.24, 0.12, 0.18]),
            "source_system": RNG.choice(["crm", "web", "partner_portal"], n, p=[0.25, 0.6, 0.15]),
        }
    )

    missing_amount_idx = RNG.choice(orders.index, 2600, replace=False)
    orders.loc[missing_amount_idx, "amount"] = np.nan

    invalid_date_idx = RNG.choice(orders.index, 500, replace=False)
    orders.loc[invalid_date_idx, "order_date"] = pd.NaT

    duplicate_rows = orders.sample(450, random_state=21)
    orders_raw = pd.concat([orders, duplicate_rows], ignore_index=True)

    customers = pd.DataFrame(
        {
            "customer_id": np.arange(1000, 50000),
            "customer_tier": RNG.choice(["bronze", "silver", "gold", "platinum"], 49000, p=[0.42, 0.31, 0.2, 0.07]),
            "country": RNG.choice(["US", "IN", "UK", "DE", "CA", "AU"], 49000),
            "is_active": RNG.choice([0, 1], 49000, p=[0.1, 0.9]),
        }
    )

    db_extract = orders_raw.sample(50000, random_state=8).copy()
    db_extract["db_sync_ts"] = pd.Timestamp("2026-02-16 00:00:00")

    orders_raw.to_csv("data/raw/orders_raw.csv", index=False)
    customers.to_csv("data/raw/customers_master.csv", index=False)
    db_extract.to_csv("data/raw/orders_db_extract.csv", index=False)
    print(f"Generated raw files: orders_raw={len(orders_raw)}, customers={len(customers)}, db_extract={len(db_extract)}")


if __name__ == "__main__":
    main()
