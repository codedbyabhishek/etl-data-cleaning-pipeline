import os
import numpy as np
import pandas as pd

RNG = np.random.default_rng(19)


def main(n: int = 35000) -> None:
    os.makedirs("data/raw", exist_ok=True)
    status = ["completed", "pending", "cancelled", "unknown"]

    df = pd.DataFrame(
        {
            "order_id": np.arange(1, n + 1),
            "customer_id": RNG.integers(1000, 9500, n),
            "order_date": RNG.choice(pd.date_range("2025-01-01", "2026-02-10", freq="D"), n),
            "amount": np.round(RNG.gamma(2.4, 45, n), 2),
            "status": RNG.choice(status, n, p=[0.7, 0.18, 0.1, 0.02]),
        }
    )

    miss = RNG.choice(df.index, 700, replace=False)
    df.loc[miss, "amount"] = np.nan
    dup = df.sample(200, random_state=42)
    raw = pd.concat([df, dup], ignore_index=True)

    raw.to_csv("data/raw/orders_raw.csv", index=False)
    print(f"Generated raw dataset with {len(raw)} rows")


if __name__ == "__main__":
    main()
