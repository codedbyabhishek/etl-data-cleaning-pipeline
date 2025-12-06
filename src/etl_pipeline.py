import os
import pandas as pd

VALID_STATUS = {"completed", "pending", "cancelled"}


def extract() -> pd.DataFrame:
    return pd.read_csv("data/raw/orders_raw.csv", parse_dates=["order_date"])


def transform(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    before = len(df)

    df = df.drop_duplicates(subset=["order_id"]).copy()
    after_dedup = len(df)

    median_amount = df["amount"].median()
    df["amount"] = df["amount"].fillna(median_amount)

    invalid_status = ~df["status"].isin(VALID_STATUS)
    invalid_count = int(invalid_status.sum())
    df.loc[invalid_status, "status"] = "pending"

    df = df[df["amount"] >= 0].copy()
    df["amount_bucket"] = pd.cut(df["amount"], bins=[0, 50, 150, 5000], labels=["low", "medium", "high"], include_lowest=True)

    quality = pd.DataFrame(
        {
            "check": [
                "raw_rows",
                "rows_after_dedup",
                "duplicates_removed",
                "missing_amount_imputed",
                "invalid_status_fixed",
            ],
            "value": [
                before,
                after_dedup,
                before - after_dedup,
                int(df["amount"].isna().sum()),
                invalid_count,
            ],
        }
    )

    return df, quality


def load(df: pd.DataFrame, quality: pd.DataFrame) -> None:
    os.makedirs("data/staging", exist_ok=True)
    os.makedirs("data/curated", exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    df.to_csv("data/staging/staged_orders.csv", index=False)
    df.to_csv("data/curated/curated_orders.csv", index=False)
    quality.to_csv("reports/data_quality_report.csv", index=False)


if __name__ == "__main__":
    raw = extract()
    curated, quality_report = transform(raw)
    load(curated, quality_report)
    print("ETL pipeline completed.")
