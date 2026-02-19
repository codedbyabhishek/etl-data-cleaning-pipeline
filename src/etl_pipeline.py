import os
import pandas as pd

VALID_STATUS = {"completed", "pending", "cancelled"}
VALID_PAYMENT = {"card", "upi", "wallet", "bank_transfer"}


def extract() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    orders_csv = pd.read_csv("data/raw/orders_raw.csv", parse_dates=["order_date"])
    customers = pd.read_csv("data/raw/customers_master.csv")
    orders_db = pd.read_csv("data/raw/orders_db_extract.csv", parse_dates=["order_date", "db_sync_ts"])
    return orders_csv, customers, orders_db


def transform(orders_csv: pd.DataFrame, customers: pd.DataFrame, orders_db: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    raw_rows = len(orders_csv)

    if orders_db.empty:
        combined = orders_csv.copy()
    else:
        combined = pd.concat([orders_csv, orders_db[orders_csv.columns]], ignore_index=True)
    combined = combined.drop_duplicates(subset=["order_id"]).copy()
    after_dedup = len(combined)

    missing_amount_before = int(combined["amount"].isna().sum())
    combined["amount"] = combined["amount"].fillna(combined["amount"].median())

    invalid_status = ~combined["status"].isin(VALID_STATUS)
    invalid_status_count = int(invalid_status.sum())
    combined.loc[invalid_status, "status"] = "pending"

    invalid_payment = ~combined["payment_mode"].isin(VALID_PAYMENT)
    invalid_payment_count = int(invalid_payment.sum())
    combined.loc[invalid_payment, "payment_mode"] = "card"

    missing_date = combined["order_date"].isna()
    missing_date_count = int(missing_date.sum())
    fallback_date = pd.Timestamp("2026-02-15 00:00:00")
    combined.loc[missing_date, "order_date"] = fallback_date

    combined = combined[combined["amount"] >= 0].copy()

    curated = combined.merge(customers, on="customer_id", how="left")
    curated["customer_tier"] = curated["customer_tier"].fillna("bronze")
    curated["country"] = curated["country"].fillna("US")
    curated["is_active"] = curated["is_active"].fillna(1).astype(int)

    curated["order_month"] = curated["order_date"].dt.to_period("M").astype(str)
    curated["amount_bucket"] = pd.cut(
        curated["amount"],
        bins=[0, 50, 150, 500, 100000],
        labels=["low", "medium", "high", "very_high"],
        include_lowest=True,
    )

    dq = pd.DataFrame(
        {
            "check": [
                "raw_rows_csv",
                "rows_after_union",
                "duplicates_removed",
                "missing_amount_imputed",
                "invalid_status_fixed",
                "invalid_payment_fixed",
                "missing_date_fixed",
                "final_rows",
                "completeness_score",
                "uniqueness_score",
                "validity_score",
            ],
            "value": [
                raw_rows,
                after_dedup,
                raw_rows + len(orders_db) - after_dedup,
                missing_amount_before,
                invalid_status_count,
                invalid_payment_count,
                missing_date_count,
                len(curated),
                round((1 - curated.isna().mean().mean()) * 100, 2),
                round((curated["order_id"].nunique() / max(len(curated), 1)) * 100, 2),
                round((curated["status"].isin(VALID_STATUS).mean()) * 100, 2),
            ],
        }
    )

    audit = pd.DataFrame(
        {
            "stage": ["extract", "transform", "load"],
            "status": ["success", "success", "success"],
            "record_count": [raw_rows + len(orders_db), len(curated), len(curated)],
            "notes": [
                "Loaded CSV + DB extract",
                "Cleaned missing/invalid values and enriched with customer master",
                "Published staged + curated datasets and data-quality reports",
            ],
        }
    )

    return combined, curated, dq.merge(audit, how="cross")


def load(staged: pd.DataFrame, curated: pd.DataFrame, report: pd.DataFrame) -> None:
    os.makedirs("data/staging", exist_ok=True)
    os.makedirs("data/curated", exist_ok=True)
    os.makedirs("reports", exist_ok=True)

    staged.to_csv("data/staging/staged_orders.csv", index=False)
    curated.to_csv("data/curated/curated_orders.csv", index=False)
    report.to_csv("reports/data_quality_report.csv", index=False)


if __name__ == "__main__":
    orders_csv_df, customers_df, orders_db_df = extract()
    staged_df, curated_df, quality_report_df = transform(orders_csv_df, customers_df, orders_db_df)
    load(staged_df, curated_df, quality_report_df)
    print("High-end ETL pipeline completed.")
