import pandas as pd


def build_report() -> None:
    df = pd.read_csv("data/curated/curated_orders.csv")

    by_status = (
        df.groupby("status", as_index=False)
        .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"), avg_amount=("amount", "mean"))
    )

    by_tier = (
        df.groupby("customer_tier", as_index=False)
        .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"), avg_amount=("amount", "mean"))
    )

    monthly = (
        df.groupby("order_month", as_index=False)
        .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"))
        .sort_values("order_month")
    )

    by_status.to_csv("reports/automated_kpi_report_status.csv", index=False)
    by_tier.to_csv("reports/automated_kpi_report_tier.csv", index=False)
    monthly.to_csv("reports/automated_kpi_report_monthly.csv", index=False)

    master = pd.DataFrame(
        {
            "metric": [
                "total_orders",
                "total_amount",
                "avg_order_amount",
                "distinct_customers",
                "completed_rate",
            ],
            "value": [
                len(df),
                float(df["amount"].sum()),
                float(df["amount"].mean()),
                int(df["customer_id"].nunique()),
                float((df["status"] == "completed").mean()),
            ],
        }
    )
    master.to_csv("reports/automated_kpi_report.csv", index=False)
    print("Automated reporting suite created.")


if __name__ == "__main__":
    build_report()
