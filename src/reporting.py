import pandas as pd


def build_report() -> None:
    df = pd.read_csv("data/curated/curated_orders.csv")
    out = (
        df.groupby("status", as_index=False)
        .agg(total_orders=("order_id", "count"), total_amount=("amount", "sum"), avg_amount=("amount", "mean"))
        .sort_values("total_amount", ascending=False)
    )
    out.to_csv("reports/automated_kpi_report.csv", index=False)
    print("Automated report created.")


if __name__ == "__main__":
    build_report()
