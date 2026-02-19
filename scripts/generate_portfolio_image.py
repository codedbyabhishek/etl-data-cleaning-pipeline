import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def main() -> None:
    dq = pd.read_csv("reports/data_quality_report.csv")
    kpi = pd.read_csv("reports/automated_kpi_report.csv")
    monthly = pd.read_csv("reports/automated_kpi_report_monthly.csv")

    fig, axes = plt.subplots(2, 2, figsize=(14, 8))
    fig.suptitle("ETL & Data Cleaning Pipeline Preview", fontsize=16, fontweight="bold")

    axes[0, 0].plot(monthly["order_month"], monthly["total_orders"], marker="o")
    axes[0, 0].set_title("Monthly Orders")
    axes[0, 0].tick_params(axis="x", rotation=45)

    dq_top = dq[["check", "value"]].head(8)
    axes[0, 1].axis("off")
    table1 = axes[0, 1].table(cellText=dq_top.values, colLabels=dq_top.columns, cellLoc="center", loc="center")
    table1.auto_set_font_size(False)
    table1.set_fontsize(8)
    axes[0, 1].set_title("Data Quality Snapshot")

    axes[1, 0].axis("off")
    table2 = axes[1, 0].table(cellText=kpi.values, colLabels=kpi.columns, cellLoc="center", loc="center")
    table2.auto_set_font_size(False)
    table2.set_fontsize(9)
    axes[1, 0].set_title("Automated KPI Summary")

    axes[1, 1].text(
        0.02,
        0.9,
        "Highlights:\n- Multi-source ingest\n- DQ scoring\n- Audit reporting\n- Curated analytics layer",
        fontsize=11,
        va="top",
    )
    axes[1, 1].axis("off")

    plt.tight_layout()
    plt.savefig("assets/etl_pipeline_preview.png", dpi=170)


if __name__ == "__main__":
    main()
