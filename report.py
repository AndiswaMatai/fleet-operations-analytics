"""Generates a fleet performance dashboard chart."""
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pathlib import Path

PROCESSED = Path(__file__).resolve().parent.parent / "data" / "processed"


def run():
    kpis = pd.read_csv(PROCESSED / "vehicle_kpis.csv")

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    fig.suptitle("Fleet Operations Dashboard", fontsize=13, fontweight="bold")

    axes[0].bar(kpis["vehicle_id"], kpis["km_per_litre"], color="#2E75B6")
    axes[0].axhline(7.0, color="red", linestyle="--", linewidth=1, label="Min threshold")
    axes[0].set_title("Fuel Efficiency (km/L)"); axes[0].legend(fontsize=8)

    axes[1].bar(kpis["vehicle_id"], kpis["harsh_events_per_100km"], color="#E8A33D")
    axes[1].axhline(1.5, color="red", linestyle="--", linewidth=1, label="Alert threshold")
    axes[1].set_title("Harsh Events / 100 km"); axes[1].legend(fontsize=8)

    colours = ["#C00000" if v < 80 else "#2E75B6" for v in kpis["maint_compliance_pct"]]
    axes[2].bar(kpis["vehicle_id"], kpis["maint_compliance_pct"], color=colours)
    axes[2].axhline(80, color="red", linestyle="--", linewidth=1, label="80% target")
    axes[2].set_ylim(0, 110); axes[2].set_title("Maintenance Compliance (%)"); axes[2].legend(fontsize=8)

    fig.tight_layout()
    out = PROCESSED / "fleet_dashboard.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == "__main__":
    run()
