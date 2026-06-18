"""
Computes fleet KPIs from telematics, fuel, and maintenance data:
- Fuel efficiency (km/litre) per vehicle
- Fleet utilisation rate (active days / available days)
- Harsh braking rate (events per 100 km) — a driver behaviour and safety KPI
- Idle time ratio
- Maintenance compliance rate
"""
from pathlib import Path

import pandas as pd

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
PROCESSED = Path(__file__).resolve().parent.parent / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)


def load():
    tel = pd.read_csv(RAW / "telematics.csv", parse_dates=["date"])
    fuel = pd.read_csv(RAW / "fuel_logs.csv", parse_dates=["date"])
    maint = pd.read_csv(RAW / "maintenance.csv")
    return tel, fuel, maint


def vehicle_kpis(tel: pd.DataFrame, fuel: pd.DataFrame, maint: pd.DataFrame) -> pd.DataFrame:
    tel_agg = tel.groupby("vehicle_id").agg(
        vehicle_type=("vehicle_type", "first"),
        route=("route", "first"),
        total_km=("km_driven", "sum"),
        active_days=("date", "nunique"),
        total_idle_min=("idle_minutes", "sum"),
        harsh_events=("harsh_braking_events", "sum"),
    ).reset_index()

    fuel_agg = fuel.groupby("vehicle_id").agg(
        total_litres=("litres", "sum"),
        total_fuel_cost=("total_cost", "sum"),
    ).reset_index()

    maint_agg = maint.groupby("vehicle_id").agg(
        scheduled_jobs=("completed", "count"),
        completed_jobs=("completed", lambda x: (x == "Y").sum()),
        total_maint_cost=("cost", "sum"),
    ).reset_index()

    df = tel_agg.merge(fuel_agg, on="vehicle_id", how="left") \
                .merge(maint_agg, on="vehicle_id", how="left")

    available_days = 30
    df["utilisation_pct"] = (df["active_days"] / available_days * 100).round(1)
    df["km_per_litre"] = (df["total_km"] / df["total_litres"]).round(2)
    df["fuel_cost_per_km"] = (df["total_fuel_cost"] / df["total_km"]).round(3)
    df["harsh_events_per_100km"] = (df["harsh_events"] / df["total_km"] * 100).round(2)
    df["idle_ratio"] = (df["total_idle_min"] / (df["active_days"] * 60 * 10)).round(3)
    df["maint_compliance_pct"] = (df["completed_jobs"] / df["scheduled_jobs"] * 100).round(1)

    return df.fillna(0)


def flag_alerts(df: pd.DataFrame) -> pd.DataFrame:
    """Flags vehicles needing attention — the exception list for ops managers."""
    alerts = []
    for _, row in df.iterrows():
        if row["km_per_litre"] < 7.0:
            alerts.append({"vehicle_id": row["vehicle_id"], "alert": "LOW_FUEL_EFFICIENCY",
                           "detail": f"{row['km_per_litre']} km/l below 7.0 threshold"})
        if row["harsh_events_per_100km"] > 1.5:
            alerts.append({"vehicle_id": row["vehicle_id"], "alert": "HIGH_HARSH_EVENTS",
                           "detail": f"{row['harsh_events_per_100km']} events/100 km — review driver behaviour"})
        if row["maint_compliance_pct"] < 80:
            alerts.append({"vehicle_id": row["vehicle_id"], "alert": "MAINTENANCE_OVERDUE",
                           "detail": f"Only {row['maint_compliance_pct']}% of scheduled jobs completed"})
        if row["utilisation_pct"] < 60:
            alerts.append({"vehicle_id": row["vehicle_id"], "alert": "LOW_UTILISATION",
                           "detail": f"Vehicle active only {row['utilisation_pct']}% of available days"})
    return pd.DataFrame(alerts) if alerts else pd.DataFrame(columns=["vehicle_id", "alert", "detail"])


def run():
    tel, fuel, maint = load()
    kpis = vehicle_kpis(tel, fuel, maint)
    alerts = flag_alerts(kpis)

    kpis.to_csv(PROCESSED / "vehicle_kpis.csv", index=False)
    alerts.to_csv(PROCESSED / "fleet_alerts.csv", index=False)
    return kpis, alerts


if __name__ == "__main__":
    kpis, alerts = run()
    print("=== Vehicle KPIs ===")
    cols = ["vehicle_id", "vehicle_type", "route", "total_km", "utilisation_pct",
            "km_per_litre", "fuel_cost_per_km", "harsh_events_per_100km", "maint_compliance_pct"]
    print(kpis[cols].to_string(index=False))
    print(f"\n=== Fleet Alerts ({len(alerts)}) ===")
    print(alerts.to_string(index=False) if len(alerts) else "None")
