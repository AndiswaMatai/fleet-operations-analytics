"""
Generates synthetic telematics, fuel, and maintenance records for a small
fleet — the kind of data that arrives from a GPS/telematics provider via
REST API or daily file drop, as used at GUUD Mobility.
"""
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(99)
RAW = Path(__file__).resolve().parent.parent / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

VEHICLES = [
    ("V001", "Truck",  "Cape Town–JHB",  35000),
    ("V002", "Van",    "JHB–Durban",     28000),
    ("V003", "Truck",  "PE–Cape Town",   32000),
    ("V004", "Van",    "JHB–Bloemfontein", 15000),
    ("V005", "Bakkie", "Cape Town–PE",   12000),
]

start = datetime(2026, 5, 1)
telematics, fuel_logs, maintenance = [], [], []

for day in range(30):
    d = start + timedelta(days=day)
    for vid, vtype, route, _ in VEHICLES:
        idle_minutes = random.randint(10, 90)
        km = round(random.uniform(80, 650), 1)
        speed_avg = round(random.uniform(55, 110), 1)
        harsh_events = random.randint(0, 5)
        telematics.append({
            "date": d.strftime("%Y-%m-%d"),
            "vehicle_id": vid,
            "vehicle_type": vtype,
            "route": route,
            "km_driven": km,
            "avg_speed_kmh": speed_avg,
            "idle_minutes": idle_minutes,
            "harsh_braking_events": harsh_events,
        })

        if random.random() < 0.6:
            litres = round(km / random.uniform(6, 12), 2)
            cost_per_litre = round(random.uniform(21.5, 23.5), 2)
            fuel_logs.append({
                "date": d.strftime("%Y-%m-%d"),
                "vehicle_id": vid,
                "litres": litres,
                "cost_per_litre": cost_per_litre,
                "total_cost": round(litres * cost_per_litre, 2),
            })

for vid, vtype, route, base_km in VEHICLES:
    for i in range(random.randint(1, 3)):
        sched_date = (start + timedelta(days=random.randint(0, 29))).strftime("%Y-%m-%d")
        maint_types = ["Oil change", "Tyre rotation", "Brake inspection", "Full service"]
        maintenance.append({
            "vehicle_id": vid,
            "scheduled_date": sched_date,
            "maintenance_type": random.choice(maint_types),
            "cost": round(random.uniform(800, 8500), 2),
            "completed": random.choice(["Y", "Y", "Y", "N"]),
        })

for fname, rows in [("telematics.csv", telematics), ("fuel_logs.csv", fuel_logs), ("maintenance.csv", maintenance)]:
    with open(RAW / fname, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader(); w.writerows(rows)

print(f"telematics: {len(telematics)} | fuel: {len(fuel_logs)} | maintenance: {len(maintenance)}")
