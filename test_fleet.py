"""Run with: python -m unittest discover -s tests -v"""
import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from analyse import vehicle_kpis, flag_alerts


def _make_inputs():
    tel = pd.DataFrame([{
        "date": pd.Timestamp("2026-05-01"), "vehicle_id": "V001", "vehicle_type": "Truck",
        "route": "A–B", "km_driven": 400.0, "avg_speed_kmh": 80.0,
        "idle_minutes": 30, "harsh_braking_events": 2,
    }])
    fuel = pd.DataFrame([{
        "date": pd.Timestamp("2026-05-01"), "vehicle_id": "V001",
        "litres": 50.0, "cost_per_litre": 22.0, "total_cost": 1100.0,
    }])
    maint = pd.DataFrame([{
        "vehicle_id": "V001", "scheduled_date": "2026-05-10",
        "maintenance_type": "Oil change", "cost": 1500.0, "completed": "Y",
    }])
    return tel, fuel, maint


class TestFleetKPIs(unittest.TestCase):
    def test_km_per_litre(self):
        kpis = vehicle_kpis(*_make_inputs())
        self.assertAlmostEqual(kpis.iloc[0]["km_per_litre"], 400 / 50, places=1)

    def test_fuel_cost_per_km(self):
        kpis = vehicle_kpis(*_make_inputs())
        self.assertAlmostEqual(kpis.iloc[0]["fuel_cost_per_km"], 1100 / 400, places=2)

    def test_maintenance_compliance_100pct(self):
        kpis = vehicle_kpis(*_make_inputs())
        self.assertEqual(kpis.iloc[0]["maint_compliance_pct"], 100.0)

    def test_low_efficiency_alert(self):
        tel, fuel, maint = _make_inputs()
        fuel["litres"] = 100.0  # 400 km / 100 L = 4 km/L → below threshold
        fuel["total_cost"] = 2200.0
        kpis = vehicle_kpis(tel, fuel, maint)
        alerts = flag_alerts(kpis)
        self.assertIn("LOW_FUEL_EFFICIENCY", alerts["alert"].values)

    def test_maintenance_overdue_alert(self):
        tel, fuel, maint = _make_inputs()
        maint["completed"] = "N"
        kpis = vehicle_kpis(tel, fuel, maint)
        alerts = flag_alerts(kpis)
        self.assertIn("MAINTENANCE_OVERDUE", alerts["alert"].values)


if __name__ == "__main__":
    unittest.main()
