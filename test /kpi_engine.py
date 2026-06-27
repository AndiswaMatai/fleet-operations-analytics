"""
fleet-operations-analytics/tests/test_kpi_engine.py
Unit tests for KPI computation and alert generation using PySpark.
Run with: pytest tests/ --no-header -v
"""

import pytest
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys

sys.path.insert(0, ".")
from src.kpi_engine      import (
    compute_fuel_efficiency,
    compute_driving_behaviour,
    compute_maintenance_compliance,
    compute_utilisation,
    build_master_kpi,
)
from alerts.alert_engine import (
    generate_fuel_alerts,
    generate_harsh_driving_alerts,
    build_alert_summary,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("FleetTests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def telemetry_df(spark):
    data = [
        Row(event_id="E001", vehicle_id="V001", driver_id="D001",
            event_timestamp=datetime(2024, 6, 1, 8, 0),
            event_date=date(2024, 6, 1), event_hour=8,
            speed_kmh=80.0, distance_km=120.0, odometer_km=50000.0,
            harsh_braking=2, harsh_accel=1, idling_minutes=10.0),
        Row(event_id="E002", vehicle_id="V001", driver_id="D001",
            event_timestamp=datetime(2024, 6, 2, 9, 0),
            event_date=date(2024, 6, 2), event_hour=9,
            speed_kmh=90.0, distance_km=80.0, odometer_km=50080.0,
            harsh_braking=1, harsh_accel=0, idling_minutes=5.0),
        Row(event_id="E003", vehicle_id="V002", driver_id="D002",
            event_timestamp=datetime(2024, 6, 1, 7, 0),
            event_date=date(2024, 6, 1), event_hour=7,
            speed_kmh=60.0, distance_km=200.0, odometer_km=30000.0,
            harsh_braking=0, harsh_accel=0, idling_minutes=2.0),
    ]
    return spark.createDataFrame(data)


@pytest.fixture(scope="session")
def fuel_df(spark):
    data = [
        Row(fuel_id="F001", vehicle_id="V001",
            fill_timestamp=datetime(2024, 6, 1, 18, 0),
            fill_date=date(2024, 6, 1),
            litres_filled=25.0, cost_zar=625.0, odometer_km=50120.0,
            cost_per_litre=25.0),
        Row(fuel_id="F002", vehicle_id="V002",
            fill_timestamp=datetime(2024, 6, 1, 17, 0),
            fill_date=date(2024, 6, 1),
            litres_filled=20.0, cost_zar=500.0, odometer_km=30200.0,
            cost_per_litre=25.0),
    ]
    return spark.createDataFrame(data)


@pytest.fixture(scope="session")
def maintenance_df(spark):
    data = [
        Row(record_id="M001", vehicle_id="V001",
            service_date=date(2024, 5, 1), service_type="Oil Change",
            next_service_km=55000.0, next_service_date=date(2024, 8, 1),
            cost_zar=800.0, workshop_id="W01", compliant=1,
            is_overdue=0, days_until_service=61),
        Row(record_id="M002", vehicle_id="V002",
            service_date=date(2024, 4, 1), service_type="Major Service",
            next_service_km=35000.0, next_service_date=date(2024, 6, 5),
            cost_zar=3500.0, workshop_id="W02", compliant=0,
            is_overdue=1, days_until_service=-22),
    ]
    return spark.createDataFrame(data)


# ── Fuel Efficiency Tests ─────────────────────────────────────────────────────

class TestFuelEfficiency:

    def test_efficiency_calculated(self, telemetry_df, fuel_df):
        result = compute_fuel_efficiency(telemetry_df, fuel_df)
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        # V001: 200 km / 25 L = 8.0 km/L (above threshold)
        assert v001["fuel_efficiency_km_l"] == 8.0
        assert v001["fuel_efficiency_flag"] == 0

    def test_low_efficiency_flagged(self, spark, fuel_df):
        # Inject a vehicle with poor efficiency: 100 km / 20 L = 5.0 km/L
        from pyspark.sql import Row
        low_tele = spark.createDataFrame([
            Row(event_id="E999", vehicle_id="V999", driver_id="D999",
                event_timestamp=datetime(2024, 6, 1, 8, 0),
                event_date=date(2024, 6, 1), event_hour=8,
                speed_kmh=70.0, distance_km=100.0, odometer_km=10000.0,
                harsh_braking=0, harsh_accel=0, idling_minutes=0.0)
        ])
        low_fuel = spark.createDataFrame([
            Row(fuel_id="F999", vehicle_id="V999",
                fill_timestamp=datetime(2024, 6, 1, 18, 0),
                fill_date=date(2024, 6, 1),
                litres_filled=20.0, cost_zar=500.0, odometer_km=10100.0,
                cost_per_litre=25.0)
        ])
        result = compute_fuel_efficiency(low_tele, low_fuel)
        v999 = result.filter("vehicle_id = 'V999'").collect()[0]
        assert v999["fuel_efficiency_km_l"] == 5.0
        assert v999["fuel_efficiency_flag"] == 1


# ── Harsh Driving Tests ───────────────────────────────────────────────────────

class TestDrivingBehaviour:

    def test_harsh_events_computed(self, telemetry_df):
        result = compute_driving_behaviour(telemetry_df)
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        assert v001["total_harsh_braking"] == 3
        assert v001["total_harsh_accel"] == 1
        assert v001["total_harsh_events"] == 4

    def test_harsh_flag_set_correctly(self, telemetry_df):
        result = compute_driving_behaviour(telemetry_df)
        # V001: 4 events / 200 km * 100 = 2.0 per 100km > 1.5 → flagged
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        assert v001["harsh_events_per_100km"] == 2.0
        assert v001["harsh_driving_flag"] == 1
        # V002: 0 events → not flagged
        v002 = result.filter("vehicle_id = 'V002'").collect()[0]
        assert v002["harsh_driving_flag"] == 0


# ── Maintenance Tests ─────────────────────────────────────────────────────────

class TestMaintenanceCompliance:

    def test_compliance_rate_computed(self, maintenance_df):
        result = compute_maintenance_compliance(maintenance_df)
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        assert v001["compliance_rate"] == 1.0
        assert v001["maintenance_flag"] == 0

    def test_noncompliant_flagged(self, maintenance_df):
        result = compute_maintenance_compliance(maintenance_df)
        v002 = result.filter("vehicle_id = 'V002'").collect()[0]
        assert v002["compliance_rate"] == 0.0
        assert v002["maintenance_flag"] == 1


# ── Utilisation Tests ─────────────────────────────────────────────────────────

class TestUtilisation:

    def test_active_days_counted(self, telemetry_df):
        result = compute_utilisation(telemetry_df, observation_days=30)
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        assert v001["active_days"] == 2

    def test_low_utilisation_flagged(self, telemetry_df):
        result = compute_utilisation(telemetry_df, observation_days=30)
        # V001: 2/30 = 6.7% << 60% → flagged
        v001 = result.filter("vehicle_id = 'V001'").collect()[0]
        assert v001["utilisation_flag"] == 1


# ── Alert Tests ───────────────────────────────────────────────────────────────

class TestAlerts:

    def test_fuel_alert_generated(self, spark):
        kpi_data = spark.createDataFrame([
            Row(vehicle_id="V099", fuel_efficiency_km_l=5.5,
                fuel_efficiency_flag=1, fuel_cost_per_km=3.0,
                total_fuel_cost_zar=1500.0)
        ])
        alerts = generate_fuel_alerts(kpi_data)
        assert alerts.count() == 1
        row = alerts.collect()[0]
        assert row["alert_type"] == "FUEL_EFFICIENCY"
        assert "V099" in row["alert_message"]

    def test_no_alert_when_compliant(self, spark):
        kpi_data = spark.createDataFrame([
            Row(vehicle_id="V098", fuel_efficiency_km_l=9.0,
                fuel_efficiency_flag=0, fuel_cost_per_km=2.0,
                total_fuel_cost_zar=1000.0)
        ])
        alerts = generate_fuel_alerts(kpi_data)
        assert alerts.count() == 0
