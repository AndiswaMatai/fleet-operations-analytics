"""
fleet-operations-analytics/src/kpi_engine.py
Per-vehicle KPI computation: fuel efficiency, driving behaviour, utilisation, costs.
"""

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


# ── KPI Thresholds (mirrors config/thresholds.py) ───────────────────────────

FUEL_EFFICIENCY_THRESHOLD   = 7.0    # km/L  — flag below this
HARSH_EVENTS_THRESHOLD      = 1.5    # per 100 km — flag above this
MAINTENANCE_COMPLIANCE_THRESHOLD = 0.80  # 80 %
UTILISATION_THRESHOLD       = 0.60   # 60 %


# ── Fuel Efficiency ──────────────────────────────────────────────────────────

def compute_fuel_efficiency(telemetry_df, fuel_df):
    """
    Fuel efficiency (km/L) = total distance driven ÷ total litres consumed.
    Joins on vehicle_id and aggregates over the entire dataset window.
    """
    distance = (
        telemetry_df
        .groupBy("vehicle_id")
        .agg(F.sum("distance_km").alias("total_distance_km"))
    )

    consumption = (
        fuel_df
        .groupBy("vehicle_id")
        .agg(
            F.sum("litres_filled").alias("total_litres"),
            F.sum("cost_zar").alias("total_fuel_cost_zar"),
        )
    )

    efficiency = (
        distance.join(consumption, on="vehicle_id", how="inner")
        .withColumn(
            "fuel_efficiency_km_l",
            F.round(F.col("total_distance_km") / F.col("total_litres"), 2)
        )
        .withColumn(
            "fuel_cost_per_km",
            F.round(F.col("total_fuel_cost_zar") / F.col("total_distance_km"), 4)
        )
        .withColumn(
            "fuel_efficiency_flag",
            F.when(F.col("fuel_efficiency_km_l") < FUEL_EFFICIENCY_THRESHOLD, F.lit(1))
             .otherwise(F.lit(0))
        )
    )
    return efficiency


# ── Driving Behaviour ────────────────────────────────────────────────────────

def compute_driving_behaviour(telemetry_df):
    """
    Harsh driving events per 100 km.
    Combines harsh braking and harsh acceleration counts.
    """
    behaviour = (
        telemetry_df
        .groupBy("vehicle_id")
        .agg(
            F.sum("harsh_braking").alias("total_harsh_braking"),
            F.sum("harsh_accel").alias("total_harsh_accel"),
            F.sum("distance_km").alias("total_distance_km"),
            F.sum("idling_minutes").alias("total_idling_minutes"),
            F.avg("speed_kmh").alias("avg_speed_kmh"),
        )
        .withColumn(
            "total_harsh_events",
            F.col("total_harsh_braking") + F.col("total_harsh_accel")
        )
        .withColumn(
            "harsh_events_per_100km",
            F.round(
                (F.col("total_harsh_events") / F.col("total_distance_km")) * 100, 2
            )
        )
        .withColumn(
            "harsh_driving_flag",
            F.when(F.col("harsh_events_per_100km") > HARSH_EVENTS_THRESHOLD, F.lit(1))
             .otherwise(F.lit(0))
        )
    )
    return behaviour


# ── Maintenance Compliance ───────────────────────────────────────────────────

def compute_maintenance_compliance(maintenance_df):
    """
    Compliance rate = on-time services ÷ total services per vehicle.
    Also surfaces vehicles with imminent service due (≤ 7 days).
    """
    compliance = (
        maintenance_df
        .groupBy("vehicle_id")
        .agg(
            F.count("record_id").alias("total_services"),
            F.sum("compliant").alias("compliant_services"),
            F.min("days_until_service").alias("min_days_until_service"),
        )
        .withColumn(
            "compliance_rate",
            F.round(
                F.col("compliant_services") / F.col("total_services"), 4
            )
        )
        .withColumn(
            "maintenance_flag",
            F.when(
                F.col("compliance_rate") < MAINTENANCE_COMPLIANCE_THRESHOLD, F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "service_due_soon_flag",
            F.when(F.col("min_days_until_service").between(0, 7), F.lit(1))
             .otherwise(F.lit(0))
        )
    )
    return compliance


# ── Asset Utilisation ────────────────────────────────────────────────────────

def compute_utilisation(telemetry_df, observation_days: int = 30):
    """
    Utilisation rate = active days ÷ observation window.
    A vehicle is 'active' on a day if it has at least one telemetry event.
    """
    utilisation = (
        telemetry_df
        .groupBy("vehicle_id")
        .agg(F.countDistinct("event_date").alias("active_days"))
        .withColumn(
            "utilisation_rate",
            F.round(F.col("active_days") / F.lit(observation_days), 4)
        )
        .withColumn(
            "utilisation_flag",
            F.when(F.col("utilisation_rate") < UTILISATION_THRESHOLD, F.lit(1))
             .otherwise(F.lit(0))
        )
    )
    return utilisation


# ── Master KPI Table ─────────────────────────────────────────────────────────

def build_master_kpi(telemetry_df, fuel_df, maintenance_df, observation_days: int = 30):
    """
    Joins all KPI DataFrames into a single vehicle-level KPI table.
    Adds a composite risk score (0–4) summing individual flags.
    """
    fuel_eff   = compute_fuel_efficiency(telemetry_df, fuel_df)
    behaviour  = compute_driving_behaviour(telemetry_df)
    compliance = compute_maintenance_compliance(maintenance_df)
    utilisation = compute_utilisation(telemetry_df, observation_days)

    master = (
        fuel_eff
        .join(behaviour.drop("total_distance_km"),  on="vehicle_id", how="left")
        .join(compliance,                            on="vehicle_id", how="left")
        .join(utilisation,                           on="vehicle_id", how="left")
        .withColumn(
            "risk_score",
            F.col("fuel_efficiency_flag")
            + F.col("harsh_driving_flag")
            + F.col("maintenance_flag")
            + F.col("utilisation_flag")
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") >= 3, F.lit("HIGH"))
             .when(F.col("risk_score") == 2, F.lit("MEDIUM"))
             .when(F.col("risk_score") == 1, F.lit("LOW"))
             .otherwise(F.lit("NORMAL"))
        )
    )
    return master
