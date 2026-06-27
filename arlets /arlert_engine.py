"""
fleet-operations-analytics/alerts/alert_engine.py
Rule-based alert generation from master KPI and raw telemetry events.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from datetime import datetime


# ── Alert Severity Levels ────────────────────────────────────────────────────

SEVERITY_CRITICAL = "CRITICAL"
SEVERITY_HIGH     = "HIGH"
SEVERITY_MEDIUM   = "MEDIUM"
SEVERITY_LOW      = "LOW"


# ── KPI-Level Alerts ─────────────────────────────────────────────────────────

def generate_fuel_alerts(kpi_df: DataFrame) -> DataFrame:
    """
    Flags vehicles with fuel efficiency below threshold.
    Adds alert metadata: type, severity, generated_at.
    """
    return (
        kpi_df
        .filter(F.col("fuel_efficiency_flag") == 1)
        .select(
            "vehicle_id",
            "fuel_efficiency_km_l",
            "fuel_cost_per_km",
            "total_fuel_cost_zar",
        )
        .withColumn("alert_type",      F.lit("FUEL_EFFICIENCY"))
        .withColumn("severity",        F.lit(SEVERITY_MEDIUM))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Vehicle "), F.col("vehicle_id"),
                F.lit(" fuel efficiency "),
                F.col("fuel_efficiency_km_l").cast("string"),
                F.lit(" km/L is below the 7.0 km/L threshold.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )


def generate_harsh_driving_alerts(kpi_df: DataFrame) -> DataFrame:
    """
    Flags vehicles exceeding harsh driving event threshold.
    Elevated to HIGH severity when events_per_100km > 3.0.
    """
    return (
        kpi_df
        .filter(F.col("harsh_driving_flag") == 1)
        .select(
            "vehicle_id",
            "harsh_events_per_100km",
            "total_harsh_braking",
            "total_harsh_accel",
        )
        .withColumn("alert_type", F.lit("HARSH_DRIVING"))
        .withColumn(
            "severity",
            F.when(F.col("harsh_events_per_100km") > 3.0, F.lit(SEVERITY_HIGH))
             .otherwise(F.lit(SEVERITY_MEDIUM))
        )
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Vehicle "), F.col("vehicle_id"),
                F.lit(" recorded "), F.col("harsh_events_per_100km").cast("string"),
                F.lit(" harsh events per 100 km — exceeds 1.5 threshold.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )


def generate_maintenance_alerts(kpi_df: DataFrame) -> DataFrame:
    """
    Two sub-rules:
    1. Compliance rate < 80%  → MEDIUM alert
    2. Service due in ≤ 7 days → HIGH alert
    """
    compliance_alerts = (
        kpi_df
        .filter(F.col("maintenance_flag") == 1)
        .select("vehicle_id", "compliance_rate", "total_services", "compliant_services")
        .withColumn("alert_type", F.lit("MAINTENANCE_COMPLIANCE"))
        .withColumn("severity",   F.lit(SEVERITY_MEDIUM))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Vehicle "), F.col("vehicle_id"),
                F.lit(" maintenance compliance at "),
                F.round(F.col("compliance_rate") * 100, 1).cast("string"),
                F.lit("% — below the 80% threshold.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )

    due_soon_alerts = (
        kpi_df
        .filter(F.col("service_due_soon_flag") == 1)
        .select("vehicle_id", "min_days_until_service")
        .withColumn("alert_type", F.lit("SERVICE_DUE_SOON"))
        .withColumn("severity",   F.lit(SEVERITY_HIGH))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Vehicle "), F.col("vehicle_id"),
                F.lit(" service due in "), F.col("min_days_until_service").cast("string"),
                F.lit(" day(s). Schedule immediately.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )

    return compliance_alerts.unionByName(due_soon_alerts, allowMissingColumns=True)


def generate_utilisation_alerts(kpi_df: DataFrame) -> DataFrame:
    """
    Flags underutilised vehicles (utilisation_rate < 60%).
    """
    return (
        kpi_df
        .filter(F.col("utilisation_flag") == 1)
        .select("vehicle_id", "utilisation_rate", "active_days")
        .withColumn("alert_type", F.lit("LOW_UTILISATION"))
        .withColumn("severity",   F.lit(SEVERITY_LOW))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Vehicle "), F.col("vehicle_id"),
                F.lit(" utilisation at "),
                F.round(F.col("utilisation_rate") * 100, 1).cast("string"),
                F.lit("% — below the 60% target.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )


def generate_high_risk_alerts(kpi_df: DataFrame) -> DataFrame:
    """
    CRITICAL alert for vehicles with risk_score >= 3 (multiple KPI failures).
    """
    return (
        kpi_df
        .filter(F.col("risk_tier") == "HIGH")
        .select("vehicle_id", "risk_score", "risk_tier")
        .withColumn("alert_type", F.lit("HIGH_RISK_VEHICLE"))
        .withColumn("severity",   F.lit(SEVERITY_CRITICAL))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("CRITICAL: Vehicle "), F.col("vehicle_id"),
                F.lit(" has breached "), F.col("risk_score").cast("string"),
                F.lit(" KPI thresholds simultaneously. Immediate review required.")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )


# ── Real-Time / Event-Level Alerts ───────────────────────────────────────────

def generate_speed_alerts(telemetry_df: DataFrame, speed_limit_kmh: float = 120.0) -> DataFrame:
    """
    Event-level alert: speed exceeding limit in a single telemetry record.
    """
    return (
        telemetry_df
        .filter(F.col("speed_kmh") > speed_limit_kmh)
        .select("vehicle_id", "driver_id", "event_timestamp", "speed_kmh", "latitude", "longitude")
        .withColumn("alert_type", F.lit("SPEED_VIOLATION"))
        .withColumn("severity",   F.lit(SEVERITY_HIGH))
        .withColumn(
            "alert_message",
            F.concat(
                F.lit("Speed violation: vehicle "), F.col("vehicle_id"),
                F.lit(" recorded "), F.col("speed_kmh").cast("string"),
                F.lit(" km/h at "), F.col("event_timestamp").cast("string")
            )
        )
        .withColumn("generated_at", F.current_timestamp())
    )


# ── Consolidated Alert Output ────────────────────────────────────────────────

def build_alert_summary(kpi_df: DataFrame, telemetry_df: DataFrame) -> DataFrame:
    """
    Unions all alert types into a single alerts DataFrame.
    Suitable for writing to Delta, Azure SQL, or Event Hubs downstream.
    """
    alert_frames = [
        generate_fuel_alerts(kpi_df),
        generate_harsh_driving_alerts(kpi_df),
        generate_maintenance_alerts(kpi_df),
        generate_utilisation_alerts(kpi_df),
        generate_high_risk_alerts(kpi_df),
        generate_speed_alerts(telemetry_df),
    ]

    # Normalise to common columns before union
    common_cols = ["vehicle_id", "alert_type", "severity", "alert_message", "generated_at"]
    alerts = alert_frames[0].select(common_cols)
    for frame in alert_frames[1:]:
        alerts = alerts.union(frame.select(common_cols))

    return (
        alerts
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_date", F.to_date("generated_at"))
        .orderBy(
            F.when(F.col("severity") == "CRITICAL", 0)
             .when(F.col("severity") == "HIGH",     1)
             .when(F.col("severity") == "MEDIUM",   2)
             .otherwise(3)
        )
    )
