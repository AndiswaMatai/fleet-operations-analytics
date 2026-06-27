"""
fleet-operations-analytics/reports/fleet_report.py
Generates structured fleet performance and maintenance compliance reports
as DataFrames — suitable for export to CSV, Delta, or Power BI Direct Lake.
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window


# ── Fleet Performance Report ─────────────────────────────────────────────────

def build_performance_report(scored_kpi_df: DataFrame) -> DataFrame:
    """
    Vehicle-level performance report ordered by risk priority.
    Selects and renames columns for readability.
    """
    return (
        scored_kpi_df
        .select(
            "vehicle_id",
            F.col("fuel_efficiency_km_l").alias("fuel_eff_km_l"),
            F.col("fuel_cost_per_km").alias("fuel_cost_per_km_zar"),
            F.col("total_fuel_cost_zar").alias("total_fuel_spend_zar"),
            F.col("harsh_events_per_100km"),
            F.col("total_idling_minutes").alias("total_idling_min"),
            F.col("utilisation_rate"),
            F.col("compliance_rate"),
            F.col("risk_score"),
            F.col("risk_category"),
            F.col("priority_rank"),
        )
        .withColumn(
            "utilisation_pct",
            F.round(F.col("utilisation_rate") * 100, 1)
        )
        .withColumn(
            "compliance_pct",
            F.round(F.col("compliance_rate") * 100, 1)
        )
        .orderBy("priority_rank")
    )


def build_fleet_trend_report(daily_telemetry_df: DataFrame) -> DataFrame:
    """
    Week-over-week fleet trend: total distance, harsh events, idling.
    Partitions by ISO week number for easy BI slicing.
    """
    return (
        daily_telemetry_df
        .withColumn("iso_week", F.weekofyear("event_date"))
        .withColumn("year",     F.year("event_date"))
        .groupBy("year", "iso_week")
        .agg(
            F.sum("daily_distance_km").alias("weekly_distance_km"),
            F.sum("daily_harsh_events").alias("weekly_harsh_events"),
            F.sum("daily_idling_minutes").alias("weekly_idling_minutes"),
            F.countDistinct("vehicle_id").alias("active_vehicles"),
        )
        .withColumn(
            "fleet_harsh_per_100km",
            F.round(
                (F.col("weekly_harsh_events") / F.col("weekly_distance_km")) * 100, 2
            )
        )
        .orderBy("year", "iso_week")
    )


# ── Maintenance Compliance Report ─────────────────────────────────────────────

def build_maintenance_report(maintenance_df: DataFrame) -> DataFrame:
    """
    Vehicle-level maintenance compliance report.
    Shows latest service, next service, overdue status, and days until due.
    """
    window_latest = Window.partitionBy("vehicle_id").orderBy(F.col("service_date").desc())

    return (
        maintenance_df
        .withColumn("row_num", F.row_number().over(window_latest))
        .filter(F.col("row_num") == 1)                   # keep most recent record per vehicle
        .drop("row_num")
        .select(
            "vehicle_id",
            "service_date",
            "service_type",
            "next_service_km",
            "next_service_date",
            "days_until_service",
            "is_overdue",
            "compliant",
            F.col("cost_zar").alias("last_service_cost_zar"),
        )
        .withColumn(
            "compliance_status",
            F.when(F.col("is_overdue") == 1,      F.lit("OVERDUE"))
             .when(F.col("days_until_service") <= 7, F.lit("DUE_SOON"))
             .otherwise(F.lit("ON_TRACK"))
        )
        .orderBy("is_overdue", "days_until_service")
    )


def build_cost_report(kpi_df: DataFrame) -> DataFrame:
    """
    Fuel cost report ranked by highest total spend.
    Useful for finance / operations review.
    """
    return (
        kpi_df
        .select(
            "vehicle_id",
            "total_distance_km",
            "total_litres",
            "total_fuel_cost_zar",
            "fuel_efficiency_km_l",
            "fuel_cost_per_km",
        )
        .withColumn(
            "cost_rank",
            F.rank().over(Window.orderBy(F.col("total_fuel_cost_zar").desc()))
        )
        .orderBy("cost_rank")
    )


# ── Alert Summary Report ──────────────────────────────────────────────────────

def build_alert_report(alerts_df: DataFrame) -> DataFrame:
    """
    Aggregated alert count by type and severity.
    Suitable for a daily ops summary email or BI card.
    """
    return (
        alerts_df
        .groupBy("alert_type", "severity")
        .agg(
            F.count("alert_id").alias("alert_count"),
            F.countDistinct("vehicle_id").alias("vehicles_affected"),
        )
        .orderBy(
            F.when(F.col("severity") == "CRITICAL", 0)
             .when(F.col("severity") == "HIGH",     1)
             .when(F.col("severity") == "MEDIUM",   2)
             .otherwise(3),
            "alert_type"
        )
    )
